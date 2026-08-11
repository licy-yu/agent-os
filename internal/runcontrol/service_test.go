package runcontrol

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/domain"
	"github.com/licy-yu/agent-os/internal/domain/run"
	"github.com/licy-yu/agent-os/internal/planning"
	"github.com/stretchr/testify/require"
)

// fakeStore 只保存 Service 测试需要观察的调用参数。它刻意不复制 PostgreSQL 的 SQL 逻辑，
// 这样测试失败时能够定位到用例编排，而不是在另一份“内存数据库实现”里重复生产代码。
type fakeStore struct {
	catalog Catalog
	current *RunView

	catalogCalls int
	created      *CreateRecord
	transitions  []fakeTransition
	replan       *CreateRecord
	replanReason string
	workflowRef  WorkflowRef
}

type fakeTransition struct {
	tenantID        uuid.UUID
	runID           uuid.UUID
	expectedVersion int64
	next            run.Status
	desired         string
	reason          string
}

func (s *fakeStore) PlanningCatalog(context.Context, uuid.UUID) (Catalog, error) {
	s.catalogCalls++
	return s.catalog, nil
}

func (s *fakeStore) CreateCompiledRun(_ context.Context, record CreateRecord) (*RunView, error) {
	copyRecord := record
	s.created = &copyRecord
	value := &RunView{
		ID: record.ID, TenantID: record.TenantID, ProjectID: record.ProjectID,
		Name: record.Name, Goal: record.Goal, NormalizedGoal: record.NormalizedGoal,
		Status: run.StatusCreated, DesiredState: "RUNNING", ExecutionEngine: record.ExecutionEngine,
		BudgetTokens: record.BudgetTokens, BudgetCostMicros: record.BudgetCostMicros,
		MaxAgents: record.MaxAgents, Priority: record.Priority,
		CurrentPlanVersionID: &record.PlanVersionID, CurrentPlanVersion: record.PlanVersion,
		Deadline: record.Deadline, Version: 1, CreatedAt: record.CreatedAt, UpdatedAt: record.CreatedAt,
	}
	s.current = value
	copyValue := *value
	return &copyValue, nil
}

func (s *fakeStore) GetRun(_ context.Context, tenantID, id uuid.UUID) (*RunView, error) {
	if s.current == nil || s.current.ID != id || s.current.TenantID != tenantID {
		return nil, domain.ErrNotFound
	}
	copyValue := *s.current
	return &copyValue, nil
}

func (s *fakeStore) ListRuns(context.Context, uuid.UUID, int) ([]RunView, error) {
	if s.current == nil {
		return []RunView{}, nil
	}
	return []RunView{*s.current}, nil
}

func (s *fakeStore) TransitionRun(_ context.Context, tenantID, id uuid.UUID, expectedVersion int64,
	next run.Status, desired, reason string,
) (*RunView, error) {
	s.transitions = append(s.transitions, fakeTransition{
		tenantID: tenantID, runID: id, expectedVersion: expectedVersion,
		next: next, desired: desired, reason: reason,
	})
	if s.current == nil || s.current.Version != expectedVersion {
		return nil, domain.ErrConflict
	}
	s.current.Status = next
	s.current.DesiredState = desired
	s.current.Version++
	copyValue := *s.current
	return &copyValue, nil
}

func (s *fakeStore) ActivateReplan(_ context.Context, tenantID, id uuid.UUID, expectedVersion int64,
	record CreateRecord, reason string,
) (*RunView, error) {
	if s.current == nil || s.current.ID != id || s.current.TenantID != tenantID ||
		s.current.Version != expectedVersion {
		return nil, domain.ErrConflict
	}
	copyRecord := record
	s.replan = &copyRecord
	s.replanReason = reason
	s.current.Goal = record.Goal
	s.current.NormalizedGoal = record.NormalizedGoal
	s.current.CurrentPlanVersionID = &record.PlanVersionID
	s.current.CurrentPlanVersion = record.PlanVersion
	s.current.Version++
	copyValue := *s.current
	return &copyValue, nil
}

func (s *fakeStore) GetPlan(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) (*PlanView, error) {
	return nil, domain.ErrNotFound
}

func (s *fakeStore) AttachWorkflow(_ context.Context, tenantID, runID uuid.UUID, ref WorkflowRef) error {
	if s.current == nil || s.current.ID != runID || s.current.TenantID != tenantID {
		return domain.ErrNotFound
	}
	s.workflowRef = ref
	s.current.TemporalWorkflowID, s.current.TemporalRunID = ref.WorkflowID, ref.RunID
	return nil
}

type fakeRuntime struct {
	ready   bool
	started int
	signals []string
}

func (r *fakeRuntime) Ready() bool { return r.ready }
func (r *fakeRuntime) StartRun(_ context.Context, _ *RunView) (WorkflowRef, error) {
	r.started++
	return WorkflowRef{WorkflowID: "swarmos/run/test", RunID: "temporal-run-1"}, nil
}
func (r *fakeRuntime) SignalRun(_ context.Context, _ uuid.UUID, command, _ string, _ int32, _ map[string]any) error {
	r.signals = append(r.signals, command)
	return nil
}

func TestCreateBuildsDeterministicDefaultPlan(t *testing.T) {
	tenantID := uuid.New()
	runID := uuid.New()
	taskID := uuid.New()
	planID := uuid.New()
	now := time.Date(2026, 8, 11, 10, 30, 0, 0, time.UTC)
	store := &fakeStore{catalog: Catalog{
		// 默认计划必须稳定选择排序后的第一项，而不是依赖 map 遍历顺序。
		Capabilities: map[string]float64{"zeta": 1, "alpha": .8},
		Tools:        map[string]bool{}, Models: map[string]bool{}, Permissions: map[string]bool{},
	}}
	service := NewService(store, false)
	service.clock = func() time.Time { return now }
	service.newID = sequenceIDs(runID, taskID, planID)
	ctx := WithPrincipal(context.Background(), Principal{TenantID: tenantID, Subject: "user-1"})

	view, err := service.Create(ctx, CreateRunRequest{
		Name: " 生产验收 ", Goal: " 完成交付并给出证据 ", BudgetTokens: 120_000,
	})
	require.NoError(t, err)
	require.Equal(t, runID, view.ID)
	require.NotNil(t, store.created)
	require.Equal(t, tenantID, store.created.TenantID)
	require.Equal(t, "user-1", store.created.CreatedBy)
	require.Equal(t, "生产验收", store.created.Name)
	require.Equal(t, "完成交付并给出证据", store.created.Goal)
	require.Equal(t, "完成交付并给出证据", store.created.NormalizedGoal["objective"])
	require.Equal(t, int32(8), store.created.MaxAgents)
	require.Equal(t, int32(50), store.created.Priority)
	require.Equal(t, "LEGACY", store.created.ExecutionEngine)
	require.Equal(t, "USER", store.created.PlanSource)
	require.Equal(t, int32(1), store.created.PlanVersion)
	require.Equal(t, planID, store.created.PlanVersionID)
	require.Equal(t, compilerVersion, store.created.CompilerVersion)
	require.Equal(t, now, store.created.CreatedAt)

	require.Len(t, store.created.PlanCandidate.Tasks, 1)
	task := store.created.PlanCandidate.Tasks[0]
	require.Equal(t, taskID, task.ID)
	require.Equal(t, runID, task.RunID)
	require.Equal(t, map[string]float64{"alpha": .01}, task.Requirements.Capabilities)
	require.Equal(t, "R1_SANDBOX_WRITE", task.SideEffectPolicy.MaxRisk)
	require.True(t, task.SideEffectPolicy.RequireApproval)
	require.Equal(t, int64(32_000), task.ContextPolicy.MaxTokens)
	require.Len(t, store.created.PlanCandidate.Deliverables, 1)
	require.NotEmpty(t, store.created.ExecutablePlan.GraphHash)
}

func TestCreateRejectsMissingAgentCatalog(t *testing.T) {
	store := &fakeStore{catalog: Catalog{Capabilities: map[string]float64{}}}
	service := NewService(store, false)

	_, err := service.Create(context.Background(), CreateRunRequest{Name: "run", Goal: "goal"})
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidRequest))
	require.ErrorContains(t, err, "没有启用的 Agent 能力")
	require.Nil(t, store.created, "Catalog 不可执行时不得留下半成品 Run")
}

func TestCreateTemporalStartsIdempotentWorkflowAndPersistsReference(t *testing.T) {
	store := &fakeStore{catalog: validCatalog()}
	runtime := &fakeRuntime{ready: true}
	service := NewService(store, false).WithDurableRuntime(runtime)
	view, err := service.Create(context.Background(), CreateRunRequest{
		Name: "durable", Goal: "长时间运行", ExecutionEngine: "TEMPORAL",
	})
	require.NoError(t, err)
	require.Equal(t, 1, runtime.started)
	require.Equal(t, "swarmos/run/test", store.workflowRef.WorkflowID)
	require.Equal(t, "temporal-run-1", view.TemporalRunID)
}

func TestPauseResumeCancelUseRunStateMachineAndCAS(t *testing.T) {
	tenantID := uuid.New()
	runID := uuid.New()
	store := &fakeStore{current: &RunView{
		ID: runID, TenantID: tenantID, Goal: "goal", Status: run.StatusRunning, Version: 4,
	}}
	service := NewService(store, false)
	ctx := WithPrincipal(context.Background(), Principal{TenantID: tenantID, Subject: "operator"})

	paused, err := service.Pause(ctx, runID, " 变更窗口 ")
	require.NoError(t, err)
	require.Equal(t, run.StatusPaused, paused.Status)
	require.Equal(t, fakeTransition{
		tenantID: tenantID, runID: runID, expectedVersion: 4,
		next: run.StatusPaused, desired: "PAUSED", reason: "变更窗口",
	}, store.transitions[0])

	resumed, err := service.Resume(ctx, runID, "继续")
	require.NoError(t, err)
	require.Equal(t, run.StatusRunning, resumed.Status)
	require.Equal(t, int64(5), store.transitions[1].expectedVersion)
	require.Equal(t, "RUNNING", store.transitions[1].desired)

	canceled, err := service.Cancel(ctx, runID, "用户取消")
	require.NoError(t, err)
	require.Equal(t, run.StatusCanceled, canceled.Status)
	require.Equal(t, int64(6), store.transitions[2].expectedVersion)
	require.Equal(t, "CANCELED", store.transitions[2].desired)

	_, err = service.Resume(ctx, runID, "迟到请求")
	require.ErrorIs(t, err, domain.ErrInvalidTransition)
	require.Len(t, store.transitions, 3, "终态 Run 必须在调用 Store 前被领域状态机拒绝")
}

func TestReplanRequiresSafePoint(t *testing.T) {
	tenantID := uuid.New()
	runID := uuid.New()
	store := &fakeStore{
		catalog: validCatalog(),
		current: &RunView{
			ID: runID, TenantID: tenantID, ProjectID: DefaultProjectID,
			Name: "run", Goal: "goal", Status: run.StatusRunning,
			BudgetTokens: 100_000, MaxAgents: 8, Priority: 50,
			ExecutionEngine: "LEGACY", CurrentPlanVersion: 1, Version: 7,
		},
	}
	service := NewService(store, false)
	ctx := WithPrincipal(context.Background(), Principal{TenantID: tenantID, Subject: "planner"})

	_, err := service.Replan(ctx, runID, ReplanRequest{Reason: "修正错误假设"})
	require.ErrorIs(t, err, domain.ErrConflict)
	require.ErrorContains(t, err, "必须先暂停")
	require.Equal(t, 0, store.catalogCalls, "未到安全点时不应继续读取 Catalog 或编译计划")
	require.Nil(t, store.replan)

	store.current.Status = run.StatusPaused
	result, err := service.Replan(ctx, runID, ReplanRequest{Reason: " 修正错误假设 "})
	require.NoError(t, err)
	require.NotNil(t, store.replan)
	require.Equal(t, result.CurrentPlanVersion, int32(2))
	require.Equal(t, int32(2), store.replan.PlanVersion)
	require.Equal(t, "REPLAN", store.replan.PlanSource)
	require.Equal(t, runID, store.replan.ID)
	require.Equal(t, "goal", store.replan.Goal, "空 goal 应继承当前 Run Goal")
	require.Equal(t, "修正错误假设", store.replanReason)
}

func TestReplanDoesNotActivateCompilerErrors(t *testing.T) {
	tenantID := uuid.New()
	runID := uuid.New()
	store := &fakeStore{
		catalog: validCatalog(),
		current: &RunView{
			ID: runID, TenantID: tenantID, ProjectID: DefaultProjectID,
			Name: "run", Goal: "goal", Status: run.StatusPaused,
			BudgetTokens: 100_000, MaxAgents: 8, Priority: 50,
			ExecutionEngine: "LEGACY", CurrentPlanVersion: 3, Version: 9,
		},
	}
	service := NewService(store, false)
	invalidPlan := &planning.PlanCandidate{} // 缺少 Task 和 Deliverable，必然产生确定性编译错误。
	ctx := WithPrincipal(context.Background(), Principal{TenantID: tenantID, Subject: "planner"})

	_, err := service.Replan(ctx, runID, ReplanRequest{
		Reason: "替换计划", Plan: invalidPlan,
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidRequest))
	require.ErrorContains(t, err, "至少需要一个 Task")
	require.Nil(t, store.replan, "编译失败不得激活新 PlanVersion")
	require.Equal(t, int32(3), store.current.CurrentPlanVersion)
}

func validCatalog() Catalog {
	return Catalog{
		Capabilities: map[string]float64{"general": 1},
		Tools:        map[string]bool{}, Models: map[string]bool{}, Permissions: map[string]bool{},
	}
}

func sequenceIDs(values ...uuid.UUID) func() uuid.UUID {
	index := 0
	return func() uuid.UUID {
		if index >= len(values) {
			panic("测试没有提供足够的确定性 UUID")
		}
		value := values[index]
		index++
		return value
	}
}
