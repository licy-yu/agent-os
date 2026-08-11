package runcontrol

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/domain"
	"github.com/licy-yu/agent-os/internal/domain/run"
	"github.com/licy-yu/agent-os/internal/planning"
)

const compilerVersion = "swarmos-plan-compiler/v1.5.0"

var (
	// ErrInvalidRequest 是可安全返回给调用方的合同错误。
	ErrInvalidRequest = errors.New("Run 请求不符合合同")
	// ErrTemporalDisabled 防止 API 把 Run 标为 TEMPORAL，却没有真正启动 Durable Workflow。
	ErrTemporalDisabled = errors.New("Temporal 执行引擎尚未启用")
)

// Principal 是由认证边界写入 Context 的可信调用者。TenantID 永远不从业务请求体读取。
type Principal struct {
	TenantID uuid.UUID
	Subject  string
	Scopes   map[string]bool
}

type principalKey struct{}

// WithPrincipal 只应由认证中间件调用；业务测试可以用它注入确定性身份。
func WithPrincipal(ctx context.Context, value Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, value)
}

// PrincipalFromContext 在本地单机模式回退到默认租户。生产配置启用认证后，中间件会在
// 进入 handler 前拒绝缺失身份的请求，因此这个回退不会形成跨租户入口。
func PrincipalFromContext(ctx context.Context) Principal {
	if value, ok := ctx.Value(principalKey{}).(Principal); ok && value.TenantID != uuid.Nil {
		return value
	}
	return Principal{TenantID: DefaultTenantID, Subject: "local-admin", Scopes: map[string]bool{"*": true}}
}

// Service 编排 Goal 归一化、确定性 Plan 编译和多表原子持久化。
type Service struct {
	store         Store
	compiler      *planning.PlanCompiler
	temporalReady bool
	clock         func() time.Time
	newID         func() uuid.UUID
}

// NewService 创建 Run 用例。temporalReady 只有在 Temporal Client 和 Workflow Worker 都
// 健康时才能为 true，避免出现“数据库显示运行中，但根本没有 Workflow”的假运行。
func NewService(store Store, temporalReady bool) *Service {
	return &Service{
		store: store, compiler: planning.NewPlanCompiler(planning.DefaultCompilerOptions()),
		temporalReady: temporalReady, clock: func() time.Time { return time.Now().UTC() }, newID: uuid.New,
	}
}

// Create 编译并原子创建 Run、PlanVersion、Task、Dependency 与 AcceptanceGate。
func (s *Service) Create(ctx context.Context, request CreateRunRequest) (*RunView, error) {
	if s == nil || s.store == nil {
		return nil, errors.New("Run Service 未初始化")
	}
	request.Name = strings.TrimSpace(request.Name)
	request.Goal = strings.TrimSpace(request.Goal)
	if request.Name == "" || len([]rune(request.Name)) > 128 {
		return nil, fmt.Errorf("%w: name 必须为 1~128 个字符", ErrInvalidRequest)
	}
	if request.Goal == "" {
		return nil, fmt.Errorf("%w: goal 不能为空", ErrInvalidRequest)
	}
	if request.BudgetTokens < 0 || request.BudgetCostMicros < 0 {
		return nil, fmt.Errorf("%w: 预算不能为负数", ErrInvalidRequest)
	}
	if request.MaxAgents == 0 {
		request.MaxAgents = 8
	}
	if request.MaxAgents < 1 || request.MaxAgents > 1000 {
		return nil, fmt.Errorf("%w: maxAgents 必须在 1~1000 之间", ErrInvalidRequest)
	}
	if request.Priority == 0 {
		request.Priority = 50
	}
	if request.Priority < 0 || request.Priority > 1000 {
		return nil, fmt.Errorf("%w: priority 必须在 0~1000 之间", ErrInvalidRequest)
	}
	engine, err := s.normalizeEngine(request.ExecutionEngine)
	if err != nil {
		return nil, err
	}

	principal := PrincipalFromContext(ctx)
	catalog, err := s.store.PlanningCatalog(ctx, principal.TenantID)
	if err != nil {
		return nil, fmt.Errorf("读取 Plan Catalog: %w", err)
	}
	runID := s.newID()
	candidate, err := s.prepareCandidate(runID, request.Goal, request.Priority,
		request.BudgetTokens, request.BudgetCostMicros, request.Plan, catalog)
	if err != nil {
		return nil, err
	}
	executable, compileErrors := s.compiler.Compile(planning.CompileRequest{
		RunID: runID,
		RunBudget: planning.ResourceBudget{
			MaxTokens: request.BudgetTokens, MaxCostMicros: request.BudgetCostMicros,
			MaxDurationSeconds: deadlineBudget(s.clock(), request.Deadline),
		},
		AvailableCapabilities: catalog.Capabilities,
		AvailableTools:        catalog.Tools, AvailableModels: catalog.Models,
		AllowedPermissions: catalog.Permissions, Candidate: candidate,
	})
	if len(compileErrors) > 0 {
		return nil, fmt.Errorf("%w: %s", ErrInvalidRequest, compileErrors.Error())
	}
	now := s.clock()
	record := CreateRecord{
		ID: runID, TenantID: principal.TenantID, ProjectID: DefaultProjectID,
		Name: request.Name, Goal: request.Goal, NormalizedGoal: normalizeGoal(request.Goal),
		BudgetTokens: request.BudgetTokens, BudgetCostMicros: request.BudgetCostMicros,
		MaxAgents: request.MaxAgents, Priority: request.Priority, ExecutionEngine: engine,
		Deadline: request.Deadline, PlanVersionID: s.newID(), PlanVersion: 1,
		PlanSource: "USER", PlanCandidate: candidate, ExecutablePlan: *executable,
		CompilerVersion: compilerVersion, CreatedBy: principal.Subject, CreatedAt: now,
	}
	return s.store.CreateCompiledRun(ctx, record)
}

func (s *Service) Get(ctx context.Context, id uuid.UUID) (*RunView, error) {
	return s.store.GetRun(ctx, PrincipalFromContext(ctx).TenantID, id)
}

func (s *Service) List(ctx context.Context, limit int) ([]RunView, error) {
	if limit <= 0 || limit > 200 {
		limit = 100
	}
	return s.store.ListRuns(ctx, PrincipalFromContext(ctx).TenantID, limit)
}

func (s *Service) GetPlan(ctx context.Context, runID, planID uuid.UUID) (*PlanView, error) {
	return s.store.GetPlan(ctx, PrincipalFromContext(ctx).TenantID, runID, planID)
}

// Pause 只在安全边界停止新调度；已在外部系统执行中的 Effect 仍须先确认结果并写 Checkpoint。
func (s *Service) Pause(ctx context.Context, id uuid.UUID, reason string) (*RunView, error) {
	return s.transition(ctx, id, run.StatusPaused, "PAUSED", reason)
}

func (s *Service) Resume(ctx context.Context, id uuid.UUID, reason string) (*RunView, error) {
	return s.transition(ctx, id, run.StatusRunning, "RUNNING", reason)
}

func (s *Service) Cancel(ctx context.Context, id uuid.UUID, reason string) (*RunView, error) {
	return s.transition(ctx, id, run.StatusCanceled, "CANCELED", reason)
}

func (s *Service) transition(ctx context.Context, id uuid.UUID, next run.Status, desired, reason string) (*RunView, error) {
	principal := PrincipalFromContext(ctx)
	current, err := s.store.GetRun(ctx, principal.TenantID, id)
	if err != nil {
		return nil, err
	}
	aggregate := run.Run{ID: current.ID, TenantID: current.TenantID, Goal: current.Goal,
		Status: current.Status, Version: current.Version}
	if err := aggregate.Transition(run.ActorController, next); err != nil {
		return nil, err
	}
	return s.store.TransitionRun(ctx, principal.TenantID, id, current.Version, next, desired,
		strings.TrimSpace(reason))
}

// Replan 生成新 PlanVersion，并只取消旧计划中尚未终结的 Task。成功 Task、Artifact、Effect、
// GateResult 和旧 PlanVersion 全部保留，供新计划复用和审计。
func (s *Service) Replan(ctx context.Context, id uuid.UUID, request ReplanRequest) (*RunView, error) {
	request.Reason = strings.TrimSpace(request.Reason)
	if request.Reason == "" {
		return nil, fmt.Errorf("%w: replan reason 不能为空", ErrInvalidRequest)
	}
	principal := PrincipalFromContext(ctx)
	current, err := s.store.GetRun(ctx, principal.TenantID, id)
	if err != nil {
		return nil, err
	}
	if current.Status == run.StatusCanceled || current.Status == run.StatusCompleted ||
		current.Status == run.StatusFailed || current.Status == run.StatusExpired {
		return nil, fmt.Errorf("%w: 终态 Run 不能 Replan", domain.ErrConflict)
	}
	// Legacy 执行引擎没有 Temporal Signal 帮我们等待安全点。先暂停 Run，才能保证没有新 Task
	// 被调度；正在执行的 Effect 也有机会在 Pause 前完成对账和 Checkpoint。
	if current.Status != run.StatusPaused && current.Status != run.StatusWaitingUser &&
		current.Status != run.StatusVerifying && current.Status != run.StatusDegraded {
		return nil, fmt.Errorf("%w: Replan 前必须先暂停 Run 或等待安全交互点", domain.ErrConflict)
	}
	goal := strings.TrimSpace(request.Goal)
	if goal == "" {
		goal = current.Goal
	}
	catalog, err := s.store.PlanningCatalog(ctx, principal.TenantID)
	if err != nil {
		return nil, err
	}
	candidate, err := s.prepareCandidate(id, goal, current.Priority, current.BudgetTokens,
		current.BudgetCostMicros, request.Plan, catalog)
	if err != nil {
		return nil, err
	}
	executable, compileErrors := s.compiler.Compile(planning.CompileRequest{
		RunID: id,
		RunBudget: planning.ResourceBudget{MaxTokens: current.BudgetTokens,
			MaxCostMicros: current.BudgetCostMicros, MaxDurationSeconds: deadlineBudget(s.clock(), current.Deadline)},
		AvailableCapabilities: catalog.Capabilities, AvailableTools: catalog.Tools,
		AvailableModels: catalog.Models, AllowedPermissions: catalog.Permissions, Candidate: candidate,
	})
	if len(compileErrors) > 0 {
		return nil, fmt.Errorf("%w: %s", ErrInvalidRequest, compileErrors.Error())
	}
	record := CreateRecord{
		ID: id, TenantID: principal.TenantID, ProjectID: current.ProjectID,
		Name: current.Name, Goal: goal, NormalizedGoal: normalizeGoal(goal),
		BudgetTokens: current.BudgetTokens, BudgetCostMicros: current.BudgetCostMicros,
		MaxAgents: current.MaxAgents, Priority: current.Priority,
		ExecutionEngine: current.ExecutionEngine, Deadline: current.Deadline,
		PlanVersionID: s.newID(), PlanVersion: current.CurrentPlanVersion + 1,
		PlanSource: "REPLAN", PlanCandidate: candidate, ExecutablePlan: *executable,
		CompilerVersion: compilerVersion, CreatedBy: principal.Subject, CreatedAt: s.clock(),
	}
	return s.store.ActivateReplan(ctx, principal.TenantID, id, current.Version, record, request.Reason)
}

func (s *Service) normalizeEngine(raw string) (string, error) {
	engine := strings.ToUpper(strings.TrimSpace(raw))
	if engine == "" {
		if s.temporalReady {
			return "TEMPORAL", nil
		}
		return "LEGACY", nil
	}
	if engine != "LEGACY" && engine != "TEMPORAL" {
		return "", fmt.Errorf("%w: executionEngine 只支持 LEGACY/TEMPORAL", ErrInvalidRequest)
	}
	if engine == "TEMPORAL" && !s.temporalReady {
		return "", ErrTemporalDisabled
	}
	return engine, nil
}

func (s *Service) prepareCandidate(runID uuid.UUID, goal string, priority int32, tokenBudget, costBudget int64,
	provided *planning.PlanCandidate, catalog Catalog,
) (planning.PlanCandidate, error) {
	if provided != nil {
		candidate := *provided
		for index := range candidate.Tasks {
			candidate.Tasks[index].RunID = runID
		}
		return candidate, nil
	}
	capability := firstCapability(catalog.Capabilities)
	if capability == "" {
		return planning.PlanCandidate{}, fmt.Errorf("%w: 当前没有启用的 Agent 能力，请先注册 AgentTemplate/Agent", ErrInvalidRequest)
	}
	maxTokens := tokenBudget
	if maxTokens <= 0 || maxTokens > 100_000 {
		maxTokens = 100_000
	}
	taskID := s.newID()
	task := planning.TaskContract{
		ID: taskID, RunID: runID, Name: "完成用户目标", Goal: goal, Inputs: []planning.InputSpec{},
		Output:       planning.OutputSpec{Type: "object", Artifacts: []planning.ArtifactSpec{{Name: "final-result", Type: "result"}}},
		Requirements: planning.Requirements{Capabilities: map[string]float64{capability: .01}},
		ContextPolicy: planning.ContextPolicy{MaxTokens: minInt64(maxTokens, 32_000), IncludeProjectMemory: true,
			IncludePreviousFailure: true},
		SideEffectPolicy: planning.SideEffectPolicy{MaxRisk: "R1_SANDBOX_WRITE", RequireApproval: true},
		Acceptance: []planning.AcceptanceCriterion{{Name: "output_nonempty", Type: "JSON_SCHEMA",
			Config: map[string]any{"required": []string{"output"}}}},
		RetryPolicy: planning.DefaultRetryPolicy(), RuntimeGuard: planning.DefaultRuntimeGuard(),
		Budget:   planning.ResourceBudget{MaxTokens: maxTokens, MaxCostMicros: costBudget, MaxDurationSeconds: 1800},
		Priority: priority, ExpectedDurationSeconds: 300,
	}
	return planning.PlanCandidate{
		Tasks: []planning.TaskContract{task}, Dependencies: []planning.Dependency{},
		Deliverables:    []planning.Deliverable{{Name: "final-result", Type: "result", TaskID: taskID}},
		EstimatedBudget: task.Budget,
	}, nil
}

func normalizeGoal(goal string) map[string]any {
	return map[string]any{
		"objective":    strings.TrimSpace(goal),
		"language":     "zh-CN",
		"constraints":  []any{},
		"deliverables": []any{},
	}
}

func firstCapability(values map[string]float64) string {
	keys := make([]string, 0, len(values))
	for key, level := range values {
		if strings.TrimSpace(key) != "" && level > 0 {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	if len(keys) == 0 {
		return ""
	}
	return keys[0]
}

func deadlineBudget(now time.Time, deadline *time.Time) int64 {
	if deadline == nil {
		return 0
	}
	seconds := int64(deadline.Sub(now).Seconds())
	if seconds < 1 {
		return 1
	}
	return seconds
}

func minInt64(left, right int64) int64 {
	if left < right {
		return left
	}
	return right
}
