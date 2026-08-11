package orchestrator

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/domain"
	"github.com/licy-yu/agent-os/internal/domain/agent"
	"github.com/licy-yu/agent-os/internal/domain/task"
	"github.com/licy-yu/agent-os/internal/lease"
	"github.com/stretchr/testify/require"
)

// schedulerChurnStore 用独立快照模拟 PostgreSQL CAS。Scheduler 会修改它读到的 Task，
// 所以测试不能把仓储中的同一指针直接交出去，否则两个并发 Scheduler 会产生 Go 数据竞争，
// 也无法真实复现“各自持有同一 version 的数据库快照”。
type schedulerChurnStore struct {
	mu sync.Mutex

	value          task.Task
	candidates     []Candidate
	transitionCall int
	bindAttempts   int
	bindCall       int
	bindErrors     []error
	outboxWrites   int
	decisions      []SchedulerDecision

	listed  chan struct{}
	proceed chan struct{}
}

func (s *schedulerChurnStore) ListReconcileTasks(context.Context, time.Time, int) ([]*task.Task, error) {
	return nil, nil
}
func (s *schedulerChurnStore) DependenciesSatisfied(context.Context, uuid.UUID) (bool, error) {
	return true, nil
}
func (s *schedulerChurnStore) ActivateRegisteredAgents(context.Context, int) (int, error) {
	return 0, nil
}
func (s *schedulerChurnStore) ListQueuedTasks(context.Context, int) ([]QueuedTask, error) {
	s.mu.Lock()
	value := s.value
	s.mu.Unlock()
	if value.Status != task.StatusReady {
		return nil, nil
	}
	if s.listed != nil {
		s.listed <- struct{}{}
		<-s.proceed
	}
	return []QueuedTask{{Task: &value}}, nil
}
func (s *schedulerChurnStore) ListSchedulerCandidates(context.Context, uuid.UUID, int64) ([]Candidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Candidate(nil), s.candidates...), nil
}
func (s *schedulerChurnStore) TransitionTask(_ context.Context, _ uuid.UUID, version int64,
	from, to task.Status, _ string,
) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.value.Version != version || s.value.Status != from {
		return 0, fmt.Errorf("%w: stale task snapshot", domain.ErrConflict)
	}
	s.value.Status = to
	s.value.Version++
	s.transitionCall++
	s.outboxWrites++
	return s.value.Version, nil
}
func (s *schedulerChurnStore) BindTask(_ context.Context, _ uuid.UUID, taskVersion int64,
	_ uuid.UUID, _ int64,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.value.Status != task.StatusScheduling || s.value.Version != taskVersion {
		return fmt.Errorf("%w: bind lost task CAS", domain.ErrConflict)
	}
	s.bindAttempts++
	if len(s.bindErrors) > 0 {
		err := s.bindErrors[0]
		s.bindErrors = s.bindErrors[1:]
		return err
	}
	s.value.Status = task.StatusAssigned
	s.value.Version++
	s.bindCall++
	// 真实 BindTask 原子写入 task.assigned 与 agent.reserved 两条 Outbox。
	s.outboxWrites += 2
	return nil
}
func (s *schedulerChurnStore) RecordSchedulerDecision(_ context.Context, decision SchedulerDecision) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.decisions = append(s.decisions, decision)
	return nil
}

type toggleLeaseManager struct {
	mu      sync.Mutex
	enabled bool
}

func (m *toggleLeaseManager) Reserve(_ context.Context, agentID, taskID uuid.UUID,
	ttl time.Duration,
) (*lease.Lease, bool, error) {
	m.mu.Lock()
	enabled := m.enabled
	m.mu.Unlock()
	if !enabled {
		return nil, false, nil
	}
	return &lease.Lease{AgentID: agentID, TaskID: taskID, Token: uuid.NewString(), TTL: ttl}, true, nil
}
func (*toggleLeaseManager) Release(context.Context, *lease.Lease) error { return nil }

func TestSchedulerIdlePollingDoesNotChurnAndNewAgentSchedulesNextTick(t *testing.T) {
	fixedNow := time.Date(2026, 8, 11, 9, 0, 0, 0, time.UTC)
	value := task.Task{
		ID: uuid.New(), SwarmID: uuid.New(), Status: task.StatusReady, Version: 7,
		Priority: 50, CreatedAt: fixedNow.Add(-10 * time.Minute),
		ExecutionPolicy: task.DefaultExecutionPolicy(),
	}
	store := &schedulerChurnStore{value: value}
	leases := &toggleLeaseManager{}
	scheduler := NewScheduler(store, leases, 30*time.Second, log.NewStdLogger(io.Discard))

	// 注入可控时钟模拟 30 个一秒调度周期，不用 sleep，测试既确定又能准确覆盖
	// 生产上的高频轮询。前 15 轮没有 Agent，后 15 轮有候选但 Lease 被占用。
	now := fixedNow
	scheduler.clock = func() time.Time { return now }
	for round := 0; round < 30; round++ {
		if round == 15 {
			store.mu.Lock()
			store.candidates = []Candidate{schedulableCandidate()}
			store.mu.Unlock()
		}
		bound, err := scheduler.ScheduleOnce(context.Background())
		require.NoError(t, err)
		require.False(t, bound)
		now = now.Add(time.Second)
	}

	store.mu.Lock()
	require.Equal(t, task.StatusReady, store.value.Status)
	require.Equal(t, int64(7), store.value.Version)
	require.Zero(t, store.transitionCall)
	require.Zero(t, store.outboxWrites)
	require.Len(t, store.decisions, 30, "内存仓储接收每轮 Explain；PostgreSQL 仓储负责按版本幂等合并")
	for _, decision := range store.decisions {
		require.Equal(t, int64(7), decision.TaskVersion)
	}
	store.mu.Unlock()

	// 模拟新 Agent 的租约现在可用；下一次调度周期应立即推进，而不是被持久化退避
	// 时间挡住。整个成功路径只发生 READY→SCHEDULING 和原子 Bind 所需的有限写入。
	leases.mu.Lock()
	leases.enabled = true
	leases.mu.Unlock()
	bound, err := scheduler.ScheduleOnce(context.Background())
	require.NoError(t, err)
	require.True(t, bound)
	store.mu.Lock()
	require.Equal(t, task.StatusAssigned, store.value.Status)
	require.Equal(t, int64(9), store.value.Version)
	require.Equal(t, 1, store.transitionCall)
	require.Equal(t, 1, store.bindCall)
	require.Equal(t, 3, store.outboxWrites)
	store.mu.Unlock()
}

// contendedLeaseManager 让两个 Scheduler 分别取得不同 Agent 的租约，再同时进入 Task
// CAS。这样测试确定性覆盖最危险的并发窗口，而不依赖 goroutine 的偶然调度顺序。
type contendedLeaseManager struct {
	mu        sync.Mutex
	held      map[uuid.UUID]bool
	acquired  int
	allLeased chan struct{}
	released  int
}

func newContendedLeaseManager() *contendedLeaseManager {
	return &contendedLeaseManager{held: make(map[uuid.UUID]bool), allLeased: make(chan struct{})}
}
func (m *contendedLeaseManager) Reserve(_ context.Context, agentID, taskID uuid.UUID,
	ttl time.Duration,
) (*lease.Lease, bool, error) {
	m.mu.Lock()
	if m.held[agentID] {
		m.mu.Unlock()
		return nil, false, nil
	}
	m.held[agentID] = true
	m.acquired++
	if m.acquired == 2 {
		close(m.allLeased)
	}
	barrier := m.allLeased
	m.mu.Unlock()
	<-barrier
	return &lease.Lease{AgentID: agentID, TaskID: taskID, Token: uuid.NewString(), TTL: ttl}, true, nil
}
func (m *contendedLeaseManager) Release(_ context.Context, value *lease.Lease) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.held, value.AgentID)
	m.released++
	return nil
}

func TestConcurrentSchedulersLoseCASWithoutCompensatingStateChurn(t *testing.T) {
	fixedNow := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	store := &schedulerChurnStore{
		value: task.Task{
			ID: uuid.New(), SwarmID: uuid.New(), Status: task.StatusReady, Version: 11,
			CreatedAt: fixedNow, ExecutionPolicy: task.DefaultExecutionPolicy(),
		},
		candidates: []Candidate{schedulableCandidate(), schedulableCandidate()},
		listed:     make(chan struct{}, 2),
		proceed:    make(chan struct{}),
	}
	leases := newContendedLeaseManager()
	schedulers := []*Scheduler{
		NewScheduler(store, leases, 30*time.Second, log.NewStdLogger(io.Discard)),
		NewScheduler(store, leases, 30*time.Second, log.NewStdLogger(io.Discard)),
	}
	for _, scheduler := range schedulers {
		scheduler.clock = func() time.Time { return fixedNow }
	}

	type result struct {
		bound bool
		err   error
	}
	results := make(chan result, 2)
	for _, scheduler := range schedulers {
		go func(value *Scheduler) {
			bound, err := value.ScheduleOnce(context.Background())
			results <- result{bound: bound, err: err}
		}(scheduler)
	}
	// 两个调度器都读完相同 READY/version 快照后才放行。
	<-store.listed
	<-store.listed
	close(store.proceed)

	first, second := <-results, <-results
	require.NoError(t, first.err)
	require.NoError(t, second.err)
	require.NotEqual(t, first.bound, second.bound, "数据库 Task CAS 必须只允许一个 Scheduler 获胜")

	store.mu.Lock()
	require.Equal(t, task.StatusAssigned, store.value.Status)
	require.Equal(t, int64(13), store.value.Version)
	require.Equal(t, 1, store.transitionCall, "CAS 失败者不得写 SCHEDULING→READY 补偿状态")
	require.Equal(t, 1, store.bindCall)
	store.mu.Unlock()
	leasingState := leases
	leasingState.mu.Lock()
	require.Equal(t, 2, leasingState.released, "胜者和 CAS 失败者都必须释放各自短租约")
	require.Empty(t, leasingState.held)
	leasingState.mu.Unlock()
}

// reserveErrorAfterBindConflict 复现生产审计发现的精确故障序列：第一个 Agent 已取得
// Lease，但 Bind 的 Agent CAS 失败；Scheduler 继续尝试第二个候选时 Redis 返回错误。
// 此时 Task 已经是 SCHEDULING，错误出口必须先补偿回 READY。
type reserveErrorAfterBindConflict struct {
	mu       sync.Mutex
	calls    int
	released int
	err      error
}

func (m *reserveErrorAfterBindConflict) Reserve(_ context.Context, agentID, taskID uuid.UUID,
	ttl time.Duration,
) (*lease.Lease, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.calls == 2 {
		return nil, false, m.err
	}
	return &lease.Lease{AgentID: agentID, TaskID: taskID, Token: "first-candidate", TTL: ttl}, true, nil
}
func (m *reserveErrorAfterBindConflict) Release(context.Context, *lease.Lease) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.released++
	return nil
}

func TestReserveErrorAfterBindConflictRestoresReady(t *testing.T) {
	reserveErr := fmt.Errorf("redis reserve unavailable")
	fixedNow := time.Date(2026, 8, 11, 11, 0, 0, 0, time.UTC)
	store := &schedulerChurnStore{
		value: task.Task{
			ID: uuid.New(), SwarmID: uuid.New(), Status: task.StatusReady, Version: 21,
			CreatedAt: fixedNow, ExecutionPolicy: task.DefaultExecutionPolicy(),
		},
		candidates: []Candidate{schedulableCandidate(), schedulableCandidate()},
		bindErrors: []error{fmt.Errorf("%w: first agent version changed", domain.ErrConflict)},
	}
	leases := &reserveErrorAfterBindConflict{err: reserveErr}
	scheduler := NewScheduler(store, leases, 30*time.Second, log.NewStdLogger(io.Discard))
	scheduler.clock = func() time.Time { return fixedNow }

	bound, err := scheduler.ScheduleOnce(context.Background())
	require.False(t, bound)
	require.ErrorIs(t, err, reserveErr)

	store.mu.Lock()
	require.Equal(t, task.StatusReady, store.value.Status)
	require.Equal(t, int64(23), store.value.Version)
	require.Equal(t, 2, store.transitionCall,
		"故障链只能产生 READY→SCHEDULING 与 SCHEDULING→READY 两次有限转换")
	require.Equal(t, 2, store.outboxWrites)
	require.Equal(t, 1, store.bindAttempts)
	require.Zero(t, store.bindCall)
	store.mu.Unlock()
	leases.mu.Lock()
	require.Equal(t, 2, leases.calls)
	require.Equal(t, 1, leases.released)
	leases.mu.Unlock()
}

// staleSchedulingStore 在 SQL 仓储之外再次模拟“先按 updated_at 过滤、再返回快照”。
// barrier 可让两个 Controller 必定读到同一个陈旧版本，从而确定性验证多副本 CAS。
type staleSchedulingStore struct {
	*schedulerChurnStore
	listed  chan struct{}
	proceed chan struct{}
}

func (s *staleSchedulingStore) ListReconcileTasks(_ context.Context, staleBefore time.Time,
	_ int,
) ([]*task.Task, error) {
	s.mu.Lock()
	value := s.value
	s.mu.Unlock()
	if value.Status != task.StatusScheduling || value.UpdatedAt.After(staleBefore) {
		return nil, nil
	}
	if s.listed != nil {
		s.listed <- struct{}{}
		<-s.proceed
	}
	return []*task.Task{&value}, nil
}

func TestStaleSchedulingRecoveryUsesInjectedClockBoundary(t *testing.T) {
	fixedNow := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	store := &staleSchedulingStore{schedulerChurnStore: &schedulerChurnStore{
		value: task.Task{
			ID: uuid.New(), SwarmID: uuid.New(), Status: task.StatusScheduling, Version: 31,
			UpdatedAt: fixedNow.Add(-59 * time.Second), ExecutionPolicy: task.DefaultExecutionPolicy(),
		},
	}}
	controller := NewTaskController(store, time.Minute, log.NewStdLogger(io.Discard))
	now := fixedNow
	controller.clock = func() time.Time { return now }

	changed, err := controller.ReconcileOnce(context.Background())
	require.NoError(t, err)
	require.Zero(t, changed, "未超过恢复阈值的 SCHEDULING 不能抢占正常 Bind")

	// 不 sleep，只推进注入时钟越过边界；下一轮应通过 Controller CAS 写回 READY。
	now = now.Add(2 * time.Second)
	changed, err = controller.ReconcileOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, changed)
	store.mu.Lock()
	require.Equal(t, task.StatusReady, store.value.Status)
	require.Equal(t, int64(32), store.value.Version)
	require.Equal(t, 1, store.transitionCall)
	require.Equal(t, 1, store.outboxWrites, "恢复必须沿用 TransitionTask 产生 task.ready Outbox")
	store.mu.Unlock()
}

func TestConcurrentControllersRecoverStaleSchedulingOnce(t *testing.T) {
	fixedNow := time.Date(2026, 8, 11, 13, 0, 0, 0, time.UTC)
	store := &staleSchedulingStore{
		schedulerChurnStore: &schedulerChurnStore{value: task.Task{
			ID: uuid.New(), SwarmID: uuid.New(), Status: task.StatusScheduling, Version: 41,
			UpdatedAt: fixedNow.Add(-2 * time.Minute), ExecutionPolicy: task.DefaultExecutionPolicy(),
		}},
		listed: make(chan struct{}, 2), proceed: make(chan struct{}),
	}
	controllers := []*TaskController{
		NewTaskController(store, time.Minute, log.NewStdLogger(io.Discard)),
		NewTaskController(store, time.Minute, log.NewStdLogger(io.Discard)),
	}
	for _, controller := range controllers {
		controller.clock = func() time.Time { return fixedNow }
	}
	results := make(chan struct {
		changed int
		err     error
	}, 2)
	for _, controller := range controllers {
		go func(value *TaskController) {
			changed, err := value.ReconcileOnce(context.Background())
			results <- struct {
				changed int
				err     error
			}{changed: changed, err: err}
		}(controller)
	}
	<-store.listed
	<-store.listed
	close(store.proceed)

	first, second := <-results, <-results
	require.NoError(t, first.err)
	require.NoError(t, second.err)
	require.Equal(t, 1, first.changed+second.changed)
	store.mu.Lock()
	require.Equal(t, task.StatusReady, store.value.Status)
	require.Equal(t, int64(42), store.value.Version)
	require.Equal(t, 1, store.transitionCall, "相同 status+version 只能有一个 Controller CAS 成功")
	require.Equal(t, 1, store.outboxWrites)
	store.mu.Unlock()
}

func TestSchedulingRecoveryTimeoutStaysBeyondLease(t *testing.T) {
	t.Parallel()
	require.Equal(t, time.Minute, SchedulingRecoveryTimeout(10*time.Second))
	require.Equal(t, time.Minute, SchedulingRecoveryTimeout(30*time.Second))
	require.Equal(t, 90*time.Second, SchedulingRecoveryTimeout(45*time.Second))
	require.Greater(t, SchedulingRecoveryTimeout(2*time.Minute), 2*time.Minute)
}

func schedulableCandidate() Candidate {
	return Candidate{
		Instance: &agent.Instance{ID: uuid.New(), Status: agent.StatusIdle, Version: 1},
		Template: &agent.Template{
			ID: uuid.New(), Enabled: true, Skills: map[string]float64{}, RiskZone: "sandbox",
		},
		BudgetAllowed: true, HistorySuccess: .5, ContextAffinity: .5,
		QualityScore: .5, LatencyScore: .5,
	}
}
