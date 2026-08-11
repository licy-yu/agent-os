package orchestrator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/domain/agent"
	"github.com/licy-yu/agent-os/internal/domain/task"
	"github.com/licy-yu/agent-os/internal/lease"
	"github.com/stretchr/testify/require"
)

type cycleStore struct {
	task      *task.Task
	candidate Candidate
	bound     bool
	decisions []SchedulerDecision
}

func (s *cycleStore) ListReconcileTasks(context.Context, int) ([]*task.Task, error) {
	return []*task.Task{s.task}, nil
}
func (s *cycleStore) DependenciesSatisfied(context.Context, uuid.UUID) (bool, error) {
	return true, nil
}
func (s *cycleStore) TransitionTask(_ context.Context, _ uuid.UUID, version int64, from, to task.Status, _ string) (int64, error) {
	if s.task.Version != version || s.task.Status != from {
		return 0, fmt.Errorf("stale state")
	}
	s.task.Status = to
	s.task.Version++
	return s.task.Version, nil
}
func (s *cycleStore) ActivateRegisteredAgents(context.Context, int) (int, error) { return 0, nil }
func (s *cycleStore) ListQueuedTasks(context.Context, int) ([]QueuedTask, error) {
	if s.task.Status != task.StatusReady {
		return nil, nil
	}
	return []QueuedTask{{Task: s.task}}, nil
}
func (s *cycleStore) ListSchedulerCandidates(context.Context, uuid.UUID, int64) ([]Candidate, error) {
	return []Candidate{s.candidate}, nil
}
func (s *cycleStore) BindTask(_ context.Context, _ uuid.UUID, taskVersion int64, _ uuid.UUID, agentVersion int64) error {
	if s.task.Status != task.StatusScheduling || s.task.Version != taskVersion || s.candidate.Instance.Version != agentVersion {
		return fmt.Errorf("bind snapshot mismatch")
	}
	s.bound = true
	return nil
}
func (s *cycleStore) RecordSchedulerDecision(_ context.Context, value SchedulerDecision) error {
	s.decisions = append(s.decisions, value)
	return nil
}

type memoryLeaseManager struct{}

func (memoryLeaseManager) Reserve(_ context.Context, agentID, taskID uuid.UUID, ttl time.Duration) (*lease.Lease, bool, error) {
	return &lease.Lease{AgentID: agentID, TaskID: taskID, Token: "owned", TTL: ttl}, true, nil
}
func (memoryLeaseManager) Release(context.Context, *lease.Lease) error { return nil }

func TestControllerAndSchedulerCycle(t *testing.T) {
	now := time.Now().UTC()
	swarmID := uuid.New()
	value := &task.Task{
		ID: uuid.New(), SwarmID: swarmID, Status: task.StatusCreated, Version: 1,
		ExecutionPolicy: task.DefaultExecutionPolicy(), CreatedAt: now,
	}
	store := &cycleStore{task: value, candidate: Candidate{
		Instance:      &agent.Instance{ID: uuid.New(), Status: agent.StatusIdle, Version: 1},
		Template:      &agent.Template{ID: uuid.New(), Enabled: true, Skills: map[string]float64{}, RiskZone: "sandbox"},
		BudgetAllowed: true, HistorySuccess: .5, ContextAffinity: 1, QualityScore: .5, LatencyScore: .5,
	}}
	logger := log.NewStdLogger(nil)
	controller := NewTaskController(store, logger)

	changed, err := controller.ReconcileOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, changed)
	require.Equal(t, task.StatusPlanning, value.Status)
	changed, err = controller.ReconcileOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, changed)
	require.Equal(t, task.StatusReady, value.Status)

	scheduler := NewScheduler(store, memoryLeaseManager{}, 30*time.Second, logger)
	bound, err := scheduler.ScheduleOnce(context.Background())
	require.NoError(t, err)
	require.True(t, bound)
	require.True(t, store.bound)
	require.Len(t, store.decisions, 1)
	require.Equal(t, store.candidate.Instance.ID, *store.decisions[0].SelectedAgentID)
	require.Equal(t, task.StatusScheduling, value.Status, "数据库 Bind 会在真实仓储中原子写为 ASSIGNED")
}
