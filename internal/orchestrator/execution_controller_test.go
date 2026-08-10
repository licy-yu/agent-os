package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/domain/agent"
	"github.com/licy-yu/agent-os/internal/domain/task"
	"github.com/licy-yu/agent-os/internal/execution"
	"github.com/stretchr/testify/require"
)

type reviewStore struct {
	work       *execution.Work
	evaluation execution.Evaluation
}

func (s *reviewStore) ClaimWork(context.Context, uuid.UUID, uuid.UUID, string) (*execution.Work, error) {
	return nil, nil
}
func (s *reviewStore) SaveCheckpoint(context.Context, execution.Checkpoint) error { return nil }
func (s *reviewStore) HeartbeatAttempt(context.Context, uuid.UUID, string) error  { return nil }
func (s *reviewStore) CompleteAttempt(context.Context, uuid.UUID, execution.ExecutionResult) error {
	return nil
}
func (s *reviewStore) ListReviewWork(context.Context, int) ([]*execution.Work, error) {
	return []*execution.Work{s.work}, nil
}
func (s *reviewStore) ApplyReview(_ context.Context, _ *execution.Work, value execution.Evaluation, _ time.Time) error {
	s.evaluation = value
	return nil
}
func (s *reviewStore) RecoverTimedOut(context.Context, time.Time, int) (int, error) { return 0, nil }

func TestReviewerAcceptsOnlyPersistedEvidence(t *testing.T) {
	policy := task.DefaultExecutionPolicy()
	store := &reviewStore{work: &execution.Work{
		Task: &task.Task{
			ID: uuid.New(), ExecutionPolicy: policy,
			Acceptance: task.Acceptance{Build: true, UnitTest: true},
		},
		Agent: &agent.Instance{ID: uuid.New()}, Template: &agent.Template{},
		Attempt: &execution.Attempt{
			ID: uuid.New(), Number: 1, OutputSnapshot: map[string]any{
				"checks":            map[string]any{"execution": true, "build": true, "unit_test": true},
				"policy_violations": []any{}, "quality_score": .95,
			},
		},
	}}
	changed, err := NewReviewerController(store).ReconcileOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, changed)
	require.Equal(t, execution.DecisionAccept, store.evaluation.Decision)
	require.True(t, store.evaluation.MachinePass)
}

func TestReviewerRetriesMissingCheckBeforeAttemptLimit(t *testing.T) {
	policy := task.DefaultExecutionPolicy()
	store := &reviewStore{work: &execution.Work{
		Task:  &task.Task{ID: uuid.New(), ExecutionPolicy: policy, Acceptance: task.Acceptance{UnitTest: true}},
		Agent: &agent.Instance{ID: uuid.New()}, Template: &agent.Template{},
		Attempt: &execution.Attempt{ID: uuid.New(), Number: 1, OutputSnapshot: map[string]any{
			"checks": map[string]any{"execution": true}, "policy_violations": []any{}, "quality_score": .9,
		}},
	}}
	_, err := NewReviewerController(store).ReconcileOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, execution.DecisionRetry, store.evaluation.Decision)
	require.False(t, store.evaluation.MachinePass)
}
