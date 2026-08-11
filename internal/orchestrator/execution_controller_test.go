package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/domain/agent"
	"github.com/licy-yu/agent-os/internal/domain/task"
	"github.com/licy-yu/agent-os/internal/execution"
	"github.com/stretchr/testify/require"
)

type reviewStore struct {
	work            *execution.Work
	evaluation      execution.Evaluation
	expired         int
	recovered       int
	expireErr       error
	recoverErr      error
	expireCalledAt  time.Time
	recoverCalledAt time.Time
	expireCalls     int
	recoverCalls    int
}

func (s *reviewStore) ClaimWork(context.Context, uuid.UUID, uuid.UUID, string) (*execution.Work, error) {
	return nil, nil
}
func (s *reviewStore) SuspendAttempt(context.Context, execution.AttemptOwner, execution.AttemptWait) error {
	return nil
}
func (s *reviewStore) SaveCheckpoint(context.Context, execution.AttemptOwner, execution.Checkpoint) error {
	return nil
}
func (s *reviewStore) HeartbeatAttempt(context.Context, execution.AttemptOwner) error { return nil }
func (s *reviewStore) CompleteAttempt(context.Context, execution.AttemptOwner, execution.ExecutionResult) error {
	return nil
}
func (s *reviewStore) ListReviewWork(context.Context, int) ([]*execution.Work, error) {
	return []*execution.Work{s.work}, nil
}
func (s *reviewStore) ApplyReview(_ context.Context, _ *execution.Work, value execution.Evaluation, _ time.Time) error {
	s.evaluation = value
	return nil
}
func (s *reviewStore) ExpireWaitingInteractions(_ context.Context, now time.Time, limit int) (int, error) {
	s.expireCalls++
	s.expireCalledAt = now
	if limit != 100 {
		return 0, errors.New("unexpected expiry batch")
	}
	return s.expired, s.expireErr
}
func (s *reviewStore) RecoverTimedOut(_ context.Context, cutoff time.Time, limit int) (int, error) {
	s.recoverCalls++
	s.recoverCalledAt = cutoff
	if limit != 100 {
		return 0, errors.New("unexpected recovery batch")
	}
	return s.recovered, s.recoverErr
}

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

func TestRecoveryExpiresInteractionsAndRecoversTimedOutAttempts(t *testing.T) {
	t.Parallel()
	store := &reviewStore{expired: 2, recovered: 3}
	timeout := 90 * time.Second
	before := time.Now().UTC()

	changed, err := NewRecoveryController(store, timeout).ReconcileOnce(context.Background())
	after := time.Now().UTC()

	require.NoError(t, err)
	require.Equal(t, 5, changed)
	require.Equal(t, 1, store.expireCalls)
	require.Equal(t, 1, store.recoverCalls)
	require.False(t, store.expireCalledAt.Before(before))
	require.False(t, store.expireCalledAt.After(after))
	require.Equal(t, timeout, store.expireCalledAt.Sub(store.recoverCalledAt))
}

func TestRecoveryRunsBothClosuresAndJoinsErrors(t *testing.T) {
	t.Parallel()
	expireErr := errors.New("expire unavailable")
	recoverErr := errors.New("recovery unavailable")
	store := &reviewStore{
		expired: 1, recovered: 2, expireErr: expireErr, recoverErr: recoverErr,
	}

	changed, err := NewRecoveryController(store, time.Minute).ReconcileOnce(context.Background())

	require.Equal(t, 3, changed)
	require.ErrorIs(t, err, expireErr)
	require.ErrorIs(t, err, recoverErr)
	require.Equal(t, 1, store.expireCalls)
	require.Equal(t, 1, store.recoverCalls)
}
