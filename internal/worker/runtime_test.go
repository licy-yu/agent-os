package worker

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/agentloop"
	"github.com/licy-yu/agent-os/internal/contextengine"
	"github.com/licy-yu/agent-os/internal/execution"
	"github.com/licy-yu/agent-os/internal/failure"
	"github.com/licy-yu/agent-os/internal/toolgateway"
	"github.com/stretchr/testify/require"
)

// checkpointCaptureStore 通过嵌入 Store 保留其它端口，只覆盖本测试实际调用的方法。
type checkpointCaptureStore struct {
	execution.Store
	owner      execution.AttemptOwner
	checkpoint execution.Checkpoint
}

func TestApprovalAndUnknownKeepAttemptOpenForCheckpointResume(t *testing.T) {
	t.Parallel()
	require.True(t, keepAttemptOpen(fmt.Errorf("等待: %w", toolgateway.ErrApprovalPending)))
	require.True(t, keepAttemptOpen(fmt.Errorf("对账: %w", toolgateway.ErrEffectReconcileRequired)))
	require.False(t, keepAttemptOpen(fmt.Errorf("普通模型错误")))
}

func TestRuntimeFailureClassification(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		err      error
		ctxErr   error
		expected failure.Class
	}{
		{name: "policy denial", err: fmt.Errorf("shell: %w", toolgateway.ErrDenied), expected: failure.PolicyViolation},
		{name: "context overflow", err: fmt.Errorf("pack: %w", contextengine.ErrBudgetTooSmall), expected: failure.ContextOverflow},
		{name: "no progress", err: fmt.Errorf("%w: 连续 3 轮状态没有进展", agentloop.ErrGuardExceeded), expected: failure.NoProgress},
		{name: "token guard", err: fmt.Errorf("%w: token 20 > 10", agentloop.ErrGuardExceeded), expected: failure.BudgetExceeded},
		{name: "deadline captured before cancel", err: errors.New("request stopped"), ctxErr: context.DeadlineExceeded, expected: failure.NetworkTemporary},
		{name: "rate limit fallback", err: errors.New("provider returned 429"), expected: failure.RateLimit},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decision := classifyRuntimeFailure(test.err, test.ctxErr)
			require.Equal(t, test.expected, decision.Class)
		})
	}
}

func (s *checkpointCaptureStore) SaveCheckpoint(_ context.Context, owner execution.AttemptOwner,
	value execution.Checkpoint,
) error {
	s.owner, s.checkpoint = owner, value
	return nil
}

func TestCheckpointWriterPropagatesAttemptFence(t *testing.T) {
	t.Parallel()
	owner := execution.AttemptOwner{AttemptID: uuid.New(), WorkerID: "worker-a", FencingToken: 9}
	store := new(checkpointCaptureStore)
	writer := &checkpointWriter{store: store, owner: owner, sequence: 4}

	err := writer.Save(context.Background(), "model_response", map[string]any{"round": 2}, []string{"artifact://result/1"})
	require.NoError(t, err)
	require.Equal(t, owner, store.owner)
	require.EqualValues(t, 5, store.checkpoint.Sequence)
	require.EqualValues(t, 9, store.checkpoint.FencingToken)
	require.Equal(t, owner.AttemptID, store.checkpoint.AttemptID)
}

func TestCheckpointResumeSequenceUsesLatestPersistedBoundary(t *testing.T) {
	t.Parallel()
	work := &execution.Work{
		Attempt:          &execution.Attempt{StepCount: 2},
		LatestCheckpoint: &execution.Checkpoint{Sequence: 7},
	}
	require.EqualValues(t, 7, checkpointResumeSequence(work))
	work.Attempt.StepCount = 9
	require.EqualValues(t, 9, checkpointResumeSequence(work))
}
