package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/agentloop"
	"github.com/licy-yu/agent-os/internal/contextengine"
	"github.com/licy-yu/agent-os/internal/domain/agent"
	"github.com/licy-yu/agent-os/internal/domain/task"
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

type runtimeWaitStore struct {
	execution.Store
	work       *execution.Work
	suspended  bool
	waitReason execution.AttemptWaitReason
	completed  bool
}

func (s *runtimeWaitStore) ClaimWork(context.Context, uuid.UUID, uuid.UUID, string) (*execution.Work, error) {
	return s.work, nil
}

func (s *runtimeWaitStore) SuspendAttempt(_ context.Context, owner execution.AttemptOwner,
	wait execution.AttemptWait,
) error {
	if !s.work.Attempt.OwnedBy(owner) {
		return errors.New("owner mismatch")
	}
	s.suspended, s.waitReason = true, wait.Reason
	return nil
}

func (s *runtimeWaitStore) HeartbeatAttempt(context.Context, execution.AttemptOwner) error {
	return nil
}

func (s *runtimeWaitStore) CompleteAttempt(context.Context, execution.AttemptOwner, execution.ExecutionResult) error {
	s.completed = true
	return nil
}

type waitExecutor struct{ err error }

func (e waitExecutor) Execute(context.Context, *execution.Work, execution.CheckpointWriter,
	execution.ToolCaller,
) (execution.ExecutionResult, error) {
	return execution.ExecutionResult{}, e.err
}

type waitRouter struct{ executor execution.Executor }

func (r waitRouter) ForModel(string) execution.Executor { return r.executor }

type serializedExecutor struct {
	active  atomic.Int32
	maximum atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (e *serializedExecutor) Execute(context.Context, *execution.Work, execution.CheckpointWriter,
	execution.ToolCaller,
) (execution.ExecutionResult, error) {
	current := e.active.Add(1)
	for observed := e.maximum.Load(); current > observed; observed = e.maximum.Load() {
		if e.maximum.CompareAndSwap(observed, current) {
			break
		}
	}
	e.entered <- struct{}{}
	<-e.release
	e.active.Add(-1)
	return execution.ExecutionResult{Output: map[string]any{"output": "ok"}}, nil
}

type runtimeMessage struct {
	data    []byte
	acked   bool
	retried bool
}

func (m *runtimeMessage) Data() []byte              { return m.data }
func (m *runtimeMessage) Ack(context.Context) error { m.acked = true; return nil }
func (m *runtimeMessage) Retry(time.Duration) error { m.retried = true; return nil }
func (m *runtimeMessage) InProgress() error         { return nil }

func TestApprovalAndUnknownKeepAttemptOpenForCheckpointResume(t *testing.T) {
	t.Parallel()
	require.True(t, keepAttemptOpen(fmt.Errorf("等待: %w", toolgateway.ErrApprovalPending)))
	require.True(t, keepAttemptOpen(fmt.Errorf("对账: %w", toolgateway.ErrEffectReconcileRequired)))
	require.False(t, keepAttemptOpen(fmt.Errorf("普通模型错误")))
}

func TestRuntimeDurablySuspendsAndAcknowledgesLongWait(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		err    error
		reason execution.AttemptWaitReason
	}{
		{name: "approval", err: toolgateway.ErrApprovalPending, reason: execution.WaitForApproval},
		{name: "unknown", err: toolgateway.ErrEffectReconcileRequired, reason: execution.WaitForExternal},
	} {
		t.Run(test.name, func(t *testing.T) {
			taskID, agentID, attemptID := uuid.New(), uuid.New(), uuid.New()
			store := &runtimeWaitStore{work: &execution.Work{
				Task:     &task.Task{ID: taskID, ExecutionPolicy: task.DefaultExecutionPolicy()},
				Agent:    &agent.Instance{ID: agentID},
				Template: &agent.Template{Model: "wait/test"},
				Attempt: &execution.Attempt{
					ID: attemptID, TaskID: taskID, AgentID: agentID,
					Status: execution.AttemptRunning, WorkerID: "wait-worker", FencingToken: 7,
				},
			}}
			message := &runtimeMessage{data: []byte(fmt.Sprintf(
				`{"id":%q,"agent_id":%q}`, taskID.String(), agentID.String()))}
			waitErr := &toolgateway.EffectWaitError{
				Cause: test.err, EffectID: uuid.New(), Detail: "test boundary",
			}
			runtime := NewRuntime("wait-worker", nil, store, nil,
				waitRouter{executor: waitExecutor{err: fmt.Errorf("boundary: %w", waitErr)}},
				nil, 1, time.Hour, log.NewStdLogger(io.Discard))

			err := runtime.handle(context.Background(), message)
			require.NoError(t, err)
			require.True(t, store.suspended)
			require.Equal(t, test.reason, store.waitReason)
			require.True(t, message.acked)
			require.False(t, message.retried)
			require.False(t, store.completed)
		})
	}
}

func TestRuntimeSerializesDuplicateTaskDeliveriesWithinWorker(t *testing.T) {
	t.Parallel()
	taskID, agentID, attemptID := uuid.New(), uuid.New(), uuid.New()
	store := &runtimeWaitStore{work: &execution.Work{
		Task:  &task.Task{ID: taskID, ExecutionPolicy: task.DefaultExecutionPolicy()},
		Agent: &agent.Instance{ID: agentID}, Template: &agent.Template{Model: "serial/test"},
		Attempt: &execution.Attempt{
			ID: attemptID, TaskID: taskID, AgentID: agentID,
			Status: execution.AttemptRunning, WorkerID: "serial-worker", FencingToken: 3,
		},
	}}
	executor := &serializedExecutor{
		entered: make(chan struct{}, 2), release: make(chan struct{}),
	}
	runtime := NewRuntime("serial-worker", nil, store, nil, waitRouter{executor: executor},
		nil, 2, time.Hour, log.NewStdLogger(io.Discard))
	newMessage := func() *runtimeMessage {
		return &runtimeMessage{data: []byte(fmt.Sprintf(
			`{"id":%q,"agent_id":%q}`, taskID.String(), agentID.String()))}
	}
	first, second := newMessage(), newMessage()
	errorsCh := make(chan error, 2)
	go func() { errorsCh <- runtime.handle(context.Background(), first) }()
	<-executor.entered
	go func() { errorsCh <- runtime.handle(context.Background(), second) }()

	select {
	case <-executor.entered:
		t.Fatal("duplicate delivery entered executor concurrently")
	case <-time.After(100 * time.Millisecond):
	}
	close(executor.release)
	for index := 0; index < 2; index++ {
		require.NoError(t, <-errorsCh)
	}
	require.EqualValues(t, 1, executor.maximum.Load())
	require.True(t, first.acked)
	require.True(t, second.acked)
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
