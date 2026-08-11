package agentloop

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/domain/agent"
	"github.com/licy-yu/agent-os/internal/domain/task"
	"github.com/licy-yu/agent-os/internal/execution"
	"github.com/stretchr/testify/require"
)

type fakeExecutor struct {
	result    execution.ExecutionResult
	toolCalls int
}

func (e fakeExecutor) Execute(ctx context.Context, _ *execution.Work,
	checkpoints execution.CheckpointWriter, tools execution.ToolCaller,
) (execution.ExecutionResult, error) {
	for index := 0; index < e.toolCalls; index++ {
		if _, err := tools.Call(ctx, "echo", map[string]any{"index": index}); err != nil {
			return execution.ExecutionResult{}, err
		}
	}
	if err := checkpoints.Save(ctx, "candidate", map[string]any{"progress": true}, nil); err != nil {
		return execution.ExecutionResult{}, err
	}
	return e.result, nil
}

type fakeCheckpointWriter struct{ steps []string }

func (w *fakeCheckpointWriter) Save(_ context.Context, step string, _ map[string]any, _ []string) error {
	w.steps = append(w.steps, step)
	return nil
}

type fakeToolCaller struct{ calls int }

func (t *fakeToolCaller) Call(_ context.Context, _ string, _ map[string]any) (map[string]any, error) {
	t.calls++
	return map[string]any{"ok": true}, nil
}

func TestEinoExecutorRestoresThenRunsDelegate(t *testing.T) {
	inner := fakeExecutor{result: execution.ExecutionResult{
		Output: map[string]any{"answer": "ok"}, TokensIn: 20, TokensOut: 10,
	}}
	executor, err := New(inner)
	require.NoError(t, err)
	checkpoints, tools := new(fakeCheckpointWriter), new(fakeToolCaller)
	result, err := executor.Execute(context.Background(), validWork(), checkpoints, tools)
	require.NoError(t, err)
	require.Equal(t, "ok", result.Output["answer"])
	require.Equal(t, []string{"eino_restore_context", "candidate"}, checkpoints.steps)
}

func TestEinoExecutorRejectsRealToolCountBeyondContract(t *testing.T) {
	inner := fakeExecutor{toolCalls: 2, result: execution.ExecutionResult{Output: map[string]any{"answer": "ok"}}}
	executor, err := New(inner)
	require.NoError(t, err)
	work := validWork()
	work.Task.ExecutionPolicy.MaxToolCalls = 1
	_, err = executor.Execute(context.Background(), work, new(fakeCheckpointWriter), new(fakeToolCaller))
	require.ErrorIs(t, err, ErrGuardExceeded)
}

func TestEinoExecutorRejectsTokenOverBudget(t *testing.T) {
	inner := fakeExecutor{result: execution.ExecutionResult{
		Output: map[string]any{"answer": "ok"}, TokensIn: 80, TokensOut: 30,
	}}
	executor, err := New(inner)
	require.NoError(t, err)
	work := validWork()
	work.Task.ExecutionPolicy.MaxTokens = 100
	_, err = executor.Execute(context.Background(), work, new(fakeCheckpointWriter), new(fakeToolCaller))
	require.True(t, errors.Is(err, ErrGuardExceeded))
}

func validWork() *execution.Work {
	policy := task.DefaultExecutionPolicy()
	return &execution.Work{
		Task:     &task.Task{ID: uuid.New(), Goal: "完成测试", ExecutionPolicy: policy},
		Agent:    &agent.Instance{ID: uuid.New()},
		Template: &agent.Template{ID: uuid.New(), Model: "mock/test"},
		Attempt:  &execution.Attempt{ID: uuid.New()},
	}
}
