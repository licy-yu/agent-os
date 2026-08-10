package worker

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/domain/agent"
	"github.com/licy-yu/agent-os/internal/domain/task"
	"github.com/licy-yu/agent-os/internal/execution"
	"github.com/stretchr/testify/require"
)

type memoryCheckpointWriter struct{ steps []string }

func (w *memoryCheckpointWriter) Save(_ context.Context, step string, _ map[string]any, _ []string) error {
	w.steps = append(w.steps, step)
	return nil
}

type echoTool struct{ calls int }

func (t *echoTool) Call(_ context.Context, _ string, input map[string]any) (map[string]any, error) {
	t.calls++
	return map[string]any{"echo": input}, nil
}

func TestDeterministicExecutorProducesReviewEvidence(t *testing.T) {
	policy := task.DefaultExecutionPolicy()
	work := &execution.Work{
		Task: &task.Task{
			ID: uuid.New(), Goal: "验证执行闭环", ExecutionPolicy: policy,
			Acceptance: task.Acceptance{Build: true, UnitTest: true, RequiredChecks: []string{"contract"}},
		},
		Agent:    &agent.Instance{ID: uuid.New()},
		Template: &agent.Template{Model: "mock/deterministic", CostPer1KTokensMicros: 1000},
		Attempt:  &execution.Attempt{ID: uuid.New()},
	}
	checkpoints, tools := new(memoryCheckpointWriter), new(echoTool)
	result, err := (DeterministicExecutor{}).Execute(context.Background(), work, checkpoints, tools)
	require.NoError(t, err)
	require.Equal(t, []string{"plan", "completed"}, checkpoints.steps)
	require.Equal(t, 1, tools.calls)
	require.True(t, result.Checks["build"])
	require.True(t, result.Checks["unit_test"])
	require.True(t, result.Checks["contract"])
	require.Equal(t, int64(192), result.CostMicros)
}
