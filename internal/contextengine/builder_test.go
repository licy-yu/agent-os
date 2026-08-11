package contextengine

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/domain/agent"
	"github.com/licy-yu/agent-os/internal/domain/task"
	"github.com/licy-yu/agent-os/internal/execution"
	"github.com/stretchr/testify/require"
)

func TestBuildKeepsContractsAndRedactsSecrets(t *testing.T) {
	work := validWork()
	work.Task.Input = map[string]any{"repository": "crm", "apiToken": "must-not-leak"}
	pack, err := Build(work)
	require.NoError(t, err)
	require.Greater(t, pack.UsedTokens, int64(0))
	require.Equal(t, "agent_instructions", pack.Sections[0].Name)
	raw, err := jsonBytes(pack)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "must-not-leak")
	require.Contains(t, string(raw), "[REDACTED]")
}

func jsonBytes(value any) ([]byte, error) { return json.Marshal(value) }

func TestBuildOffloadsHugeCheckpoint(t *testing.T) {
	work := validWork()
	work.LatestCheckpoint = &execution.Checkpoint{
		ID: uuid.New(), Sequence: 9, StepName: "tool_result",
		State: map[string]any{"logs": strings.Repeat("line\n", 20_000)},
	}
	pack, err := Build(work)
	require.NoError(t, err)
	raw, err := jsonBytes(pack)
	require.NoError(t, err)
	require.NotContains(t, string(raw), strings.Repeat("line\n", 100))
	require.Contains(t, string(raw), "checkpoint://")
}

func validWork() *execution.Work {
	policy := task.DefaultExecutionPolicy()
	policy.MaxTokens = 1_000
	return &execution.Work{
		Task: &task.Task{ID: uuid.New(), Name: "实现", Goal: "完成目标", Version: 3,
			ExecutionPolicy: policy},
		Template: &agent.Template{ID: uuid.New(), Prompt: "你是代码 Agent", Model: "gpt-test",
			TemplateVersion: "v1", ContextWindow: 16_000},
		Attempt: &execution.Attempt{ID: uuid.New(), Number: 2},
	}
}
