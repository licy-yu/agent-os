package service

import (
	"testing"

	"github.com/licy-yu/agent-os/internal/domain/task"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestDecodeStructAcceptsProtoCamelCase(t *testing.T) {
	t.Parallel()
	input, err := structpb.NewStruct(map[string]any{
		"maxAttempts":         5,
		"maxHandoffs":         7,
		"maxTokens":           2000,
		"maxToolCalls":        20,
		"maxNoProgressRounds": 2,
		"timeoutSeconds":      90,
	})
	require.NoError(t, err)

	policy := task.DefaultExecutionPolicy()
	require.NoError(t, decodeStruct(input, &policy))
	require.EqualValues(t, 5, policy.MaxAttempts)
	require.EqualValues(t, 90, policy.TimeoutSeconds)
}

func TestDecodeStructRejectsUnknownPolicyField(t *testing.T) {
	t.Parallel()
	input, err := structpb.NewStruct(map[string]any{"maxAttemps": 5}) // 故意拼错 attempts。
	require.NoError(t, err)

	policy := task.DefaultExecutionPolicy()
	err = decodeStruct(input, &policy)
	require.ErrorContains(t, err, "unknown field")
}
