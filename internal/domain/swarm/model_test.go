package swarm

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestSwarmTransitionHappyPath(t *testing.T) {
	t.Parallel()
	value := Swarm{ID: uuid.New(), Status: StatusPending}
	require.NoError(t, value.Transition(StatusRunning))
	require.NoError(t, value.Transition(StatusSucceeded))
	require.True(t, value.IsTerminal())
}

func TestSwarmRejectsSkippingRunning(t *testing.T) {
	t.Parallel()
	value := Swarm{ID: uuid.New(), Status: StatusPending}
	err := value.Transition(StatusSucceeded)
	require.Error(t, err)
	require.Equal(t, StatusPending, value.Status, "失败的转换不能污染内存状态")
}
