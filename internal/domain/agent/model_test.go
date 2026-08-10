package agent

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestAgentMustReserveBeforeRunning(t *testing.T) {
	t.Parallel()
	value := Instance{ID: uuid.New(), Status: StatusIdle}
	require.Error(t, value.Transition(StatusRunning))
	require.NoError(t, value.Transition(StatusReserved))
	require.NoError(t, value.Transition(StatusRunning))
}

func TestDrainingAgentCannotReturnToIdle(t *testing.T) {
	t.Parallel()
	value := Instance{ID: uuid.New(), Status: StatusDraining}
	require.Error(t, value.Transition(StatusIdle))
	require.NoError(t, value.Transition(StatusStopped))
}
