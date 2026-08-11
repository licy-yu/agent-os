package execution

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestAttemptOwnerCarriesWorkerAndFence(t *testing.T) {
	t.Parallel()
	attempt := Attempt{ID: uuid.New(), WorkerID: "worker-a", FencingToken: 7}

	owner := attempt.Owner()
	require.True(t, owner.Valid())
	require.Equal(t, attempt.ID, owner.AttemptID)
	require.Equal(t, "worker-a", owner.WorkerID)
	require.EqualValues(t, 7, owner.FencingToken)
}

func TestAttemptOwnerRejectsMigrationZeroFence(t *testing.T) {
	t.Parallel()
	require.False(t, (AttemptOwner{AttemptID: uuid.New(), WorkerID: "worker-a"}).Valid())
	require.False(t, (AttemptOwner{AttemptID: uuid.New(), WorkerID: " ", FencingToken: 1}).Valid())
}

func TestActiveAttemptRejectsDifferentWorkerReplay(t *testing.T) {
	t.Parallel()
	attempt := Attempt{
		ID: uuid.New(), Status: AttemptRunning, WorkerID: "worker-a", FencingToken: 3,
	}
	require.True(t, attempt.CanReplay("worker-a"))
	require.False(t, attempt.CanReplay("worker-b"))
	require.True(t, attempt.OwnedBy(attempt.Owner()))
	stale := attempt.Owner()
	stale.FencingToken--
	require.False(t, attempt.OwnedBy(stale))
}

func TestTypedCheckpointStepMatchesDatabaseContract(t *testing.T) {
	t.Parallel()
	require.Equal(t, StepType("CHECKPOINT"), StepCheckpoint)
	require.Equal(t, StepStatus("SUCCEEDED"), StepSucceeded)
}
