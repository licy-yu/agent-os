package run

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestRunTransitionHappyPath(t *testing.T) {
	t.Parallel()
	value := Run{ID: uuid.New(), Status: StatusCreated}

	require.NoError(t, value.Transition(ActorController, StatusAdmission))
	require.NoError(t, value.Transition(ActorAdmission, StatusPlanning))
	require.NoError(t, value.Transition(ActorPlanner, StatusReady))
	require.NoError(t, value.Transition(ActorController, StatusRunning))
	require.NoError(t, value.Transition(ActorRuntime, StatusVerifying))
	require.NoError(t, value.Transition(ActorVerifier, StatusCompleted))
	require.True(t, value.IsTerminal())
}

func TestRunAdmissionCanWaitAndRetry(t *testing.T) {
	t.Parallel()
	value := Run{ID: uuid.New(), Status: StatusAdmission}

	require.NoError(t, value.Transition(ActorAdmission, StatusPendingCapacity))
	require.True(t, value.IsWaiting())
	require.NoError(t, value.Transition(ActorController, StatusAdmission))
	require.NoError(t, value.Transition(ActorAdmission, StatusPlanning))
}

func TestRunTransitionEnforcesActorAndPreservesState(t *testing.T) {
	t.Parallel()
	value := Run{ID: uuid.New(), Status: StatusReady}

	// Runtime 不能绕过 RunController 启动 READY Run。
	err := value.Transition(ActorRuntime, StatusRunning)
	require.Error(t, err)
	require.True(t, errors.Is(err, domain.ErrInvalidTransition))
	require.Equal(t, StatusReady, value.Status)
}

func TestRunControllerCanCancelNonTerminalButNotTerminalRun(t *testing.T) {
	t.Parallel()
	value := Run{ID: uuid.New(), Status: StatusWaitingUser}
	require.NoError(t, value.Transition(ActorController, StatusCanceled))
	require.True(t, value.IsTerminal())
	require.Error(t, value.Transition(ActorController, StatusRunning))
}

func TestRunRejectsUnknownCurrentOrTargetState(t *testing.T) {
	t.Parallel()
	value := Run{ID: uuid.New(), Status: Status("UNKNOWN")}
	require.Error(t, value.Transition(ActorController, StatusCanceled))
	value.Status = StatusCreated
	require.Error(t, value.Transition(ActorController, Status("UNKNOWN")))
}

func TestRunBudgetAllowsOnlyNonNegativeInBudgetReservation(t *testing.T) {
	t.Parallel()
	budget := Budget{MaxTokens: 1_000, MaxCostMicros: 500, SpentTokens: 200, SpentCostMicros: 100}

	require.True(t, budget.Allows(800, 400))
	require.False(t, budget.Allows(801, 1))
	require.False(t, budget.Allows(1, 401))
	require.False(t, budget.Allows(-1, 0))
	// 0 上限沿用现有语义：该预算维度不设硬上限。
	require.True(t, (Budget{}).Allows(1_000_000, 1_000_000))
}
