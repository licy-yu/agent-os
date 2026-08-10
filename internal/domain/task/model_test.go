package task

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestTaskTransitionEnforcesActorOwnership(t *testing.T) {
	t.Parallel()
	value := Task{ID: uuid.New(), Status: StatusReady}

	// Worker 不能跳过 Scheduler 直接领取 READY 任务。
	require.Error(t, value.Transition(ActorWorker, StatusRunning))
	require.NoError(t, value.Transition(ActorScheduler, StatusScheduling))
	require.NoError(t, value.Transition(ActorScheduler, StatusAssigned))
	require.NoError(t, value.Transition(ActorWorker, StatusRunning))
	require.NoError(t, value.Transition(ActorWorker, StatusReview))

	// 执行 Agent 不能宣布自己成功，最终结论必须来自 Reviewer。
	require.Error(t, value.Transition(ActorWorker, StatusSucceeded))
	require.NoError(t, value.Transition(ActorReviewer, StatusSucceeded))
	require.True(t, value.IsTerminal())
}

func TestDefaultExecutionPolicyHasLoopFuses(t *testing.T) {
	t.Parallel()
	policy := DefaultExecutionPolicy()
	require.EqualValues(t, 3, policy.MaxAttempts)
	require.EqualValues(t, 3, policy.MaxNoProgressRounds)
	require.Equal(t, 30*time.Minute, policy.Timeout())
	require.Greater(t, policy.MaxTokens, int64(0))
}
