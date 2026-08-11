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

func TestControllerMayOnlyRecoverSchedulingToReady(t *testing.T) {
	t.Parallel()
	value := Task{ID: uuid.New(), Status: StatusScheduling}

	// 该权限仅供超时判断后的 Controller 使用；状态机允许恢复 READY，但不能让
	// Controller 冒充 Scheduler 完成 ASSIGNED，最终所有权仍由 Bind CAS 决定。
	require.Error(t, value.Transition(ActorController, StatusAssigned))
	require.NoError(t, value.Transition(ActorController, StatusReady))
}
