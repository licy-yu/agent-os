package durabletemporal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

func TestSwarmRunWorkflowSignalPauseResumeAndCancel(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(SwarmRunWorkflow)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalRunCommand, RunCommand{Action: "PAUSE", Reason: "人工检查"})
	}, time.Second)
	env.RegisterDelayedCallback(func() {
		encoded, err := env.QueryWorkflow(QueryRuntimeState)
		require.NoError(t, err)
		var state RuntimeState
		require.NoError(t, encoded.Get(&state))
		require.Equal(t, "PAUSED", state.Status)
		require.Equal(t, "人工检查", state.WaitingReason)
		env.SignalWorkflow(SignalRunCommand, RunCommand{Action: "RESUME"})
	}, 2*time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalRunCommand, RunCommand{Action: "CANCEL", Reason: "测试结束"})
	}, 3*time.Second)

	env.ExecuteWorkflow(SwarmRunWorkflow, WorkflowInput{
		RunID:    "00000000-0000-0000-0000-000000000101",
		TenantID: "00000000-0000-0000-0000-000000000001",
		Status:   "RUNNING", PlanVersion: 1,
	})
	require.NoError(t, env.GetWorkflowError())
	var final RuntimeState
	require.NoError(t, env.GetWorkflowResult(&final))
	require.Equal(t, "CANCELED", final.Status)
	require.Equal(t, int64(3), final.SignalSequence)
}

func TestSwarmRunWorkflowReplanIgnoresDuplicateAndStaleVersions(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(SwarmRunWorkflow)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalRunCommand, RunCommand{Action: "REPLAN", PlanVersion: 4, Reason: "新计划"})
		env.SignalWorkflow(SignalRunCommand, RunCommand{Action: "REPLAN", PlanVersion: 4, Reason: "重复投递"})
		env.SignalWorkflow(SignalRunCommand, RunCommand{Action: "REPLAN", PlanVersion: 2, Reason: "陈旧投递"})
		env.SignalWorkflow(SignalRunCommand, RunCommand{Action: "CANCEL"})
	}, time.Second)
	env.ExecuteWorkflow(SwarmRunWorkflow, WorkflowInput{RunID: "run", TenantID: "tenant", PlanVersion: 1})
	var final RuntimeState
	require.NoError(t, env.GetWorkflowResult(&final))
	require.Equal(t, int32(4), final.PlanVersion)
	require.Equal(t, int64(4), final.SignalSequence)
}

func TestApplyProjectionCorrectsPlanVersionToDatabaseTruth(t *testing.T) {
	now := time.Date(2026, time.August, 11, 10, 30, 0, 0, time.UTC)
	state := RuntimeState{
		PlanVersion: 8,
		LastError:   "上一轮投影失败",
	}

	applyProjection(&state, Projection{PlanVersion: 3}, now)

	require.Equal(t, int32(3), state.PlanVersion)
	require.Empty(t, state.LastError)
	require.Equal(t, now, state.UpdatedAt)
}
