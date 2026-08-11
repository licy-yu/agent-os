package effect

import (
	"errors"
	"math"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestRequestHashIsStableAcrossMapOrder(t *testing.T) {
	first := Request{ToolName: "ticket.create", Arguments: map[string]any{
		"title": "故障", "labels": map[string]any{"severity": "high", "team": "runtime"},
	}}
	second := Request{ToolName: "ticket.create", Arguments: map[string]any{
		"labels": map[string]any{"team": "runtime", "severity": "high"}, "title": "故障",
	}}

	firstHash, err := first.Hash()
	require.NoError(t, err)
	secondHash, err := second.Hash()
	require.NoError(t, err)
	require.Equal(t, firstHash, secondHash)
}

func TestPrepareRejectsSameIdempotencyKeyWithDifferentRequest(t *testing.T) {
	request := Request{ToolName: "pull_request.create", Arguments: map[string]any{"branch": "feature/a"}}
	value, err := Prepare(uuid.New(), uuid.New(), "run-1/task-2/create-pr", RiskR2ExternalReversible, request)
	require.NoError(t, err)
	require.Equal(t, StatusPrepared, value.Status)

	err = value.MatchesRequest(Request{
		ToolName: "pull_request.create", Arguments: map[string]any{"branch": "feature/b"},
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIdempotencyConflict))
}

func TestRequestHashRejectsNonJSONValue(t *testing.T) {
	_, err := (Request{ToolName: "metric.write", Arguments: map[string]any{"value": math.Inf(1)}}).Hash()
	require.ErrorIs(t, err, ErrInvalidRequest)
}

func TestUnknownEffectMustReconcileBeforeSettling(t *testing.T) {
	value := &Effect{Status: StatusExecuting}
	require.NoError(t, value.Transition(StatusUnknown))
	require.ErrorIs(t, value.Transition(StatusSucceeded), ErrInvalidTransition)
	require.NoError(t, value.Transition(StatusReconciling))
	require.NoError(t, value.Transition(StatusSucceeded))
}

func TestSucceededEffectCanBeCompensatedButCannotExecuteAgain(t *testing.T) {
	value := &Effect{Status: StatusSucceeded}
	require.ErrorIs(t, value.Transition(StatusExecuting), ErrInvalidTransition)
	require.NoError(t, value.Transition(StatusCompensating))
	require.NoError(t, value.Transition(StatusCompensated))
	require.ErrorIs(t, value.Transition(StatusExecuting), ErrInvalidTransition)
}
