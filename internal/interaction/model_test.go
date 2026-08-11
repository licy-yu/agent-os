package interaction

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestInteractionRequiresWaitingBeforeResolution(t *testing.T) {
	value, err := New(uuid.New(), TypeApproval, "批准生产发布", map[string]any{"environment": "prod"}, []string{"approve", "reject"})
	require.NoError(t, err)

	err = value.Resolve(Response{Action: "approve", RespondedBy: "user-1", RespondedAt: time.Now()})
	require.ErrorIs(t, err, ErrInvalidTransition)
	require.Equal(t, StatusCreated, value.Status)
}

func TestInteractionResolvePersistsAllowedAction(t *testing.T) {
	value, err := New(uuid.New(), TypeApproval, "批准 Git Push", nil, []string{"approve", "reject", "edit"})
	require.NoError(t, err)
	require.NoError(t, value.Transition(StatusWaiting))

	now := time.Now().UTC()
	require.NoError(t, value.Resolve(Response{
		Action: "APPROVE", Payload: map[string]any{"branch": "feature/a"},
		RespondedBy: "user-1", RespondedAt: now,
	}))
	require.Equal(t, StatusResolved, value.Status)
	require.True(t, value.Terminal())
	require.Equal(t, "APPROVE", value.Response.Action)
	require.Equal(t, now, value.Response.RespondedAt)
}

func TestInteractionRejectsActionAddedByCaller(t *testing.T) {
	value, err := New(uuid.New(), TypeChoice, "选择发布区域", nil, []string{"cn", "sg"})
	require.NoError(t, err)
	require.NoError(t, value.Transition(StatusWaiting))

	err = value.Resolve(Response{Action: "us", RespondedBy: "user-1", RespondedAt: time.Now()})
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidResponse))
	require.Equal(t, StatusWaiting, value.Status)
}

func TestExpiredInteractionCannotReopenOrResolve(t *testing.T) {
	value, err := New(uuid.New(), TypeInput, "补充业务参数", nil, []string{"submit", "cancel"})
	require.NoError(t, err)
	require.NoError(t, value.Transition(StatusWaiting))
	require.NoError(t, value.Transition(StatusExpired))

	require.ErrorIs(t, value.Transition(StatusWaiting), ErrInvalidTransition)
	require.ErrorIs(t, value.Resolve(Response{
		Action: "submit", RespondedBy: "user-1", RespondedAt: time.Now(),
	}), ErrInvalidTransition)
}
