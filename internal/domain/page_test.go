package domain

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestCursorRoundTrip(t *testing.T) {
	t.Parallel()
	want := Cursor{CreatedAt: time.Date(2026, 8, 11, 10, 30, 0, 123, time.UTC), ID: uuid.New()}
	token, err := EncodeCursor(want)
	require.NoError(t, err)
	got, err := DecodeCursor(token)
	require.NoError(t, err)
	require.Equal(t, want, *got)
}

func TestDecodeCursorRejectsInvalidToken(t *testing.T) {
	t.Parallel()
	_, err := DecodeCursor("这不是合法-token")
	require.Error(t, err)
}

func TestNormalizePageSize(t *testing.T) {
	t.Parallel()
	require.Equal(t, DefaultPageSize, NormalizePageSize(0))
	require.Equal(t, 20, NormalizePageSize(20))
	require.Equal(t, MaxPageSize, NormalizePageSize(10_000))
}
