package server

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadinessProbeRunsAllDependenciesAndNamesFailures(t *testing.T) {
	t.Parallel()
	called := 0
	probe := NewReadinessProbe(
		DependencyProbe{Name: "postgres", Check: func(context.Context) error { called++; return nil }},
		DependencyProbe{Name: "redis", Check: func(context.Context) error { called++; return errors.New("offline") }},
		DependencyProbe{Name: "nats", Check: func(context.Context) error { called++; return errors.New("no pong") }},
	)
	err := probe(context.Background())
	require.ErrorContains(t, err, "redis: offline")
	require.ErrorContains(t, err, "nats: no pong")
	require.Equal(t, 3, called)
}

func TestReadinessProbePassesWhenEveryDependencyPasses(t *testing.T) {
	t.Parallel()
	probe := NewReadinessProbe(DependencyProbe{Name: "postgres", Check: func(context.Context) error { return nil }})
	require.NoError(t, probe(context.Background()))
}
