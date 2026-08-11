package toolgateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/domain/agent"
	"github.com/licy-yu/agent-os/internal/domain/task"
	"github.com/licy-yu/agent-os/internal/effect"
	"github.com/licy-yu/agent-os/internal/execution"
	"github.com/stretchr/testify/require"
)

type memoryToolStore struct {
	definition   *Definition
	records      []CallRecord
	owners       []execution.AttemptOwner
	finished     int
	permit       *EffectPermit
	effectCalls  []EffectRequest
	effectHashes map[string]string
	begunEffects int
	completions  []EffectCompletion
}

func (s *memoryToolStore) PrepareToolEffect(_ context.Context, _ execution.AttemptOwner,
	request EffectRequest,
) (*EffectPermit, error) {
	if s.effectHashes == nil {
		s.effectHashes = map[string]string{}
	}
	if existing, ok := s.effectHashes[request.IdempotencyKey]; ok && existing != request.RequestHash {
		return nil, effect.ErrIdempotencyConflict
	}
	s.effectHashes[request.IdempotencyKey] = request.RequestHash
	s.effectCalls = append(s.effectCalls, request)
	if s.permit != nil {
		return s.permit, nil
	}
	return &EffectPermit{EffectID: uuid.New(), Disposition: EffectExecute, Status: effect.StatusAuthorized}, nil
}
func (s *memoryToolStore) BeginToolEffect(context.Context, execution.AttemptOwner, uuid.UUID, string) error {
	s.begunEffects++
	return nil
}
func (s *memoryToolStore) FinishToolEffect(_ context.Context, _ execution.AttemptOwner,
	_ uuid.UUID, completion EffectCompletion,
) error {
	s.completions = append(s.completions, completion)
	return nil
}

type countingAdapter struct {
	calls int
	err   error
}

func (a *countingAdapter) Call(context.Context, *Definition, map[string]any) (map[string]any, error) {
	a.calls++
	if a.err != nil {
		return nil, a.err
	}
	return map[string]any{"external_ref": "change-1"}, nil
}

func (s *memoryToolStore) GetToolDefinition(context.Context, string) (*Definition, error) {
	return s.definition, nil
}
func (s *memoryToolStore) BeginToolCall(_ context.Context, owner execution.AttemptOwner, value CallRecord, _ int32) error {
	s.owners = append(s.owners, owner)
	s.records = append(s.records, value)
	return nil
}
func (s *memoryToolStore) FinishToolCall(_ context.Context, owner execution.AttemptOwner, _ uuid.UUID,
	_ string, _ map[string]any, _ string,
) error {
	s.owners = append(s.owners, owner)
	s.finished++
	return nil
}

func gatewayWork(tools, permissions []string, risk string) *execution.Work {
	return &execution.Work{
		Task:     &task.Task{ID: uuid.New(), ExecutionPolicy: task.DefaultExecutionPolicy()},
		Agent:    &agent.Instance{ID: uuid.New()},
		Attempt:  &execution.Attempt{ID: uuid.New(), WorkerID: "worker-1", FencingToken: 7},
		Template: &agent.Template{Tools: tools, Permissions: permissions, RiskZone: risk},
	}
}

func TestGatewayAllowsAndAuditsEcho(t *testing.T) {
	store := &memoryToolStore{definition: &Definition{Name: "echo", Adapter: "native", RiskLevel: "sandbox", Enabled: true}}
	gateway := New(store, gatewayWork([]string{"echo"}, nil, "sandbox"), map[string]Adapter{
		"native:echo": EchoAdapter{},
	})
	result, err := gateway.Call(context.Background(), "echo", map[string]any{"value": "ok"})
	require.NoError(t, err)
	require.Equal(t, "ok", result["echo"].(map[string]any)["value"])
	require.Len(t, store.records, 1)
	require.Equal(t, 1, store.finished)
	require.Len(t, store.owners, 2)
	require.Equal(t, int64(7), store.owners[0].FencingToken)
	require.Equal(t, "worker-1", store.owners[1].WorkerID)
}

func TestGatewayDeniesMissingPermissionAndStillAudits(t *testing.T) {
	store := &memoryToolStore{definition: &Definition{
		Name: "deploy", Adapter: "native", RiskLevel: "production", Enabled: true,
		RequiredPermissions: []string{"deploy:production"},
	}}
	gateway := New(store, gatewayWork([]string{"deploy"}, nil, "production"), map[string]Adapter{})
	_, err := gateway.Call(context.Background(), "deploy", map[string]any{})
	require.ErrorIs(t, err, ErrDenied)
	require.Len(t, store.records, 1)
	require.Equal(t, "DENIED", store.records[0].Status)
}

func TestGatewayFailsClosedForUnknownRiskZone(t *testing.T) {
	store := &memoryToolStore{definition: &Definition{
		Name: "echo", Adapter: "native", RiskLevel: "read-only", Enabled: true,
	}}
	gateway := New(store, gatewayWork([]string{"echo"}, nil, "unknown-zone"), map[string]Adapter{
		"native:echo": EchoAdapter{},
	})
	_, err := gateway.Call(context.Background(), "echo", map[string]any{})
	require.ErrorIs(t, err, ErrDenied)
	require.Equal(t, "DENIED", store.records[0].Status)
}

func TestR3ApprovalPendingNeverCallsAdapter(t *testing.T) {
	interactionID := uuid.New()
	store := &memoryToolStore{
		definition: &Definition{Name: "deploy", Adapter: "native", RiskLevel: "production", Enabled: true},
		permit: &EffectPermit{EffectID: uuid.New(), InteractionID: &interactionID,
			Disposition: EffectApprovalPending, Status: effect.StatusPrepared},
	}
	adapter := new(countingAdapter)
	gateway := New(store, gatewayWork([]string{"deploy"}, nil, "production"), map[string]Adapter{
		"native:deploy": adapter,
	})
	_, err := gateway.Call(context.Background(), "deploy", map[string]any{
		"idempotency_key": "run-1/task-1/deploy", "target": "production",
	})
	require.ErrorIs(t, err, ErrApprovalPending)
	require.Zero(t, adapter.calls)
	require.Len(t, store.effectCalls, 1)
	require.Equal(t, effect.RiskR3ProductionDestructive, store.effectCalls[0].RiskLevel)
}

func TestUnknownEffectIsReconcileOnlyAndNeverCallsAdapter(t *testing.T) {
	store := &memoryToolStore{
		definition: &Definition{Name: "create-pr", Adapter: "native", RiskLevel: "trusted", Enabled: true},
		permit:     &EffectPermit{EffectID: uuid.New(), Disposition: EffectReconcileOnly, Status: effect.StatusUnknown},
	}
	adapter := new(countingAdapter)
	gateway := New(store, gatewayWork([]string{"create-pr"}, nil, "trusted"), map[string]Adapter{
		"native:create-pr": adapter,
	})
	_, err := gateway.Call(context.Background(), "create-pr", map[string]any{
		"idempotency_key": "run-1/task-1/create-pr", "branch": "main",
	})
	require.ErrorIs(t, err, ErrEffectReconcileRequired)
	require.Zero(t, adapter.calls)
	require.Zero(t, store.begunEffects)
}

func TestSameEffectKeyWithDifferentArgumentsConflicts(t *testing.T) {
	store := &memoryToolStore{
		definition: &Definition{Name: "deploy", Adapter: "native", RiskLevel: "production", Enabled: true},
		permit:     &EffectPermit{EffectID: uuid.New(), Disposition: EffectApprovalPending, Status: effect.StatusPrepared},
	}
	gateway := New(store, gatewayWork([]string{"deploy"}, nil, "production"), map[string]Adapter{
		"native:deploy": new(countingAdapter),
	})
	_, firstErr := gateway.Call(context.Background(), "deploy", map[string]any{
		"idempotency_key": "stable-key", "target": "blue",
	})
	require.ErrorIs(t, firstErr, ErrApprovalPending)
	_, secondErr := gateway.Call(context.Background(), "deploy", map[string]any{
		"idempotency_key": "stable-key", "target": "green",
	})
	require.True(t, errors.Is(secondErr, effect.ErrIdempotencyConflict))
}

func TestR2AuthorizedEffectPersistsExecutionBoundary(t *testing.T) {
	store := &memoryToolStore{
		definition: &Definition{Name: "create-pr", Adapter: "native", RiskLevel: "trusted", Enabled: true},
		permit:     &EffectPermit{EffectID: uuid.New(), Disposition: EffectExecute, Status: effect.StatusAuthorized},
	}
	adapter := new(countingAdapter)
	gateway := New(store, gatewayWork([]string{"create-pr"}, nil, "trusted"), map[string]Adapter{
		"native:create-pr": adapter,
	})
	result, err := gateway.Call(context.Background(), "create-pr", map[string]any{
		"idempotency_key": "run-1/task-1/create-pr", "branch": "feature",
	})
	require.NoError(t, err)
	require.Equal(t, "change-1", result["external_ref"])
	require.Equal(t, 1, adapter.calls)
	require.Equal(t, 1, store.begunEffects)
	require.Len(t, store.completions, 1)
	require.Equal(t, effect.StatusSucceeded, store.completions[0].Status)
}

func TestAdapterErrorPersistsUnknownAndStopsDirectRetry(t *testing.T) {
	store := &memoryToolStore{
		definition: &Definition{Name: "deploy", Adapter: "native", RiskLevel: "production", Enabled: true},
		permit:     &EffectPermit{EffectID: uuid.New(), Disposition: EffectExecute, Status: effect.StatusAuthorized},
	}
	adapter := &countingAdapter{err: errors.New("connection reset after request write")}
	gateway := New(store, gatewayWork([]string{"deploy"}, nil, "production"), map[string]Adapter{
		"native:deploy": adapter,
	})
	_, err := gateway.Call(context.Background(), "deploy", map[string]any{
		"idempotency_key": "run-1/task-1/deploy", "target": "production",
	})
	require.ErrorIs(t, err, ErrEffectReconcileRequired)
	require.Equal(t, 1, adapter.calls)
	require.Len(t, store.completions, 1)
	require.Equal(t, effect.StatusUnknown, store.completions[0].Status)
}

func TestMCPAdapterUsesCurrentStatelessHeaders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		require.Equal(t, "2026-07-28", request.Header.Get("MCP-Protocol-Version"))
		require.Equal(t, "tools/call", request.Header.Get("Mcp-Method"))
		require.Equal(t, "weather", request.Header.Get("Mcp-Name"))
		var body map[string]any
		require.NoError(t, json.NewDecoder(request.Body).Decode(&body))
		require.Equal(t, "tools/call", body["method"])
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"jsonrpc":"2.0","id":"1","result":{"content":[],"structuredContent":{"temperature":22}}}`))
	}))
	defer server.Close()

	adapter := NewMCPAdapter(server.Client())
	result, err := adapter.Call(context.Background(), &Definition{
		Name: "weather", Config: map[string]any{"endpoint": server.URL},
	}, map[string]any{"city": "Shanghai"})
	require.NoError(t, err)
	require.Equal(t, float64(22), result["temperature"])
}
