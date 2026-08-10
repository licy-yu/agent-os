package toolgateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/domain/agent"
	"github.com/licy-yu/agent-os/internal/domain/task"
	"github.com/licy-yu/agent-os/internal/execution"
	"github.com/stretchr/testify/require"
)

type memoryToolStore struct {
	definition *Definition
	records    []CallRecord
	finished   int
}

func (s *memoryToolStore) GetToolDefinition(context.Context, string) (*Definition, error) {
	return s.definition, nil
}
func (s *memoryToolStore) BeginToolCall(_ context.Context, value CallRecord, _ int32) error {
	s.records = append(s.records, value)
	return nil
}
func (s *memoryToolStore) FinishToolCall(context.Context, uuid.UUID, string, map[string]any, string) error {
	s.finished++
	return nil
}

func gatewayWork(tools, permissions []string, risk string) *execution.Work {
	return &execution.Work{
		Task:  &task.Task{ID: uuid.New(), ExecutionPolicy: task.DefaultExecutionPolicy()},
		Agent: &agent.Instance{ID: uuid.New()}, Attempt: &execution.Attempt{ID: uuid.New()},
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
