package conf

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadAndEnvironmentOverride(t *testing.T) {
	// t.Setenv 会在测试结束后恢复环境，避免污染其他测试与开发终端。
	t.Setenv("SWARMOS_DATABASE_DSN", "postgres://override.example/swarmos")
	path := filepath.Join(t.TempDir(), "config.yaml")
	err := os.WriteFile(path, []byte(`
server:
  name: test-control-plane
  environment: test
  http_addr: 127.0.0.1:8080
  grpc_addr: 127.0.0.1:9090
  shutdown_timeout: 5s
data:
  database_dsn: postgres://file.example/swarmos
runtime:
  heartbeat_timeout: 60s
  lease_ttl: 30s
  reconcile_interval: 5s
  outbox_interval: 1s
`), 0o600)
	require.NoError(t, err)

	cfg, err := Load(path)
	require.NoError(t, err)
	require.Equal(t, "postgres://override.example/swarmos", cfg.Data.DatabaseDSN)
	require.Equal(t, "test-control-plane", cfg.Server.Name)
}

func TestLoadRejectsInvalidDurationOverride(t *testing.T) {
	t.Setenv("SWARMOS_LEASE_TTL", "not-a-duration")
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
server:
  name: test
  http_addr: 127.0.0.1:8080
  grpc_addr: 127.0.0.1:9090
  shutdown_timeout: 5s
data:
  database_dsn: postgres://example/swarmos
runtime:
  heartbeat_timeout: 60s
  lease_ttl: 30s
  reconcile_interval: 5s
  outbox_interval: 1s
`), 0o600))

	_, err := Load(path)
	require.ErrorContains(t, err, "lease_ttl")
}

func TestLoadTemporalEnvironmentAndValidation(t *testing.T) {
	t.Setenv("SWARMOS_TEMPORAL_ENABLED", "true")
	t.Setenv("SWARMOS_TEMPORAL_ADDRESS", "temporal.internal:7233")
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
server:
  name: test
  http_addr: 127.0.0.1:8080
  grpc_addr: 127.0.0.1:9090
  shutdown_timeout: 5s
data:
  database_dsn: postgres://example/swarmos
runtime:
  heartbeat_timeout: 60s
  lease_ttl: 30s
  reconcile_interval: 5s
  outbox_interval: 1s
`), 0o600))
	cfg, err := Load(path)
	require.NoError(t, err)
	require.True(t, cfg.Temporal.Enabled)
	require.Equal(t, "temporal.internal:7233", cfg.Temporal.Address)
	require.Equal(t, "swarmos-runs-v1-5", cfg.Temporal.TaskQueue)
}

func TestLoadRejectsInvalidTemporalBoolean(t *testing.T) {
	t.Setenv("SWARMOS_TEMPORAL_ENABLED", "sometimes")
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
server:
  name: test
  http_addr: 127.0.0.1:8080
  grpc_addr: 127.0.0.1:9090
  shutdown_timeout: 5s
data:
  database_dsn: postgres://example/swarmos
runtime:
  heartbeat_timeout: 60s
  lease_ttl: 30s
  reconcile_interval: 5s
  outbox_interval: 1s
`), 0o600))
	_, err := Load(path)
	require.ErrorContains(t, err, "SWARMOS_TEMPORAL_ENABLED")
}

func TestProductionRequiresStrongAPIKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
server:
  name: test
  environment: production
  http_addr: 127.0.0.1:8080
  grpc_addr: 127.0.0.1:9090
  shutdown_timeout: 5s
data:
  database_dsn: postgres://example/swarmos
runtime:
  heartbeat_timeout: 60s
  lease_ttl: 30s
  reconcile_interval: 5s
  outbox_interval: 1s
`), 0o600))
	_, err := Load(path)
	require.ErrorContains(t, err, "SWARMOS_API_KEY")
	t.Setenv("SWARMOS_API_KEY", "0123456789abcdef0123456789abcdef")
	_, err = Load(path)
	require.NoError(t, err)
}
