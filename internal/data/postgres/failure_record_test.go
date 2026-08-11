package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/licy-yu/agent-os/internal/domain"
	"github.com/licy-yu/agent-os/internal/failure"
	"github.com/stretchr/testify/require"
)

type failureRecordTxStub struct {
	exec     func(string, ...any) (pgconn.CommandTag, error)
	queryRow func(string, ...any) pgx.Row
}

func (s failureRecordTxStub) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return s.exec(sql, args...)
}

func (s failureRecordTxStub) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	return s.queryRow(sql, args...)
}

type failureRecordRow struct {
	class, level, code, message, action string
	details                             []byte
	err                                 error
}

func (r failureRecordRow) Scan(destinations ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(destinations) != 6 {
		return fmt.Errorf("unexpected destination count: %d", len(destinations))
	}
	*destinations[0].(*string) = r.class
	*destinations[1].(*string) = r.level
	*destinations[2].(*string) = r.code
	*destinations[3].(*string) = r.message
	*destinations[4].(*[]byte) = r.details
	*destinations[5].(*string) = r.action
	return nil
}

func TestParseRuntimeFailureAcceptsClassifierLevels(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		level failure.RetryLevel
	}{
		{name: "attempt", level: failure.RetryAttempt},
		{name: "agent switch", level: failure.RetryAgent},
		{name: "model switch", level: failure.RetryModel},
		{name: "task replan", level: failure.RetryTask},
		{name: "run replan", level: failure.RetryRun},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			record, err := parseRuntimeFailure(map[string]any{
				"execution_error": "HTTP 429",
				"failure_class":   string(failure.RateLimit),
				"recovery_action": string(failure.ActionProviderFallback),
				"retry_level":     string(test.level),
				"retryable":       true,
			})
			require.NoError(t, err)
			require.Equal(t, test.level, record.RetryLevel)
			require.True(t, record.Retryable)
		})
	}
}

func TestParseRuntimeFailureSkipsNormalOutputAndRejectsPartialEnvelope(t *testing.T) {
	t.Parallel()
	record, err := parseRuntimeFailure(map[string]any{"output": "正常业务结果"})
	require.NoError(t, err)
	require.Nil(t, record)

	_, err = parseRuntimeFailure(map[string]any{
		"failure_class": string(failure.RateLimit),
		"retryable":     true,
	})
	require.ErrorContains(t, err, "execution_error 缺失")
}

func TestPersistRuntimeFailureRecordIsIdempotent(t *testing.T) {
	t.Parallel()
	tenantID, runID, taskID, attemptID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	output := map[string]any{
		"execution_error": "provider temporarily unavailable",
		"failure_class":   string(failure.ModelProviderError),
		"recovery_action": string(failure.ActionProviderFallback),
		"retry_level":     string(failure.RetryModel),
		"retryable":       true,
	}

	inserted := failureRecordTxStub{
		exec: func(sql string, args ...any) (pgconn.CommandTag, error) {
			require.Contains(t, sql, "ON CONFLICT(id) DO NOTHING")
			require.Equal(t, stableExecutionID("runtime-failure-record", attemptID.String()), args[0])
			require.Equal(t, failure.ModelProviderError, args[5])
			require.Equal(t, failure.RetryModel, args[6])
			require.Equal(t, runtimeFailureCode, args[7])
			require.Equal(t, failure.ActionProviderFallback, args[10])
			var details map[string]any
			require.NoError(t, json.Unmarshal(args[9].([]byte), &details))
			require.Equal(t, map[string]any{"retryable": true, "source": "worker.runtime"}, details)
			return pgconn.NewCommandTag("INSERT 0 1"), nil
		},
		queryRow: func(string, ...any) pgx.Row {
			panic("新记录不应进入重放查询")
		},
	}
	require.NoError(t, persistRuntimeFailureRecord(context.Background(), inserted,
		tenantID, runID, taskID, attemptID, output))

	// 模拟相同 Attempt 的数据库事务/消息重放：稳定 ID 已存在且载荷完全相同，返回成功。
	replayed := failureRecordTxStub{
		exec: func(string, ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("INSERT 0 0"), nil
		},
		queryRow: func(sql string, args ...any) pgx.Row {
			require.Contains(t, sql, "WHERE id=$1")
			require.Equal(t, tenantID, args[1])
			return failureRecordRow{
				class: string(failure.ModelProviderError), level: string(failure.RetryModel),
				code: runtimeFailureCode, message: "provider temporarily unavailable",
				details: []byte(`{"source":"worker.runtime","retryable":true}`),
				action:  string(failure.ActionProviderFallback),
			}
		},
	}
	require.NoError(t, persistRuntimeFailureRecord(context.Background(), replayed,
		tenantID, runID, taskID, attemptID, output))
}

func TestPersistRuntimeFailureRecordRejectsDifferentReplay(t *testing.T) {
	t.Parallel()
	tenantID, runID, taskID, attemptID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	tx := failureRecordTxStub{
		exec: func(string, ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("INSERT 0 0"), nil
		},
		queryRow: func(string, ...any) pgx.Row {
			return failureRecordRow{
				class: string(failure.RateLimit), level: string(failure.RetryModel),
				code: runtimeFailureCode, message: "另一个失败",
				details: []byte(`{"source":"worker.runtime","retryable":true}`),
				action:  string(failure.ActionProviderFallback),
			}
		},
	}
	err := persistRuntimeFailureRecord(context.Background(), tx, tenantID, runID, taskID, attemptID,
		map[string]any{
			"execution_error": "当前失败", "failure_class": string(failure.RateLimit),
			"recovery_action": string(failure.ActionProviderFallback),
			"retry_level":     string(failure.RetryModel), "retryable": true,
		})
	require.ErrorIs(t, err, domain.ErrConflict)
}
