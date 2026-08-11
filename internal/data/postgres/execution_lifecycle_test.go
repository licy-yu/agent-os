package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// lifecycleTx 只实现本文件目标测试所需的 QueryRow/Exec。其余方法一旦被意外调用就
// 立即 panic，避免一个过度宽松的假事务掩盖生产事务边界变化。
type lifecycleTx struct {
	queryRow func(string, ...any) pgx.Row
	exec     func(string, ...any) (pgconn.CommandTag, error)
}

func (t *lifecycleTx) Begin(context.Context) (pgx.Tx, error) { panic("unexpected Begin") }
func (t *lifecycleTx) Commit(context.Context) error          { panic("unexpected Commit") }
func (t *lifecycleTx) Rollback(context.Context) error        { panic("unexpected Rollback") }
func (t *lifecycleTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	panic("unexpected CopyFrom")
}
func (t *lifecycleTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults {
	panic("unexpected SendBatch")
}
func (t *lifecycleTx) LargeObjects() pgx.LargeObjects { panic("unexpected LargeObjects") }
func (t *lifecycleTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	panic("unexpected Prepare")
}
func (t *lifecycleTx) Exec(_ context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	if t.exec == nil {
		panic("unexpected Exec")
	}
	return t.exec(sql, arguments...)
}
func (t *lifecycleTx) Query(context.Context, string, ...any) (pgx.Rows, error) {
	panic("unexpected Query")
}
func (t *lifecycleTx) QueryRow(_ context.Context, sql string, arguments ...any) pgx.Row {
	if t.queryRow == nil {
		panic("unexpected QueryRow")
	}
	return t.queryRow(sql, arguments...)
}
func (t *lifecycleTx) Conn() *pgx.Conn { return nil }

type lifecycleRow struct {
	values []any
	err    error
}

func (r lifecycleRow) Scan(destinations ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(destinations) != len(r.values) {
		return fmt.Errorf("scan destinations=%d values=%d", len(destinations), len(r.values))
	}
	for index, value := range r.values {
		target := reflect.ValueOf(destinations[index])
		if target.Kind() != reflect.Pointer || target.IsNil() {
			return fmt.Errorf("scan destination %d 不是非空指针", index)
		}
		target.Elem().Set(reflect.ValueOf(value))
	}
	return nil
}

func TestFinalizeRunCountsOnlyCurrentPlanTasks(t *testing.T) {
	t.Parallel()
	tenantID, runID := uuid.New(), uuid.New()
	tx := &lifecycleTx{queryRow: func(sql string, arguments ...any) pgx.Row {
		require.Contains(t, sql, "run_scope.current_plan_version_id IS NULL")
		require.Contains(t, sql, "t.plan_version_id=run_scope.current_plan_version_id")
		require.Equal(t, []any{tenantID, runID}, arguments)
		// 尚有一个当前计划 Task 未成功，函数应在读取任何 Artifact 前直接返回。
		return lifecycleRow{values: []any{1}}
	}}

	completed, err := finalizeRunIfComplete(context.Background(), tx, tenantID, runID,
		uuid.New(), uuid.New(), "reviewer", time.Now().UTC())
	require.NoError(t, err)
	require.False(t, completed)
}

func TestFailRunForTaskWritesTerminalStateAndOutboxAtomically(t *testing.T) {
	t.Parallel()
	tenantID, runID, taskID, attemptID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	execCalls := 0
	tx := &lifecycleTx{
		queryRow: func(sql string, arguments ...any) pgx.Row {
			require.Contains(t, sql, "SET status='FAILED'")
			require.Contains(t, sql, "'SUCCEEDED','COMPLETED','FAILED','CANCELED','EXPIRED'")
			require.Equal(t, runID, arguments[0])
			require.Equal(t, tenantID, arguments[1])
			return lifecycleRow{values: []any{int64(9)}}
		},
		exec: func(sql string, arguments ...any) (pgconn.CommandTag, error) {
			execCalls++
			require.Contains(t, sql, "INSERT INTO event_outbox")
			require.Equal(t, tenantID, arguments[1])
			require.Equal(t, "swarm", arguments[2])
			require.Equal(t, runID, arguments[3])
			require.Equal(t, "run.failed", arguments[4])
			require.Equal(t, int64(9), arguments[5])
			var payload map[string]any
			require.NoError(t, json.Unmarshal(arguments[6].([]byte), &payload))
			require.Equal(t, "TASK_REJECTED", payload["failure_code"])
			require.Equal(t, taskID.String(), payload["task_id"])
			return pgconn.NewCommandTag("INSERT 0 1"), nil
		},
	}

	changed, err := failRunForTask(context.Background(), tx, tenantID, runID, taskID, attemptID,
		"TASK_REJECTED", " required gate failed ", time.Now().UTC())
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, 1, execCalls)
}

func TestFailRunForTaskDoesNotOverwriteExistingTerminalRun(t *testing.T) {
	t.Parallel()
	tx := &lifecycleTx{queryRow: func(sql string, _ ...any) pgx.Row {
		require.True(t, strings.Contains(sql, "status NOT IN"))
		return lifecycleRow{err: pgx.ErrNoRows}
	}}

	changed, err := failRunForTask(context.Background(), tx, uuid.New(), uuid.New(),
		uuid.New(), uuid.New(), "TASK_REJECTED", "ignored", time.Now().UTC())
	require.NoError(t, err)
	require.False(t, changed)
}
