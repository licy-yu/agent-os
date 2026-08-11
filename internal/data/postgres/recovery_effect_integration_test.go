package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/licy-yu/agent-os/internal/domain/task"
	"github.com/licy-yu/agent-os/internal/effect"
	"github.com/stretchr/testify/require"
)

// TestRecoverTimedOutEffectCommitWindowPostgres 是显式启用的 PostgreSQL 故障窗口合同测试。
// 它模拟 Effect 事务已经提交，而 Worker 还没来得及 SuspendAttempt 就崩溃的场景，确保
// Recovery 不会创建第二个 Attempt 或重复调用外部系统。测试只允许指向可丢弃数据库。
func TestRecoverTimedOutEffectCommitWindowPostgres(t *testing.T) {
	dsn := os.Getenv("SWARMOS_INTEGRATION_DATABASE_URL")
	if dsn == "" {
		t.Skip("未设置 SWARMOS_INTEGRATION_DATABASE_URL，跳过 Effect Recovery PostgreSQL 合同测试")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	repository, err := New(ctx, dsn)
	require.NoError(t, err)
	defer repository.Close()
	if os.Getenv("SWARMOS_INTEGRATION_MIGRATE") == "1" {
		require.NoError(t, repository.Migrate(ctx))
	}

	tests := []struct {
		name            string
		effectStatus    effect.Status
		waitingApproval bool
		blocked         bool
		attemptCount    int32
		wantAttempt     string
		wantTask        string
		wantRun         string
		wantAgent       string
		wantEffect      effect.Status
		wantTaskEvent   string
		wantEffectEvent string
	}{
		{
			name: "prepared approval becomes durable waiting", effectStatus: effect.StatusPrepared,
			waitingApproval: true, attemptCount: 1, wantAttempt: "WAITING",
			wantTask: "WAITING_APPROVAL", wantRun: "RUNNING", wantAgent: "WAITING_TOOL",
			wantEffect: effect.StatusPrepared, wantTaskEvent: "task.waiting_approval",
		},
		{
			name: "authorized is immediately requeued on same attempt", effectStatus: effect.StatusAuthorized,
			attemptCount: 1, wantAttempt: "WAITING", wantTask: "ASSIGNED", wantRun: "RUNNING",
			wantAgent: "RESERVED", wantEffect: effect.StatusAuthorized, wantTaskEvent: "task.assigned",
		},
		{
			name: "executing first becomes unknown", effectStatus: effect.StatusExecuting,
			attemptCount: 1, wantAttempt: "WAITING", wantTask: "WAITING_EXTERNAL", wantRun: "RUNNING",
			wantAgent: "WAITING_TOOL", wantEffect: effect.StatusUnknown,
			wantTaskEvent: "task.waiting_external", wantEffectEvent: "effect.unknown",
		},
		{
			name: "unknown stays externally waiting", effectStatus: effect.StatusUnknown,
			attemptCount: 1, wantAttempt: "WAITING", wantTask: "WAITING_EXTERNAL", wantRun: "RUNNING",
			wantAgent: "WAITING_TOOL", wantEffect: effect.StatusUnknown,
			wantTaskEvent: "task.waiting_external",
		},
		{
			name: "reconciling stays externally waiting", effectStatus: effect.StatusReconciling,
			attemptCount: 1, wantAttempt: "WAITING", wantTask: "WAITING_EXTERNAL", wantRun: "RUNNING",
			wantAgent: "WAITING_TOOL", wantEffect: effect.StatusReconciling,
			wantTaskEvent: "task.waiting_external",
		},
		{
			name: "succeeded is immediately requeued on same attempt", effectStatus: effect.StatusSucceeded,
			attemptCount: 1, wantAttempt: "WAITING", wantTask: "ASSIGNED", wantRun: "RUNNING",
			wantAgent: "RESERVED", wantEffect: effect.StatusSucceeded, wantTaskEvent: "task.assigned",
		},
		{
			name: "failed closes task and run", effectStatus: effect.StatusFailed,
			attemptCount: 1, wantAttempt: "ABORTED", wantTask: "FAILED", wantRun: "FAILED",
			wantAgent: "IDLE", wantEffect: effect.StatusFailed, wantTaskEvent: "task.failed",
		},
		{
			name: "blocked approval closes task and run", effectStatus: effect.StatusPrepared, blocked: true,
			attemptCount: 1, wantAttempt: "ABORTED", wantTask: "FAILED", wantRun: "FAILED",
			wantAgent: "IDLE", wantEffect: effect.StatusPrepared, wantTaskEvent: "task.failed",
		},
		{
			name: "ordinary timeout without effect still retries", attemptCount: 1,
			wantAttempt: "ABORTED", wantTask: "RETRY_WAIT", wantRun: "RUNNING",
			wantAgent: "OFFLINE", wantTaskEvent: "task.retry_wait",
		},
		{
			name: "ordinary timeout at max attempts still fails run", attemptCount: 3,
			wantAttempt: "ABORTED", wantTask: "FAILED", wantRun: "FAILED",
			wantAgent: "OFFLINE", wantTaskEvent: "task.failed",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			fixture := seedTimedOutRecoveryFixture(t, ctx, repository, test.name,
				test.effectStatus, test.waitingApproval, test.blocked, test.attemptCount)
			t.Cleanup(func() { cleanupTimedOutRecoveryFixture(t, repository, fixture) })

			recovered, err := repository.RecoverTimedOut(ctx, time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), 1)
			require.NoError(t, err)
			require.Equal(t, 1, recovered)

			var attemptStatus, taskStatus, runStatus, agentStatus string
			require.NoError(t, repository.pool.QueryRow(ctx, `
				SELECT a.status,t.status,r.status,i.status
				FROM task_attempts a
				JOIN tasks t ON t.id=a.task_id
				JOIN swarms r ON r.id=t.swarm_id
				JOIN agent_instances i ON i.id=a.agent_id
				WHERE a.id=$1`, fixture.attemptID).
				Scan(&attemptStatus, &taskStatus, &runStatus, &agentStatus))
			require.Equal(t, test.wantAttempt, attemptStatus)
			require.Equal(t, test.wantTask, taskStatus)
			require.Equal(t, test.wantRun, runStatus)
			require.Equal(t, test.wantAgent, agentStatus)

			var attemptRows int
			require.NoError(t, repository.pool.QueryRow(ctx,
				`SELECT count(*) FROM task_attempts WHERE task_id=$1`, fixture.taskID).Scan(&attemptRows))
			require.Equal(t, 1, attemptRows, "恢复不得创建第二个 Attempt")

			if test.effectStatus != "" {
				var persistedEffect effect.Status
				require.NoError(t, repository.pool.QueryRow(ctx,
					`SELECT status FROM effects WHERE tenant_id=$1 AND id=$2`, fixture.tenantID,
					fixture.effectID).Scan(&persistedEffect))
				require.Equal(t, test.wantEffect, persistedEffect)
				var ordinaryRetries int
				require.NoError(t, repository.pool.QueryRow(ctx, `
					SELECT count(*) FROM event_outbox
					WHERE tenant_id=$1 AND aggregate_type='task' AND aggregate_id=$2
					  AND event_type='task.retry_wait'`, fixture.tenantID, fixture.taskID).
					Scan(&ordinaryRetries))
				require.Zero(t, ordinaryRetries, "有关联 Effect 时禁止走普通重试")
			}
			assertTimedOutRecoveryEvent(t, ctx, repository, fixture, "task", fixture.taskID,
				test.wantTaskEvent)
			if test.wantEffectEvent != "" {
				assertTimedOutRecoveryEvent(t, ctx, repository, fixture, "effect", fixture.effectID,
					test.wantEffectEvent)
			}
		})
	}

	t.Run("prepare holding attempt lock cannot trigger no-effect abort", func(t *testing.T) {
		fixture := seedTimedOutRecoveryFixture(t, ctx, repository, "prepare-toctou", "", false, false, 1)
		t.Cleanup(func() { cleanupTimedOutRecoveryFixture(t, repository, fixture) })

		prepareTx, err := repository.pool.Begin(ctx)
		require.NoError(t, err)
		defer func() { _ = prepareTx.Rollback(context.Background()) }()
		var lockedID uuid.UUID
		require.NoError(t, prepareTx.QueryRow(ctx,
			`SELECT id FROM task_attempts WHERE id=$1 FOR UPDATE`, fixture.attemptID).Scan(&lockedID))
		require.Equal(t, fixture.attemptID, lockedID)
		require.NoError(t, insertRecoveryFixtureEffect(ctx, prepareTx, fixture,
			effect.StatusAuthorized, false))

		type recoveryResult struct {
			count int
			err   error
		}
		resultCh := make(chan recoveryResult, 1)
		go func() {
			count, recoverErr := repository.RecoverTimedOut(ctx,
				time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), 1)
			resultCh <- recoveryResult{count: count, err: recoverErr}
		}()
		select {
		case result := <-resultCh:
			require.NoError(t, result.err)
			require.Zero(t, result.count, "Prepare 持锁期间本轮必须跳过，而不是使用过期的空 Effect 结果")
		case <-time.After(3 * time.Second):
			t.Fatal("Recovery 阻塞等待 Prepare Attempt 锁，存在空 Effect TOCTOU")
		}
		require.NoError(t, prepareTx.Commit(ctx))

		recovered, err := repository.RecoverTimedOut(ctx,
			time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), 1)
		require.NoError(t, err)
		require.Equal(t, 1, recovered)
		var attemptStatus, taskStatus string
		require.NoError(t, repository.pool.QueryRow(ctx, `
			SELECT a.status,t.status FROM task_attempts a JOIN tasks t ON t.id=a.task_id
			WHERE a.id=$1`, fixture.attemptID).Scan(&attemptStatus, &taskStatus))
		require.Equal(t, "WAITING", attemptStatus)
		require.Equal(t, "ASSIGNED", taskStatus)
		var attemptRows int
		require.NoError(t, repository.pool.QueryRow(ctx,
			`SELECT count(*) FROM task_attempts WHERE task_id=$1`, fixture.taskID).Scan(&attemptRows))
		require.Equal(t, 1, attemptRows)
	})
}

type timedOutRecoveryFixture struct {
	tenantID, runID, templateID, agentID uuid.UUID
	taskID, attemptID, effectID          uuid.UUID
	interactionID                        uuid.UUID
	keyPrefix                            string
}

func seedTimedOutRecoveryFixture(t *testing.T, ctx context.Context, repository *Repository,
	label string, effectStatus effect.Status, waitingApproval, blocked bool, attemptCount int32,
) timedOutRecoveryFixture {
	t.Helper()
	fixture := timedOutRecoveryFixture{
		tenantID: uuid.New(), runID: uuid.New(), templateID: uuid.New(), agentID: uuid.New(),
		taskID: uuid.New(), attemptID: uuid.New(), effectID: uuid.New(), interactionID: uuid.New(),
		keyPrefix: "recovery-window-" + uuid.NewString(),
	}
	policyRaw, err := json.Marshal(task.ExecutionPolicy{MaxAttempts: 3})
	require.NoError(t, err)
	err = repository.withTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO tenants(id,slug,name) VALUES($1,$2,$3)`,
			fixture.tenantID, fixture.keyPrefix, "Recovery "+label); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO swarms(id,tenant_id,name,goal,status,desired_state,execution_engine)
			VALUES($1,$2,$3,'effect recovery failure window','RUNNING','RUNNING','LEGACY')`,
			fixture.runID, fixture.tenantID, fixture.keyPrefix); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO agent_templates(id,tenant_id,name,role,prompt,model,template_version)
			VALUES($1,$2,$3,'recovery','recovery','mock/recovery',$4)`,
			fixture.templateID, fixture.tenantID, fixture.keyPrefix, fixture.keyPrefix); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO agent_instances(id,tenant_id,template_id,swarm_id,name,status)
			VALUES($1,$2,$3,$4,$5,'IDLE')`, fixture.agentID, fixture.tenantID,
			fixture.templateID, fixture.runID, fixture.keyPrefix); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO tasks(
				id,tenant_id,swarm_id,name,goal,status,assigned_agent_id,attempt_count,execution_policy
			) VALUES($1,$2,$3,$4,'effect recovery task','RUNNING',$5,$6,$7)`,
			fixture.taskID, fixture.tenantID, fixture.runID, fixture.keyPrefix, fixture.agentID,
			attemptCount, policyRaw); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE agent_instances SET status='RUNNING',current_task_id=$3
			WHERE tenant_id=$1 AND id=$2`, fixture.tenantID, fixture.agentID, fixture.taskID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO task_attempts(
				id,tenant_id,task_id,agent_id,attempt_no,status,worker_id,fencing_token,
				started_at,heartbeat_at
			) VALUES($1,$2,$3,$4,$5,'RUNNING','crashed-worker',1,$6,$6)`,
			fixture.attemptID, fixture.tenantID, fixture.taskID, fixture.agentID, attemptCount,
			time.Date(1990, 1, 1, 0, 0, 0, 0, time.UTC)); err != nil {
			return err
		}
		if effectStatus == "" {
			return nil
		}
		if err := insertRecoveryFixtureEffect(ctx, tx, fixture, effectStatus, blocked); err != nil {
			return err
		}
		if !waitingApproval {
			return nil
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO interactions(
				id,tenant_id,run_id,task_id,attempt_id,effect_id,interaction_type,status,
				title,allowed_actions,idempotency_key,requested_by
			) VALUES($1,$2,$3,$4,$5,$6,'APPROVAL','WAITING','recovery approval',
			         '["approve","reject"]'::jsonb,$7,'recovery-test')`, fixture.interactionID,
			fixture.tenantID, fixture.runID, fixture.taskID, fixture.attemptID, fixture.effectID,
			fixture.keyPrefix+"-interaction"); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
			UPDATE effects SET approval_interaction_id=$2,version=version+1 WHERE id=$1`,
			fixture.effectID, fixture.interactionID)
		return err
	})
	require.NoError(t, err)
	return fixture
}

func insertRecoveryFixtureEffect(ctx context.Context, tx pgx.Tx, fixture timedOutRecoveryFixture,
	status effect.Status, blocked bool,
) error {
	result := map[string]any{}
	if blocked {
		result["_authorization"] = map[string]any{
			"blocked": true, "reason": "production change rejected",
		}
	}
	resultRaw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	errorMessage := ""
	if status == effect.StatusFailed {
		errorMessage = "external operation failed"
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO effects(
			id,tenant_id,run_id,task_id,attempt_id,idempotency_key,effect_type,risk_level,
			status,request_hash,sanitized_request,sanitized_result,error_message,fencing_token
		) VALUES($1,$2,$3,$4,$5,$6,'integration.recovery','R3_PRODUCTION_DESTRUCTIVE',
		         $7,$8,'{}'::jsonb,$9,NULLIF($10,''),1)`, fixture.effectID, fixture.tenantID,
		fixture.runID, fixture.taskID, fixture.attemptID, fixture.keyPrefix+"-effect", status,
		"sha256:"+fixture.effectID.String(), resultRaw, errorMessage)
	return err
}

func assertTimedOutRecoveryEvent(t *testing.T, ctx context.Context, repository *Repository,
	fixture timedOutRecoveryFixture, aggregateType string, aggregateID uuid.UUID, eventType string,
) {
	t.Helper()
	var count int
	require.NoError(t, repository.pool.QueryRow(ctx, `
		SELECT count(*) FROM event_outbox
		WHERE tenant_id=$1 AND aggregate_type=$2 AND aggregate_id=$3 AND event_type=$4`,
		fixture.tenantID, aggregateType, aggregateID, eventType).Scan(&count))
	require.Equal(t, 1, count)
}

func cleanupTimedOutRecoveryFixture(t *testing.T, repository *Repository,
	fixture timedOutRecoveryFixture,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _ = repository.pool.Exec(ctx, `
		DELETE FROM event_outbox
		WHERE tenant_id=$1 AND (
			payload->>'run_id'=$2 OR aggregate_id IN ($3,$4,$5,$6,$7)
		)`, fixture.tenantID, fixture.runID.String(), fixture.runID, fixture.taskID,
		fixture.attemptID, fixture.agentID, fixture.effectID)
	_, _ = repository.pool.Exec(ctx, `DELETE FROM swarms WHERE tenant_id=$1 AND id=$2`,
		fixture.tenantID, fixture.runID)
	_, err := repository.pool.Exec(ctx, `DELETE FROM agent_instances WHERE tenant_id=$1 AND id=$2`,
		fixture.tenantID, fixture.agentID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		t.Logf("cleanup recovery agent: %v", err)
	}
	_, _ = repository.pool.Exec(ctx, `DELETE FROM agent_templates WHERE tenant_id=$1 AND id=$2`,
		fixture.tenantID, fixture.templateID)
	_, _ = repository.pool.Exec(ctx, `DELETE FROM tenants WHERE id=$1`, fixture.tenantID)
}
