package postgres

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/licy-yu/agent-os/internal/domain"
	"github.com/licy-yu/agent-os/internal/effect"
	"github.com/licy-yu/agent-os/internal/execution"
	"github.com/licy-yu/agent-os/internal/runcontrol"
	"github.com/licy-yu/agent-os/internal/toolgateway"
	"github.com/stretchr/testify/require"
)

// TestToolEffectFinishPreservesAuthorizationPostgres 覆盖 PostgreSQL jsonb 的真实合并语义。
// 测试默认跳过，只能显式指向可丢弃数据库，避免开发机误写生产数据。
func TestToolEffectFinishPreservesAuthorizationPostgres(t *testing.T) {
	dsn := os.Getenv("SWARMOS_INTEGRATION_DATABASE_URL")
	if dsn == "" {
		t.Skip("未设置 SWARMOS_INTEGRATION_DATABASE_URL，跳过 PostgreSQL Effect 合同测试")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	repository, err := New(ctx, dsn)
	require.NoError(t, err)
	defer repository.Close()
	if os.Getenv("SWARMOS_INTEGRATION_MIGRATE") == "1" {
		require.NoError(t, repository.Migrate(ctx))
	}

	tenantID := runcontrol.DefaultTenantID
	runID, templateID, agentID := uuid.New(), uuid.New(), uuid.New()
	taskID, attemptID, effectID := uuid.New(), uuid.New(), uuid.New()
	workerID := "effect-finish-integration"
	defer cleanupToolEffectIntegrationData(t, repository, tenantID, runID, templateID, agentID, effectID)
	require.NoError(t, seedToolEffectIntegrationData(ctx, repository, tenantID, runID,
		templateID, agentID, taskID, attemptID, effectID, workerID))

	owner := execution.AttemptOwner{AttemptID: attemptID, WorkerID: workerID, FencingToken: 7}
	require.NoError(t, repository.BeginToolEffect(ctx, owner, effectID, "hash:effect-finish"))
	finishedAt := time.Now().UTC().Truncate(time.Microsecond)
	completion := toolgateway.EffectCompletion{
		Status: effect.StatusSucceeded,
		Result: map[string]any{
			"deployment_id": "deploy-17", "api_token": "must-be-redacted",
		},
		ExternalRef: "deploy-17", FinishedAt: finishedAt,
	}
	require.NoError(t, repository.FinishToolEffect(ctx, owner, effectID, completion))

	var raw []byte
	require.NoError(t, repository.pool.QueryRow(ctx,
		`SELECT sanitized_result FROM effects WHERE tenant_id=$1 AND id=$2`, tenantID, effectID).Scan(&raw))
	var persisted map[string]any
	require.NoError(t, json.Unmarshal(raw, &persisted))
	authorization := persisted["_authorization"].(map[string]any)
	require.Equal(t, "integration-reviewer", authorization["approved_by"])
	result := persisted["result"].(map[string]any)
	require.Equal(t, "deploy-17", result["deployment_id"])
	require.Equal(t, "[REDACTED]", result["api_token"])

	// 相同完成命令在终态上安全回放，且不会产生第二条 effect.succeeded 事件。
	require.NoError(t, repository.FinishToolEffect(ctx, owner, effectID, completion))
	var succeededEvents int
	require.NoError(t, repository.pool.QueryRow(ctx, `
		SELECT count(*) FROM event_outbox
		WHERE tenant_id=$1 AND aggregate_type='effect' AND aggregate_id=$2
		  AND event_type='effect.succeeded'`, tenantID, effectID).Scan(&succeededEvents))
	require.Equal(t, 1, succeededEvents)

	different := completion
	different.ExternalRef = "deploy-18"
	err = repository.FinishToolEffect(ctx, owner, effectID, different)
	require.ErrorIs(t, err, domain.ErrConflict)
}

func seedToolEffectIntegrationData(ctx context.Context, repository *Repository, tenantID, runID,
	templateID, agentID, taskID, attemptID, effectID uuid.UUID, workerID string,
) error {
	return repository.withTx(ctx, func(tx pgx.Tx) error {
		name := "effect-finish-it-" + runID.String()
		if _, err := tx.Exec(ctx, `
			INSERT INTO swarms(id,tenant_id,name,goal,status,desired_state,execution_engine)
			VALUES($1,$2,$3,'effect finish integration','RUNNING','RUNNING','LEGACY')`,
			runID, tenantID, name); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO agent_templates(id,tenant_id,name,role,prompt,model,template_version)
			VALUES($1,$2,$3,'integration','integration','integration-model',$4)`,
			templateID, tenantID, name, runID.String()); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO agent_instances(id,tenant_id,template_id,swarm_id,name,status)
			VALUES($1,$2,$3,$4,$5,'IDLE')`, agentID, tenantID, templateID, runID, name); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO tasks(id,tenant_id,swarm_id,name,goal,status,assigned_agent_id)
			VALUES($1,$2,$3,$4,'effect finish integration','RUNNING',$5)`,
			taskID, tenantID, runID, name, agentID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE agent_instances SET status='RUNNING',current_task_id=$2 WHERE id=$1`,
			agentID, taskID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO task_attempts(
				id,tenant_id,task_id,agent_id,attempt_no,status,worker_id,fencing_token
			) VALUES($1,$2,$3,$4,1,'RUNNING',$5,7)`,
			attemptID, tenantID, taskID, agentID, workerID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO effects(
				id,tenant_id,run_id,task_id,attempt_id,idempotency_key,effect_type,risk_level,
				status,request_hash,sanitized_request,sanitized_result,fencing_token,authorized_at
			) VALUES($1,$2,$3,$4,$5,$6,'integration.production_change',
				'R3_PRODUCTION_DESTRUCTIVE','AUTHORIZED','hash:effect-finish','{}'::jsonb,
				'{"_authorization":{"approved_by":"integration-reviewer","blocked":false}}'::jsonb,
				7,now())`, effectID, tenantID, runID, taskID, attemptID, name)
		return err
	})
}

func cleanupToolEffectIntegrationData(t *testing.T, repository *Repository, tenantID, runID,
	templateID, agentID, effectID uuid.UUID,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _ = repository.pool.Exec(ctx, `
		DELETE FROM event_outbox
		WHERE tenant_id=$1 AND (aggregate_id=$2 OR payload->>'run_id'=$3)`,
		tenantID, effectID, runID.String())
	_, _ = repository.pool.Exec(ctx, `DELETE FROM swarms WHERE tenant_id=$1 AND id=$2`, tenantID, runID)
	_, _ = repository.pool.Exec(ctx, `DELETE FROM agent_instances WHERE tenant_id=$1 AND id=$2`, tenantID, agentID)
	_, _ = repository.pool.Exec(ctx, `DELETE FROM agent_templates WHERE tenant_id=$1 AND id=$2`, tenantID, templateID)
}
