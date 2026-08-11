package postgres

import (
	"context"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/licy-yu/agent-os/internal/effect"
	"github.com/licy-yu/agent-os/internal/interaction"
	"github.com/licy-yu/agent-os/internal/runcontrol"
	"github.com/licy-yu/agent-os/internal/safetycontrol"
)

// TestSafetyControlPostgresAtomicApproval 是显式启用的 PostgreSQL 合同测试。它只应指向
// 可丢弃的独立测试库；默认跳过，避免开发者误把集成测试写入生产数据库。
func TestSafetyControlPostgresAtomicApproval(t *testing.T) {
	dsn := os.Getenv("SWARMOS_INTEGRATION_DATABASE_URL")
	if dsn == "" {
		t.Skip("未设置 SWARMOS_INTEGRATION_DATABASE_URL，跳过 PostgreSQL 原子审批测试")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	repository, err := New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	// CI 创建全新空库时可显式要求测试先迁移；运维侧已经按 SQL 验证过的升级库则不
	// 重放迁移，避免绕过其既有 migration bookkeeping。
	if os.Getenv("SWARMOS_INTEGRATION_MIGRATE") == "1" {
		if err := repository.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
	}

	tenantID := runcontrol.DefaultTenantID
	runID, taskID, attemptID := uuid.New(), uuid.New(), uuid.New()
	templateID, agentID := uuid.New(), uuid.New()
	effectApprovedID, effectRejectedID := uuid.New(), uuid.New()
	keyPrefix := "safety-it-" + uuid.NewString()
	interactionApprovedKey := keyPrefix + "-create-approved"
	interactionRejectedKey := keyPrefix + "-create-rejected"
	approveKey := keyPrefix + "-approve"
	rejectKey := keyPrefix + "-reject"

	// 固定 ID 让清理只触及本测试创建的事实；即使断言失败，defer 也不会扫描或删除其他 Run。
	defer cleanupSafetyIntegrationData(t, repository, tenantID, runID, templateID, agentID,
		[]uuid.UUID{effectApprovedID, effectRejectedID},
		[]string{interactionApprovedKey, interactionRejectedKey, approveKey, rejectKey})
	if err := seedSafetyIntegrationData(ctx, repository, tenantID, runID, taskID, attemptID,
		templateID, agentID, effectApprovedID, effectRejectedID, keyPrefix); err != nil {
		t.Fatal(err)
	}

	service := safetycontrol.NewService(repository)
	callCtx := runcontrol.WithPrincipal(ctx, runcontrol.Principal{
		TenantID: tenantID, Subject: "postgres-integration-reviewer",
		Scopes: map[string]bool{
			safetycontrol.ScopeInteractionCreate:  true,
			safetycontrol.ScopeInteractionResolve: true,
		},
	})
	createApproval := func(effectID uuid.UUID, key string) *safetycontrol.InteractionView {
		value, createErr := service.CreateInteraction(callCtx, key, safetycontrol.CreateInteractionRequest{
			RunID: runID, TaskID: &taskID, AttemptID: &attemptID, EffectID: &effectID,
			InteractionType: interaction.TypeApproval, Title: "批准生产变更",
		})
		if createErr != nil {
			t.Fatal(createErr)
		}
		return value
	}

	approvedInteraction := createApproval(effectApprovedID, interactionApprovedKey)
	approved, err := service.Approve(callCtx, approvedInteraction.ID, approveKey,
		safetycontrol.InteractionCommandRequest{Version: approvedInteraction.Version})
	if err != nil {
		t.Fatal(err)
	}
	if approved.Status != interaction.StatusResolved || approved.Response == nil || approved.Response.Action != "approve" {
		t.Fatalf("批准后的 Interaction 不完整: %#v", approved)
	}
	approvedEffect, err := service.GetEffect(callCtx, effectApprovedID)
	if err != nil {
		t.Fatal(err)
	}
	if approvedEffect.Status != effect.StatusAuthorized || approvedEffect.ApprovedBy != "postgres-integration-reviewer" || approvedEffect.AuthorizationBlocked {
		t.Fatalf("Effect 未被原子授权: %#v", approvedEffect)
	}

	// 相同 key+参数在聚合状态已经变化后仍必须回放最初响应，不得再次写 Effect。
	replayed, err := service.Approve(callCtx, approvedInteraction.ID, approveKey,
		safetycontrol.InteractionCommandRequest{Version: approvedInteraction.Version})
	if err != nil || replayed.Version != approved.Version {
		t.Fatalf("幂等回放失败: replay=%#v err=%v", replayed, err)
	}
	_, err = service.Approve(callCtx, approvedInteraction.ID, approveKey,
		safetycontrol.InteractionCommandRequest{
			Version: approvedInteraction.Version, Resolution: map[string]any{"changed": true},
		})
	if !errors.Is(err, safetycontrol.ErrIdempotencyConflict) {
		t.Fatalf("同键异参未被拒绝: %v", err)
	}

	rejectedInteraction := createApproval(effectRejectedID, interactionRejectedKey)
	_, err = service.Reject(callCtx, rejectedInteraction.ID, rejectKey,
		safetycontrol.InteractionCommandRequest{
			Version: rejectedInteraction.Version, Resolution: map[string]any{"reason": "生产窗口关闭"},
		})
	if err != nil {
		t.Fatal(err)
	}
	rejectedEffect, err := service.GetEffect(callCtx, effectRejectedID)
	if err != nil {
		t.Fatal(err)
	}
	if rejectedEffect.Status != effect.StatusPrepared || !rejectedEffect.AuthorizationBlocked ||
		rejectedEffect.AuthorizationBlockReason != "生产窗口关闭" {
		t.Fatalf("拒绝未永久封存 PREPARED Effect: %#v", rejectedEffect)
	}
	var rejectionEvents int
	if err := repository.pool.QueryRow(ctx, `
		SELECT count(*) FROM event_outbox
		WHERE tenant_id=$1 AND aggregate_type='effect' AND aggregate_id=$2
		  AND event_type='effect.authorization_rejected'`, tenantID, effectRejectedID).
		Scan(&rejectionEvents); err != nil || rejectionEvents != 1 {
		t.Fatalf("拒绝审计事件 count=%d err=%v", rejectionEvents, err)
	}

	// 随机租户读取同一个全局 ID 必须表现为不存在，不能先读出对象再在内存中过滤。
	_, err = repository.GetEffect(ctx, uuid.New(), effectApprovedID)
	if err == nil {
		t.Fatal("跨租户读取 Effect 意外成功")
	}
}

func seedSafetyIntegrationData(ctx context.Context, repository *Repository, tenantID, runID, taskID,
	attemptID, templateID, agentID, approvedEffectID, rejectedEffectID uuid.UUID, keyPrefix string,
) error {
	return repository.withTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO swarms(id,tenant_id,name,goal,status,desired_state,execution_engine)
			VALUES($1,$2,$3,'integration safety contract','RUNNING','RUNNING','LEGACY')`,
			runID, tenantID, keyPrefix); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO agent_templates(id,tenant_id,name,role,prompt,model,template_version)
			VALUES($1,$2,$3,'integration','integration','integration-model',$4)`,
			templateID, tenantID, keyPrefix, keyPrefix); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO agent_instances(id,tenant_id,template_id,swarm_id,name,status)
			VALUES($1,$2,$3,$4,$5,'IDLE')`, agentID, tenantID, templateID, runID, keyPrefix); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO tasks(id,tenant_id,swarm_id,name,goal,status)
			VALUES($1,$2,$3,$4,'integration approval','WAITING_APPROVAL')`,
			taskID, tenantID, runID, keyPrefix); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO task_attempts(id,tenant_id,task_id,agent_id,attempt_no,status,fencing_token)
			VALUES($1,$2,$3,$4,1,'RUNNING',1)`, attemptID, tenantID, taskID, agentID); err != nil {
			return err
		}
		for index, id := range []uuid.UUID{approvedEffectID, rejectedEffectID} {
			if _, err := tx.Exec(ctx, `
				INSERT INTO effects(
					id,tenant_id,run_id,task_id,attempt_id,idempotency_key,effect_type,
					risk_level,status,request_hash,sanitized_request,fencing_token
				) VALUES($1,$2,$3,$4,$5,$6,'integration.production_change',
					'R3_PRODUCTION_DESTRUCTIVE','PREPARED',$7,'{}'::jsonb,1)`,
				id, tenantID, runID, taskID, attemptID, keyPrefix+"-effect-"+strconv.Itoa(index),
				"sha256:"+keyPrefix+strconv.Itoa(index)); err != nil {
				return err
			}
		}
		return nil
	})
}

func cleanupSafetyIntegrationData(t *testing.T, repository *Repository, tenantID, runID, templateID,
	agentID uuid.UUID, effectIDs []uuid.UUID, mutationKeys []string,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, key := range mutationKeys {
		_, _ = repository.pool.Exec(ctx, `
			DELETE FROM processed_events
			WHERE tenant_id=$1 AND consumer_name=$2 AND event_id=$3`,
			tenantID, safetyMutationConsumer, mutationEventID(key))
	}
	_, _ = repository.pool.Exec(ctx, `
		DELETE FROM event_outbox
		WHERE tenant_id=$1 AND (aggregate_id=ANY($2) OR payload->>'run_id'=$3)`,
		tenantID, effectIDs, runID.String())
	_, _ = repository.pool.Exec(ctx, `DELETE FROM swarms WHERE tenant_id=$1 AND id=$2`, tenantID, runID)
	_, _ = repository.pool.Exec(ctx, `DELETE FROM agent_instances WHERE tenant_id=$1 AND id=$2`, tenantID, agentID)
	_, _ = repository.pool.Exec(ctx, `DELETE FROM agent_templates WHERE tenant_id=$1 AND id=$2`, tenantID, templateID)
}
