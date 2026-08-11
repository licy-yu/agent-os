package postgres

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/licy-yu/agent-os/internal/execution"
	"github.com/licy-yu/agent-os/internal/runcontrol"
	"github.com/licy-yu/agent-os/internal/safetycontrol"
)

// TestHITLWaitingAttemptResumeAndReject 是显式启用的 PostgreSQL 合同测试。
// 它验证跨 HTTP/Worker 的关键事务事实，而不是只验证内存状态机：
//  1. Suspend 后 Attempt=WAITING，消息可被 ACK；
//  2. approve 原子授权 Effect 并重新发布原 Task；
//  3. ClaimWork 沿用同一 Attempt、递增 fence，且同步续签 AUTHORIZED Effect；
//  4. reject 原子终止 Attempt/Task/Run 并释放 Agent。
//
// 默认跳过，避免开发者误写生产库。只能把 SWARMOS_INTEGRATION_DATABASE_URL
// 指向可丢弃测试库；SWARMOS_INTEGRATION_MIGRATE=1 时会先执行全部迁移。
func TestHITLWaitingAttemptResumeAndReject(t *testing.T) {
	dsn := os.Getenv("SWARMOS_INTEGRATION_DATABASE_URL")
	if dsn == "" {
		t.Skip("未设置 SWARMOS_INTEGRATION_DATABASE_URL，跳过 HITL PostgreSQL 合同测试")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	repository, err := New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	if os.Getenv("SWARMOS_INTEGRATION_MIGRATE") == "1" {
		if err := repository.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("approve resumes same fenced attempt", func(t *testing.T) {
		fixture := seedHITLFixture(t, ctx, repository, "approve")
		defer cleanupHITLFixture(t, repository, fixture)

		owner := execution.AttemptOwner{
			AttemptID: fixture.attemptID, WorkerID: "worker-before-approval", FencingToken: 1,
		}
		if err := repository.SuspendAttempt(ctx, owner, execution.AttemptWait{
			Reason: execution.WaitForApproval, EffectID: fixture.effectID,
		}); err != nil {
			t.Fatal(err)
		}
		service := safetycontrol.NewService(repository)
		approved, err := service.Approve(hitlPrincipal(ctx, fixture.tenantID), fixture.interactionID,
			fixture.mutationKey, safetycontrol.InteractionCommandRequest{Version: 1})
		if err != nil {
			t.Fatal(err)
		}
		if approved.Response == nil || approved.Response.Action != "approve" {
			t.Fatalf("approval response = %#v", approved.Response)
		}

		work, err := repository.ClaimWork(ctx, fixture.taskID, fixture.agentID, "worker-after-approval")
		if err != nil {
			t.Fatal(err)
		}
		if work == nil || work.Attempt.ID != fixture.attemptID || work.Attempt.Number != 1 ||
			work.Attempt.FencingToken != 2 {
			t.Fatalf("resumed work = %#v", work)
		}
		if err := repository.BeginToolEffect(ctx, work.Attempt.Owner(), fixture.effectID,
			fixture.requestHash); err != nil {
			t.Fatalf("new fence cannot execute authorized effect: %v", err)
		}
		var attempts, taskVersion int
		if err := repository.pool.QueryRow(ctx, `
			SELECT count(*),max(t.version)
			FROM task_attempts a JOIN tasks t ON t.id=a.task_id
			WHERE a.task_id=$1 GROUP BY t.id`, fixture.taskID).Scan(&attempts, &taskVersion); err != nil {
			t.Fatal(err)
		}
		if attempts != 1 || taskVersion != 4 {
			t.Fatalf("attempts=%d taskVersion=%d", attempts, taskVersion)
		}
	})

	t.Run("reject fails waiting lifecycle", func(t *testing.T) {
		fixture := seedHITLFixture(t, ctx, repository, "reject")
		defer cleanupHITLFixture(t, repository, fixture)
		owner := execution.AttemptOwner{
			AttemptID: fixture.attemptID, WorkerID: "worker-before-approval", FencingToken: 1,
		}
		if err := repository.SuspendAttempt(ctx, owner, execution.AttemptWait{
			Reason: execution.WaitForApproval, EffectID: fixture.effectID,
		}); err != nil {
			t.Fatal(err)
		}
		service := safetycontrol.NewService(repository)
		_, err := service.Reject(hitlPrincipal(ctx, fixture.tenantID), fixture.interactionID, fixture.mutationKey,
			safetycontrol.InteractionCommandRequest{
				Version: 1, Resolution: map[string]any{"reason": "production window closed"},
			})
		if err != nil {
			t.Fatal(err)
		}
		var attemptStatus, taskStatus, runStatus, agentStatus string
		if err := repository.pool.QueryRow(ctx, `
			SELECT a.status,t.status,r.status,i.status
			FROM task_attempts a
			JOIN tasks t ON t.id=a.task_id
			JOIN swarms r ON r.id=t.swarm_id
			JOIN agent_instances i ON i.id=a.agent_id
			WHERE a.id=$1`, fixture.attemptID).
			Scan(&attemptStatus, &taskStatus, &runStatus, &agentStatus); err != nil {
			t.Fatal(err)
		}
		if attemptStatus != "ABORTED" || taskStatus != "FAILED" ||
			runStatus != "FAILED" || agentStatus != "IDLE" {
			t.Fatalf("attempt=%s task=%s run=%s agent=%s",
				attemptStatus, taskStatus, runStatus, agentStatus)
		}
	})
}

type hitlFixture struct {
	tenantID, runID, templateID, agentID uuid.UUID
	taskID, attemptID, effectID          uuid.UUID
	interactionID                        uuid.UUID
	requestHash, mutationKey             string
}

func seedHITLFixture(t *testing.T, ctx context.Context, repository *Repository, label string) hitlFixture {
	t.Helper()
	fixture := hitlFixture{
		tenantID: uuid.New(), runID: uuid.New(), templateID: uuid.New(),
		agentID: uuid.New(), taskID: uuid.New(), attemptID: uuid.New(), effectID: uuid.New(),
		interactionID: uuid.New(), requestHash: "sha256:" + uuid.NewString(),
		mutationKey: "hitl-" + label + "-" + uuid.NewString(),
	}
	err := repository.withTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO tenants(id,slug,name) VALUES($1,$2,$2)`,
			fixture.tenantID, "hitl-"+fixture.tenantID.String()); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO swarms(id,tenant_id,name,goal,status,desired_state,execution_engine)
			VALUES($1,$2,$3,'HITL integration','RUNNING','RUNNING','LEGACY')`,
			fixture.runID, fixture.tenantID, fixture.mutationKey); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO agent_templates(
				id,tenant_id,name,role,prompt,model,template_version,tools,permissions,risk_zone
			) VALUES($1,$2,$3,'integration','integration','mock/hitl',$4,
			         '["deploy"]'::jsonb,'["deploy:production"]'::jsonb,'production')`,
			fixture.templateID, fixture.tenantID, fixture.mutationKey, fixture.mutationKey); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO agent_instances(
				id,tenant_id,template_id,swarm_id,name,status
			) VALUES($1,$2,$3,$4,$5,'RUNNING')`, fixture.agentID, fixture.tenantID,
			fixture.templateID, fixture.runID, fixture.mutationKey); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO tasks(
				id,tenant_id,swarm_id,name,goal,status,assigned_agent_id,attempt_count
			) VALUES($1,$2,$3,$4,'HITL task','RUNNING',$5,1)`, fixture.taskID,
			fixture.tenantID, fixture.runID, fixture.mutationKey, fixture.agentID); err != nil {
			return err
		}
		// Agent.current_task_id 与 Task.assigned_agent_id 构成有意的双向引用。两条外键
		// 都是即时校验，测试夹具必须先创建两端，再补 Agent 指针，不能依赖插入顺序绕过。
		if _, err := tx.Exec(ctx, `
			UPDATE agent_instances SET current_task_id=$1 WHERE tenant_id=$2 AND id=$3`,
			fixture.taskID, fixture.tenantID, fixture.agentID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO task_attempts(
				id,tenant_id,task_id,agent_id,attempt_no,status,worker_id,fencing_token,
				started_at,heartbeat_at
			) VALUES($1,$2,$3,$4,1,'RUNNING','worker-before-approval',1,now(),now())`,
			fixture.attemptID, fixture.tenantID, fixture.taskID, fixture.agentID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO effects(
				id,tenant_id,run_id,task_id,attempt_id,idempotency_key,effect_type,risk_level,
				status,request_hash,sanitized_request,fencing_token,approval_interaction_id
			) VALUES($1,$2,$3,$4,$5,$6,'integration.deploy','R3_PRODUCTION_DESTRUCTIVE',
			         'PREPARED',$7,'{}'::jsonb,1,NULL)`, fixture.effectID, fixture.tenantID,
			fixture.runID, fixture.taskID, fixture.attemptID, fixture.mutationKey, fixture.requestHash); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO interactions(
				id,tenant_id,run_id,task_id,attempt_id,effect_id,interaction_type,status,
				title,allowed_actions,idempotency_key,requested_by
			) VALUES($1,$2,$3,$4,$5,$6,'APPROVAL','WAITING','approve integration effect',
			         '["approve","reject"]'::jsonb,$7,'integration')`, fixture.interactionID,
			fixture.tenantID, fixture.runID, fixture.taskID, fixture.attemptID, fixture.effectID,
			fixture.mutationKey+"-interaction"); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE effects SET approval_interaction_id=$2 WHERE id=$1`,
			fixture.effectID, fixture.interactionID)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return fixture
}

func hitlPrincipal(ctx context.Context, tenantID uuid.UUID) context.Context {
	return runcontrol.WithPrincipal(ctx, runcontrol.Principal{
		TenantID: tenantID, Subject: "hitl-reviewer",
		Scopes: map[string]bool{safetycontrol.ScopeInteractionResolve: true},
	})
}

func cleanupHITLFixture(t *testing.T, repository *Repository, fixture hitlFixture) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _ = repository.pool.Exec(ctx, `
		DELETE FROM processed_events
		WHERE tenant_id=$1 AND consumer_name=$2 AND event_id=$3`, fixture.tenantID,
		safetyMutationConsumer, mutationEventID(fixture.mutationKey))
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
		t.Logf("cleanup agent: %v", err)
	}
	_, _ = repository.pool.Exec(ctx, `DELETE FROM agent_templates WHERE tenant_id=$1 AND id=$2`,
		fixture.tenantID, fixture.templateID)
	_, _ = repository.pool.Exec(ctx, `DELETE FROM tenants WHERE id=$1`, fixture.tenantID)
}
