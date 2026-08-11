package postgres

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/licy-yu/agent-os/internal/effect"
	"github.com/licy-yu/agent-os/internal/runcontrol"
	"github.com/licy-yu/agent-os/internal/safetycontrol"
)

// TestEffectReconcileEvidencePostgresContract 验证真实 PostgreSQL 上的对账收敛合同：
//
//  1. RECONCILING -> UNKNOWN 能安全计算下次对账时间，不会因 pgx 参数
//     类型推断冲突而回滚；
//  2. 稳定账本中已有的 external_ref 可被最终结论复用；
//  3. result/evidence 被脱敏后持久化，且同幂等键更换证据会被拒绝。
//
// 默认跳过；只允许把 SWARMOS_INTEGRATION_DATABASE_URL 指向可丢弃的独立测试库。
func TestEffectReconcileEvidencePostgresContract(t *testing.T) {
	dsn := os.Getenv("SWARMOS_INTEGRATION_DATABASE_URL")
	if dsn == "" {
		t.Skip("未设置 SWARMOS_INTEGRATION_DATABASE_URL，跳过 Effect 对账证据测试")
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

	fixture := seedHITLFixture(t, ctx, repository, "reconcile-evidence")
	defer cleanupHITLFixture(t, repository, fixture)
	mutationKeys := []string{
		fixture.mutationKey + "-start", fixture.mutationKey + "-unknown",
		fixture.mutationKey + "-restart", fixture.mutationKey + "-succeeded",
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		// processed_events 对 tenant 没有 ON DELETE CASCADE，因此只按本测试
		// 已知的幂等键精确删除，避免污染测试库或扫描其它 Run。
		for _, key := range mutationKeys {
			_, _ = repository.pool.Exec(cleanupCtx, `
				DELETE FROM processed_events
				WHERE tenant_id=$1 AND consumer_name=$2 AND event_id=$3`,
				fixture.tenantID, safetyMutationConsumer, mutationEventID(key))
		}
	}()
	// 把通用 HITL 夹具转成“Worker 已暂停、Effect 结果未知”的真实崩溃现场。
	// 保留 external_ref 模拟 Adapter 已获得外部资源 ID，但在回传最终状态时断线。
	if _, err := repository.pool.Exec(ctx, `
		UPDATE effects
		SET status='UNKNOWN',risk_level='R2_EXTERNAL_REVERSIBLE',external_ref='provider-job-42',
		    approval_interaction_id=NULL,version=1,reconcile_after=now()
		WHERE tenant_id=$1 AND id=$2`, fixture.tenantID, fixture.effectID); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.pool.Exec(ctx, `
		UPDATE task_attempts SET status='WAITING' WHERE tenant_id=$1 AND id=$2`,
		fixture.tenantID, fixture.attemptID); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.pool.Exec(ctx, `
		UPDATE tasks SET status='WAITING_EXTERNAL' WHERE tenant_id=$1 AND id=$2`,
		fixture.tenantID, fixture.taskID); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.pool.Exec(ctx, `
		UPDATE agent_instances SET status='WAITING_TOOL' WHERE tenant_id=$1 AND id=$2`,
		fixture.tenantID, fixture.agentID); err != nil {
		t.Fatal(err)
	}

	service := safetycontrol.NewService(repository)
	callCtx := runcontrol.WithPrincipal(ctx, runcontrol.Principal{
		TenantID: fixture.tenantID, Subject: "postgres-reconciler",
		Scopes: map[string]bool{safetycontrol.ScopeEffectReconcile: true},
	})
	started, err := service.Reconcile(callCtx, fixture.effectID, mutationKeys[0],
		safetycontrol.EffectCommandRequest{Version: 1, Reason: "begin provider query"})
	if err != nil || started.Status != effect.StatusReconciling || started.Version != 2 {
		t.Fatalf("启动对账 = %#v, err=%v", started, err)
	}
	unknown, err := service.Reconcile(callCtx, fixture.effectID, mutationKeys[1],
		safetycontrol.EffectCommandRequest{
			Version: 2, Outcome: effect.StatusUnknown, Reason: "provider query timed out",
		})
	if err != nil || unknown.Status != effect.StatusUnknown || unknown.Version != 3 ||
		unknown.ReconcileAfter == nil || !unknown.ReconcileAfter.After(unknown.UpdatedAt) {
		t.Fatalf("UNKNOWN 持久化 = %#v, err=%v", unknown, err)
	}

	started, err = service.Reconcile(callCtx, fixture.effectID, mutationKeys[2],
		safetycontrol.EffectCommandRequest{Version: 3, Reason: "retry provider query"})
	if err != nil || started.Status != effect.StatusReconciling || started.Version != 4 {
		t.Fatalf("重新启动对账 = %#v, err=%v", started, err)
	}
	finalRequest := safetycontrol.EffectCommandRequest{
		Version: 4, Outcome: effect.StatusSucceeded,
		Result: map[string]any{"resourceState": "ready"},
		Evidence: map[string]any{
			"source": "provider.getJob", "queryId": "query-42",
			"observedAt": "2026-08-11T09:30:00Z", "token": "must-be-redacted",
		},
		// ExternalRef 有意留空：服务必须使用 effects.external_ref 中的稳定引用。
	}
	finalKey := mutationKeys[3]
	settled, err := service.Reconcile(callCtx, fixture.effectID, finalKey, finalRequest)
	if err != nil || settled.Status != effect.StatusSucceeded || settled.Version != 5 ||
		settled.ExternalRef != "provider-job-42" {
		t.Fatalf("对账收敛 = %#v, err=%v", settled, err)
	}
	reconciliation, ok := settled.SanitizedResult["reconciliation"].(map[string]any)
	if !ok {
		t.Fatalf("缺少 reconciliation 证据: %#v", settled.SanitizedResult)
	}
	evidence, ok := reconciliation["evidence"].(map[string]any)
	if !ok || evidence["source"] != "provider.getJob" || evidence["token"] != "[REDACTED]" {
		t.Fatalf("证据未持久化或未脱敏: %#v", reconciliation)
	}

	// 聚合状态已经改变后，同 key+同请求仍必须回放原结果。
	replayed, err := service.Reconcile(callCtx, fixture.effectID, finalKey, finalRequest)
	if err != nil || replayed.Version != settled.Version {
		t.Fatalf("对账幂等回放 = %#v, err=%v", replayed, err)
	}
	changedEvidence := finalRequest
	changedEvidence.Evidence = map[string]any{
		"source": "provider.getJob", "queryId": "different-query",
	}
	_, err = service.Reconcile(callCtx, fixture.effectID, finalKey, changedEvidence)
	if !errors.Is(err, safetycontrol.ErrIdempotencyConflict) {
		t.Fatalf("同键更换证据未被拒绝: %v", err)
	}
}
