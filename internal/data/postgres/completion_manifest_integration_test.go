package postgres

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/licy-yu/agent-os/internal/runcontrol"
	"github.com/licy-yu/agent-os/internal/safetycontrol"
)

// TestCompletionManifestGateMetricsJSONContract 是不依赖数据库的最小回归测试，确保 CI 即使
// 没有启用 PostgreSQL 集成环境，也会锁住 GateResultView 必须接受异构 JSONB 的 API 合同。
func TestCompletionManifestGateMetricsJSONContract(t *testing.T) {
	var view safetycontrol.GateResultView
	raw := []byte(`{
		"required":true,
		"coverage":97.5,
		"engine":"json-schema",
		"details":{"violations":0}
	}`)
	if err := json.Unmarshal(raw, &view.Metrics); err != nil {
		t.Fatalf("合法异构 Gate metrics 不应解析失败: %v", err)
	}
	if view.Metrics["required"] != true || view.Metrics["engine"] != "json-schema" ||
		view.Metrics["coverage"] != float64(97.5) {
		t.Fatalf("Gate metrics 类型或值丢失: %#v", view.Metrics)
	}
	details, ok := view.Metrics["details"].(map[string]any)
	if !ok || details["violations"] != float64(0) {
		t.Fatalf("Gate metrics 嵌套诊断丢失: %#v", view.Metrics)
	}
}

// TestGetCompletionManifestPostgresPreservesJSONMetricsAndRunScope 回归两个容易被单元桩遗漏的
// PostgreSQL 读取合同：
//  1. gate_results.metrics 是任意 JSON 对象，布尔、字符串和嵌套诊断都必须无损返回；
//  2. Run 级 Manifest 的 task_id/attempt_id 允许为空，此时 Verification 仍只能来自同一 Run。
//
// 测试默认跳过，防止误写开发或生产数据库；调用者必须显式提供可丢弃测试库。
func TestGetCompletionManifestPostgresPreservesJSONMetricsAndRunScope(t *testing.T) {
	dsn := os.Getenv("SWARMOS_INTEGRATION_DATABASE_URL")
	if dsn == "" {
		t.Skip("未设置 SWARMOS_INTEGRATION_DATABASE_URL，跳过 Completion Manifest PostgreSQL 读取合同测试")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	fixture := newCompletionManifestReadFixture()
	defer cleanupCompletionManifestReadFixture(t, repository, fixture)
	if err := seedCompletionManifestReadFixture(ctx, repository, fixture); err != nil {
		t.Fatal(err)
	}

	manifest, err := repository.GetCompletionManifest(ctx, fixture.tenantID, fixture.manifestID)
	if err != nil {
		t.Fatalf("读取含异构 Gate metrics 的 Completion Manifest 失败: %v", err)
	}
	if manifest.VerificationID == nil || *manifest.VerificationID != fixture.targetVerificationID {
		t.Fatalf("Manifest 串读了其他 Run 的 Verification: got=%v want=%s",
			manifest.VerificationID, fixture.targetVerificationID)
	}
	if len(manifest.Evidence) != 1 {
		t.Fatalf("Manifest Evidence 数量错误: got=%d want=1", len(manifest.Evidence))
	}
	metrics := manifest.Evidence[0].Metrics
	if required, ok := metrics["required"].(bool); !ok || !required {
		t.Fatalf("布尔 Gate metric 未保留: %#v", metrics)
	}
	if engine, ok := metrics["engine"].(string); !ok || engine != "json-schema" {
		t.Fatalf("字符串 Gate metric 未保留: %#v", metrics)
	}
	if coverage, ok := metrics["coverage"].(float64); !ok || coverage != 97.5 {
		t.Fatalf("数值 Gate metric 未保留: %#v", metrics)
	}
	details, ok := metrics["details"].(map[string]any)
	if !ok || details["violations"] != float64(0) {
		t.Fatalf("嵌套 Gate metric 未保留: %#v", metrics)
	}
}

type completionManifestReadFixture struct {
	tenantID             uuid.UUID
	manifestID           uuid.UUID
	targetVerificationID uuid.UUID
	decoyVerificationID  uuid.UUID
	runIDs               []uuid.UUID
	taskIDs              []uuid.UUID
	attemptIDs           []uuid.UUID
	templateIDs          []uuid.UUID
	agentIDs             []uuid.UUID
	gateIDs              []uuid.UUID
	gateResultID         uuid.UUID
	label                string
}

func newCompletionManifestReadFixture() completionManifestReadFixture {
	return completionManifestReadFixture{
		tenantID:             runcontrol.DefaultTenantID,
		manifestID:           uuid.New(),
		targetVerificationID: uuid.New(),
		decoyVerificationID:  uuid.New(),
		runIDs:               []uuid.UUID{uuid.New(), uuid.New()},
		taskIDs:              []uuid.UUID{uuid.New(), uuid.New()},
		attemptIDs:           []uuid.UUID{uuid.New(), uuid.New()},
		templateIDs:          []uuid.UUID{uuid.New(), uuid.New()},
		agentIDs:             []uuid.UUID{uuid.New(), uuid.New()},
		gateIDs:              []uuid.UUID{uuid.New(), uuid.New()},
		gateResultID:         uuid.New(),
		label:                "manifest-read-" + uuid.NewString(),
	}
}

func seedCompletionManifestReadFixture(ctx context.Context, repository *Repository,
	fixture completionManifestReadFixture,
) error {
	metrics, err := json.Marshal(map[string]any{
		"required": true, "deterministic": true, "coverage": 97.5,
		"engine": "json-schema", "details": map[string]any{"violations": 0},
	})
	if err != nil {
		return err
	}
	return repository.withTx(ctx, func(tx pgx.Tx) error {
		// 第二个 Run 的 Verification 故意更新；旧查询只按 tenant_id 排序时会错误选择它。
		createdAt := []time.Time{time.Now().UTC().Add(-time.Hour), time.Now().UTC()}
		verificationIDs := []uuid.UUID{fixture.targetVerificationID, fixture.decoyVerificationID}
		for index := range fixture.runIDs {
			if _, err := tx.Exec(ctx, `
				INSERT INTO swarms(id,tenant_id,name,goal,status,desired_state,execution_engine)
				VALUES($1,$2,$3,'manifest read contract','RUNNING','RUNNING','LEGACY')`,
				fixture.runIDs[index], fixture.tenantID, fixture.label+"-run-"+string(rune('0'+index))); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO agent_templates(id,tenant_id,name,role,prompt,model,template_version)
				VALUES($1,$2,$3,'manifest-read','manifest-read','test-model',$4)`,
				fixture.templateIDs[index], fixture.tenantID,
				fixture.label+"-template-"+string(rune('0'+index)), fixture.label); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO agent_instances(id,tenant_id,template_id,swarm_id,name,status)
				VALUES($1,$2,$3,$4,$5,'IDLE')`, fixture.agentIDs[index], fixture.tenantID,
				fixture.templateIDs[index], fixture.runIDs[index], fixture.label+"-agent"); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO tasks(id,tenant_id,swarm_id,name,goal,status)
				VALUES($1,$2,$3,$4,'manifest read','SUCCEEDED')`, fixture.taskIDs[index],
				fixture.tenantID, fixture.runIDs[index], fixture.label+"-task"); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO task_attempts(id,tenant_id,task_id,agent_id,attempt_no,status,fencing_token)
				VALUES($1,$2,$3,$4,1,'SUCCEEDED',1)`, fixture.attemptIDs[index], fixture.tenantID,
				fixture.taskIDs[index], fixture.agentIDs[index]); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO acceptance_gates(id,tenant_id,task_id,gate_key,gate_type,config,required)
				VALUES($1,$2,$3,$4,'JSON_SCHEMA','{}'::jsonb,true)`, fixture.gateIDs[index],
				fixture.tenantID, fixture.taskIDs[index], fixture.label+"-gate"); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO verification_runs(
					id,tenant_id,run_id,task_id,attempt_id,status,verifier_version,
					started_at,finished_at,created_at
				) VALUES($1,$2,$3,$4,$5,'PASSED','manifest-read-test/v1',$6,$6,$6)`,
				verificationIDs[index], fixture.tenantID, fixture.runIDs[index],
				fixture.taskIDs[index], fixture.attemptIDs[index], createdAt[index]); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO gate_results(
				id,tenant_id,verification_run_id,gate_id,status,metrics,
				output_excerpt,error_message,started_at,finished_at
			) VALUES($1,$2,$3,$4,'PASSED',$5,'异构指标验证通过','',now(),now())`,
			fixture.gateResultID, fixture.tenantID, fixture.targetVerificationID,
			fixture.gateIDs[0], metrics); err != nil {
			return err
		}
		// task_id/attempt_id 同时为空，精确模拟最容易跨 Run 串读的历史 Run 级 Manifest。
		if _, err := tx.Exec(ctx, `
			INSERT INTO completion_manifests(
				id,tenant_id,run_id,task_id,attempt_id,status,summary,known_issues,
				assumptions,content_hash,created_by,created_at,validated_at
			) VALUES($1,$2,$3,NULL,NULL,'VALID','manifest read contract','[]'::jsonb,
				'[]'::jsonb,$4,'integration-test',now(),now())`, fixture.manifestID,
			fixture.tenantID, fixture.runIDs[0], fixture.label); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO completion_manifest_evidence(manifest_id,gate_result_id)
			VALUES($1,$2)`, fixture.manifestID, fixture.gateResultID)
		return err
	})
}

func cleanupCompletionManifestReadFixture(t *testing.T, repository *Repository,
	fixture completionManifestReadFixture,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// 只删除本测试生成的随机 ID；Run 级联清理 Task、Attempt、Verification、Gate 与 Manifest。
	_, _ = repository.pool.Exec(ctx, `DELETE FROM swarms WHERE tenant_id=$1 AND id=ANY($2)`,
		fixture.tenantID, fixture.runIDs)
	_, _ = repository.pool.Exec(ctx, `DELETE FROM agent_instances WHERE tenant_id=$1 AND id=ANY($2)`,
		fixture.tenantID, fixture.agentIDs)
	_, _ = repository.pool.Exec(ctx, `DELETE FROM agent_templates WHERE tenant_id=$1 AND id=ANY($2)`,
		fixture.tenantID, fixture.templateIDs)
}
