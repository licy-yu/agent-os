package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/licy-yu/agent-os/internal/execution"
)

// loadVerificationClosure 在 Reviewer 事务内重新读取数据库 Gate 事实。
// 即使 Work.OutputSnapshot 或 Evaluation 声称成功，缺失/失败的 required Gate 仍会阻断成功。
func loadVerificationClosure(ctx context.Context, tx pgx.Tx, tenantID, taskID, attemptID uuid.UUID) (verificationClosure, error) {
	closure := verificationClosure{TenantID: tenantID, ArtifactID: stableExecutionID("candidate-artifact", attemptID.String())}
	var verificationStatus string
	if err := tx.QueryRow(ctx, `
		SELECT id,run_id,status FROM verification_runs
		WHERE tenant_id=$1 AND task_id=$2 AND attempt_id=$3
		ORDER BY created_at DESC,id DESC LIMIT 1`, tenantID, taskID, attemptID).
		Scan(&closure.VerificationID, &closure.RunID, &verificationStatus); err != nil {
		return closure, mapReadError("读取 VerificationRun", err)
	}

	rows, err := tx.Query(ctx, `
		SELECT g.gate_key,g.required,COALESCE(gr.status,''),gr.id
		FROM acceptance_gates g
		LEFT JOIN gate_results gr
		  ON gr.gate_id=g.id AND gr.verification_run_id=$2
		WHERE g.task_id=$1 ORDER BY g.gate_key,g.id`, taskID, closure.VerificationID)
	if err != nil {
		return closure, fmt.Errorf("读取 required GateResult: %w", err)
	}
	defer rows.Close()
	closure.RequiredPassed, closure.PolicyPassed = true, true
	requiredCount := 0
	for rows.Next() {
		var key, status string
		var required bool
		var resultID *uuid.UUID
		if err := rows.Scan(&key, &required, &status, &resultID); err != nil {
			return closure, err
		}
		if resultID != nil {
			closure.GateResultIDs = append(closure.GateResultIDs, *resultID)
		}
		if required {
			requiredCount++
			if status != gatePassed {
				closure.RequiredPassed = false
				closure.FailedGateKeys = append(closure.FailedGateKeys, key)
			}
		}
		if key == "system.policy" && status != gatePassed {
			closure.PolicyPassed = false
		}
	}
	if err := rows.Err(); err != nil {
		return closure, err
	}
	if requiredCount == 0 {
		closure.RequiredPassed = false
		closure.FailedGateKeys = append(closure.FailedGateKeys, "required_gate_missing")
	}
	if verificationStatus != "PASSED" {
		closure.RequiredPassed = false
		if len(closure.FailedGateKeys) == 0 {
			closure.FailedGateKeys = append(closure.FailedGateKeys, "verification_run_not_passed")
		}
	}
	sort.Strings(closure.FailedGateKeys)
	return closure, nil
}

// enforceRequiredGates 把 Reviewer 建议收敛为平台最终决策。只有 ACCEPT 会被硬门覆盖；
// Reviewer 主动要求 RETRY/REJECT 时仍保持更保守的决定。
func enforceRequiredGates(evaluation execution.Evaluation, closure verificationClosure,
	attemptNo, maxAttempts int32,
) execution.Evaluation {
	if evaluation.Decision != execution.DecisionAccept || closure.RequiredPassed {
		return evaluation
	}
	evaluation.MachinePass = false
	evaluation.PolicyPass = evaluation.PolicyPass && closure.PolicyPassed
	evaluation.Findings = append(evaluation.Findings,
		"required AcceptanceGate 未全部通过: "+strings.Join(closure.FailedGateKeys, ", "))
	if !closure.PolicyPassed || (maxAttempts > 0 && attemptNo >= maxAttempts) {
		evaluation.Decision = execution.DecisionReject
	} else {
		evaluation.Decision = execution.DecisionRetry
	}
	return evaluation
}

// createTaskCompletionManifest 只在全部 required Gate PASS 后调用。Manifest、Artifact 状态和
// Evidence 连接都在 Reviewer 的同一个事务内提交，不存在“Task 成功但证据尚未落库”的窗口。
func createTaskCompletionManifest(ctx context.Context, tx pgx.Tx, closure verificationClosure,
	taskID, attemptID uuid.UUID, evaluation execution.Evaluation, now time.Time,
) (uuid.UUID, error) {
	var artifactHash string
	if err := tx.QueryRow(ctx, `SELECT content_hash FROM artifacts WHERE id=$1 FOR UPDATE`, closure.ArtifactID).
		Scan(&artifactHash); err != nil {
		return uuid.Nil, mapReadError("锁定候选 Artifact", err)
	}
	sort.Slice(closure.GateResultIDs, func(i, j int) bool {
		return closure.GateResultIDs[i].String() < closure.GateResultIDs[j].String()
	})
	manifestPayload := map[string]any{
		"scope": "task", "task_id": taskID, "attempt_id": attemptID,
		"artifact_id": closure.ArtifactID, "artifact_hash": artifactHash,
		"verification_run_id": closure.VerificationID, "gate_result_ids": closure.GateResultIDs,
	}
	raw, err := json.Marshal(manifestPayload)
	if err != nil {
		return uuid.Nil, fmt.Errorf("序列化 Task CompletionManifest: %w", err)
	}
	contentHash := sha256Hex(raw)
	manifestID := stableExecutionID("task-completion-manifest", taskID.String(), attemptID.String(), contentHash)
	knownIssues, err := marshalJSON(evaluation.Findings, "completion_manifest.known_issues")
	if err != nil {
		return uuid.Nil, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO completion_manifests(
			id,tenant_id,run_id,task_id,attempt_id,status,summary,known_issues,assumptions,
			content_hash,created_by,created_at,validated_at
		) VALUES($1,$2,$3,$4,$5,'VALID',$6,$7,'[]'::jsonb,$8,$9,$10,$10)
		ON CONFLICT(tenant_id,run_id,task_id,content_hash) DO NOTHING`, manifestID, closure.TenantID,
		closure.RunID, taskID, attemptID, "Task 已通过全部 required AcceptanceGate",
		knownIssues, contentHash, evaluation.Reviewer, now); err != nil {
		return uuid.Nil, mapWriteError("保存 Task CompletionManifest", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO completion_manifest_artifacts(manifest_id,artifact_id,role)
		VALUES($1,$2,'OUTPUT') ON CONFLICT DO NOTHING`, manifestID, closure.ArtifactID); err != nil {
		return uuid.Nil, mapWriteError("关联 Task Manifest Artifact", err)
	}
	for _, resultID := range closure.GateResultIDs {
		if _, err := tx.Exec(ctx, `
			INSERT INTO completion_manifest_evidence(manifest_id,gate_result_id)
			VALUES($1,$2) ON CONFLICT DO NOTHING`, manifestID, resultID); err != nil {
			return uuid.Nil, mapWriteError("关联 Task Manifest Evidence", err)
		}
	}
	validation, err := marshalJSON(map[string]any{
		"verification_run_id": closure.VerificationID, "completion_manifest_id": manifestID,
		"required_passed": true,
	}, "artifact.validation")
	if err != nil {
		return uuid.Nil, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE artifacts SET status='VALID',validation=$2,validated_at=$3 WHERE id=$1`,
		closure.ArtifactID, validation, now); err != nil {
		return uuid.Nil, fmt.Errorf("标记候选 Artifact VALID: %w", err)
	}
	return manifestID, nil
}

func invalidateCandidateArtifact(ctx context.Context, tx pgx.Tx, closure verificationClosure,
	decision execution.ReviewDecision, now time.Time,
) error {
	validation, err := marshalJSON(map[string]any{
		"verification_run_id": closure.VerificationID, "required_passed": closure.RequiredPassed,
		"failed_gates": closure.FailedGateKeys, "review_decision": decision,
	}, "artifact.validation")
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE artifacts SET status='INVALID',validation=$2,validated_at=$3 WHERE id=$1`,
		closure.ArtifactID, validation, now)
	return err
}

// finalizeRunIfComplete 在已持有 swarm 行锁时检查所有 Task。最后一个成功 Task 会生成
// Run 级最终证据 Artifact 和 CompletionManifest，再原子地把 Run 标记为 COMPLETED。
func finalizeRunIfComplete(ctx context.Context, tx pgx.Tx, tenantID, runID, taskID, attemptID uuid.UUID,
	createdBy string, now time.Time,
) (bool, error) {
	var remaining int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE swarm_id=$1 AND status<>'SUCCEEDED'`, runID).
		Scan(&remaining); err != nil {
		return false, fmt.Errorf("检查 Run Task 完成度: %w", err)
	}
	if remaining != 0 {
		return false, nil
	}

	type artifactEvidence struct {
		ID         uuid.UUID
		Hash, Type string
	}
	artifacts := make([]artifactEvidence, 0)
	rows, err := tx.Query(ctx, `
		SELECT id,content_hash,artifact_type FROM artifacts
		WHERE tenant_id=$1 AND run_id=$2 AND status='VALID' AND artifact_type<>'RUN_COMPLETION'
		ORDER BY id`, tenantID, runID)
	if err != nil {
		return false, err
	}
	for rows.Next() {
		var value artifactEvidence
		if err := rows.Scan(&value.ID, &value.Hash, &value.Type); err != nil {
			rows.Close()
			return false, err
		}
		artifacts = append(artifacts, value)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return false, err
	}
	rows.Close()

	manifestIDs := make([]uuid.UUID, 0)
	rows, err = tx.Query(ctx, `
		SELECT id FROM completion_manifests
		WHERE tenant_id=$1 AND run_id=$2 AND task_id IS NOT NULL AND status='VALID' ORDER BY id`, tenantID, runID)
	if err != nil {
		return false, err
	}
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return false, err
		}
		manifestIDs = append(manifestIDs, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return false, err
	}
	rows.Close()

	gateResultIDs := make([]uuid.UUID, 0)
	rows, err = tx.Query(ctx, `
		SELECT gr.id FROM gate_results gr
		JOIN verification_runs vr ON vr.id=gr.verification_run_id
		WHERE vr.tenant_id=$1 AND vr.run_id=$2 ORDER BY gr.id`, tenantID, runID)
	if err != nil {
		return false, err
	}
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return false, err
		}
		gateResultIDs = append(gateResultIDs, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return false, err
	}
	rows.Close()

	evidencePayload := map[string]any{
		"scope": "run", "run_id": runID, "task_manifests": manifestIDs,
		"artifacts": artifacts, "gate_results": gateResultIDs,
	}
	evidenceRaw, err := json.Marshal(evidencePayload)
	if err != nil {
		return false, err
	}
	evidenceHash := sha256Hex(evidenceRaw)
	finalArtifactID := stableExecutionID("run-completion-artifact", runID.String(), evidenceHash)
	metadata, err := marshalJSON(evidencePayload, "run_completion_artifact.metadata")
	if err != nil {
		return false, err
	}
	uri := "swarmos://runs/" + runID.String() + "/completion-evidence"
	validation, err := marshalJSON(map[string]any{"all_tasks_succeeded": true}, "run_completion_artifact.validation")
	if err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO artifacts(
			id,tenant_id,run_id,task_id,attempt_id,artifact_type,uri,version,content_hash,
			metadata,name,status,media_type,schema_version,version_no,size_bytes,validation,validated_at,created_at
		) VALUES($1,$2,$3,$4,$5,'RUN_COMPLETION',$6,'1',$7,$8,'run-completion-evidence',
		         'VALID','application/json','1',1,$9,$10,$11,$11)
		ON CONFLICT(id) DO NOTHING`, finalArtifactID, tenantID, runID, taskID, attemptID, uri,
		evidenceHash, metadata, len(evidenceRaw), validation, now); err != nil {
		return false, mapWriteError("保存 Run 最终 Artifact", err)
	}
	artifacts = append(artifacts, artifactEvidence{ID: finalArtifactID, Hash: evidenceHash, Type: "RUN_COMPLETION"})
	runManifestRaw, err := json.Marshal(map[string]any{
		"run_id": runID, "task_manifests": manifestIDs, "artifacts": artifacts,
		"gate_results": gateResultIDs, "final_artifact_id": finalArtifactID,
	})
	if err != nil {
		return false, err
	}
	runManifestHash := sha256Hex(runManifestRaw)
	runManifestID := stableExecutionID("run-completion-manifest", runID.String(), runManifestHash)
	if _, err := tx.Exec(ctx, `
		INSERT INTO completion_manifests(
			id,tenant_id,run_id,task_id,attempt_id,status,summary,known_issues,assumptions,
			content_hash,created_by,created_at,validated_at
		) VALUES($1,$2,$3,NULL,$4,'VALID','Run 所有 Task 已通过 required AcceptanceGate',
		         '[]'::jsonb,'[]'::jsonb,$5,$6,$7,$7)
		ON CONFLICT(id) DO NOTHING`, runManifestID, tenantID, runID, attemptID,
		runManifestHash, createdBy, now); err != nil {
		return false, mapWriteError("保存 Run CompletionManifest", err)
	}
	for _, artifact := range artifacts {
		role := "TASK_OUTPUT"
		if artifact.ID == finalArtifactID {
			role = "FINAL_EVIDENCE"
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO completion_manifest_artifacts(manifest_id,artifact_id,role)
			VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, runManifestID, artifact.ID, role); err != nil {
			return false, err
		}
	}
	for _, resultID := range gateResultIDs {
		if _, err := tx.Exec(ctx, `
			INSERT INTO completion_manifest_evidence(manifest_id,gate_result_id)
			VALUES($1,$2) ON CONFLICT DO NOTHING`, runManifestID, resultID); err != nil {
			return false, err
		}
	}
	var runVersion int64
	if err := tx.QueryRow(ctx, `
		UPDATE swarms SET status='COMPLETED',completion_manifest_id=$2,version=version+1,updated_at=$3
		WHERE id=$1 AND status NOT IN ('COMPLETED','FAILED','CANCELED','EXPIRED') RETURNING version`,
		runID, runManifestID, now).Scan(&runVersion); err != nil {
		return false, fmt.Errorf("完成 Run: %w", err)
	}
	if err := insertTenantOutbox(ctx, tx, tenantID, "swarm", runID, "run.completed", runVersion, map[string]any{
		"id": runID, "completion_manifest_id": runManifestID, "final_artifact_id": finalArtifactID,
		"version": runVersion,
	}); err != nil {
		return false, err
	}
	return true, nil
}
