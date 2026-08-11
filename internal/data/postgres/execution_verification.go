package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/licy-yu/agent-os/internal/execution"
)

const (
	gatePassed = "PASSED"
	gateFailed = "FAILED"
)

// verificationClosure 是 Worker 候选输出经过独立硬验收后的数据库事实。
// Reviewer 只能读取这份事实，不能根据模型在 output/checks 中的自然语言自报来放行。
type verificationClosure struct {
	TenantID       uuid.UUID
	RunID          uuid.UUID
	ArtifactID     uuid.UUID
	VerificationID uuid.UUID
	RequiredPassed bool
	PolicyPassed   bool
	FailedGateKeys []string
	GateResultIDs  []uuid.UUID
}

type persistedGate struct {
	ID       uuid.UUID
	Key      string
	Type     string
	Config   map[string]any
	Required bool
}

// persistCandidateVerification 在 CompleteAttempt 的同一事务内完成：候选 Artifact、
// VerificationRun、全部 GateResult。三个 system.* Gate 永远由确定性代码计算。
func persistCandidateVerification(ctx context.Context, tx pgx.Tx, attempt *execution.Attempt,
	result execution.ExecutionResult, now time.Time,
) (verificationClosure, error) {
	var closure verificationClosure
	var tenantID, runID uuid.UUID
	var outputSpecRaw, sideEffectPolicyRaw []byte
	if err := tx.QueryRow(ctx, `
		SELECT tenant_id,swarm_id,output_spec,side_effect_policy
		FROM tasks WHERE id=$1 FOR UPDATE`, attempt.TaskID).
		Scan(&tenantID, &runID, &outputSpecRaw, &sideEffectPolicyRaw); err != nil {
		return closure, mapReadError("读取验收 Task Contract", err)
	}

	outputRaw, err := marshalJSON(result.Output, "candidate_artifact.output")
	if err != nil {
		return closure, err
	}
	// Gate 在 JSON 持久化边界后的规范值上执行，避免 []string 与 []any、int 与
	// float64 等 Go 表示差异造成同一 JSON 文档得到不同验收结论。
	var normalizedOutput map[string]any
	if err := json.Unmarshal(outputRaw, &normalizedOutput); err != nil {
		return closure, fmt.Errorf("规范化 candidate output: %w", err)
	}
	verificationResult := result
	verificationResult.Output = normalizedOutput
	// Runtime 已把原始错误归一化成 FailureClass/RecoveryAction/RetryLevel。
	// 失败账本与候选产物共用当前事务，保证 Reviewer 不会看到只有失败输出、
	// 却缺少可恢复策略事实的中间状态。
	if err := persistRuntimeFailureRecord(ctx, tx, tenantID, runID, attempt.TaskID,
		attempt.ID, normalizedOutput); err != nil {
		return closure, err
	}
	policyViolations, err := collectDeterministicPolicyViolations(
		ctx, tx, attempt.ID, sideEffectPolicyRaw, result.PolicyViolations,
	)
	if err != nil {
		return closure, err
	}
	verificationResult.PolicyViolations = policyViolations
	outputHash := sha256Hex(outputRaw)
	artifactID := stableExecutionID("candidate-artifact", attempt.ID.String())
	artifactURI := "swarmos://attempts/" + attempt.ID.String() + "/candidate-output"
	metadata, err := marshalJSON(map[string]any{
		"output": result.Output,
		// checks_claimed 仅用于审计；system Gate 从不读取它。
		"checks_claimed": result.Checks,
	}, "candidate_artifact.metadata")
	if err != nil {
		return closure, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO artifacts(
			id,tenant_id,run_id,task_id,attempt_id,artifact_type,uri,version,content_hash,
			metadata,name,status,media_type,schema_version,version_no,size_bytes,validation,created_at
		) VALUES($1,$2,$3,$4,$5,'CANDIDATE_OUTPUT',$6,'1',$7,$8,$9,'VALIDATING',
		         'application/json','1',1,$10,'{}'::jsonb,$11)
		ON CONFLICT(id) DO NOTHING`, artifactID, tenantID, runID, attempt.TaskID, attempt.ID,
		artifactURI, outputHash, metadata, "candidate-output", len(outputRaw), now); err != nil {
		return closure, mapWriteError("保存候选 Artifact", err)
	}
	var persistedHash string
	if err := tx.QueryRow(ctx, `SELECT content_hash FROM artifacts WHERE id=$1`, artifactID).
		Scan(&persistedHash); err != nil {
		return closure, mapReadError("校验候选 Artifact", err)
	}
	if persistedHash != outputHash {
		return closure, fmt.Errorf("候选 Artifact 幂等键已被不同输出占用")
	}

	var outputSpec map[string]any
	if err := json.Unmarshal(outputSpecRaw, &outputSpec); err != nil {
		return closure, fmt.Errorf("解析 task.output_spec: %w", err)
	}
	if err := ensureSystemAcceptanceGates(ctx, tx, tenantID, attempt.TaskID, outputSpec); err != nil {
		return closure, err
	}
	gates, err := loadAcceptanceGates(ctx, tx, attempt.TaskID)
	if err != nil {
		return closure, err
	}

	verificationID := stableExecutionID("verification-run", attempt.ID.String())
	if _, err := tx.Exec(ctx, `
		INSERT INTO verification_runs(
			id,tenant_id,run_id,task_id,attempt_id,status,verifier_version,started_at,created_at
		) VALUES($1,$2,$3,$4,$5,'RUNNING','builtin-deterministic/v1',$6,$6)
		ON CONFLICT(id) DO NOTHING`, verificationID, tenantID, runID, attempt.TaskID, attempt.ID, now); err != nil {
		return closure, mapWriteError("创建 VerificationRun", err)
	}

	requiredPassed, policyPassed := true, true
	failed := make([]string, 0)
	resultIDs := make([]uuid.UUID, 0, len(gates))
	for _, gate := range gates {
		status, excerpt, metrics := evaluateAcceptanceGate(gate, verificationResult)
		if gate.Required && status != gatePassed {
			requiredPassed = false
			failed = append(failed, gate.Key)
		}
		if gate.Key == "system.policy" && status != gatePassed {
			policyPassed = false
		}
		metricsRaw, marshalErr := marshalJSON(metrics, "gate_result.metrics")
		if marshalErr != nil {
			return closure, marshalErr
		}
		gateResultID := stableExecutionID("gate-result", verificationID.String(), gate.ID.String())
		if _, err := tx.Exec(ctx, `
			INSERT INTO gate_results(
				id,tenant_id,verification_run_id,gate_id,status,metrics,evidence_artifact_id,
				output_excerpt,error_message,started_at,finished_at
			) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'',$9,$9)
			ON CONFLICT(verification_run_id,gate_id) DO NOTHING`, gateResultID, tenantID,
			verificationID, gate.ID, status, metricsRaw, artifactID, truncateExcerpt(excerpt), now); err != nil {
			return closure, mapWriteError("保存 GateResult", err)
		}
		// 唯一约束是 (verification_run_id,gate_id)。若数据库中已有兼容版本生成的
		// 随机 ID，读取真实主键，Manifest 不能引用本地推导但并不存在的 UUID。
		if err := tx.QueryRow(ctx, `
			SELECT id FROM gate_results WHERE verification_run_id=$1 AND gate_id=$2`,
			verificationID, gate.ID).Scan(&gateResultID); err != nil {
			return closure, mapReadError("读取幂等 GateResult", err)
		}
		resultIDs = append(resultIDs, gateResultID)
	}
	sort.Strings(failed)
	verificationStatus := "PASSED"
	if !requiredPassed {
		verificationStatus = "FAILED"
	}
	if _, err := tx.Exec(ctx, `
		UPDATE verification_runs SET status=$2,finished_at=$3
		WHERE id=$1 AND status IN ('RUNNING',$2)`, verificationID, verificationStatus, now); err != nil {
		return closure, fmt.Errorf("完成 VerificationRun: %w", err)
	}
	validation, err := marshalJSON(map[string]any{
		"verification_run_id": verificationID,
		"required_passed":     requiredPassed,
		"failed_gates":        failed,
	}, "artifact.validation")
	if err != nil {
		return closure, err
	}
	if _, err := tx.Exec(ctx, `UPDATE artifacts SET validation=$2 WHERE id=$1`, artifactID, validation); err != nil {
		return closure, fmt.Errorf("记录 Artifact 验收结果: %w", err)
	}
	return verificationClosure{
		TenantID: tenantID, RunID: runID, ArtifactID: artifactID,
		VerificationID: verificationID, RequiredPassed: requiredPassed,
		PolicyPassed: policyPassed, FailedGateKeys: failed, GateResultIDs: resultIDs,
	}, nil
}

// ensureSystemAcceptanceGates 把平台不可绕过的硬门写进同一份 Gate 账本。
func ensureSystemAcceptanceGates(ctx context.Context, tx pgx.Tx, tenantID, taskID uuid.UUID,
	outputSpec map[string]any,
) error {
	schema := extractOutputSchema(outputSpec)
	specs := []struct {
		key, gateType string
		config        map[string]any
	}{
		{"system.output_nonempty", "ARTIFACT", map[string]any{}},
		{"system.output_json_schema", "JSON_SCHEMA", map[string]any{"schema": schema}},
		{"system.policy", "POLICY", map[string]any{}},
	}
	for _, spec := range specs {
		config, err := marshalJSON(spec.config, "system_gate.config")
		if err != nil {
			return err
		}
		id := stableExecutionID("acceptance-gate", taskID.String(), spec.key)
		if _, err := tx.Exec(ctx, `
			INSERT INTO acceptance_gates(id,tenant_id,task_id,gate_key,gate_type,config,required,version)
			VALUES($1,$2,$3,$4,$5,$6,true,1)
			ON CONFLICT(task_id,gate_key,version) DO UPDATE
			SET gate_type=EXCLUDED.gate_type,config=EXCLUDED.config,required=true`,
			id, tenantID, taskID, spec.key, spec.gateType, config); err != nil {
			return mapWriteError("创建系统 AcceptanceGate", err)
		}
	}
	return nil
}

func loadAcceptanceGates(ctx context.Context, tx pgx.Tx, taskID uuid.UUID) ([]persistedGate, error) {
	rows, err := tx.Query(ctx, `
		SELECT id,gate_key,gate_type,config,required
		FROM acceptance_gates WHERE task_id=$1 ORDER BY gate_key,id`, taskID)
	if err != nil {
		return nil, fmt.Errorf("读取 AcceptanceGate: %w", err)
	}
	defer rows.Close()
	values := make([]persistedGate, 0)
	for rows.Next() {
		var value persistedGate
		var raw []byte
		if err := rows.Scan(&value.ID, &value.Key, &value.Type, &raw, &value.Required); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(raw, &value.Config); err != nil {
			return nil, fmt.Errorf("解析 AcceptanceGate %s config: %w", value.Key, err)
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

// collectDeterministicPolicyViolations 从数据库事实计算策略结果。Runtime 上报只能增加违规，
// 不能通过提交空数组抹掉 DENIED ToolCall、悬而未决的 Effect 或缺失审批。
func collectDeterministicPolicyViolations(ctx context.Context, tx pgx.Tx, attemptID uuid.UUID,
	rawPolicy []byte, reported []string,
) ([]string, error) {
	type sideEffectPolicy struct {
		MaxRisk         string   `json:"max_risk"`
		AllowedEffects  []string `json:"allowed_effects"`
		RequireApproval bool     `json:"require_approval"`
	}
	var policy sideEffectPolicy
	if len(rawPolicy) > 0 {
		if err := json.Unmarshal(rawPolicy, &policy); err != nil {
			return nil, fmt.Errorf("解析 task.side_effect_policy: %w", err)
		}
	}
	violations := make(map[string]struct{})
	for _, value := range reported {
		if value = strings.TrimSpace(value); value != "" {
			violations["Runtime 报告: "+value] = struct{}{}
		}
	}
	var deniedCalls int
	if err := tx.QueryRow(ctx, `
		SELECT count(*) FROM tool_calls WHERE attempt_id=$1 AND status='DENIED'`, attemptID).
		Scan(&deniedCalls); err != nil {
		return nil, fmt.Errorf("检查 DENIED ToolCall: %w", err)
	}
	if deniedCalls > 0 {
		violations[fmt.Sprintf("存在 %d 个被策略拒绝的 ToolCall", deniedCalls)] = struct{}{}
	}

	allowed := make(map[string]struct{}, len(policy.AllowedEffects))
	for _, effectType := range policy.AllowedEffects {
		allowed[strings.ToLower(strings.TrimSpace(effectType))] = struct{}{}
	}
	rows, err := tx.Query(ctx, `
		SELECT effect_type,risk_level,status,approval_interaction_id
		FROM effects WHERE attempt_id=$1 ORDER BY id`, attemptID)
	if err != nil {
		return nil, fmt.Errorf("读取 Attempt Effect: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var effectType, risk, status string
		var approvalID *uuid.UUID
		if err := rows.Scan(&effectType, &risk, &status, &approvalID); err != nil {
			return nil, err
		}
		if status != "SUCCEEDED" && status != "COMPENSATED" {
			violations[fmt.Sprintf("Effect %s 状态尚未安全收敛: %s", effectType, status)] = struct{}{}
		}
		if len(allowed) > 0 {
			if _, ok := allowed[strings.ToLower(strings.TrimSpace(effectType))]; !ok {
				violations["Effect 不在 Task 允许列表: "+effectType] = struct{}{}
			}
		}
		if maximum, constrained := policyRiskRank(policy.MaxRisk); constrained {
			if actual, _ := policyRiskRank(risk); actual > maximum {
				violations[fmt.Sprintf("Effect %s 风险 %s 超过上限 %s", effectType, risk, policy.MaxRisk)] = struct{}{}
			}
		}
		if policy.RequireApproval {
			if actual, _ := policyRiskRank(risk); actual >= 2 && approvalID == nil {
				violations["高风险 Effect 缺少审批证据: "+effectType] = struct{}{}
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	result := make([]string, 0, len(violations))
	for value := range violations {
		result = append(result, value)
	}
	sort.Strings(result)
	return result, nil
}

func policyRiskRank(value string) (int, bool) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "R0_READ_ONLY", "READ-ONLY", "READ_ONLY":
		return 0, true
	case "R1_SANDBOX_WRITE", "SANDBOX":
		return 1, true
	case "R2_EXTERNAL_REVERSIBLE", "TRUSTED":
		return 2, true
	case "R3_PRODUCTION_DESTRUCTIVE", "PRODUCTION":
		return 3, true
	default:
		return 0, false
	}
}

// evaluateAcceptanceGate 的 system 分支完全忽略 result.Checks，确保模型把任意 check
// 自报为 true 也不能越过空输出、JSON Schema 或策略违规硬门。
func evaluateAcceptanceGate(gate persistedGate, result execution.ExecutionResult) (string, string, map[string]any) {
	metrics := map[string]any{"required": gate.Required, "deterministic": true}
	switch gate.Key {
	case "system.output_nonempty":
		if len(result.Output) == 0 {
			return gateFailed, "候选输出为空", metrics
		}
		return gatePassed, "候选输出非空", metrics
	case "system.output_json_schema":
		return evaluateSchemaGate(gate.Config, result.Output, metrics)
	case "system.policy":
		if len(result.PolicyViolations) > 0 {
			return gateFailed, "策略违规: " + strings.Join(result.PolicyViolations, "; "), metrics
		}
		return gatePassed, "未发现策略违规", metrics
	}

	switch strings.ToUpper(gate.Type) {
	case "JSON_SCHEMA":
		return evaluateSchemaGate(gate.Config, result.Output, metrics)
	case "POLICY":
		if len(result.PolicyViolations) > 0 {
			return gateFailed, "策略违规: " + strings.Join(result.PolicyViolations, "; "), metrics
		}
		return gatePassed, "策略检查通过", metrics
	case "ARTIFACT":
		if len(result.Output) == 0 {
			return gateFailed, "没有候选 Artifact 内容", metrics
		}
		return gatePassed, "候选 Artifact 已持久化", metrics
	default:
		// COMMAND/UNIT_TEST 等 Gate 的布尔值由受控执行器产生。缺失证据按失败处理，
		// 绝不把模型自然语言中的“已通过”解析为机器证据。
		metrics["deterministic"] = false
		if lookupCheck(result.Checks, gate.Key) {
			return gatePassed, "受控执行器证据通过", metrics
		}
		return gateFailed, "缺少受控执行器通过证据", metrics
	}
}

func evaluateSchemaGate(config map[string]any, output map[string]any, metrics map[string]any) (string, string, map[string]any) {
	schema := config
	if nested, ok := config["schema"].(map[string]any); ok {
		schema = nested
	}
	errors := validateJSONSchema(output, schema, "$")
	metrics["schema_errors"] = len(errors)
	if len(errors) > 0 {
		return gateFailed, strings.Join(errors, "; "), metrics
	}
	return gatePassed, "JSON Schema 验证通过", metrics
}

func lookupCheck(checks map[string]bool, key string) bool {
	if checks[key] {
		return true
	}
	for name, passed := range checks {
		if strings.EqualFold(strings.TrimSpace(name), strings.TrimSpace(key)) {
			return passed
		}
	}
	return false
}

func extractOutputSchema(outputSpec map[string]any) map[string]any {
	if schema, ok := outputSpec["schema"].(map[string]any); ok {
		return schema
	}
	if _, looksLikeSchema := outputSpec["properties"]; looksLikeSchema {
		return outputSpec
	}
	return map[string]any{}
}

// validateJSONSchema 实现验收闭环所需的确定性 JSON Schema 子集。未知关键字保持向前兼容，
// 已支持 type/required/properties/additionalProperties/enum 及常用长度、数值边界。
func validateJSONSchema(value any, schema map[string]any, path string) []string {
	if len(schema) == 0 {
		return nil
	}
	errors := make([]string, 0)
	if rawType, exists := schema["type"]; exists && !matchesJSONType(value, rawType) {
		return []string{fmt.Sprintf("%s 类型不符合 %v", path, rawType)}
	}
	if enum, ok := schema["enum"].([]any); ok {
		matched := false
		for _, candidate := range enum {
			if reflect.DeepEqual(value, candidate) {
				matched = true
				break
			}
		}
		if !matched {
			errors = append(errors, path+" 不在 enum 中")
		}
	}
	if object, ok := value.(map[string]any); ok {
		if required, ok := schema["required"].([]any); ok {
			for _, item := range required {
				name, _ := item.(string)
				if _, exists := object[name]; name != "" && !exists {
					errors = append(errors, path+" 缺少必填字段 "+name)
				}
			}
		}
		properties, _ := schema["properties"].(map[string]any)
		for name, propertySchema := range properties {
			child, exists := object[name]
			childSchema, ok := propertySchema.(map[string]any)
			if exists && ok {
				errors = append(errors, validateJSONSchema(child, childSchema, path+"."+name)...)
			}
		}
		if allow, ok := schema["additionalProperties"].(bool); ok && !allow {
			for name := range object {
				if _, declared := properties[name]; !declared {
					errors = append(errors, path+" 含未声明字段 "+name)
				}
			}
		}
	}
	if text, ok := value.(string); ok {
		if min, ok := numberAsInt(schema["minLength"]); ok && len([]rune(text)) < min {
			errors = append(errors, fmt.Sprintf("%s 长度小于 %d", path, min))
		}
		if max, ok := numberAsInt(schema["maxLength"]); ok && len([]rune(text)) > max {
			errors = append(errors, fmt.Sprintf("%s 长度大于 %d", path, max))
		}
	}
	if list, ok := value.([]any); ok {
		if min, ok := numberAsInt(schema["minItems"]); ok && len(list) < min {
			errors = append(errors, fmt.Sprintf("%s 元素少于 %d", path, min))
		}
		if itemSchema, ok := schema["items"].(map[string]any); ok {
			for index, item := range list {
				errors = append(errors, validateJSONSchema(item, itemSchema, fmt.Sprintf("%s[%d]", path, index))...)
			}
		}
	}
	if number, ok := numberAsFloat(value); ok {
		if min, exists := numberAsFloat(schema["minimum"]); exists && number < min {
			errors = append(errors, fmt.Sprintf("%s 小于 minimum %v", path, min))
		}
		if max, exists := numberAsFloat(schema["maximum"]); exists && number > max {
			errors = append(errors, fmt.Sprintf("%s 大于 maximum %v", path, max))
		}
	}
	sort.Strings(errors)
	return errors
}

func matchesJSONType(value any, raw any) bool {
	types := make([]string, 0, 1)
	switch typed := raw.(type) {
	case string:
		types = append(types, typed)
	case []any:
		for _, item := range typed {
			if text, ok := item.(string); ok {
				types = append(types, text)
			}
		}
	}
	for _, expected := range types {
		switch strings.ToLower(expected) {
		case "object":
			_, ok := value.(map[string]any)
			if ok {
				return true
			}
		case "array":
			_, ok := value.([]any)
			if ok {
				return true
			}
		case "string":
			_, ok := value.(string)
			if ok {
				return true
			}
		case "boolean":
			_, ok := value.(bool)
			if ok {
				return true
			}
		case "number":
			_, ok := numberAsFloat(value)
			if ok {
				return true
			}
		case "integer":
			if number, ok := numberAsFloat(value); ok && number == float64(int64(number)) {
				return true
			}
		case "null":
			if value == nil {
				return true
			}
		}
	}
	return len(types) == 0
}

func numberAsFloat(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case json.Number:
		value, err := typed.Float64()
		return value, err == nil
	default:
		return 0, false
	}
}

func numberAsInt(value any) (int, bool) {
	number, ok := numberAsFloat(value)
	return int(number), ok
}

func sha256Hex(raw []byte) string {
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func stableExecutionID(parts ...string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join(parts, "\x1f")))
}

func truncateExcerpt(value string) string {
	runes := []rune(value)
	if len(runes) > 2000 {
		return string(runes[:2000])
	}
	return value
}
