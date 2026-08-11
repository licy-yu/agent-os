package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/licy-yu/agent-os/internal/domain"
	"github.com/licy-yu/agent-os/internal/effect"
	"github.com/licy-yu/agent-os/internal/execution"
	"github.com/licy-yu/agent-os/internal/toolgateway"
)

type toolEffectRow struct {
	ID                    uuid.UUID
	AttemptID             uuid.UUID
	ToolCallID            *uuid.UUID
	Status                effect.Status
	RequestHash           string
	Risk                  effect.RiskLevel
	Result                map[string]any
	ExternalRef           string
	ErrorCode             string
	ErrorMessage          string
	ApprovalInteractionID *uuid.UUID
	AuthorizationBlocked  bool
	BlockReason           string
	Version               int64
	UpdatedAt             time.Time
}

type gatewayEffectPolicy struct {
	MaxRisk         string   `json:"max_risk"`
	AllowedEffects  []string `json:"allowed_effects"`
	RequireApproval bool     `json:"require_approval"`
}

// finishToolEffectUpdateSQL 使用 jsonb 顶层合并，而不是整列替换 sanitized_result。
// Adapter 只能写入 result 键；审批流程已经写入的 _authorization 审计证据因此会保留。
const finishToolEffectUpdateSQL = `
	UPDATE effects SET status=$5,
		sanitized_result=COALESCE(sanitized_result,'{}'::jsonb) || $6::jsonb,
		external_ref=NULLIF($7,''),
		error_code=NULLIF($8,''),error_message=NULLIF($9,''),reconcile_after=$10,
		finished_at=$11,version=version+1,updated_at=$11
	WHERE id=$1 AND tenant_id=$2 AND attempt_id=$3 AND fencing_token=$4 AND status='EXECUTING'
	RETURNING version`

// PrepareToolEffect 是 R2/R3 的唯一准备入口。它在 Attempt 行锁和 tenant+idempotency_key
// advisory lock 下完成同键异参判定；R3 的 Effect 与 WAITING Interaction 也在本事务原子创建。
func (r *Repository) PrepareToolEffect(ctx context.Context, owner execution.AttemptOwner,
	request toolgateway.EffectRequest,
) (*toolgateway.EffectPermit, error) {
	if !owner.Valid() || request.ToolCallID == uuid.Nil || !request.RiskLevel.Valid() ||
		strings.TrimSpace(request.IdempotencyKey) == "" || len(request.IdempotencyKey) > 256 {
		return nil, fmt.Errorf("%w: Effect 请求标识、风险或幂等键非法", domain.ErrConflict)
	}
	candidate, err := effect.Prepare(uuid.New(), owner.AttemptID, request.IdempotencyKey,
		request.RiskLevel, request.Request)
	if err != nil {
		return nil, err
	}
	if candidate.RequestHash != request.RequestHash {
		return nil, fmt.Errorf("%w: Gateway request_hash 与规范请求不一致", effect.ErrIdempotencyConflict)
	}
	if request.RequestedAt.IsZero() {
		request.RequestedAt = time.Now().UTC()
	}
	var permit *toolgateway.EffectPermit
	err = r.withTx(ctx, func(tx pgx.Tx) error {
		attempt, err := lockOwnedAttempt(ctx, tx, owner)
		if err != nil {
			return err
		}
		if attempt.Status != execution.AttemptRunning {
			return fmt.Errorf("%w: Attempt 已不在 RUNNING", domain.ErrConflict)
		}
		var tenantID, runID uuid.UUID
		var rawPolicy []byte
		if err := tx.QueryRow(ctx, `
			SELECT tenant_id,swarm_id,side_effect_policy FROM tasks WHERE id=$1`, attempt.TaskID).
			Scan(&tenantID, &runID, &rawPolicy); err != nil {
			return mapReadError("读取 Effect Task 策略", err)
		}
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`,
			tenantID.String()+":"+request.IdempotencyKey); err != nil {
			return fmt.Errorf("锁定 Effect 幂等键: %w", err)
		}
		current, err := loadToolEffectByKey(ctx, tx, tenantID, request.IdempotencyKey)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if current != nil {
			if current.AttemptID != owner.AttemptID {
				return fmt.Errorf("%w: Effect 属于其它 Attempt，必须走受控接管", domain.ErrConflict)
			}
			if current.RequestHash != request.RequestHash {
				return fmt.Errorf("%w: 同一幂等键对应不同请求", effect.ErrIdempotencyConflict)
			}
			permit, err = permitForExistingEffect(ctx, tx, tenantID, current, request.RequestedAt)
			return err
		}

		// ToolCall 必须已由同一 owner 完成额度预留；Effect 不能凭空关联其它 Attempt 的审计行。
		var toolCallExists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(
			SELECT 1 FROM tool_calls WHERE id=$1 AND attempt_id=$2)`, request.ToolCallID, owner.AttemptID).
			Scan(&toolCallExists); err != nil {
			return err
		}
		if !toolCallExists {
			return fmt.Errorf("%w: Effect 缺少对应 ToolCall 审计", domain.ErrConflict)
		}
		var policy gatewayEffectPolicy
		if len(rawPolicy) > 0 {
			if err := json.Unmarshal(rawPolicy, &policy); err != nil {
				return fmt.Errorf("解析 side_effect_policy: %w", err)
			}
		}
		sanitizedRequest, err := marshalJSON(sanitizeEffectValue(map[string]any{
			"tool_name": request.Request.ToolName, "operation": request.Request.Operation,
			"resource": request.Request.Resource, "arguments": request.Request.Arguments,
			"credential_ref": request.Request.CredentialRef,
		}), "effect.sanitized_request")
		if err != nil {
			return err
		}
		effectID := stableGatewayID("effect", tenantID.String(), request.IdempotencyKey)
		if _, err := tx.Exec(ctx, `
			INSERT INTO effects(
				id,tenant_id,run_id,task_id,attempt_id,tool_call_id,idempotency_key,effect_type,
				risk_level,status,request_hash,sanitized_request,sanitized_result,
				compensation_spec,fencing_token,version,prepared_at,updated_at
			) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'PREPARED',$10,$11,'{}'::jsonb,
			         '{}'::jsonb,$12,1,$13,$13)`, effectID, tenantID, runID, attempt.TaskID,
			owner.AttemptID, request.ToolCallID, request.IdempotencyKey, request.EffectType,
			request.RiskLevel, request.RequestHash, sanitizedRequest, owner.FencingToken,
			request.RequestedAt); err != nil {
			return mapWriteError("创建 PREPARED Effect", err)
		}
		if err := insertTenantOutbox(ctx, tx, tenantID, "effect", effectID, "effect.prepared", 1, map[string]any{
			"id": effectID, "run_id": runID, "task_id": attempt.TaskID,
			"risk_level": request.RiskLevel, "idempotency_key": request.IdempotencyKey,
		}); err != nil {
			return err
		}

		if reason := effectPolicyDenial(policy, request.RiskLevel, request.EffectType); reason != "" {
			metadata, _ := marshalJSON(map[string]any{
				"_authorization": map[string]any{"blocked": true, "reason": reason},
			}, "effect.authorization")
			if _, err := tx.Exec(ctx, `
				UPDATE effects SET sanitized_result=$2,version=version+1,updated_at=$3 WHERE id=$1`,
				effectID, metadata, request.RequestedAt); err != nil {
				return err
			}
			permit = &toolgateway.EffectPermit{
				EffectID: effectID, Disposition: toolgateway.EffectDenied,
				Status: effect.StatusPrepared, Version: 2, Reason: reason,
			}
			return nil
		}

		requiresApproval := request.RiskLevel == effect.RiskR3ProductionDestructive ||
			(request.RiskLevel == effect.RiskR2ExternalReversible && policy.RequireApproval)
		if requiresApproval {
			interactionID := stableGatewayID("effect-approval", tenantID.String(), request.IdempotencyKey)
			payload, _ := marshalJSON(map[string]any{
				"effect_id": effectID, "effect_type": request.EffectType,
				"risk_level": request.RiskLevel, "request": sanitizeEffectValue(request.Request.Arguments),
			}, "effect.approval.payload")
			actions, _ := marshalJSON([]string{"approve", "reject"}, "effect.approval.actions")
			interactionKey := "effect-approval:" + sha256Text(request.IdempotencyKey)
			command, err := tx.Exec(ctx, `
				INSERT INTO interactions(
					id,tenant_id,run_id,task_id,attempt_id,effect_id,interaction_type,status,
					title,description,payload,allowed_actions,response,idempotency_key,requested_by,
					expires_at,version,created_at,updated_at
				) VALUES($1,$2,$3,$4,$5,$6,'APPROVAL','WAITING',$7,$8,$9,$10,'{}'::jsonb,
				         $11,$12,$13,1,$14,$14)
				ON CONFLICT(tenant_id,idempotency_key) DO NOTHING`, interactionID, tenantID, runID,
				attempt.TaskID, owner.AttemptID, effectID, "审批外部副作用: "+request.EffectType,
				"只有批准后 Tool Gateway 才会调用外部 Adapter", payload, actions,
				interactionKey, "toolgateway/"+owner.WorkerID, request.RequestedAt.Add(24*time.Hour),
				request.RequestedAt)
			if err != nil {
				return mapWriteError("创建 Effect APPROVAL Interaction", err)
			}
			if command.RowsAffected() != 1 {
				return fmt.Errorf("%w: Effect 审批幂等键冲突", domain.ErrConflict)
			}
			if _, err := tx.Exec(ctx, `
				UPDATE effects SET approval_interaction_id=$2,version=version+1,updated_at=$3
				WHERE id=$1 AND status='PREPARED'`, effectID, interactionID, request.RequestedAt); err != nil {
				return err
			}
			if err := insertTenantOutbox(ctx, tx, tenantID, "interaction", interactionID,
				"interaction.waiting", 1, map[string]any{
					"id": interactionID, "run_id": runID, "effect_id": effectID,
					"type": "APPROVAL", "status": "WAITING",
				}); err != nil {
				return err
			}
			permit = &toolgateway.EffectPermit{
				EffectID: effectID, InteractionID: &interactionID,
				Disposition: toolgateway.EffectApprovalPending, Status: effect.StatusPrepared, Version: 2,
			}
			return nil
		}

		if _, err := tx.Exec(ctx, `
			UPDATE effects SET status='AUTHORIZED',authorized_at=$2,version=version+1,updated_at=$2
			WHERE id=$1 AND status='PREPARED'`, effectID, request.RequestedAt); err != nil {
			return err
		}
		if err := insertTenantOutbox(ctx, tx, tenantID, "effect", effectID,
			"effect.authorized", 2, map[string]any{
				"id": effectID, "run_id": runID, "authorized_by": "policy",
			}); err != nil {
			return err
		}
		permit = &toolgateway.EffectPermit{
			EffectID: effectID, Disposition: toolgateway.EffectExecute,
			Status: effect.StatusAuthorized, Version: 2,
		}
		return nil
	})
	return permit, err
}

func permitForExistingEffect(ctx context.Context, tx pgx.Tx, tenantID uuid.UUID,
	current *toolEffectRow, now time.Time,
) (*toolgateway.EffectPermit, error) {
	permit := &toolgateway.EffectPermit{
		EffectID: current.ID, InteractionID: current.ApprovalInteractionID,
		Status: current.Status, Version: current.Version,
	}
	if current.AuthorizationBlocked {
		permit.Disposition, permit.Reason = toolgateway.EffectDenied, current.BlockReason
		return permit, nil
	}
	switch current.Status {
	case effect.StatusPrepared:
		permit.Disposition = toolgateway.EffectApprovalPending
	case effect.StatusAuthorized:
		permit.Disposition = toolgateway.EffectExecute
	case effect.StatusExecuting:
		// 活跃 EXECUTING 可能仍在另一个重复消费者中执行；先禁止第二次 Adapter 调用。
		// 超过保守窗口仍未落终态时，原 Worker 已无法证明结果，收敛为 UNKNOWN。
		if current.UpdatedAt.Before(now.Add(-2 * time.Minute)) {
			if _, err := tx.Exec(ctx, `
				UPDATE effects SET status='UNKNOWN',error_code='EXECUTION_LOST',
					error_message='执行进程失联，外部结果不确定',reconcile_after=$3,
					version=version+1,updated_at=$2 WHERE tenant_id=$1 AND id=$4 AND status='EXECUTING'`,
				tenantID, now, now.Add(30*time.Second), current.ID); err != nil {
				return nil, err
			}
			permit.Status, permit.Version = effect.StatusUnknown, current.Version+1
		}
		permit.Disposition = toolgateway.EffectReconcileOnly
	case effect.StatusUnknown, effect.StatusReconciling:
		permit.Disposition = toolgateway.EffectReconcileOnly
	case effect.StatusSucceeded:
		permit.Disposition = toolgateway.EffectAlreadyDone
		if result, ok := current.Result["result"].(map[string]any); ok {
			permit.Result = result
		} else {
			permit.Result = current.Result
		}
	case effect.StatusFailed, effect.StatusCompensated:
		permit.Disposition = toolgateway.EffectTerminalFailure
		permit.Reason = "Effect 已终止，若需新操作必须使用新的 idempotency_key"
	default:
		permit.Disposition = toolgateway.EffectReconcileOnly
	}
	return permit, nil
}

// BeginToolEffect 以 AUTHORIZED -> EXECUTING CAS 作为调用 Adapter 的最后一道闸门。
func (r *Repository) BeginToolEffect(ctx context.Context, owner execution.AttemptOwner,
	effectID uuid.UUID, requestHash string,
) error {
	return r.withTx(ctx, func(tx pgx.Tx) error {
		attempt, err := lockOwnedAttempt(ctx, tx, owner)
		if err != nil {
			return err
		}
		if attempt.Status != execution.AttemptRunning {
			return fmt.Errorf("%w: Attempt 已不在 RUNNING", domain.ErrConflict)
		}
		var tenantID, runID uuid.UUID
		if err := tx.QueryRow(ctx, `SELECT tenant_id,run_id FROM effects WHERE id=$1`, effectID).
			Scan(&tenantID, &runID); err != nil {
			return mapReadError("读取待执行 Effect", err)
		}
		var nextVersion int64
		err = tx.QueryRow(ctx, `
			UPDATE effects SET status='EXECUTING',started_at=now(),version=version+1,updated_at=now()
			WHERE id=$1 AND tenant_id=$2 AND attempt_id=$3 AND fencing_token=$4
			  AND request_hash=$5 AND status='AUTHORIZED'
			  AND COALESCE(sanitized_result #>> '{_authorization,blocked}','false')<>'true'
			RETURNING version`, effectID, tenantID, owner.AttemptID, owner.FencingToken, requestHash).
			Scan(&nextVersion)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("%w: Effect 未获授权、fence 失效或已开始执行", domain.ErrConflict)
			}
			return mapWriteError("Effect 进入 EXECUTING", err)
		}
		return insertTenantOutbox(ctx, tx, tenantID, "effect", effectID, "effect.executing", nextVersion,
			map[string]any{"id": effectID, "run_id": runID, "worker_id": owner.WorkerID})
	})
}

// FinishToolEffect 只接受 EXECUTING -> SUCCEEDED/UNKNOWN。UNKNOWN 设置 reconcile_after，
// 后续 Prepare 只返回 RECONCILE_ONLY，绝不会再次授予 Adapter 调用权。
func (r *Repository) FinishToolEffect(ctx context.Context, owner execution.AttemptOwner,
	effectID uuid.UUID, completion toolgateway.EffectCompletion,
) error {
	if completion.Status != effect.StatusSucceeded && completion.Status != effect.StatusUnknown {
		return fmt.Errorf("%w: Tool Gateway 只能提交 SUCCEEDED 或 UNKNOWN", effect.ErrInvalidTransition)
	}
	if completion.FinishedAt.IsZero() {
		completion.FinishedAt = time.Now().UTC()
	}
	sanitized, err := marshalJSON(map[string]any{
		"result": sanitizeEffectValue(completion.Result),
	}, "effect.sanitized_result")
	if err != nil {
		return err
	}
	return r.withTx(ctx, func(tx pgx.Tx) error {
		attempt, err := lockOwnedAttempt(ctx, tx, owner)
		if err != nil {
			return err
		}
		if attempt.Status != execution.AttemptRunning {
			return fmt.Errorf("%w: Attempt 已不在 RUNNING", domain.ErrConflict)
		}
		var tenantID, runID uuid.UUID
		if err := tx.QueryRow(ctx, `SELECT tenant_id,run_id FROM effects WHERE id=$1`, effectID).
			Scan(&tenantID, &runID); err != nil {
			return mapReadError("读取执行中 Effect", err)
		}
		reconcileAfter := any(nil)
		errorCode := ""
		if completion.Status == effect.StatusUnknown {
			reconcileAfter = completion.FinishedAt.Add(30 * time.Second)
			errorCode = "EXTERNAL_RESULT_UNKNOWN"
		}
		var nextVersion int64
		err = tx.QueryRow(ctx, finishToolEffectUpdateSQL,
			effectID, tenantID, owner.AttemptID, owner.FencingToken, completion.Status,
			sanitized, completion.ExternalRef, errorCode, completion.ErrorMessage, reconcileAfter,
			completion.FinishedAt).Scan(&nextVersion)
		if err != nil {
			if !errors.Is(err, pgx.ErrNoRows) {
				return mapWriteError("完成 Tool Effect", err)
			}
			current, readErr := loadToolEffectByID(ctx, tx, tenantID, effectID)
			if readErr == nil && sameToolEffectCompletion(current, completion, errorCode, sanitized) {
				return nil
			}
			return fmt.Errorf("%w: Effect 已由其它结果结束或 fence 失效", domain.ErrConflict)
		}
		event := "effect." + strings.ToLower(string(completion.Status))
		return insertTenantOutbox(ctx, tx, tenantID, "effect", effectID, event, nextVersion, map[string]any{
			"id": effectID, "run_id": runID, "status": completion.Status,
			"external_ref": completion.ExternalRef,
		})
	})
}

// sameToolEffectCompletion 只比较本次完成命令拥有的字段。sanitized_result 中的
// _authorization 属于审批流程，既不能被 Adapter 覆盖，也不应导致同结果重放失配。
func sameToolEffectCompletion(current *toolEffectRow, completion toolgateway.EffectCompletion,
	errorCode string, normalizedResult []byte,
) bool {
	var expected map[string]any
	if err := json.Unmarshal(normalizedResult, &expected); err != nil {
		return false
	}
	return current != nil && current.Status == completion.Status &&
		current.ExternalRef == completion.ExternalRef && current.ErrorCode == errorCode &&
		current.ErrorMessage == completion.ErrorMessage &&
		reflect.DeepEqual(current.Result["result"], expected["result"])
}

func loadToolEffectByKey(ctx context.Context, tx pgx.Tx, tenantID uuid.UUID, key string) (*toolEffectRow, error) {
	return scanToolEffect(tx.QueryRow(ctx, toolEffectSelect+`
		WHERE tenant_id=$1 AND idempotency_key=$2 FOR UPDATE`, tenantID, key))
}

func loadToolEffectByID(ctx context.Context, tx pgx.Tx, tenantID, id uuid.UUID) (*toolEffectRow, error) {
	return scanToolEffect(tx.QueryRow(ctx, toolEffectSelect+`
		WHERE tenant_id=$1 AND id=$2 FOR UPDATE`, tenantID, id))
}

const toolEffectSelect = `
	SELECT id,attempt_id,tool_call_id,status,request_hash,risk_level,sanitized_result,
	       COALESCE(external_ref,''),COALESCE(error_code,''),COALESCE(error_message,''),
	       approval_interaction_id,
	       (COALESCE(sanitized_result #>> '{_authorization,blocked}','false')='true'),
	       COALESCE(sanitized_result #>> '{_authorization,reason}',''),version,updated_at
	FROM effects`

func scanToolEffect(row rowScanner) (*toolEffectRow, error) {
	value := new(toolEffectRow)
	var raw []byte
	if err := row.Scan(&value.ID, &value.AttemptID, &value.ToolCallID, &value.Status,
		&value.RequestHash, &value.Risk, &raw, &value.ExternalRef, &value.ErrorCode,
		&value.ErrorMessage, &value.ApprovalInteractionID,
		&value.AuthorizationBlocked, &value.BlockReason, &value.Version, &value.UpdatedAt); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(raw, &value.Result); err != nil {
		return nil, err
	}
	return value, nil
}

func effectPolicyDenial(policy gatewayEffectPolicy, risk effect.RiskLevel, effectType string) string {
	if maximum, constrained := gatewayRiskRank(policy.MaxRisk); constrained {
		actual, _ := gatewayRiskRank(string(risk))
		if actual > maximum {
			return fmt.Sprintf("Effect 风险 %s 超过 Task 上限 %s", risk, policy.MaxRisk)
		}
	}
	if len(policy.AllowedEffects) > 0 {
		allowed := false
		for _, item := range policy.AllowedEffects {
			if strings.EqualFold(strings.TrimSpace(item), strings.TrimSpace(effectType)) {
				allowed = true
				break
			}
		}
		if !allowed {
			return "Effect 不在 Task 允许列表: " + effectType
		}
	}
	return ""
}

func gatewayRiskRank(value string) (int, bool) {
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

func stableGatewayID(parts ...string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join(parts, "\x1f")))
}

func sha256Text(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

// sanitizeEffectValue 只用于持久化/审批展示；请求哈希仍覆盖原始参数。任何看起来像凭据的
// key 都只留下 REDACTED，防止 Tool 参数把 Secret 带进 PostgreSQL 或控制台。
func sanitizeEffectValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		result := make(map[string]any, len(typed))
		for _, key := range keys {
			lower := strings.ToLower(key)
			if strings.Contains(lower, "password") || strings.Contains(lower, "secret") ||
				strings.Contains(lower, "token") || strings.Contains(lower, "authorization") ||
				strings.Contains(lower, "api_key") || strings.Contains(lower, "apikey") {
				result[key] = "[REDACTED]"
			} else {
				result[key] = sanitizeEffectValue(typed[key])
			}
		}
		return result
	case []any:
		result := make([]any, len(typed))
		for index := range typed {
			result[index] = sanitizeEffectValue(typed[index])
		}
		return result
	default:
		return value
	}
}
