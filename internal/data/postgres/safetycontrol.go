package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/licy-yu/agent-os/internal/domain"
	"github.com/licy-yu/agent-os/internal/interaction"
	"github.com/licy-yu/agent-os/internal/safetycontrol"
	"github.com/licy-yu/agent-os/internal/verification"
)

// 编译期断言可以在 Store 合同新增方法时立即让构建失败，避免某个生产读写路径被遗漏。
var _ safetycontrol.Store = (*Repository)(nil)

const safetyMutationConsumer = "swarmos.safetycontrol.mutations.v1"

// mutationEnvelope 是 processed_events.result 中的稳定格式。processed_events 原本用于
// “至少一次”消息消费去重；这里复用其 (tenant, consumer, event_id) 主键保存 HTTP 命令结果，
// 使业务变更、审计 Outbox 和幂等回放在同一个 PostgreSQL 事务里提交。
type mutationEnvelope struct {
	Operation   string                         `json:"operation"`
	RequestHash string                         `json:"requestHash"`
	Interaction *safetycontrol.InteractionView `json:"interaction,omitempty"`
	Effect      *safetycontrol.EffectView      `json:"effect,omitempty"`
}

func (e mutationEnvelope) result() *safetycontrol.MutationResult {
	return &safetycontrol.MutationResult{
		Operation: e.Operation, Interaction: e.Interaction, Effect: e.Effect,
	}
}

// mutationEventID 把最长 256 字符的用户幂等键映射成固定 UUID。tenant_id 已在主键中，
// 因而同租户同键必定竞争同一行；SHA-256 的前 128 位仅用作数据库键，不用于认证。
func mutationEventID(key string) uuid.UUID {
	digest := sha256.Sum256([]byte(key))
	value, _ := uuid.FromBytes(digest[:16])
	return value
}

// EnsureRunTenant 只使用 tenant_id + run_id 判定所有权。不存在和跨租户都返回同一种
// NotFound，避免控制台被用来探测其他租户的 Run ID。
func (r *Repository) EnsureRunTenant(ctx context.Context, tenantID, runID uuid.UUID) error {
	var exists bool
	if err := r.pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM swarms WHERE tenant_id=$1 AND id=$2)`, tenantID, runID).
		Scan(&exists); err != nil {
		return fmt.Errorf("校验 Run 租户边界: %w", err)
	}
	if !exists {
		return fmt.Errorf("%w: Run 不存在", domain.ErrNotFound)
	}
	return nil
}

// FindMutation 在进入状态机校验前提供快速回放。同键异参必须显式报错，不能把旧响应
// 当成当前请求的成功结果。最终并发正确性仍由写事务中的 advisory lock 保证。
func (r *Repository) FindMutation(ctx context.Context, tenantID uuid.UUID, key, requestHash string) (*safetycontrol.MutationResult, error) {
	envelope, found, err := readMutation(ctx, r.pool, tenantID, key)
	if err != nil || !found {
		return nil, err
	}
	if envelope.RequestHash != requestHash {
		return nil, fmt.Errorf("%w: 同一 Idempotency-Key 对应不同请求", safetycontrol.ErrIdempotencyConflict)
	}
	return envelope.result(), nil
}

// beginSafetyMutation 对 tenant+key 取事务级锁，关闭“两个请求都未读到幂等行”这一竞态。
// 锁字符串包含 tenant，既隔离租户，也不会泄漏明文 key 到持久化业务表。
func beginSafetyMutation(ctx context.Context, tx pgx.Tx, metadata safetycontrol.MutationMetadata) (*safetycontrol.MutationResult, error) {
	lockKey := metadata.TenantID.String() + ":" + metadata.IdempotencyKey
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, lockKey); err != nil {
		return nil, fmt.Errorf("获取安全命令幂等锁: %w", err)
	}
	envelope, found, err := readMutation(ctx, tx, metadata.TenantID, metadata.IdempotencyKey)
	if err != nil || !found {
		return nil, err
	}
	if envelope.RequestHash != metadata.RequestHash {
		return nil, fmt.Errorf("%w: 同一 Idempotency-Key 对应不同请求", safetycontrol.ErrIdempotencyConflict)
	}
	return envelope.result(), nil
}

type queryRower interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func readMutation(ctx context.Context, db queryRower, tenantID uuid.UUID, key string) (mutationEnvelope, bool, error) {
	var raw []byte
	err := db.QueryRow(ctx, `
		SELECT result FROM processed_events
		WHERE tenant_id=$1 AND consumer_name=$2 AND event_id=$3`,
		tenantID, safetyMutationConsumer, mutationEventID(key)).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return mutationEnvelope{}, false, nil
	}
	if err != nil {
		return mutationEnvelope{}, false, fmt.Errorf("读取安全命令幂等结果: %w", err)
	}
	var envelope mutationEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		// 数据库中的幂等记录损坏时必须失败关闭；继续执行可能造成外部副作用重复发生。
		return mutationEnvelope{}, false, fmt.Errorf("解析安全命令幂等结果: %w", err)
	}
	return envelope, true, nil
}

func finishSafetyMutation(ctx context.Context, tx pgx.Tx, metadata safetycontrol.MutationMetadata, result *safetycontrol.MutationResult) error {
	envelope := mutationEnvelope{Operation: metadata.Operation, RequestHash: metadata.RequestHash}
	if result != nil {
		envelope.Interaction = result.Interaction
		envelope.Effect = result.Effect
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("编码安全命令幂等结果: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO processed_events(tenant_id,consumer_name,event_id,result,processed_at)
		VALUES($1,$2,$3,$4,$5)`, metadata.TenantID, safetyMutationConsumer,
		mutationEventID(metadata.IdempotencyKey), raw, metadata.OccurredAt)
	if err != nil {
		return mapWriteError("保存安全命令幂等结果", err)
	}
	return nil
}

// ListInteractions 返回统一人工信箱。所有过滤条件都进入 SQL，绝不先按全局 ID 读取后
// 再在 Go 内存中判断租户。
func (r *Repository) ListInteractions(ctx context.Context, tenantID uuid.UUID, query safetycontrol.InteractionQuery) ([]safetycontrol.InteractionView, error) {
	sql := interactionViewSelect + ` WHERE i.tenant_id=$1`
	args := []any{tenantID}
	if query.RunID != uuid.Nil {
		args = append(args, query.RunID)
		sql += fmt.Sprintf(" AND i.run_id=$%d", len(args))
	}
	if query.Status != "" {
		args = append(args, query.Status)
		sql += fmt.Sprintf(" AND i.status=$%d", len(args))
	}
	args = append(args, query.Limit)
	sql += fmt.Sprintf(" ORDER BY i.created_at DESC,i.id DESC LIMIT $%d", len(args))
	rows, err := r.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("列出 Interactions: %w", err)
	}
	defer rows.Close()
	items := make([]safetycontrol.InteractionView, 0, query.Limit)
	for rows.Next() {
		item, err := scanInteractionView(rows)
		if err != nil {
			return nil, fmt.Errorf("扫描 Interaction: %w", err)
		}
		items = append(items, *item)
	}
	return items, rows.Err()
}

func (r *Repository) GetInteraction(ctx context.Context, tenantID, id uuid.UUID) (*safetycontrol.InteractionView, error) {
	item, err := scanInteractionView(r.pool.QueryRow(ctx, interactionViewSelect+`
		WHERE i.tenant_id=$1 AND i.id=$2`, tenantID, id))
	if err != nil {
		return nil, mapReadError("读取 Interaction", err)
	}
	return item, nil
}

// CreateInteraction 把 Interaction、可选 Effect 审批绑定、Outbox 与幂等结果原子提交。
func (r *Repository) CreateInteraction(ctx context.Context, record safetycontrol.CreateInteractionRecord) (*safetycontrol.InteractionView, error) {
	if record.Mutation.AggregateID != record.Interaction.ID {
		return nil, fmt.Errorf("%w: Interaction 与幂等聚合 ID 不一致", safetycontrol.ErrInvalidStoreResult)
	}
	if record.Binding != nil && (record.Interaction.EffectID == nil ||
		*record.Interaction.EffectID != record.Binding.EffectID ||
		record.Interaction.InteractionType != interaction.TypeApproval) {
		return nil, fmt.Errorf("%w: Effect 只能绑定指向自身的 APPROVAL Interaction", safetycontrol.ErrApprovalRequired)
	}
	var result *safetycontrol.InteractionView
	err := r.withTx(ctx, func(tx pgx.Tx) error {
		replay, err := beginSafetyMutation(ctx, tx, record.Mutation)
		if err != nil {
			return err
		}
		if replay != nil {
			if replay.Interaction == nil {
				return safetycontrol.ErrInvalidStoreResult
			}
			result = replay.Interaction
			return nil
		}
		if record.Interaction.TenantID != record.Mutation.TenantID {
			return safetycontrol.ErrTenantBoundary
		}
		payload, err := json.Marshal(record.Interaction.Payload)
		if err != nil {
			return fmt.Errorf("编码 Interaction payload: %w", err)
		}
		actions, err := json.Marshal(record.Interaction.AllowedActions)
		if err != nil {
			return fmt.Errorf("编码 Interaction actions: %w", err)
		}
		// SELECT/EXISTS 同时验证所有可选关联都位于同一 tenant/run，避免仅依赖全局外键。
		command, err := tx.Exec(ctx, `
			INSERT INTO interactions(
				id,tenant_id,run_id,task_id,attempt_id,effect_id,interaction_type,status,title,
				description,payload,allowed_actions,response,idempotency_key,requested_by,
				expires_at,version,created_at,updated_at
			)
			SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'{}'::jsonb,$13,$14,$15,$16,$17,$17
			WHERE EXISTS(SELECT 1 FROM swarms s WHERE s.id=$3 AND s.tenant_id=$2)
			  AND ($4::uuid IS NULL OR EXISTS(
				SELECT 1 FROM tasks t WHERE t.id=$4 AND t.tenant_id=$2 AND t.swarm_id=$3))
			  AND ($5::uuid IS NULL OR EXISTS(
				SELECT 1 FROM task_attempts a JOIN tasks t ON t.id=a.task_id
				WHERE a.id=$5 AND a.tenant_id=$2 AND t.swarm_id=$3))
			  AND ($6::uuid IS NULL OR EXISTS(
				SELECT 1 FROM effects e WHERE e.id=$6 AND e.tenant_id=$2 AND e.run_id=$3))`,
			record.Interaction.ID, record.Interaction.TenantID, record.Interaction.RunID,
			record.Interaction.TaskID, record.Interaction.AttemptID, record.Interaction.EffectID,
			record.Interaction.InteractionType, record.Interaction.Status, record.Interaction.Title,
			record.Interaction.Description, payload, actions, record.Mutation.IdempotencyKey,
			record.Interaction.RequestedBy, record.Interaction.ExpiresAt, record.Interaction.Version,
			record.Interaction.CreatedAt)
		if err != nil {
			return mapWriteError("创建 Interaction", err)
		}
		if command.RowsAffected() != 1 {
			return fmt.Errorf("%w: Interaction 关联不属于当前租户或 Run", domain.ErrConflict)
		}
		if record.Binding != nil {
			command, err = tx.Exec(ctx, `
				UPDATE effects SET approval_interaction_id=$4,version=version+1,updated_at=$5
				WHERE tenant_id=$1 AND id=$2 AND version=$3 AND run_id=$6
				  AND status='PREPARED' AND approval_interaction_id IS NULL`,
				record.Mutation.TenantID, record.Binding.EffectID,
				record.Binding.ExpectedEffectVersion, record.Interaction.ID,
				record.Mutation.OccurredAt, record.Interaction.RunID)
			if err != nil {
				return mapWriteError("绑定 Effect 审批", err)
			}
			if command.RowsAffected() != 1 {
				return fmt.Errorf("%w: Effect 审批绑定 CAS 失败", safetycontrol.ErrVersionConflict)
			}
			if err := insertTenantOutbox(ctx, tx, record.Mutation.TenantID, "effect",
				record.Binding.EffectID, "effect.approval_bound",
				record.Binding.ExpectedEffectVersion+1, map[string]any{
					"interaction_id": record.Interaction.ID, "run_id": record.Interaction.RunID,
					"actor": record.Mutation.Actor,
				}); err != nil {
				return err
			}
		}
		result, err = scanInteractionView(tx.QueryRow(ctx, interactionViewSelect+`
			WHERE i.tenant_id=$1 AND i.id=$2`, record.Mutation.TenantID, record.Interaction.ID))
		if err != nil {
			return fmt.Errorf("回读 Interaction: %w", err)
		}
		if err := insertTenantOutbox(ctx, tx, record.Mutation.TenantID, "interaction",
			record.Interaction.ID, "interaction.waiting", result.Version, map[string]any{
				"id": result.ID, "run_id": result.RunID, "type": result.InteractionType,
				"status": result.Status, "actor": record.Mutation.Actor,
			}); err != nil {
			return err
		}
		return finishSafetyMutation(ctx, tx, record.Mutation, &safetycontrol.MutationResult{
			Operation: record.Mutation.Operation, Interaction: result,
		})
	})
	return result, err
}

// ResolveInteraction 使用 status+version 做 CAS，并在同一事务内授权或永久封存 R3 Effect。
func (r *Repository) ResolveInteraction(ctx context.Context, record safetycontrol.ResolveInteractionRecord) (*safetycontrol.InteractionView, error) {
	if record.Mutation.AggregateID != record.InteractionID {
		return nil, fmt.Errorf("%w: Interaction 与幂等聚合 ID 不一致", safetycontrol.ErrInvalidStoreResult)
	}
	if record.Authorization != nil && record.Rejection != nil {
		return nil, fmt.Errorf("%w: 同一次审批不能同时批准和拒绝", safetycontrol.ErrInvalidStoreResult)
	}
	if record.ResumeWaitingAttempt && record.FailWaitingAttempt {
		return nil, fmt.Errorf("%w: 同一审批不能同时恢复和终止 Attempt", safetycontrol.ErrInvalidStoreResult)
	}
	if record.ResumeWaitingAttempt && record.Authorization == nil {
		return nil, fmt.Errorf("%w: 恢复 Attempt 必须携带 Effect 授权", safetycontrol.ErrInvalidStoreResult)
	}
	if record.FailWaitingAttempt && record.Rejection == nil {
		return nil, fmt.Errorf("%w: 终止 Attempt 必须携带 Effect 拒绝事实", safetycontrol.ErrInvalidStoreResult)
	}
	var result *safetycontrol.InteractionView
	err := r.withTx(ctx, func(tx pgx.Tx) error {
		replay, err := beginSafetyMutation(ctx, tx, record.Mutation)
		if err != nil {
			return err
		}
		if replay != nil {
			if replay.Interaction == nil {
				return safetycontrol.ErrInvalidStoreResult
			}
			result = replay.Interaction
			return nil
		}
		response, err := json.Marshal(record.Response)
		if err != nil {
			return fmt.Errorf("编码 Interaction response: %w", err)
		}
		command, err := tx.Exec(ctx, `
			UPDATE interactions
			SET status='RESOLVED',response=$6,resolved_by=$7,resolved_at=$8,
				version=version+1,updated_at=$8
			WHERE tenant_id=$1 AND id=$2 AND status=$3 AND version=$4
			  AND (expires_at IS NULL OR expires_at>$5)`,
			record.Mutation.TenantID, record.InteractionID, record.ExpectedStatus,
			record.ExpectedVersion, record.Mutation.OccurredAt, response,
			record.Response.RespondedBy, record.Response.RespondedAt)
		if err != nil {
			return mapWriteError("解决 Interaction", err)
		}
		if command.RowsAffected() != 1 {
			return fmt.Errorf("%w: Interaction 状态或版本已变化", safetycontrol.ErrVersionConflict)
		}

		if record.Authorization != nil {
			metadata, _ := json.Marshal(map[string]any{
				"blocked": false, "approvedBy": record.Authorization.ApprovedBy,
				"approvedAt": record.Authorization.ApprovedAt,
			})
			command, err = tx.Exec(ctx, `
				UPDATE effects AS target
				SET status=$5,authorized_at=$6,version=version+1,updated_at=$6,
					sanitized_result=jsonb_set(COALESCE(sanitized_result,'{}'::jsonb),
						'{_authorization}',$7::jsonb,true)
				WHERE target.tenant_id=$1 AND target.id=$2 AND target.version=$3 AND target.status=$4
				  AND target.approval_interaction_id=$8
				  AND EXISTS(SELECT 1 FROM interactions i
				      WHERE i.tenant_id=$1 AND i.id=$8 AND i.effect_id=target.id)
				  AND COALESCE(target.sanitized_result #>> '{_authorization,blocked}','false')<>'true'`,
				record.Mutation.TenantID, record.Authorization.EffectID,
				record.Authorization.ExpectedEffectVersion, record.Authorization.ExpectedStatus,
				record.Authorization.NextStatus, record.Authorization.ApprovedAt, metadata,
				record.InteractionID)
			if err != nil {
				return mapWriteError("授权 Effect", err)
			}
			if command.RowsAffected() != 1 {
				return fmt.Errorf("%w: Effect 已变化、已封存或审批证据不匹配", safetycontrol.ErrApprovalRequired)
			}
			if err := insertTenantOutbox(ctx, tx, record.Mutation.TenantID, "effect",
				record.Authorization.EffectID, "effect.authorized",
				record.Authorization.ExpectedEffectVersion+1, map[string]any{
					"interaction_id": record.InteractionID, "approved_by": record.Authorization.ApprovedBy,
				}); err != nil {
				return err
			}
			if record.ResumeWaitingAttempt {
				if err := resumeWaitingAttemptAfterApproval(ctx, tx, record.Mutation.TenantID,
					record.Authorization.EffectID, record.Mutation.OccurredAt); err != nil {
					return err
				}
			}
		}

		if record.Rejection != nil {
			// 拒绝不伪造成 Effect 失败；PREPARED 状态保留事实含义，_authorization.blocked
			// 则形成不可逆授权闸门，后续所有读取与 CAS 都会失败关闭。
			metadata, _ := json.Marshal(map[string]any{
				"blocked": true, "reason": record.Rejection.Reason,
				"rejectedBy": record.Rejection.RejectedBy, "rejectedAt": record.Rejection.RejectedAt,
			})
			command, err = tx.Exec(ctx, `
				UPDATE effects AS target
				SET version=version+1,updated_at=$5,
					sanitized_result=jsonb_set(COALESCE(sanitized_result,'{}'::jsonb),
						'{_authorization}',$6::jsonb,true)
				WHERE target.tenant_id=$1 AND target.id=$2 AND target.version=$3 AND target.status=$4
				  AND target.approval_interaction_id=$7
				  AND EXISTS(SELECT 1 FROM interactions i
				      WHERE i.tenant_id=$1 AND i.id=$7 AND i.effect_id=target.id)
				  AND COALESCE(target.sanitized_result #>> '{_authorization,blocked}','false')<>'true'`,
				record.Mutation.TenantID, record.Rejection.EffectID,
				record.Rejection.ExpectedEffectVersion, record.Rejection.ExpectedStatus,
				record.Rejection.RejectedAt, metadata, record.InteractionID)
			if err != nil {
				return mapWriteError("封存 Effect 授权", err)
			}
			if command.RowsAffected() != 1 {
				return fmt.Errorf("%w: Effect 已变化或审批证据不匹配", safetycontrol.ErrVersionConflict)
			}
			if _, err := tx.Exec(ctx, `
				UPDATE tool_calls tc
				SET status='DENIED',error_message=$3,finished_at=$4
				FROM effects e
				WHERE e.tenant_id=$1 AND e.id=$2 AND tc.id=e.tool_call_id
				  AND tc.attempt_id=e.attempt_id AND tc.status='STARTED'`,
				record.Mutation.TenantID, record.Rejection.EffectID,
				record.Rejection.Reason, record.Rejection.RejectedAt); err != nil {
				return mapWriteError("结束审批拒绝 ToolCall", err)
			}
			if err := insertTenantOutbox(ctx, tx, record.Mutation.TenantID, "effect",
				record.Rejection.EffectID, "effect.authorization_rejected",
				record.Rejection.ExpectedEffectVersion+1, map[string]any{
					"interaction_id": record.InteractionID, "rejected_by": record.Rejection.RejectedBy,
					"reason": record.Rejection.Reason, "authorization_blocked": true,
				}); err != nil {
				return err
			}
			if record.FailWaitingAttempt {
				if err := failWaitingAttemptAfterRejection(ctx, tx, record.Mutation.TenantID,
					record.Rejection.EffectID, record.Rejection.Reason,
					record.Mutation.OccurredAt); err != nil {
					return err
				}
			}
		}

		result, err = scanInteractionView(tx.QueryRow(ctx, interactionViewSelect+`
			WHERE i.tenant_id=$1 AND i.id=$2`, record.Mutation.TenantID, record.InteractionID))
		if err != nil {
			return fmt.Errorf("回读已解决 Interaction: %w", err)
		}
		if err := insertTenantOutbox(ctx, tx, record.Mutation.TenantID, "interaction",
			record.InteractionID, "interaction.resolved", result.Version, map[string]any{
				"id": result.ID, "run_id": result.RunID, "action": record.Response.Action,
				"actor": record.Response.RespondedBy, "status": result.Status,
			}); err != nil {
			return err
		}
		return finishSafetyMutation(ctx, tx, record.Mutation, &safetycontrol.MutationResult{
			Operation: record.Mutation.Operation, Interaction: result,
		})
	})
	return result, err
}

// resumeWaitingAttemptAfterApproval 把 Effect 授权和原 Task 的重新投递放在同一事务。
// 若数据来自升级前版本、Attempt 仍为 RUNNING，则保持兼容并不重复投递；新路径只接管
// 明确处于 WAITING 的 Attempt。
func resumeWaitingAttemptAfterApproval(ctx context.Context, tx pgx.Tx, tenantID, effectID uuid.UUID,
	now time.Time,
) error {
	return resumeWaitingAttempt(ctx, tx, tenantID, effectID, "WAITING_APPROVAL", now)
}

func resumeWaitingAttemptAfterReconcile(ctx context.Context, tx pgx.Tx, tenantID, effectID uuid.UUID,
	now time.Time,
) error {
	return resumeWaitingAttempt(ctx, tx, tenantID, effectID, "WAITING_EXTERNAL", now)
}

func resumeWaitingAttempt(ctx context.Context, tx pgx.Tx, tenantID, effectID uuid.UUID,
	waitingTaskStatus string, now time.Time,
) error {
	var attemptID, taskID, runID, agentID uuid.UUID
	var attemptStatus string
	err := tx.QueryRow(ctx, `
		SELECT e.attempt_id,e.task_id,e.run_id,a.agent_id,a.status
		FROM effects e JOIN task_attempts a ON a.id=e.attempt_id
		WHERE e.tenant_id=$1 AND e.id=$2 FOR UPDATE OF a`, tenantID, effectID).
		Scan(&attemptID, &taskID, &runID, &agentID, &attemptStatus)
	if err != nil {
		return mapReadError("读取审批关联 Attempt", err)
	}
	if attemptStatus != "WAITING" {
		return nil
	}

	var taskVersion int64
	err = tx.QueryRow(ctx, `
		UPDATE tasks
		SET status='ASSIGNED',version=version+1,updated_at=$5
		WHERE tenant_id=$1 AND id=$2 AND status=$6
		  AND assigned_agent_id=$3 AND swarm_id=$4
		RETURNING version`, tenantID, taskID, agentID, runID, now, waitingTaskStatus).Scan(&taskVersion)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: 待审批 Task 状态或 Agent 绑定已变化", safetycontrol.ErrVersionConflict)
		}
		return mapWriteError("重新分配审批 Task", err)
	}
	var agentVersion int64
	err = tx.QueryRow(ctx, `
		UPDATE agent_instances
		SET status='RESERVED',version=version+1,updated_at=$4
		WHERE tenant_id=$1 AND id=$2 AND status='WAITING_TOOL' AND current_task_id=$3
		RETURNING version`, tenantID, agentID, taskID, now).Scan(&agentVersion)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: 待审批 Agent 状态或 Task 绑定已变化", safetycontrol.ErrVersionConflict)
		}
		return mapWriteError("重新预留审批 Agent", err)
	}
	if err := insertTenantOutbox(ctx, tx, tenantID, "task", taskID, "task.assigned",
		taskVersion, map[string]any{
			"id": taskID, "run_id": runID, "agent_id": agentID,
			"attempt_id": attemptID, "resumed": true, "version": taskVersion,
		}); err != nil {
		return err
	}
	return insertTenantOutbox(ctx, tx, tenantID, "agent_instance", agentID,
		"agent.reserved", agentVersion, map[string]any{
			"id": agentID, "run_id": runID, "task_id": taskID,
			"resumed": true, "version": agentVersion,
		})
}

// failWaitingAttemptAfterRejection 让“拒绝”成为完整终态，而不是留下永远无法恢复的
// PREPARED Effect 与 WAITING Task。Effect 自身仍按安全合同保留 PREPARED+blocked 事实；
// Attempt、Task 和 Run 则以明确的审批拒绝原因失败，便于审计和告警。
func failWaitingAttemptAfterRejection(ctx context.Context, tx pgx.Tx, tenantID, effectID uuid.UUID,
	reason string, now time.Time,
) error {
	if strings.TrimSpace(reason) == "" {
		reason = "人工审批已拒绝"
	}
	return failWaitingAttempt(ctx, tx, tenantID, effectID, "WAITING_APPROVAL",
		"APPROVAL_REJECTED", "approval_rejected", reason, now)
}

func failWaitingAttemptAfterReconcile(ctx context.Context, tx pgx.Tx, tenantID, effectID uuid.UUID,
	reason string, now time.Time,
) error {
	if strings.TrimSpace(reason) == "" {
		reason = "外部对账确认 Effect 失败"
	}
	return failWaitingAttempt(ctx, tx, tenantID, effectID, "WAITING_EXTERNAL",
		"EFFECT_RECONCILED_FAILED", "effect_reconciled_failed", reason, now)
}

func failWaitingAttempt(ctx context.Context, tx pgx.Tx, tenantID, effectID uuid.UUID,
	waitingTaskStatus, failureCode, reasonCode, reason string, now time.Time,
) error {
	var attemptID, taskID, runID, agentID uuid.UUID
	var attemptStatus string
	err := tx.QueryRow(ctx, `
		SELECT e.attempt_id,e.task_id,e.run_id,a.agent_id,a.status
		FROM effects e JOIN task_attempts a ON a.id=e.attempt_id
		WHERE e.tenant_id=$1 AND e.id=$2 FOR UPDATE OF a`, tenantID, effectID).
		Scan(&attemptID, &taskID, &runID, &agentID, &attemptStatus)
	if err != nil {
		return mapReadError("读取拒绝关联 Attempt", err)
	}
	if attemptStatus != "WAITING" {
		return nil
	}
	reason = strings.TrimSpace(reason)
	command, err := tx.Exec(ctx, `
		UPDATE task_attempts
		SET status='ABORTED',finished_at=$3,error_code=$4,
		    error_message=$5,updated_at=$3
		WHERE tenant_id=$1 AND id=$2 AND status='WAITING'`,
		tenantID, attemptID, now, failureCode, reason)
	if err != nil {
		return mapWriteError("终止被拒绝 Attempt", err)
	}
	if command.RowsAffected() != 1 {
		return fmt.Errorf("%w: 待拒绝 Attempt 已变化", safetycontrol.ErrVersionConflict)
	}
	var taskVersion int64
	err = tx.QueryRow(ctx, `
		UPDATE tasks
		SET status='FAILED',assigned_agent_id=NULL,version=version+1,updated_at=$5
		WHERE tenant_id=$1 AND id=$2 AND swarm_id=$3 AND status=$6
		  AND assigned_agent_id=$4 RETURNING version`,
		tenantID, taskID, runID, agentID, now, waitingTaskStatus).Scan(&taskVersion)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: 待拒绝 Task 已变化", safetycontrol.ErrVersionConflict)
		}
		return mapWriteError("拒绝审批 Task", err)
	}
	var agentVersion int64
	err = tx.QueryRow(ctx, `
		UPDATE agent_instances
		SET status='IDLE',current_task_id=NULL,load=0,version=version+1,updated_at=$4
		WHERE tenant_id=$1 AND id=$2 AND status='WAITING_TOOL' AND current_task_id=$3
		RETURNING version`, tenantID, agentID, taskID, now).Scan(&agentVersion)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return mapWriteError("释放被拒绝 Agent", err)
	}
	if err := insertTenantOutbox(ctx, tx, tenantID, "attempt", attemptID,
		"attempt.aborted", 2, map[string]any{
			"id": attemptID, "run_id": runID, "task_id": taskID,
			"reason": reasonCode,
		}); err != nil {
		return err
	}
	if err := insertTenantOutbox(ctx, tx, tenantID, "task", taskID, "task.failed",
		taskVersion, map[string]any{
			"id": taskID, "run_id": runID, "attempt_id": attemptID,
			"reason": reason, "version": taskVersion,
		}); err != nil {
		return err
	}
	if err == nil {
		if err := insertTenantOutbox(ctx, tx, tenantID, "agent_instance", agentID,
			"agent.idle", agentVersion, map[string]any{
				"id": agentID, "run_id": runID, "reason": reasonCode,
				"version": agentVersion,
			}); err != nil {
			return err
		}
	}
	_, err = failRunForTask(ctx, tx, tenantID, runID, taskID, attemptID,
		failureCode, reason, now)
	return err
}

const interactionViewSelect = `
	SELECT i.id,i.tenant_id,i.run_id,i.task_id,i.attempt_id,i.effect_id,
	       i.interaction_type,i.status,i.title,i.description,i.payload,i.allowed_actions,
	       i.response,COALESCE(e.risk_level,''),i.requested_by,COALESCE(i.resolved_by,''),
	       i.expires_at,i.version,i.created_at,i.resolved_at,i.updated_at
	FROM interactions i LEFT JOIN effects e
	  ON e.id=i.effect_id AND e.tenant_id=i.tenant_id`

func scanInteractionView(row rowScanner) (*safetycontrol.InteractionView, error) {
	value := new(safetycontrol.InteractionView)
	var payload, actions, response []byte
	if err := row.Scan(
		&value.ID, &value.TenantID, &value.RunID, &value.TaskID, &value.AttemptID,
		&value.EffectID, &value.InteractionType, &value.Status, &value.Title,
		&value.Description, &payload, &actions, &response, &value.RiskLevel,
		&value.RequestedBy, &value.ResolvedBy, &value.ExpiresAt, &value.Version,
		&value.CreatedAt, &value.ResolvedAt, &value.UpdatedAt,
	); err != nil {
		return nil, err
	}
	value.Prompt = value.Description
	if err := json.Unmarshal(payload, &value.Payload); err != nil {
		return nil, fmt.Errorf("解析 interaction.payload: %w", err)
	}
	if err := json.Unmarshal(actions, &value.AllowedActions); err != nil {
		return nil, fmt.Errorf("解析 interaction.allowed_actions: %w", err)
	}
	if len(response) > 0 && string(response) != "{}" {
		var parsed safetycontrol.InteractionResponseView
		if err := json.Unmarshal(response, &parsed); err != nil {
			return nil, fmt.Errorf("解析 interaction.response: %w", err)
		}
		value.Response = &parsed
	}
	return value, nil
}

// ListEffects/GetEffect 暴露经过脱敏的 Effect 账本。拒绝封存和批准者保存在
// sanitized_result._authorization；这是向后兼容既有迁移、同时保持原子性的保留字段。
func (r *Repository) ListEffects(ctx context.Context, tenantID uuid.UUID, query safetycontrol.EffectQuery) ([]safetycontrol.EffectView, error) {
	sql := effectViewSelect + ` WHERE e.tenant_id=$1 AND e.run_id=$2`
	args := []any{tenantID, query.RunID}
	if query.Status != "" {
		args = append(args, query.Status)
		sql += fmt.Sprintf(" AND e.status=$%d", len(args))
	}
	if query.RiskLevel != "" {
		args = append(args, query.RiskLevel)
		sql += fmt.Sprintf(" AND e.risk_level=$%d", len(args))
	}
	args = append(args, query.Limit)
	sql += fmt.Sprintf(" ORDER BY e.prepared_at DESC,e.id DESC LIMIT $%d", len(args))
	rows, err := r.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("列出 Effects: %w", err)
	}
	defer rows.Close()
	items := make([]safetycontrol.EffectView, 0, query.Limit)
	for rows.Next() {
		item, err := scanEffectView(rows)
		if err != nil {
			return nil, fmt.Errorf("扫描 Effect: %w", err)
		}
		items = append(items, *item)
	}
	return items, rows.Err()
}

func (r *Repository) GetEffect(ctx context.Context, tenantID, id uuid.UUID) (*safetycontrol.EffectView, error) {
	item, err := scanEffectView(r.pool.QueryRow(ctx, effectViewSelect+`
		WHERE e.tenant_id=$1 AND e.id=$2`, tenantID, id))
	if err != nil {
		return nil, mapReadError("读取 Effect", err)
	}
	return item, nil
}

func (r *Repository) TransitionEffect(ctx context.Context, record safetycontrol.TransitionEffectRecord) (*safetycontrol.EffectView, error) {
	if record.Mutation.AggregateID != record.EffectID {
		return nil, fmt.Errorf("%w: Effect 与幂等聚合 ID 不一致", safetycontrol.ErrInvalidStoreResult)
	}
	if record.ResumeWaitingAttempt && record.FailWaitingAttempt {
		return nil, fmt.Errorf("%w: 对账不能同时恢复和终止 Attempt", safetycontrol.ErrInvalidStoreResult)
	}
	// 保留顶层 result 以兼容既有 Effect 读模型，同时把最终结论与支撑证据
	// 封装到 reconciliation，避免与 Adapter 原始返回或 _authorization 审批证据混淆。
	// sanitizeEffectValue 仍是最后一道密钥脱敏边界。
	resultPatch, err := marshalJSON(map[string]any{
		"result": sanitizeEffectValue(record.Result),
		"reconciliation": map[string]any{
			"outcome":  record.NextStatus,
			"result":   sanitizeEffectValue(record.Result),
			"evidence": sanitizeEffectValue(record.Evidence),
			"reason":   record.Reason,
		},
	}, "effect.reconcile_result")
	if err != nil {
		return nil, err
	}
	var result *safetycontrol.EffectView
	err = r.withTx(ctx, func(tx pgx.Tx) error {
		replay, err := beginSafetyMutation(ctx, tx, record.Mutation)
		if err != nil {
			return err
		}
		if replay != nil {
			if replay.Effect == nil {
				return safetycontrol.ErrInvalidStoreResult
			}
			result = replay.Effect
			return nil
		}
		command, err := tx.Exec(ctx, `
			UPDATE effects SET status=$5::varchar,version=version+1,updated_at=$6::timestamptz,
				reconcile_after=CASE
					WHEN $5::varchar='RECONCILING' THEN NULL
					WHEN $5::varchar='UNKNOWN' THEN $6::timestamptz + interval '30 seconds'
					ELSE reconcile_after END,
				sanitized_result=CASE WHEN $5::varchar IN ('SUCCEEDED','FAILED')
					THEN COALESCE(sanitized_result,'{}'::jsonb) || $7::jsonb
					ELSE sanitized_result END,
				external_ref=CASE WHEN $5::varchar IN ('SUCCEEDED','FAILED')
					THEN COALESCE(NULLIF($8::text,''),external_ref) ELSE external_ref END,
				finished_at=CASE WHEN $5::varchar IN ('SUCCEEDED','FAILED')
					THEN $6::timestamptz ELSE finished_at END,
				error_code=CASE WHEN $5::varchar='FAILED' THEN 'RECONCILED_FAILED'
					WHEN $5::varchar='SUCCEEDED' THEN NULL ELSE error_code END,
				error_message=CASE WHEN $5::varchar='FAILED' THEN NULLIF($9::text,'')
					WHEN $5::varchar='SUCCEEDED' THEN NULL ELSE error_message END
			WHERE tenant_id=$1 AND id=$2 AND version=$3 AND status=$4::varchar
			  AND COALESCE(sanitized_result #>> '{_authorization,blocked}','false')<>'true'`,
			record.Mutation.TenantID, record.EffectID, record.ExpectedVersion,
			record.ExpectedStatus, record.NextStatus, record.Mutation.OccurredAt,
			resultPatch, record.ExternalRef, record.Reason)
		if err != nil {
			return mapWriteError("转换 Effect", err)
		}
		if command.RowsAffected() != 1 {
			return fmt.Errorf("%w: Effect 状态、版本已变化或授权已封存", safetycontrol.ErrVersionConflict)
		}
		result, err = scanEffectView(tx.QueryRow(ctx, effectViewSelect+`
			WHERE e.tenant_id=$1 AND e.id=$2`, record.Mutation.TenantID, record.EffectID))
		if err != nil {
			return fmt.Errorf("回读 Effect: %w", err)
		}
		if err := insertTenantOutbox(ctx, tx, record.Mutation.TenantID, "effect", record.EffectID,
			"effect."+strings.ToLower(string(record.NextStatus)), result.Version, map[string]any{
				"id": record.EffectID, "run_id": result.RunID, "from": record.ExpectedStatus,
				"to": record.NextStatus, "reason": record.Reason, "actor": record.Mutation.Actor,
			}); err != nil {
			return err
		}
		if record.ResumeWaitingAttempt {
			if err := resumeWaitingAttemptAfterReconcile(ctx, tx, record.Mutation.TenantID,
				record.EffectID, record.Mutation.OccurredAt); err != nil {
				return err
			}
		}
		if record.FailWaitingAttempt {
			// UNKNOWN 期间稳定 ToolCall 保持 STARTED；只有外部对账明确 FAILED，才与
			// Effect/Attempt/Task 的失败在同一事务内收口，保留原始错误并追加结论。
			if _, err := tx.Exec(ctx, `
				UPDATE tool_calls tc
				SET status='FAILED',error_message=COALESCE(NULLIF($3,''),e.error_message),
				    finished_at=$4
				FROM effects e
				WHERE e.tenant_id=$1 AND e.id=$2 AND tc.id=e.tool_call_id
				  AND tc.attempt_id=e.attempt_id AND tc.status='STARTED'`,
				record.Mutation.TenantID, record.EffectID, record.Reason,
				record.Mutation.OccurredAt); err != nil {
				return mapWriteError("结束对账失败 ToolCall", err)
			}
			if err := failWaitingAttemptAfterReconcile(ctx, tx, record.Mutation.TenantID,
				record.EffectID, record.Reason, record.Mutation.OccurredAt); err != nil {
				return err
			}
		}
		return finishSafetyMutation(ctx, tx, record.Mutation, &safetycontrol.MutationResult{
			Operation: record.Mutation.Operation, Effect: result,
		})
	})
	return result, err
}

const effectViewSelect = `
	SELECT e.id,e.tenant_id,e.run_id,e.task_id,e.attempt_id,e.tool_call_id,
	       e.idempotency_key,e.effect_type,e.risk_level,e.status,e.request_hash,
	       e.sanitized_request,e.sanitized_result,COALESCE(e.external_ref,''),
	       e.approval_interaction_id,
	       COALESCE(e.sanitized_result #>> '{_authorization,approvedBy}',''),
	       (COALESCE(e.sanitized_result #>> '{_authorization,blocked}','false')='true'),
	       COALESCE(e.sanitized_result #>> '{_authorization,reason}',''),
	       e.reconcile_after,e.retry_count,e.compensation_spec,COALESCE(e.error_code,''),
	       COALESCE(e.error_message,''),e.fencing_token,e.version,e.prepared_at,
	       e.authorized_at,e.started_at,e.finished_at,e.updated_at
	FROM effects e`

func scanEffectView(row rowScanner) (*safetycontrol.EffectView, error) {
	value := new(safetycontrol.EffectView)
	var request, result, compensation []byte
	if err := row.Scan(
		&value.ID, &value.TenantID, &value.RunID, &value.TaskID, &value.AttemptID,
		&value.ToolCallID, &value.IdempotencyKey, &value.EffectType, &value.RiskLevel,
		&value.Status, &value.RequestHash, &request, &result, &value.ExternalRef,
		&value.ApprovalInteractionID, &value.ApprovedBy, &value.AuthorizationBlocked,
		&value.AuthorizationBlockReason, &value.ReconcileAfter, &value.RetryCount,
		&compensation, &value.ErrorCode, &value.ErrorMessage, &value.FencingToken,
		&value.Version, &value.PreparedAt, &value.AuthorizedAt, &value.StartedAt,
		&value.FinishedAt, &value.UpdatedAt,
	); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(request, &value.SanitizedRequest); err != nil {
		return nil, fmt.Errorf("解析 effect.sanitized_request: %w", err)
	}
	if err := json.Unmarshal(result, &value.SanitizedResult); err != nil {
		return nil, fmt.Errorf("解析 effect.sanitized_result: %w", err)
	}
	if err := json.Unmarshal(compensation, &value.CompensationSpec); err != nil {
		return nil, fmt.Errorf("解析 effect.compensation_spec: %w", err)
	}
	return value, nil
}

// ListArtifacts 只返回小型描述符；uri/object_key 指向真正的大对象，数据库和 API 都不
// 内联文件内容。旧 V1 字段 version 仍作为兼容数据保留，V1.5 对外使用 version_no。
func (r *Repository) ListArtifacts(ctx context.Context, tenantID uuid.UUID, query safetycontrol.ArtifactQuery) ([]safetycontrol.ArtifactView, error) {
	sql := artifactViewSelect + ` WHERE a.tenant_id=$1 AND a.run_id=$2`
	args := []any{tenantID, query.RunID}
	if query.Status != "" {
		args = append(args, query.Status)
		sql += fmt.Sprintf(" AND a.status=$%d", len(args))
	}
	args = append(args, query.Limit)
	sql += fmt.Sprintf(" ORDER BY a.created_at DESC,a.id DESC LIMIT $%d", len(args))
	rows, err := r.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("列出 Artifacts: %w", err)
	}
	defer rows.Close()
	items := make([]safetycontrol.ArtifactView, 0, query.Limit)
	for rows.Next() {
		item, err := scanArtifactView(rows)
		if err != nil {
			return nil, fmt.Errorf("扫描 Artifact: %w", err)
		}
		items = append(items, *item)
	}
	return items, rows.Err()
}

func (r *Repository) GetArtifact(ctx context.Context, tenantID, id uuid.UUID) (*safetycontrol.ArtifactView, error) {
	item, err := scanArtifactView(r.pool.QueryRow(ctx, artifactViewSelect+`
		WHERE a.tenant_id=$1 AND a.id=$2`, tenantID, id))
	if err != nil {
		return nil, mapReadError("读取 Artifact", err)
	}
	return item, nil
}

const artifactViewSelect = `
	SELECT a.id,a.tenant_id,a.run_id,a.task_id,a.attempt_id,a.name,a.artifact_type,
	       a.status,a.media_type,a.schema_version,a.version_no,a.uri,COALESCE(a.object_key,''),
	       a.content_hash,a.size_bytes,a.metadata,a.validation,a.validated_at,a.created_at,a.created_at
	FROM artifacts a`

func scanArtifactView(row rowScanner) (*safetycontrol.ArtifactView, error) {
	value := new(safetycontrol.ArtifactView)
	var metadata, validation []byte
	if err := row.Scan(
		&value.ID, &value.TenantID, &value.RunID, &value.TaskID, &value.AttemptID,
		&value.Name, &value.ArtifactType, &value.Status, &value.MediaType,
		&value.SchemaVersion, &value.VersionNo, &value.ContentURI, &value.ObjectKey,
		&value.ContentHash, &value.SizeBytes, &metadata, &validation, &value.ValidatedAt,
		&value.CreatedAt, &value.UpdatedAt,
	); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(metadata, &value.Metadata); err != nil {
		return nil, fmt.Errorf("解析 artifact.metadata: %w", err)
	}
	if err := json.Unmarshal(validation, &value.Validation); err != nil {
		return nil, fmt.Errorf("解析 artifact.validation: %w", err)
	}
	return value, nil
}

// GetArtifactLineage 使用带 path 的递归 CTE，既限制深度也阻止环路。ancestors 按
// from -> to 遍历，descendants 按 to -> from 遍历；both 同时开放两个方向。
func (r *Repository) GetArtifactLineage(ctx context.Context, tenantID, rootID uuid.UUID, query safetycontrol.ArtifactLineageQuery) (*safetycontrol.ArtifactLineageView, error) {
	root, err := r.GetArtifact(ctx, tenantID, rootID)
	if err != nil {
		return nil, err
	}
	rows, err := r.pool.Query(ctx, `
		WITH RECURSIVE nodes(id,depth,path) AS (
			SELECT $2::uuid,0,ARRAY[$2::uuid]
			UNION ALL
			SELECT CASE
				WHEN $4 IN ('ancestors','both') AND ar.from_artifact_id=n.id THEN ar.to_artifact_id
				ELSE ar.from_artifact_id
			END,
			n.depth+1,
			n.path || CASE
				WHEN $4 IN ('ancestors','both') AND ar.from_artifact_id=n.id THEN ar.to_artifact_id
				ELSE ar.from_artifact_id
			END
			FROM nodes n JOIN artifact_relations ar ON ar.tenant_id=$1 AND (
				($4 IN ('ancestors','both') AND ar.from_artifact_id=n.id) OR
				($4 IN ('descendants','both') AND ar.to_artifact_id=n.id)
			)
			WHERE n.depth<$3 AND NOT (
				CASE
					WHEN $4 IN ('ancestors','both') AND ar.from_artifact_id=n.id THEN ar.to_artifact_id
					ELSE ar.from_artifact_id
				END = ANY(n.path)
			)
		)
		SELECT DISTINCT id FROM nodes`, tenantID, rootID, query.Depth, query.Direction)
	if err != nil {
		return nil, fmt.Errorf("遍历 Artifact lineage: %w", err)
	}
	ids := make([]uuid.UUID, 0)
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, fmt.Errorf("扫描 Artifact lineage 节点: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	artifactRows, err := r.pool.Query(ctx, artifactViewSelect+`
		WHERE a.tenant_id=$1 AND a.id=ANY($2)
		ORDER BY CASE WHEN a.id=$3 THEN 0 ELSE 1 END,a.created_at,a.id`, tenantID, ids, rootID)
	if err != nil {
		return nil, fmt.Errorf("读取 Artifact lineage 节点: %w", err)
	}
	artifacts := make([]safetycontrol.ArtifactView, 0, len(ids))
	for artifactRows.Next() {
		item, scanErr := scanArtifactView(artifactRows)
		if scanErr != nil {
			artifactRows.Close()
			return nil, fmt.Errorf("扫描 Artifact lineage 节点: %w", scanErr)
		}
		artifacts = append(artifacts, *item)
	}
	if err := artifactRows.Err(); err != nil {
		artifactRows.Close()
		return nil, err
	}
	artifactRows.Close()
	if len(artifacts) == 0 {
		// root 已在上方通过 tenant 查询成功；这里为空代表并发删除，按 NotFound 失败关闭。
		return nil, fmt.Errorf("%w: Artifact lineage root 已不存在", domain.ErrNotFound)
	}

	edgeRows, err := r.pool.Query(ctx, `
		SELECT tenant_id,from_artifact_id,to_artifact_id,relation_type,metadata,created_at
		FROM artifact_relations
		WHERE tenant_id=$1 AND from_artifact_id=ANY($2) AND to_artifact_id=ANY($2)
		ORDER BY created_at,from_artifact_id,to_artifact_id,relation_type`, tenantID, ids)
	if err != nil {
		return nil, fmt.Errorf("读取 Artifact lineage 边: %w", err)
	}
	defer edgeRows.Close()
	edges := make([]safetycontrol.ArtifactLineageEdgeView, 0)
	for edgeRows.Next() {
		var value safetycontrol.ArtifactLineageEdgeView
		var metadata []byte
		if err := edgeRows.Scan(&value.TenantID, &value.FromArtifactID, &value.ToArtifactID,
			&value.RelationType, &metadata, &value.CreatedAt); err != nil {
			return nil, fmt.Errorf("扫描 Artifact lineage 边: %w", err)
		}
		value.ID = relationID(value.FromArtifactID, value.ToArtifactID, value.RelationType)
		if err := json.Unmarshal(metadata, &value.Metadata); err != nil {
			return nil, fmt.Errorf("解析 artifact_relation.metadata: %w", err)
		}
		edges = append(edges, value)
	}
	_ = root // root 的读取既是租户校验，也防止递归 CTE 对不存在 ID 返回一个虚拟节点。
	return &safetycontrol.ArtifactLineageView{
		RootArtifactID: rootID, Artifacts: artifacts, Edges: edges,
	}, edgeRows.Err()
}

func relationID(from, to uuid.UUID, relationType string) uuid.UUID {
	digest := sha256.Sum256([]byte(from.String() + ":" + to.String() + ":" + relationType))
	value, _ := uuid.FromBytes(digest[:16])
	return value
}

// ListTimeline 返回 Outbox 的审计投影，并把 published_at 一并展示。Timeline 本身只追加，
// API 排序使用 occurred_at+id，时间相同也保持确定性。
func (r *Repository) ListTimeline(ctx context.Context, tenantID uuid.UUID, query safetycontrol.TimelineQuery) ([]safetycontrol.TimelineEventView, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT te.id,te.tenant_id,te.run_id,te.task_id,te.attempt_id,te.event_type,
		       te.aggregate_type,te.aggregate_id,COALESCE(te.actor_id,''),te.actor_type,
		       te.summary,te.payload,te.correlation_id,te.causation_id,COALESCE(te.trace_id,''),
		       te.occurred_at,eo.published_at
		FROM timeline_events te
		LEFT JOIN event_outbox eo ON eo.id=te.source_event_id AND eo.tenant_id=te.tenant_id
		WHERE te.tenant_id=$1 AND te.run_id=$2
		ORDER BY te.occurred_at DESC,te.id DESC LIMIT $3`, tenantID, query.RunID, query.Limit)
	if err != nil {
		return nil, fmt.Errorf("列出 Timeline: %w", err)
	}
	defer rows.Close()
	items := make([]safetycontrol.TimelineEventView, 0, query.Limit)
	for rows.Next() {
		var value safetycontrol.TimelineEventView
		var payload []byte
		if err := rows.Scan(
			&value.ID, &value.TenantID, &value.RunID, &value.TaskID, &value.AttemptID,
			&value.EventType, &value.AggregateType, &value.AggregateID, &value.Actor,
			&value.ActorType, &value.Summary, &payload, &value.CorrelationID,
			&value.CausationID, &value.TraceID, &value.CreatedAt, &value.PublishedAt,
		); err != nil {
			return nil, fmt.Errorf("扫描 Timeline: %w", err)
		}
		if err := json.Unmarshal(payload, &value.Payload); err != nil {
			return nil, fmt.Errorf("解析 timeline.payload: %w", err)
		}
		value.Message = value.Summary
		value.Status = timelineStatus(value.Payload)
		items = append(items, value)
	}
	return items, rows.Err()
}

func timelineStatus(payload map[string]any) string {
	for _, key := range []string{"status", "to", "decision"} {
		if value, ok := payload[key].(string); ok {
			return value
		}
	}
	return ""
}

// ListSchedulerDecisions 保留候选和过滤详情，并联表补齐任务/Agent 名称；Explain 页因此
// 可以回答“为什么选它”和“为什么没有候选”，而不依赖不可审计的日志文本。
func (r *Repository) ListSchedulerDecisions(ctx context.Context, tenantID uuid.UUID, query safetycontrol.SchedulerDecisionQuery) ([]safetycontrol.SchedulerDecisionView, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT d.id,d.tenant_id,d.run_id,d.task_id,COALESCE(t.name,''),d.scheduler_id,
		       d.task_version,d.queue_score,d.selected_agent_id,COALESCE(ai.name,''),
		       d.selected_score,d.candidates,d.filters,d.reason,d.created_at
		FROM scheduler_decisions d
		JOIN tasks t ON t.id=d.task_id AND t.tenant_id=d.tenant_id AND t.swarm_id=d.run_id
		LEFT JOIN agent_instances ai ON ai.id=d.selected_agent_id AND ai.tenant_id=d.tenant_id
		WHERE d.tenant_id=$1 AND d.run_id=$2
		ORDER BY d.created_at DESC,d.id DESC LIMIT $3`, tenantID, query.RunID, query.Limit)
	if err != nil {
		return nil, fmt.Errorf("列出 Scheduler Decisions: %w", err)
	}
	defer rows.Close()
	items := make([]safetycontrol.SchedulerDecisionView, 0, query.Limit)
	for rows.Next() {
		var value safetycontrol.SchedulerDecisionView
		var score *float64
		var candidates, filters []byte
		if err := rows.Scan(
			&value.ID, &value.TenantID, &value.RunID, &value.TaskID, &value.TaskName,
			&value.SchedulerID, &value.TaskVersion, &value.QueueScore,
			&value.SelectedAgentID, &value.AgentName, &score, &candidates, &filters,
			&value.Reason, &value.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("扫描 Scheduler Decision: %w", err)
		}
		if score != nil {
			value.SelectedScore = *score
		}
		value.AgentID = value.SelectedAgentID
		if value.SelectedAgentID == nil {
			value.Outcome = "NO_MATCH"
		} else {
			value.Outcome = "SELECTED"
		}
		if err := json.Unmarshal(candidates, &value.Candidates); err != nil {
			return nil, fmt.Errorf("解析 scheduler_decision.candidates: %w", err)
		}
		if err := json.Unmarshal(filters, &value.Filters); err != nil {
			return nil, fmt.Errorf("解析 scheduler_decision.filters: %w", err)
		}
		value.Details = map[string]any{
			"schedulerId": value.SchedulerID, "taskVersion": value.TaskVersion,
			"queueScore": value.QueueScore, "selectedScore": value.SelectedScore,
		}
		items = append(items, value)
	}
	return items, rows.Err()
}

// ListVerificationRuns/GetVerificationRun 聚合 Gate 规格与独立执行证据。数据库状态 PASSED/
// FAILED 在 API 边界映射为领域协议 PASS/FAIL，避免把存储枚举泄漏给前端。
func (r *Repository) ListVerificationRuns(ctx context.Context, tenantID uuid.UUID, query safetycontrol.VerificationQuery) ([]safetycontrol.VerificationRunView, error) {
	rows, err := r.pool.Query(ctx, verificationViewSelect+`
		WHERE v.tenant_id=$1 AND v.run_id=$2
		ORDER BY v.created_at DESC,v.id DESC LIMIT $3`, tenantID, query.RunID, query.Limit)
	if err != nil {
		return nil, fmt.Errorf("列出 Verification Runs: %w", err)
	}
	defer rows.Close()
	items := make([]safetycontrol.VerificationRunView, 0, query.Limit)
	for rows.Next() {
		item, err := scanVerificationView(rows)
		if err != nil {
			return nil, fmt.Errorf("扫描 Verification Run: %w", err)
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for index := range items {
		if err := r.loadVerificationEvidence(ctx, tenantID, &items[index]); err != nil {
			return nil, err
		}
	}
	return items, nil
}

func (r *Repository) GetVerificationRun(ctx context.Context, tenantID, id uuid.UUID) (*safetycontrol.VerificationRunView, error) {
	item, err := scanVerificationView(r.pool.QueryRow(ctx, verificationViewSelect+`
		WHERE v.tenant_id=$1 AND v.id=$2`, tenantID, id))
	if err != nil {
		return nil, mapReadError("读取 Verification Run", err)
	}
	if err := r.loadVerificationEvidence(ctx, tenantID, item); err != nil {
		return nil, err
	}
	return item, nil
}

const verificationViewSelect = `
	SELECT v.id,v.tenant_id,v.run_id,v.task_id,v.attempt_id,v.status,v.created_at,v.finished_at
	FROM verification_runs v`

func scanVerificationView(row rowScanner) (*safetycontrol.VerificationRunView, error) {
	value := new(safetycontrol.VerificationRunView)
	if err := row.Scan(&value.ID, &value.TenantID, &value.RunID, &value.TaskID,
		&value.AttemptID, &value.Status, &value.CreatedAt, &value.FinishedAt); err != nil {
		return nil, err
	}
	value.Gates = []safetycontrol.AcceptanceGateView{}
	value.Results = []safetycontrol.GateResultView{}
	return value, nil
}

func (r *Repository) loadVerificationEvidence(ctx context.Context, tenantID uuid.UUID, value *safetycontrol.VerificationRunView) error {
	rows, err := r.pool.Query(ctx, `
		SELECT g.id,g.gate_key,g.gate_type,g.required,g.config,g.created_at,
		       gr.id,gr.status,gr.metrics,gr.evidence_artifact_id,
		       gr.output_excerpt,gr.error_message,gr.started_at,gr.finished_at
		FROM acceptance_gates g
		LEFT JOIN gate_results gr ON gr.gate_id=g.id AND gr.verification_run_id=$3
		WHERE g.tenant_id=$1 AND g.task_id=$2
		ORDER BY g.created_at,g.id`, tenantID, value.TaskID, value.ID)
	if err != nil {
		return fmt.Errorf("读取 Verification Gate 证据: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var gate safetycontrol.AcceptanceGateView
		var config []byte
		var resultID *uuid.UUID
		var status *string
		var metrics []byte
		var evidenceArtifactID *uuid.UUID
		var excerpt, errorMessage *string
		var startedAt, finishedAt *time.Time
		if err := rows.Scan(
			&gate.ID, &gate.Name, &gate.GateType, &gate.Required, &config, &gate.CreatedAt,
			&resultID, &status, &metrics, &evidenceArtifactID, &excerpt, &errorMessage,
			&startedAt, &finishedAt,
		); err != nil {
			return fmt.Errorf("扫描 Verification Gate 证据: %w", err)
		}
		if err := json.Unmarshal(config, &gate.Config); err != nil {
			return fmt.Errorf("解析 acceptance_gate.config: %w", err)
		}
		value.Gates = append(value.Gates, gate)
		if resultID == nil {
			continue
		}
		result := safetycontrol.GateResultView{
			ID: *resultID, GateID: gate.ID, GateName: gate.Name,
			Status: mapGateStatus(stringValue(status)), Metrics: map[string]any{},
			StartedAt: startedAt, FinishedAt: finishedAt,
		}
		if len(metrics) > 0 {
			if err := json.Unmarshal(metrics, &result.Metrics); err != nil {
				return fmt.Errorf("解析 gate_result.metrics: %w", err)
			}
		}
		if evidenceArtifactID != nil {
			result.EvidenceRefs = []string{"artifact:" + evidenceArtifactID.String()}
		} else {
			result.EvidenceRefs = []string{}
		}
		result.Message = stringValue(excerpt)
		if result.Message == "" {
			result.Message = stringValue(errorMessage)
		}
		value.Results = append(value.Results, result)
	}
	return rows.Err()
}

func mapGateStatus(value string) verification.GateStatus {
	switch value {
	case "PASSED":
		return verification.GatePass
	case "FAILED":
		return verification.GateFail
	case "ERROR":
		return verification.GateError
	case "SKIPPED":
		return verification.GateSkipped
	default:
		return verification.GatePending
	}
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

// GetCompletionManifest 返回任务或 Run 的不可变完成证据，并内联小型 Artifact 描述符与
// GateResult 索引。manifest.summary 是历史 TEXT 字段；若其中是 JSON 则恢复结构，否则
// 以 text 键包装，保持 API 始终是稳定对象。
//
// Run 级 Manifest 的 task_id 可以为空，因此关联 Verification 时必须始终先约束 run_id；
// 不能依赖 task_id/attempt_id 这两个可空字段，否则历史数据中两者均为空时会误取同租户
// 其他 Run 的最新 Verification，破坏完成证据的运行隔离。
func (r *Repository) GetCompletionManifest(ctx context.Context, tenantID, id uuid.UUID) (*safetycontrol.CompletionManifestView, error) {
	value := new(safetycontrol.CompletionManifestView)
	var storedStatus string
	var summary string
	var issues, assumptions []byte
	err := r.pool.QueryRow(ctx, `
		SELECT m.id,m.tenant_id,m.run_id,m.task_id,m.attempt_id,
		       (SELECT v.id FROM verification_runs v
		        WHERE v.tenant_id=m.tenant_id
		          AND v.run_id=m.run_id
		          AND (m.attempt_id IS NULL OR v.attempt_id=m.attempt_id)
		          AND (m.task_id IS NULL OR v.task_id=m.task_id)
		        ORDER BY v.created_at DESC,v.id DESC LIMIT 1),
		       m.status,m.known_issues,m.assumptions,m.summary,m.content_hash,m.created_at
		FROM completion_manifests m WHERE m.tenant_id=$1 AND m.id=$2`, tenantID, id).
		Scan(&value.ID, &value.TenantID, &value.RunID, &value.TaskID, &value.AttemptID,
			&value.VerificationID, &storedStatus, &issues, &assumptions, &summary,
			&value.ContentHash, &value.CreatedAt)
	if err != nil {
		return nil, mapReadError("读取 Completion Manifest", err)
	}
	value.Status = mapManifestStatus(storedStatus)
	if err := json.Unmarshal(issues, &value.KnownIssues); err != nil {
		return nil, fmt.Errorf("解析 completion_manifest.known_issues: %w", err)
	}
	if err := json.Unmarshal(assumptions, &value.Assumptions); err != nil {
		return nil, fmt.Errorf("解析 completion_manifest.assumptions: %w", err)
	}
	if err := json.Unmarshal([]byte(summary), &value.Summary); err != nil {
		value.Summary = map[string]any{"text": summary}
	}

	artifactRows, err := r.pool.Query(ctx, artifactViewSelect+`
		JOIN completion_manifest_artifacts ma ON ma.artifact_id=a.id
		WHERE ma.manifest_id=$1 AND a.tenant_id=$2 AND a.run_id=$3
		ORDER BY ma.role,a.created_at,a.id`, id, tenantID, value.RunID)
	if err != nil {
		return nil, fmt.Errorf("读取 Completion Manifest Artifacts: %w", err)
	}
	value.Artifacts = []safetycontrol.ArtifactView{}
	for artifactRows.Next() {
		item, scanErr := scanArtifactView(artifactRows)
		if scanErr != nil {
			artifactRows.Close()
			return nil, fmt.Errorf("扫描 Completion Manifest Artifact: %w", scanErr)
		}
		value.Artifacts = append(value.Artifacts, *item)
	}
	if err := artifactRows.Err(); err != nil {
		artifactRows.Close()
		return nil, err
	}
	artifactRows.Close()

	evidenceRows, err := r.pool.Query(ctx, `
		SELECT gr.id,gr.gate_id,g.gate_key,gr.status,gr.metrics,gr.evidence_artifact_id,
		       gr.output_excerpt,gr.error_message,gr.started_at,gr.finished_at
		FROM completion_manifest_evidence me
		JOIN gate_results gr ON gr.id=me.gate_result_id AND gr.tenant_id=$2
		JOIN acceptance_gates g ON g.id=gr.gate_id AND g.tenant_id=$2
		WHERE me.manifest_id=$1 ORDER BY g.gate_key,gr.id`, id, tenantID)
	if err != nil {
		return nil, fmt.Errorf("读取 Completion Manifest Evidence: %w", err)
	}
	defer evidenceRows.Close()
	value.Evidence = []safetycontrol.GateResultView{}
	for evidenceRows.Next() {
		var result safetycontrol.GateResultView
		var status string
		var metrics []byte
		var artifactID *uuid.UUID
		var excerpt, errorMessage string
		if err := evidenceRows.Scan(
			&result.ID, &result.GateID, &result.GateName, &status, &metrics, &artifactID,
			&excerpt, &errorMessage, &result.StartedAt, &result.FinishedAt,
		); err != nil {
			return nil, fmt.Errorf("扫描 Completion Manifest Evidence: %w", err)
		}
		result.Status = mapGateStatus(status)
		if err := json.Unmarshal(metrics, &result.Metrics); err != nil {
			return nil, fmt.Errorf("解析 manifest gate metrics: %w", err)
		}
		if artifactID != nil {
			result.EvidenceRefs = []string{"artifact:" + artifactID.String()}
		} else {
			result.EvidenceRefs = []string{}
		}
		result.Message = excerpt
		if result.Message == "" {
			result.Message = errorMessage
		}
		value.Evidence = append(value.Evidence, result)
	}
	return value, evidenceRows.Err()
}

func mapManifestStatus(value string) verification.ManifestStatus {
	switch value {
	case "VALID":
		return verification.ManifestAccepted
	case "INVALID":
		return verification.ManifestRejected
	default:
		return verification.ManifestIncomplete
	}
}
