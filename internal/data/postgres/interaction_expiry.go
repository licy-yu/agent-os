package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/licy-yu/agent-os/internal/effect"
	"github.com/licy-yu/agent-os/internal/interaction"
)

const interactionExpiryActor = "system/recovery-controller"

// expiringInteraction 是 RecoveryController 在一次事务中锁住的最小快照。
// tenant_id 必须随每一条后续 SQL 传递，不能因为 UUID 全局唯一就省略租户边界。
type expiringInteraction struct {
	ID        uuid.UUID
	TenantID  uuid.UUID
	RunID     uuid.UUID
	TaskID    *uuid.UUID
	AttemptID *uuid.UUID
	EffectID  *uuid.UUID
	Type      interaction.Type
	Version   int64
	ExpiresAt time.Time
}

// ExpireWaitingInteractions 收敛超过 expires_at 的 WAITING Interaction。
//
// PostgreSQL 的 FOR UPDATE SKIP LOCKED 让多个 control-plane 实例可以并行扫描；人工审批
// 也首先更新 Interaction 行，因此“审批”和“自动过期”会争用同一把数据库行锁。先提交者
// 获胜，后提交者的 status+version CAS 返回 0 行，杜绝一边 AUTHORIZED、一边 EXPIRED 的
// 撕裂状态。R3 审批链按 Interaction → Effect → Attempt → Task 的固定顺序加锁，降低与
// ResolveInteraction 并发时的死锁风险。
func (r *Repository) ExpireWaitingInteractions(ctx context.Context, now time.Time, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	// 防止错误配置让单个事务持锁过久；控制循环下一轮会继续处理余量。
	if limit > 1000 {
		limit = 1000
	}
	now = now.UTC()
	expired := 0
	err := r.withTx(ctx, func(tx pgx.Tx) error {
		// withTx 遇到 PostgreSQL 40001/40P01 会重放闭包；每次重放必须从 0 计数，
		// 否则一次实际提交会被控制器误报为多次状态变化。
		transactionExpired := 0
		rows, err := tx.Query(ctx, `
			SELECT id,tenant_id,run_id,task_id,attempt_id,effect_id,
			       interaction_type,version,expires_at
			FROM interactions
			WHERE status='WAITING' AND expires_at IS NOT NULL AND expires_at<=$1
			ORDER BY expires_at,id
			FOR UPDATE SKIP LOCKED
			LIMIT $2`, now, limit)
		if err != nil {
			return fmt.Errorf("扫描已过期 Interaction: %w", err)
		}
		items := make([]expiringInteraction, 0, limit)
		for rows.Next() {
			var item expiringInteraction
			if err := rows.Scan(
				&item.ID, &item.TenantID, &item.RunID, &item.TaskID, &item.AttemptID,
				&item.EffectID, &item.Type, &item.Version, &item.ExpiresAt,
			); err != nil {
				rows.Close()
				return fmt.Errorf("扫描已过期 Interaction 行: %w", err)
			}
			items = append(items, item)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("遍历已过期 Interaction: %w", err)
		}
		rows.Close()

		for _, item := range items {
			if err := expireWaitingInteraction(ctx, tx, item, now); err != nil {
				return err
			}
			transactionExpired++
		}
		expired = transactionExpired
		return nil
	})
	if err != nil {
		// 事务已回滚时不向上层报告任何已完成变更。
		return 0, err
	}
	return expired, nil
}

// expireWaitingInteraction 在调用方已经锁定 Interaction 的前提下完成一条过期闭环。
// 普通 INPUT/CHOICE 等 Interaction 没有 Effect，只需进入 EXPIRED 并发出审计事件；
// 绑定 PREPARED Effect 的 APPROVAL 还必须终止其持久化等待执行链。
func expireWaitingInteraction(ctx context.Context, tx pgx.Tx, item expiringInteraction, now time.Time) error {
	var lockedEffect *expiringApprovalEffect
	if item.Type == interaction.TypeApproval && item.EffectID != nil {
		value, err := lockExpiringApprovalEffect(ctx, tx, item)
		if err != nil {
			return err
		}
		lockedEffect = value
	}

	var interactionVersion int64
	err := tx.QueryRow(ctx, `
		UPDATE interactions
		SET status='EXPIRED',resolved_by=$6,resolved_at=$5,
		    version=version+1,updated_at=$5
		WHERE tenant_id=$1 AND id=$2 AND status='WAITING' AND version=$3
		  AND expires_at IS NOT NULL AND expires_at<=$4
		RETURNING version`, item.TenantID, item.ID, item.Version, now, now,
		interactionExpiryActor).Scan(&interactionVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("过期 Interaction %s 的状态或版本已变化", item.ID)
	}
	if err != nil {
		return mapWriteError("过期 Interaction", err)
	}

	if lockedEffect != nil {
		if err := expireApprovalEffect(ctx, tx, item, *lockedEffect, now); err != nil {
			return err
		}
	}

	return insertTenantOutbox(ctx, tx, item.TenantID, "interaction", item.ID,
		"interaction.expired", interactionVersion, map[string]any{
			"id": item.ID, "run_id": item.RunID, "status": interaction.StatusExpired,
			"reason": "deadline_exceeded", "expired_at": now,
			"version": interactionVersion,
		})
}

type expiringApprovalEffect struct {
	ID         uuid.UUID
	RunID      uuid.UUID
	TaskID     uuid.UUID
	AttemptID  uuid.UUID
	ToolCallID *uuid.UUID
	Status     effect.Status
	Version    int64
}

// lockExpiringApprovalEffect 在 Interaction 锁之后依次锁 Effect、Attempt。Task 行由
// failWaitingAttempt 在更新前锁定/做 CAS，从而与人工 approve/reject 保持相同锁序。
func lockExpiringApprovalEffect(ctx context.Context, tx pgx.Tx,
	item expiringInteraction,
) (*expiringApprovalEffect, error) {
	value := new(expiringApprovalEffect)
	err := tx.QueryRow(ctx, `
		SELECT id,run_id,task_id,attempt_id,tool_call_id,status,version
		FROM effects
		WHERE tenant_id=$1 AND id=$2 AND approval_interaction_id=$3
		FOR UPDATE`, item.TenantID, *item.EffectID, item.ID).Scan(
		&value.ID, &value.RunID, &value.TaskID, &value.AttemptID, &value.ToolCallID,
		&value.Status, &value.Version,
	)
	if err != nil {
		return nil, mapReadError("锁定过期审批关联 Effect", err)
	}
	if value.Status != effect.StatusPrepared {
		return nil, fmt.Errorf("过期审批 %s 关联 Effect %s 状态不是 PREPARED: %s",
			item.ID, value.ID, value.Status)
	}
	if value.RunID != item.RunID || (item.TaskID != nil && value.TaskID != *item.TaskID) ||
		(item.AttemptID != nil && value.AttemptID != *item.AttemptID) {
		return nil, fmt.Errorf("过期审批 %s 与 Effect %s 的租户内执行链不一致", item.ID, value.ID)
	}

	var attemptStatus string
	err = tx.QueryRow(ctx, `
		SELECT status FROM task_attempts
		WHERE tenant_id=$1 AND id=$2 AND task_id=$3
		FOR UPDATE`, item.TenantID, value.AttemptID, value.TaskID).Scan(&attemptStatus)
	if err != nil {
		return nil, mapReadError("锁定过期审批关联 Attempt", err)
	}
	if attemptStatus != "WAITING" {
		return nil, fmt.Errorf("过期审批 %s 关联 Attempt %s 状态不是 WAITING: %s",
			item.ID, value.AttemptID, attemptStatus)
	}
	return value, nil
}

func expireApprovalEffect(ctx context.Context, tx pgx.Tx, item expiringInteraction,
	value expiringApprovalEffect, now time.Time,
) error {
	const reason = "审批 Interaction 已过期（24 小时未响应）"
	metadata, err := marshalJSON(map[string]any{
		"blocked": true, "reason": reason, "expiredAt": now,
		"interactionId": item.ID, "actor": interactionExpiryActor,
	}, "effect.expired_authorization")
	if err != nil {
		return err
	}
	var effectVersion int64
	err = tx.QueryRow(ctx, `
		UPDATE effects
		SET sanitized_result=jsonb_set(
				COALESCE(sanitized_result,'{}'::jsonb),'{_authorization}',
				COALESCE(sanitized_result->'_authorization','{}'::jsonb) || $6::jsonb,true),
		    error_code='APPROVAL_EXPIRED',error_message=$7,
		    version=version+1,updated_at=$5
		WHERE tenant_id=$1 AND id=$2 AND version=$3 AND status='PREPARED'
		  AND approval_interaction_id=$4
		  AND COALESCE(sanitized_result #>> '{_authorization,blocked}','false')<>'true'
		RETURNING version`, item.TenantID, value.ID, value.Version, item.ID, now,
		metadata, reason).Scan(&effectVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("过期审批 %s 的 Effect 状态、版本或授权闸门已变化", item.ID)
	}
	if err != nil {
		return mapWriteError("封存过期审批 Effect", err)
	}

	toolCallChanged := false
	if value.ToolCallID != nil {
		command, err := tx.Exec(ctx, `
			UPDATE tool_calls
			SET status='DENIED',error_message=$5,finished_at=$4
			WHERE tenant_id=$1 AND id=$2 AND attempt_id=$3 AND status='STARTED'`,
			item.TenantID, *value.ToolCallID, value.AttemptID, now, reason)
		if err != nil {
			return mapWriteError("结束过期审批 ToolCall", err)
		}
		toolCallChanged = command.RowsAffected() == 1
		if !toolCallChanged {
			var status string
			if err := tx.QueryRow(ctx, `
				SELECT status FROM tool_calls
				WHERE tenant_id=$1 AND id=$2 AND attempt_id=$3`,
				item.TenantID, *value.ToolCallID, value.AttemptID).Scan(&status); err != nil {
				return mapReadError("核对过期审批 ToolCall", err)
			}
			if status != "DENIED" {
				return fmt.Errorf("过期审批 ToolCall %s 状态无法收敛: %s", *value.ToolCallID, status)
			}
		}
	}

	// 该 helper 使用 tenant+effect 精确定位原 WAITING Attempt，并依次把 Attempt、Task、
	// Agent、Run 收敛到安全终态；Effect 本身按审计合同保留 PREPARED+blocked。
	if err := failWaitingAttempt(ctx, tx, item.TenantID, value.ID, "WAITING_APPROVAL",
		"APPROVAL_EXPIRED", "approval_expired", reason, now); err != nil {
		return err
	}
	if err := insertTenantOutbox(ctx, tx, item.TenantID, "effect", value.ID,
		"effect.authorization_expired", effectVersion, map[string]any{
			"id": value.ID, "run_id": value.RunID, "task_id": value.TaskID,
			"attempt_id": value.AttemptID, "interaction_id": item.ID,
			"authorization_blocked": true, "reason": reason, "version": effectVersion,
		}); err != nil {
		return err
	}
	if toolCallChanged {
		if err := insertTenantOutbox(ctx, tx, item.TenantID, "tool_call", *value.ToolCallID,
			"tool_call.denied", 2, map[string]any{
				"id": *value.ToolCallID, "run_id": value.RunID, "task_id": value.TaskID,
				"attempt_id": value.AttemptID, "reason": "approval_expired",
			}); err != nil {
			return err
		}
	}
	return nil
}
