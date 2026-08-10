package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/domain"
	"github.com/licy-yu/agent-os/internal/event"
)

// ClaimOutbox 使用 FOR UPDATE SKIP LOCKED 让多个控制面实例可以并发领取事件。
// 数据库中的 locked_until 负责进程崩溃后的自动回收，避免事件永久卡死。
func (r *Repository) ClaimOutbox(ctx context.Context, owner string, limit int, lockTTL time.Duration) ([]event.OutboxEvent, error) {
	lockUntil := time.Now().UTC().Add(lockTTL)
	rows, err := r.pool.Query(ctx, `
		WITH candidates AS (
			SELECT id
			FROM event_outbox
			WHERE published_at IS NULL
			  AND available_at <= now()
			  AND (locked_until IS NULL OR locked_until < now())
			ORDER BY created_at,id
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		UPDATE event_outbox e
		SET locked_by=$2, locked_until=$3
		FROM candidates c
		WHERE e.id=c.id
		RETURNING e.id,e.aggregate_type,e.aggregate_id,e.event_type,e.aggregate_version,
		          e.payload,e.attempts,e.created_at`, limit, owner, lockUntil)
	if err != nil {
		return nil, fmt.Errorf("领取 Outbox 事件: %w", err)
	}
	defer rows.Close()
	items := make([]event.OutboxEvent, 0, limit)
	for rows.Next() {
		var value event.OutboxEvent
		if err := rows.Scan(&value.ID, &value.AggregateType, &value.AggregateID, &value.Type,
			&value.AggregateVersion, &value.Payload, &value.Attempts, &value.CreatedAt); err != nil {
			return nil, fmt.Errorf("扫描 Outbox 事件: %w", err)
		}
		items = append(items, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历 Outbox 事件: %w", err)
	}
	return items, nil
}

// MarkOutboxPublished 只有当前 owner 可以确认消息，防止过期 Worker 覆盖新 owner。
func (r *Repository) MarkOutboxPublished(ctx context.Context, id uuid.UUID, owner string) error {
	result, err := r.pool.Exec(ctx, `
		UPDATE event_outbox
		SET published_at=now(),locked_by=NULL,locked_until=NULL,last_error=NULL
		WHERE id=$1 AND locked_by=$2 AND published_at IS NULL`, id, owner)
	if err != nil {
		return fmt.Errorf("标记 Outbox 已发布: %w", err)
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("%w: Outbox %s 的锁不属于 %s", domain.ErrConflict, id, owner)
	}
	return nil
}

// RescheduleOutbox 释放当前锁并设置指数退避后的下一次可见时间。
func (r *Repository) RescheduleOutbox(ctx context.Context, id uuid.UUID, owner string, availableAt time.Time, reason string) error {
	result, err := r.pool.Exec(ctx, `
		UPDATE event_outbox
		SET attempts=attempts+1,available_at=$3,last_error=$4,locked_by=NULL,locked_until=NULL
		WHERE id=$1 AND locked_by=$2 AND published_at IS NULL`, id, owner, availableAt, reason)
	if err != nil {
		return fmt.Errorf("重排 Outbox: %w", err)
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("%w: Outbox %s 的锁不属于 %s", domain.ErrConflict, id, owner)
	}
	return nil
}
