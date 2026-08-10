// Package lease 定义 Scheduler Reserve 阶段的租约端口。
package lease

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Lease 是成功 Reserve 后的所有权凭据。
type Lease struct {
	AgentID uuid.UUID
	TaskID  uuid.UUID
	Token   string
	TTL     time.Duration
}

// Manager 可由 Redis 实现，也可在单元测试中由内存实现替代。
type Manager interface {
	Reserve(context.Context, uuid.UUID, uuid.UUID, time.Duration) (*Lease, bool, error)
	Release(context.Context, *Lease) error
}
