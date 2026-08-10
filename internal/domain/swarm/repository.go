package swarm

import (
	"context"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/domain"
)

// Repository 隔离领域层与 PostgreSQL 实现，便于 Controller 单元测试。
type Repository interface {
	Create(context.Context, *Swarm) error
	Get(context.Context, uuid.UUID) (*Swarm, error)
	List(context.Context, domain.Page) ([]*Swarm, error)
}
