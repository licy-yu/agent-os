package agent

import (
	"context"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/domain"
)

// Repository 提供模板与实例的持久化端口。
type Repository interface {
	CreateTemplate(context.Context, *Template) error
	GetTemplate(context.Context, uuid.UUID) (*Template, error)
	ListTemplates(context.Context, domain.Page) ([]*Template, error)
	CreateInstance(context.Context, *Instance) error
	GetInstance(context.Context, uuid.UUID) (*Instance, error)
	ListInstances(context.Context, *uuid.UUID, domain.Page) ([]*Instance, error)
}
