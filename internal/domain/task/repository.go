package task

import (
	"context"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/domain"
)

// ListFilter 是任务列表的数据库过滤条件。
type ListFilter struct {
	SwarmID uuid.UUID
	Status  *Status
	Page    domain.Page
}

// Repository 是 Task 聚合的持久化端口。
type Repository interface {
	CreateTask(context.Context, *Task) error
	GetTask(context.Context, uuid.UUID) (*Task, error)
	ListTasks(context.Context, ListFilter) ([]*Task, error)
}
