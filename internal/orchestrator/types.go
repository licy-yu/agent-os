// Package orchestrator 实现声明式 Controller 和插件式 Scheduler。
package orchestrator

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/domain/agent"
	"github.com/licy-yu/agent-os/internal/domain/task"
)

// QueuedTask 附带影响 QueueSort 的 DAG 信息。
type QueuedTask struct {
	Task            *task.Task
	BlockedChildren int32
	QueueScore      float64
}

// Candidate 是一次调度周期中 AgentInstance 与 AgentTemplate 的快照。
type Candidate struct {
	Instance        *agent.Instance
	Template        *agent.Template
	BudgetAllowed   bool
	HistorySuccess  float64
	ContextAffinity float64
	QualityScore    float64
	LatencyScore    float64
	FinalScore      float64
}

// Store 聚合 Controller/Scheduler 所需的原子数据库操作。
type Store interface {
	ListReconcileTasks(context.Context, int) ([]*task.Task, error)
	DependenciesSatisfied(context.Context, uuid.UUID) (bool, error)
	TransitionTask(context.Context, uuid.UUID, int64, task.Status, task.Status, string) (int64, error)
	ActivateRegisteredAgents(context.Context, int) (int, error)

	ListQueuedTasks(context.Context, int) ([]QueuedTask, error)
	ListSchedulerCandidates(context.Context, uuid.UUID, int64) ([]Candidate, error)
	BindTask(context.Context, uuid.UUID, int64, uuid.UUID, int64) error
}

// Clock 使队列等待时间、deadline 和重试条件可以确定性测试。
type Clock func() time.Time
