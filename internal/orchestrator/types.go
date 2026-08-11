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

// CandidateDecision 是 Scheduler Explain 的单候选证据。Reasons 保存硬条件拒绝原因；
// Accepted=true 的候选才有最终 Score 并进入 Reserve/Bind。
type CandidateDecision struct {
	AgentID    uuid.UUID `json:"agentId"`
	TemplateID uuid.UUID `json:"templateId"`
	Accepted   bool      `json:"accepted"`
	Score      float64   `json:"score"`
	Reasons    []string  `json:"reasons"`
}

// SchedulerDecision 固化一次 Filter/Score/Reserve 选择，控制台可据此解释为什么选择某个
// Agent，或为什么任务仍留在 READY。
type SchedulerDecision struct {
	SchedulerID     string              `json:"schedulerId"`
	TaskID          uuid.UUID           `json:"taskId"`
	TaskVersion     int64               `json:"taskVersion"`
	QueueScore      float64             `json:"queueScore"`
	SelectedAgentID *uuid.UUID          `json:"selectedAgentId,omitempty"`
	SelectedScore   *float64            `json:"selectedScore,omitempty"`
	Candidates      []CandidateDecision `json:"candidates"`
	Reason          string              `json:"reason"`
}

// Store 聚合 Controller/Scheduler 所需的原子数据库操作。
type Store interface {
	// ListReconcileTasks 除常规 Controller 状态外，只返回 updated_at 不晚于
	// staleSchedulingBefore 的 SCHEDULING；必须在 SQL 的 LIMIT 前过滤，避免新鲜任务
	// 占满批次并让真正的孤儿状态饥饿。
	ListReconcileTasks(context.Context, time.Time, int) ([]*task.Task, error)
	DependenciesSatisfied(context.Context, uuid.UUID) (bool, error)
	TransitionTask(context.Context, uuid.UUID, int64, task.Status, task.Status, string) (int64, error)
	ActivateRegisteredAgents(context.Context, int) (int, error)

	ListQueuedTasks(context.Context, int) ([]QueuedTask, error)
	ListSchedulerCandidates(context.Context, uuid.UUID, int64) ([]Candidate, error)
	BindTask(context.Context, uuid.UUID, int64, uuid.UUID, int64) error
	// RecordSchedulerDecision 对未选中决策按 Task 版本幂等合并；Scheduler 可以在
	// READY 轮询中持续提供最新 Explain，而不会把“仍然无候选”放大成无限审计写入。
	RecordSchedulerDecision(context.Context, SchedulerDecision) error
}

// Clock 使队列等待时间、deadline 和重试条件可以确定性测试。
type Clock func() time.Time
