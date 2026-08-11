// Package run 定义一次用户目标执行的 Run 聚合及其生命周期状态机。
//
// Run 是 SwarmOS 的第一等执行对象；Agent 只是 Run 中被调度的执行资源。
// 本包只表达领域规则，不调用模型、工具、数据库或工作流引擎，因而可被 HTTP、
// Temporal Workflow Activity 和离线恢复程序共同复用。
package run

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/domain"
)

// Status 是 Run 对外可审计的生命周期状态。
type Status string

const (
	StatusCreated         Status = "CREATED"
	StatusAdmission       Status = "ADMISSION"
	StatusPendingCapacity Status = "PENDING_CAPACITY"
	StatusPendingQuota    Status = "PENDING_QUOTA"
	StatusPendingResource Status = "PENDING_RESOURCE"
	StatusPlanning        Status = "PLANNING"
	StatusReady           Status = "READY"
	StatusRunning         Status = "RUNNING"
	StatusWaitingUser     Status = "WAITING_USER"
	StatusWaitingExternal Status = "WAITING_EXTERNAL"
	StatusPaused          Status = "PAUSED"
	StatusRecovering      Status = "RECOVERING"
	StatusDegraded        Status = "DEGRADED"
	StatusVerifying       Status = "VERIFYING"
	StatusCompleted       Status = "COMPLETED"
	StatusFailed          Status = "FAILED"
	StatusCanceled        Status = "CANCELED"
	StatusExpired         Status = "EXPIRED"
)

func (s Status) valid() bool {
	switch s {
	case StatusCreated, StatusAdmission, StatusPendingCapacity, StatusPendingQuota,
		StatusPendingResource, StatusPlanning, StatusReady, StatusRunning,
		StatusWaitingUser, StatusWaitingExternal, StatusPaused, StatusRecovering,
		StatusDegraded, StatusVerifying, StatusCompleted, StatusFailed,
		StatusCanceled, StatusExpired:
		return true
	default:
		return false
	}
}

// Actor 表示有权提交特定状态边的控制面组件。
// 用户操作应先成为 Workflow Signal，再由 RunController 提交状态转换；这样用户请求、
// 运行时事实和持久化投影之间不会出现多写者。
type Actor string

const (
	ActorController Actor = "RUN_CONTROLLER"
	ActorAdmission  Actor = "ADMISSION_CONTROLLER"
	ActorPlanner    Actor = "PLANNER"
	ActorRuntime    Actor = "RUNTIME"
	ActorVerifier   Actor = "VERIFIER"
)

// Budget 是 Run 的资源上限和已消费快照。上限为 0 表示该维度不设硬上限，
// 这与现有 Swarm 预算语义兼容。
type Budget struct {
	MaxTokens       int64
	MaxCostMicros   int64
	SpentTokens     int64
	SpentCostMicros int64
}

// Allows 判断新增预留是否仍位于 Run 预算内。负数不是合法的“释放预算”表达，
// 调用方必须通过独立的记账操作处理冲正。
func (b Budget) Allows(tokens, costMicros int64) bool {
	if tokens < 0 || costMicros < 0 {
		return false
	}
	if b.MaxTokens > 0 && b.SpentTokens+tokens > b.MaxTokens {
		return false
	}
	return b.MaxCostMicros == 0 || b.SpentCostMicros+costMicros <= b.MaxCostMicros
}

// Run 表示一次完整目标执行。DefinitionID 指向可复用的 SwarmDefinition，Run 自身保存
// 当次目标、预算和当前计划版本，避免修改 Definition 影响正在运行的历史执行。
type Run struct {
	ID                 uuid.UUID
	TenantID           uuid.UUID
	DefinitionID       uuid.UUID
	Goal               string
	NormalizedGoal     map[string]any
	Status             Status
	CurrentPlanVersion int32
	Priority           int32
	Deadline           *time.Time
	Budget             Budget
	CreatedBy          string
	Version            int64
	CreatedAt          time.Time
	StartedAt          *time.Time
	FinishedAt         *time.Time
	UpdatedAt          time.Time
}

type transitionKey struct {
	actor Actor
	from  Status
	to    Status
}

// runTransitions 同时限制状态边和写入者。Cancel/Expire 是 RunController 的横切终止操作，
// 在 Transition 中单独处理，以免为每个非终态复制相同的状态边。
var runTransitions = map[transitionKey]bool{
	{ActorController, StatusCreated, StatusAdmission}: true,

	{ActorAdmission, StatusAdmission, StatusPlanning}:         true,
	{ActorAdmission, StatusAdmission, StatusPendingCapacity}:  true,
	{ActorAdmission, StatusAdmission, StatusPendingQuota}:     true,
	{ActorAdmission, StatusAdmission, StatusPendingResource}:  true,
	{ActorController, StatusPendingCapacity, StatusAdmission}: true,
	{ActorController, StatusPendingQuota, StatusAdmission}:    true,
	{ActorController, StatusPendingResource, StatusAdmission}: true,

	{ActorPlanner, StatusPlanning, StatusReady}:       true,
	{ActorPlanner, StatusPlanning, StatusWaitingUser}: true,
	{ActorPlanner, StatusPlanning, StatusFailed}:      true,
	{ActorController, StatusReady, StatusRunning}:     true,

	{ActorRuntime, StatusRunning, StatusWaitingUser}:     true,
	{ActorRuntime, StatusRunning, StatusWaitingExternal}: true,
	{ActorRuntime, StatusRunning, StatusRecovering}:      true,
	{ActorRuntime, StatusRunning, StatusDegraded}:        true,
	{ActorRuntime, StatusRunning, StatusVerifying}:       true,
	{ActorController, StatusRunning, StatusPaused}:       true,
	{ActorController, StatusPaused, StatusRunning}:       true,
	{ActorController, StatusWaitingUser, StatusRunning}:  true,
	{ActorRuntime, StatusWaitingExternal, StatusRunning}: true,

	{ActorRuntime, StatusRecovering, StatusRunning}:  true,
	{ActorRuntime, StatusRecovering, StatusDegraded}: true,
	{ActorRuntime, StatusRecovering, StatusFailed}:   true,
	{ActorRuntime, StatusDegraded, StatusRunning}:    true,
	{ActorRuntime, StatusDegraded, StatusVerifying}:  true,
	{ActorRuntime, StatusDegraded, StatusFailed}:     true,

	{ActorVerifier, StatusVerifying, StatusCompleted}: true,
	{ActorVerifier, StatusVerifying, StatusFailed}:    true,

	// Verification 或用户补充信息暴露了错误假设时，Planner 可以创建新的 PlanVersion。
	{ActorPlanner, StatusVerifying, StatusPlanning}:   true,
	{ActorPlanner, StatusWaitingUser, StatusPlanning}: true,
}

// CanTransition 以只读方式检查状态边，便于 API 在提交事务前给出稳定错误。
func (r Run) CanTransition(actor Actor, next Status) bool {
	if !r.Status.valid() || !next.valid() || r.IsTerminal() || r.Status == next {
		return false
	}
	if actor == ActorController && (next == StatusCanceled || next == StatusExpired) {
		return true
	}
	return runTransitions[transitionKey{actor: actor, from: r.Status, to: next}]
}

// Transition 只在状态边合法时修改内存对象；失败不会污染原状态。
func (r *Run) Transition(actor Actor, next Status) error {
	if r == nil {
		return fmt.Errorf("%w: run 不能为空", domain.ErrInvalidTransition)
	}
	if !r.CanTransition(actor, next) {
		return fmt.Errorf("%w: %s 无权把 run %s 从 %s 变为 %s", domain.ErrInvalidTransition, actor, r.ID, r.Status, next)
	}
	r.Status = next
	return nil
}

// IsWaiting 表示 Run 暂时不能自行向前推进，但尚未结束。
func (r Run) IsWaiting() bool {
	switch r.Status {
	case StatusPendingCapacity, StatusPendingQuota, StatusPendingResource,
		StatusWaitingUser, StatusWaitingExternal, StatusPaused:
		return true
	default:
		return false
	}
}

// IsTerminal 表示 Run 已经到达不可逆终态。
func (r Run) IsTerminal() bool {
	return r.Status == StatusCompleted || r.Status == StatusFailed ||
		r.Status == StatusCanceled || r.Status == StatusExpired
}
