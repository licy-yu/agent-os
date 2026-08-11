// Package task 定义 Task Contract、依赖关系和状态机。
package task

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/domain"
)

// Status 是任务的可恢复生命周期。
type Status string

const (
	StatusCreated         Status = "CREATED"
	StatusPlanning        Status = "PLANNING"
	StatusBlocked         Status = "BLOCKED"
	StatusReady           Status = "READY"
	StatusScheduling      Status = "SCHEDULING"
	StatusAssigned        Status = "ASSIGNED"
	StatusRunning         Status = "RUNNING"
	StatusWaitingTool     Status = "WAITING_TOOL"
	StatusWaitingInput    Status = "WAITING_INPUT"
	StatusWaitingApproval Status = "WAITING_APPROVAL"
	StatusWaitingExternal Status = "WAITING_EXTERNAL"
	StatusReview          Status = "REVIEW"
	StatusRetryWait       Status = "RETRY_WAIT"
	StatusSucceeded       Status = "SUCCEEDED"
	StatusFailed          Status = "FAILED"
	StatusCanceled        Status = "CANCELED"
	StatusRejected        Status = "REJECTED"
)

// Actor 表示有权发起特定状态转换的系统组件。
type Actor string

const (
	ActorPlanner    Actor = "PLANNER"
	ActorController Actor = "CONTROLLER"
	ActorScheduler  Actor = "SCHEDULER"
	ActorWorker     Actor = "WORKER"
	ActorReviewer   Actor = "REVIEWER"
	ActorRecovery   Actor = "RECOVERY"
)

// Requirements 是调度 Filter 的硬约束输入。
type Requirements struct {
	Skills           map[string]float64 `json:"skills,omitempty"`
	Tools            []string           `json:"tools,omitempty"`
	Permissions      []string           `json:"permissions,omitempty"`
	Models           []string           `json:"models,omitempty"`
	MaxContextTokens int64              `json:"max_context_tokens,omitempty"`
	RiskZone         string             `json:"risk_zone,omitempty"`
}

// Acceptance 是 Reviewer 判断“完成”的机器可读合同。
type Acceptance struct {
	Build          bool     `json:"build,omitempty"`
	UnitTest       bool     `json:"unit_test,omitempty"`
	MinCoverage    float64  `json:"min_coverage,omitempty"`
	SecurityReview bool     `json:"security_review,omitempty"`
	RequiredChecks []string `json:"required_checks,omitempty"`
}

// ExecutionPolicy 为每个任务设置独立的资源与死循环保险丝。
type ExecutionPolicy struct {
	MaxAttempts         int32 `json:"max_attempts"`
	MaxHandoffs         int32 `json:"max_handoffs"`
	MaxTokens           int64 `json:"max_tokens"`
	MaxToolCalls        int32 `json:"max_tool_calls"`
	MaxNoProgressRounds int32 `json:"max_no_progress_rounds"`
	TimeoutSeconds      int64 `json:"timeout_seconds"`
}

// DefaultExecutionPolicy 给 Planner 未显式设置的任务提供安全上限。
func DefaultExecutionPolicy() ExecutionPolicy {
	return ExecutionPolicy{
		MaxAttempts: 3, MaxHandoffs: 10, MaxTokens: 100_000,
		MaxToolCalls: 100, MaxNoProgressRounds: 3, TimeoutSeconds: int64((30 * time.Minute).Seconds()),
	}
}

// Timeout 把持久化的秒数转换为 Go 时长，Runtime 不直接解释 JSON 数值单位。
func (p ExecutionPolicy) Timeout() time.Duration {
	return time.Duration(p.TimeoutSeconds) * time.Second
}

// Task 是逻辑工作单元；每次实际执行由独立 Attempt 记录。
type Task struct {
	ID              uuid.UUID
	SwarmID         uuid.UUID
	ParentID        *uuid.UUID
	Name            string
	Goal            string
	Status          Status
	Priority        int32
	Input           map[string]any
	Requirements    Requirements
	Acceptance      Acceptance
	ExecutionPolicy ExecutionPolicy
	DependencyIDs   []uuid.UUID
	AssignedAgentID *uuid.UUID
	AttemptCount    int32
	AvailableAt     time.Time
	Deadline        *time.Time
	Version         int64
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type transitionKey struct {
	actor Actor
	from  Status
	to    Status
}

// taskTransitions 同时约束“从哪里到哪里”和“谁有权修改”。
var taskTransitions = map[transitionKey]bool{
	{ActorPlanner, StatusCreated, StatusPlanning}:   true,
	{ActorPlanner, StatusPlanning, StatusBlocked}:   true,
	{ActorPlanner, StatusPlanning, StatusReady}:     true,
	{ActorController, StatusBlocked, StatusReady}:   true,
	{ActorController, StatusRetryWait, StatusReady}: true,
	// Scheduler 进程可能在 READY→SCHEDULING CAS 提交后被 SIGKILL。超过租约安全窗口后，
	// Controller 有权把这个孤儿状态恢复为 READY；正常短窗口内仍只归 Scheduler 所有。
	{ActorController, StatusScheduling, StatusReady}:     true,
	{ActorScheduler, StatusReady, StatusScheduling}:      true,
	{ActorScheduler, StatusScheduling, StatusAssigned}:   true,
	{ActorScheduler, StatusScheduling, StatusReady}:      true,
	{ActorWorker, StatusAssigned, StatusRunning}:         true,
	{ActorWorker, StatusRunning, StatusWaitingTool}:      true,
	{ActorWorker, StatusWaitingTool, StatusRunning}:      true,
	{ActorWorker, StatusRunning, StatusWaitingInput}:     true,
	{ActorWorker, StatusWaitingInput, StatusRunning}:     true,
	{ActorWorker, StatusRunning, StatusWaitingApproval}:  true,
	{ActorWorker, StatusWaitingApproval, StatusAssigned}: true,
	{ActorWorker, StatusRunning, StatusWaitingExternal}:  true,
	{ActorWorker, StatusWaitingExternal, StatusAssigned}: true,
	{ActorWorker, StatusRunning, StatusReview}:           true,
	{ActorReviewer, StatusReview, StatusSucceeded}:       true,
	{ActorReviewer, StatusReview, StatusRetryWait}:       true,
	{ActorReviewer, StatusReview, StatusFailed}:          true,
	{ActorRecovery, StatusRunning, StatusRetryWait}:      true,
	{ActorRecovery, StatusRunning, StatusFailed}:         true,
}

// Transition 执行受角色约束的状态转换。
func (t *Task) Transition(actor Actor, next Status) error {
	if !taskTransitions[transitionKey{actor: actor, from: t.Status, to: next}] {
		return fmt.Errorf("%w: %s 无权把 task %s 从 %s 变为 %s", domain.ErrInvalidTransition, actor, t.ID, t.Status, next)
	}
	t.Status = next
	return nil
}

// IsTerminal 表示 Task 不再进入调度队列。
func (t Task) IsTerminal() bool {
	return t.Status == StatusSucceeded || t.Status == StatusFailed || t.Status == StatusCanceled || t.Status == StatusRejected
}
