// Package durabletemporal 实现 SwarmOS 的 Temporal Durable Run Runtime。
//
// Temporal 只拥有可重放的运行时协调状态；PostgreSQL 仍是长期业务事实和控制台投影的
// 唯一来源。Workflow 不能直接访问数据库、网络或系统时间，所有 I/O 都必须走 Activity。
package durabletemporal

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	WorkflowName           = "swarmos.v1_5.run"
	ReadProjectionActivity = "swarmos.v1_5.read_run_projection"
	SignalRunCommand       = "swarmos.run.command"
	QueryRuntimeState      = "swarmos.run.state"
)

// WorkflowInput 是启动一次 Durable Run 所需的不可变业务身份。
type WorkflowInput struct {
	RunID       string `json:"runId"`
	TenantID    string `json:"tenantId"`
	Status      string `json:"status"`
	PlanVersion int32  `json:"planVersion"`
}

// RunCommand 统一承载 API、Interaction 和控制器发出的 Signal。Action 使用封闭的大写值，
// Payload 只放可 JSON 序列化的无密钥数据。
type RunCommand struct {
	Action      string         `json:"action"`
	Reason      string         `json:"reason,omitempty"`
	PlanVersion int32          `json:"planVersion,omitempty"`
	Payload     map[string]any `json:"payload,omitempty"`
}

// RuntimeState 是 Query 返回的短期运行态。控制台的历史列表仍读 PostgreSQL，避免把
// Temporal Visibility 当业务查询数据库使用。
type RuntimeState struct {
	RunID          string    `json:"runId"`
	TenantID       string    `json:"tenantId"`
	Status         string    `json:"status"`
	DesiredState   string    `json:"desiredState"`
	WaitingReason  string    `json:"waitingReason,omitempty"`
	CurrentTaskID  string    `json:"currentTaskId,omitempty"`
	CurrentStep    string    `json:"currentStep,omitempty"`
	PlanVersion    int32     `json:"planVersion"`
	SignalSequence int64     `json:"signalSequence"`
	LastError      string    `json:"lastError,omitempty"`
	UpdatedAt      time.Time `json:"updatedAt"`
}

// Projection 是 Activity 从 PostgreSQL 读取的最小快照。它刻意不包含 Task/Artifact 正文，
// 以控制 Workflow History 的体积。
type Projection struct {
	Status        string `json:"status"`
	DesiredState  string `json:"desiredState"`
	PlanVersion   int32  `json:"planVersion"`
	CurrentTaskID string `json:"currentTaskId,omitempty"`
	CurrentStep   string `json:"currentStep,omitempty"`
}

type ProjectionRequest struct {
	RunID    string `json:"runId"`
	TenantID string `json:"tenantId"`
}

// SwarmRunWorkflow 是一条可重放的 Run 协调循环。它通过 Signal 立即响应用户控制，通过
// 有界 Activity 轮询修复“数据库事务已提交、Signal 暂时发送失败”的双写窗口。
func SwarmRunWorkflow(ctx workflow.Context, input WorkflowInput) (RuntimeState, error) {
	now := workflow.Now(ctx)
	state := RuntimeState{
		RunID: input.RunID, TenantID: input.TenantID, Status: input.Status,
		DesiredState: input.Status, PlanVersion: input.PlanVersion, UpdatedAt: now,
	}
	if state.Status == "" {
		state.Status, state.DesiredState = "RUNNING", "RUNNING"
	}
	if err := workflow.SetQueryHandler(ctx, QueryRuntimeState, func() (RuntimeState, error) {
		return state, nil
	}); err != nil {
		return state, err
	}

	commandChannel := workflow.GetSignalChannel(ctx, SignalRunCommand)
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout:    10 * time.Second,
		ScheduleToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Second, BackoffCoefficient: 2,
			MaximumInterval: 10 * time.Second, MaximumAttempts: 3,
		},
	})

	for !terminalStatus(state.Status) {
		poll := false
		selector := workflow.NewSelector(ctx)
		selector.AddReceive(commandChannel, func(channel workflow.ReceiveChannel, _ bool) {
			var command RunCommand
			channel.Receive(ctx, &command)
			applyCommand(&state, command, workflow.Now(ctx))
		})
		selector.AddFuture(workflow.NewTimer(ctx, 15*time.Second), func(workflow.Future) { poll = true })
		selector.Select(ctx)

		if poll {
			var projection Projection
			err := workflow.ExecuteActivity(activityCtx, ReadProjectionActivity, ProjectionRequest{
				RunID: state.RunID, TenantID: state.TenantID,
			}).Get(activityCtx, &projection)
			if err != nil {
				// Activity 失败不破坏整个长时 Workflow；保留错误供 Query/告警读取，下一轮再修复。
				state.LastError = err.Error()
				state.UpdatedAt = workflow.Now(ctx)
				continue
			}
			applyProjection(&state, projection, workflow.Now(ctx))
		}
	}
	return state, nil
}

func applyCommand(state *RuntimeState, command RunCommand, now time.Time) {
	state.SignalSequence++
	state.UpdatedAt = now
	state.LastError = ""
	switch command.Action {
	case "PAUSE":
		if !terminalStatus(state.Status) {
			state.Status, state.DesiredState = "PAUSED", "PAUSED"
			state.WaitingReason = command.Reason
		}
	case "RESUME":
		if !terminalStatus(state.Status) {
			state.Status, state.DesiredState = "RUNNING", "RUNNING"
			state.WaitingReason = ""
		}
	case "CANCEL":
		state.Status, state.DesiredState = "CANCELED", "CANCELED"
		state.WaitingReason = command.Reason
	case "REPLAN":
		if command.PlanVersion > state.PlanVersion {
			state.PlanVersion = command.PlanVersion
		} else {
			state.PlanVersion++
		}
		state.WaitingReason = command.Reason
	case "WAIT_INPUT", "WAIT_APPROVAL":
		state.Status, state.DesiredState = "WAITING_USER", "WAITING_USER"
		state.WaitingReason = command.Reason
	case "PROVIDE_INPUT", "APPROVE", "REJECT":
		if state.Status == "WAITING_USER" {
			state.Status, state.DesiredState = "RUNNING", "RUNNING"
			state.WaitingReason = ""
		}
	case "COMPLETE":
		state.Status, state.DesiredState = "COMPLETED", "COMPLETED"
	case "FAIL":
		state.Status, state.DesiredState = "FAILED", "FAILED"
		state.WaitingReason = command.Reason
	default:
		state.LastError = "未知 Run Command: " + command.Action
	}
}

func applyProjection(state *RuntimeState, projection Projection, now time.Time) {
	if projection.Status != "" {
		state.Status = projection.Status
	}
	if projection.DesiredState != "" {
		state.DesiredState = projection.DesiredState
	}
	if projection.PlanVersion > state.PlanVersion {
		state.PlanVersion = projection.PlanVersion
	}
	state.CurrentTaskID = projection.CurrentTaskID
	state.CurrentStep = projection.CurrentStep
	state.LastError = ""
	state.UpdatedAt = now
}

func terminalStatus(value string) bool {
	switch value {
	case "COMPLETED", "FAILED", "CANCELED", "EXPIRED":
		return true
	default:
		return false
	}
}
