package durabletemporal

import (
	"context"
	"fmt"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/conf"
	"github.com/licy-yu/agent-os/internal/runcontrol"
)

// WorkflowRef 是 PostgreSQL 需要持久化的 Temporal 身份。WorkflowID 使用 Run UUID
// 确定生成，天然支持 API/Outbox 重试；RunID 由 Temporal Server 分配。
type WorkflowRef struct {
	WorkflowID string
	RunID      string
}

// Gateway 同时供控制面启动/Signal Workflow，并负责连接健康状态。
type Gateway struct {
	client    client.Client
	taskQueue string
}

func Dial(ctx context.Context, cfg conf.TemporalConfig) (*Gateway, error) {
	value, err := client.DialContext(ctx, client.Options{
		HostPort: cfg.Address, Namespace: cfg.Namespace,
		Identity: "swarmos-control-plane",
	})
	if err != nil {
		return nil, fmt.Errorf("连接 Temporal %s: %w", cfg.Address, err)
	}
	if _, err := value.CheckHealth(ctx, nil); err != nil {
		value.Close()
		return nil, fmt.Errorf("Temporal 健康检查失败: %w", err)
	}
	return &Gateway{client: value, taskQueue: cfg.TaskQueue}, nil
}

func (g *Gateway) Ready() bool {
	return g != nil && g.client != nil && strings.TrimSpace(g.taskQueue) != ""
}

// Ping 供 HTTP readiness 探针做真实 gRPC 健康检查。Ready 只说明 Client 已构造，
// Temporal Server 后续失联时必须由 CheckHealth 暴露给滚动发布和运维脚本。
func (g *Gateway) Ping(ctx context.Context) error {
	if !g.Ready() {
		return fmt.Errorf("Temporal Gateway 未就绪")
	}
	if _, err := g.client.CheckHealth(ctx, nil); err != nil {
		return fmt.Errorf("Temporal CheckHealth: %w", err)
	}
	return nil
}

// StartRun 使用 RejectDuplicate，保证同一个业务 Run 永远不会误启动第二条 Workflow。
func (g *Gateway) StartRun(ctx context.Context, view *runcontrol.RunView) (runcontrol.WorkflowRef, error) {
	if !g.Ready() || view == nil {
		return runcontrol.WorkflowRef{}, fmt.Errorf("Temporal Gateway 未就绪")
	}
	workflowID := "swarmos/run/" + view.ID.String()
	run, err := g.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: workflowID, TaskQueue: g.taskQueue,
		WorkflowExecutionTimeout:                 365 * 24 * time.Hour,
		WorkflowTaskTimeout:                      10 * time.Second,
		WorkflowIDReusePolicy:                    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowIDConflictPolicy:                 enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		WorkflowExecutionErrorWhenAlreadyStarted: false,
		Memo:                                     map[string]any{"runId": view.ID.String(), "tenantId": view.TenantID.String()},
	}, WorkflowName, WorkflowInput{
		RunID: view.ID.String(), TenantID: view.TenantID.String(),
		Status: string(view.Status), PlanVersion: view.CurrentPlanVersion,
	})
	if err != nil {
		return runcontrol.WorkflowRef{}, fmt.Errorf("启动 Run Workflow: %w", err)
	}
	return runcontrol.WorkflowRef{WorkflowID: run.GetID(), RunID: run.GetRunID()}, nil
}

func (g *Gateway) SignalRun(ctx context.Context, runID uuid.UUID, command string,
	reason string, planVersion int32, payload map[string]any,
) error {
	if !g.Ready() || runID == uuid.Nil {
		return fmt.Errorf("Temporal Gateway 未就绪或 runId 非法")
	}
	return g.client.SignalWorkflow(ctx, "swarmos/run/"+runID.String(), "", SignalRunCommand, RunCommand{
		Action: strings.ToUpper(strings.TrimSpace(command)), Reason: strings.TrimSpace(reason),
		PlanVersion: planVersion, Payload: payload,
	})
}

func (g *Gateway) QueryRun(ctx context.Context, runID uuid.UUID) (RuntimeState, error) {
	var state RuntimeState
	encoded, err := g.client.QueryWorkflow(ctx, "swarmos/run/"+runID.String(), "", QueryRuntimeState)
	if err != nil {
		return state, err
	}
	if err := encoded.Get(&state); err != nil {
		return state, err
	}
	return state, nil
}

func (g *Gateway) Close() {
	if g != nil && g.client != nil {
		g.client.Close()
	}
}
