package durabletemporal

import (
	"fmt"

	"github.com/licy-yu/agent-os/internal/conf"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// NewWorker 注册显式名称，避免 Go 函数重命名破坏历史 Workflow 的重放兼容性。
func NewWorker(temporalClient client.Client, cfg conf.TemporalConfig, activities *Activities) (worker.Worker, error) {
	if temporalClient == nil || activities == nil || activities.Reader == nil {
		return nil, fmt.Errorf("Temporal Worker 依赖不完整")
	}
	value := worker.New(temporalClient, cfg.TaskQueue, worker.Options{})
	value.RegisterWorkflowWithOptions(SwarmRunWorkflow, workflow.RegisterOptions{Name: WorkflowName})
	value.RegisterActivityWithOptions(activities.ReadRunProjection, activity.RegisterOptions{Name: ReadProjectionActivity})
	return value, nil
}
