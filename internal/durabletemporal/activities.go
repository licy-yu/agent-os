package durabletemporal

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/runcontrol"
)

// ProjectionReader 是 Activity 对 PostgreSQL 的最小依赖。Repository 已经满足该接口；
// 测试可用内存实现，Workflow 本身不需要知道 SQL。
type ProjectionReader interface {
	GetRun(context.Context, uuid.UUID, uuid.UUID) (*runcontrol.RunView, error)
}

type Activities struct {
	Reader ProjectionReader
}

// ReadRunProjection 在 Activity 边界访问数据库。UUID 在这里解析，非法历史输入会形成
// 明确的不可恢复错误，而不是把零 UUID 传入租户查询。
func (a *Activities) ReadRunProjection(ctx context.Context, request ProjectionRequest) (Projection, error) {
	if a == nil || a.Reader == nil {
		return Projection{}, fmt.Errorf("Temporal Projection Reader 未初始化")
	}
	runID, err := uuid.Parse(request.RunID)
	if err != nil {
		return Projection{}, fmt.Errorf("解析 runId: %w", err)
	}
	tenantID, err := uuid.Parse(request.TenantID)
	if err != nil {
		return Projection{}, fmt.Errorf("解析 tenantId: %w", err)
	}
	view, err := a.Reader.GetRun(ctx, tenantID, runID)
	if err != nil {
		return Projection{}, err
	}
	return Projection{
		Status: string(view.Status), DesiredState: view.DesiredState,
		PlanVersion: view.CurrentPlanVersion,
	}, nil
}
