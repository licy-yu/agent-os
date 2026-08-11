// Package runcontrol 提供 V1.5 Run 控制面的用例合同。
//
// 这里的 JSON DTO 与领域对象分离：领域对象只关心状态机，DTO 还要承载名称、预算、
// 执行引擎和控制台聚合计数。这样后续把 HTTP 换成生成式 Protobuf 时，不会污染领域层。
package runcontrol

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/domain/run"
	"github.com/licy-yu/agent-os/internal/planning"
)

var (
	// DefaultTenantID/DefaultProjectID 只服务于单机开箱即用。认证中间件解析出真实租户后，
	// Service 会使用调用上下文中的 ID，而不是接受请求体伪造 tenantId。
	DefaultTenantID  = uuid.MustParse("00000000-0000-0000-0000-000000000001")
	DefaultProjectID = uuid.MustParse("00000000-0000-0000-0000-000000000002")
)

// CreateRunRequest 是创建一次执行的用户输入。Plan 为空时，服务会基于当前 Agent Catalog
// 生成一个可执行的单任务计划；不是把自然语言 Goal 直接交给 Worker。
type CreateRunRequest struct {
	Name             string                  `json:"name"`
	Goal             string                  `json:"goal"`
	BudgetTokens     int64                   `json:"budgetTokens"`
	BudgetCostMicros int64                   `json:"budgetCostMicros"`
	MaxAgents        int32                   `json:"maxAgents"`
	Priority         int32                   `json:"priority"`
	ExecutionEngine  string                  `json:"executionEngine,omitempty"`
	Deadline         *time.Time              `json:"deadline,omitempty"`
	Plan             *planning.PlanCandidate `json:"plan,omitempty"`
}

// ReplanRequest 必须记录原因。Plan 为空时会按新的 Goal 生成单任务候选；旧 PlanVersion、
// 已完成 Task 和 Artifact 都保留，不会被原地覆盖。
type ReplanRequest struct {
	Reason string                  `json:"reason"`
	Goal   string                  `json:"goal,omitempty"`
	Plan   *planning.PlanCandidate `json:"plan,omitempty"`
}

// ResolveInteractionRequest 是人工输入/审批的统一响应。Resolution 可以保存编辑后的参数，
// 但认证类交互只能保存 credentialRef，不能提交明文密钥。
type ResolveInteractionRequest struct {
	Action     string         `json:"action,omitempty"`
	Resolution map[string]any `json:"resolution,omitempty"`
	Version    int64          `json:"version,omitempty"`
}

// RunView 是控制台的稳定读模型。Task/Interaction 计数来自数据库聚合，不写回 Run 行。
type RunView struct {
	ID                   uuid.UUID        `json:"id"`
	TenantID             uuid.UUID        `json:"tenantId"`
	ProjectID            uuid.UUID        `json:"projectId"`
	Name                 string           `json:"name"`
	Goal                 string           `json:"goal"`
	NormalizedGoal       map[string]any   `json:"normalizedGoal"`
	Status               run.Status       `json:"status"`
	DesiredState         string           `json:"desiredState"`
	ExecutionEngine      string           `json:"executionEngine"`
	BudgetTokens         int64            `json:"budgetTokens"`
	BudgetCostMicros     int64            `json:"budgetCostMicros"`
	SpentTokens          int64            `json:"spentTokens"`
	SpentCostMicros      int64            `json:"spentCostMicros"`
	MaxAgents            int32            `json:"maxAgents"`
	Priority             int32            `json:"priority"`
	CurrentPlanVersionID *uuid.UUID       `json:"currentPlanVersionId,omitempty"`
	CurrentPlanVersion   int32            `json:"currentPlanVersion"`
	TaskStatuses         map[string]int64 `json:"taskStatuses"`
	PendingInteractions  int64            `json:"pendingInteractions"`
	Deadline             *time.Time       `json:"deadline,omitempty"`
	Version              int64            `json:"version"`
	CreatedAt            time.Time        `json:"createdAt"`
	UpdatedAt            time.Time        `json:"updatedAt"`
}

// PlanView 暴露不可变计划版本及 Compiler 结果。Candidate/Compiled 是 JSON 快照，
// 控制台可以显示 Plan Diff，但不能通过 PUT 修改历史版本。
type PlanView struct {
	ID               uuid.UUID      `json:"id"`
	RunID            uuid.UUID      `json:"runId"`
	Version          int32          `json:"version"`
	Source           string         `json:"source"`
	Status           string         `json:"status"`
	Candidate        map[string]any `json:"candidate"`
	Compiled         map[string]any `json:"compiled"`
	ValidationErrors []any          `json:"validationErrors"`
	Diff             map[string]any `json:"diff"`
	CompilerVersion  string         `json:"compilerVersion"`
	ContentHash      string         `json:"contentHash"`
	CreatedBy        string         `json:"createdBy"`
	CreatedAt        time.Time      `json:"createdAt"`
}

// Catalog 是 PlanCompiler 使用的平台事实快照。Planner 声称“有某工具/权限”不会自动
// 扩大这个集合；Catalog 必须来自已启用的 AgentTemplate 与 Tool Registry。
type Catalog struct {
	Capabilities map[string]float64
	Tools        map[string]bool
	Models       map[string]bool
	Permissions  map[string]bool
}

// CreateRecord 是传给 Store 的已校验事实；ID 和时间在用例层生成，便于单元测试和
// Temporal Activity 重放时明确控制非确定性边界。
type CreateRecord struct {
	ID               uuid.UUID
	TenantID         uuid.UUID
	ProjectID        uuid.UUID
	Name             string
	Goal             string
	NormalizedGoal   map[string]any
	BudgetTokens     int64
	BudgetCostMicros int64
	MaxAgents        int32
	Priority         int32
	ExecutionEngine  string
	Deadline         *time.Time
	PlanVersionID    uuid.UUID
	PlanVersion      int32
	PlanSource       string
	PlanCandidate    planning.PlanCandidate
	ExecutablePlan   planning.ExecutablePlan
	CompilerVersion  string
	CreatedBy        string
	CreatedAt        time.Time
}

// Store 把 Run 的多表原子写入封装在 PostgreSQL 事务内。任何实现都必须保证 Run、Plan、
// Task、Gate、Dependency 与 Outbox 要么一起提交，要么一起回滚。
type Store interface {
	PlanningCatalog(context.Context, uuid.UUID) (Catalog, error)
	CreateCompiledRun(context.Context, CreateRecord) (*RunView, error)
	GetRun(context.Context, uuid.UUID, uuid.UUID) (*RunView, error)
	ListRuns(context.Context, uuid.UUID, int) ([]RunView, error)
	TransitionRun(context.Context, uuid.UUID, uuid.UUID, int64, run.Status, string, string) (*RunView, error)
	ActivateReplan(context.Context, uuid.UUID, uuid.UUID, int64, CreateRecord, string) (*RunView, error)
	GetPlan(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) (*PlanView, error)
}
