// Package safetycontrol 提供 SwarmOS 安全控制面的应用层合同。
//
// 该包刻意把 HTTP DTO、领域状态机与持久化事务分开：DTO 使用稳定的 camelCase JSON，
// Service 负责租户、权限和状态校验，Store 则负责 CAS、幂等记录、审计事件与业务数据的原子提交。
package safetycontrol

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/effect"
	"github.com/licy-yu/agent-os/internal/interaction"
	"github.com/licy-yu/agent-os/internal/verification"
)

const (
	// 下列 scope 是安全控制面所有写操作的最小权限。认证边界可以授予具体 scope，
	// 本地单机管理员则由 runcontrol.Principal 的 "*" scope 兼容。
	ScopeInteractionCreate  = "safety:interaction:create"
	ScopeInteractionResolve = "safety:interaction:resolve"
	ScopeEffectReconcile    = "safety:effect:reconcile"
	ScopeEffectCompensate   = "safety:effect:compensate"
)

// InteractionQuery 是 Interaction 列表的服务端过滤条件。
// Status 接受领域状态；Service 还会把前端历史值 PENDING 归一化为 WAITING。
type InteractionQuery struct {
	RunID  uuid.UUID          `json:"runId,omitempty"`
	Status interaction.Status `json:"status,omitempty"`
	Limit  int                `json:"limit,omitempty"`
}

// CreateInteractionRequest 只包含调用方可声明的字段。tenantId、requestedBy、ID 和时间均由可信边界生成，
// 防止请求体伪造租户或审计身份。EffectID 非空时，Store 必须在同一事务内固化双向关联。
type CreateInteractionRequest struct {
	RunID           uuid.UUID        `json:"runId"`
	TaskID          *uuid.UUID       `json:"taskId,omitempty"`
	AttemptID       *uuid.UUID       `json:"attemptId,omitempty"`
	EffectID        *uuid.UUID       `json:"effectId,omitempty"`
	InteractionType interaction.Type `json:"interactionType"`
	Title           string           `json:"title"`
	Description     string           `json:"description,omitempty"`
	Payload         map[string]any   `json:"payload,omitempty"`
	AllowedActions  []string         `json:"allowedActions,omitempty"`
	ExpiresAt       *time.Time       `json:"expiresAt,omitempty"`
}

// InteractionCommandRequest 是 approve/reject/resolve 的统一请求体。
// Version 是浏览器最后看到的版本，用于 CAS；Resolution 只允许保存结构化、已脱敏内容。
type InteractionCommandRequest struct {
	Action     string         `json:"action,omitempty"`
	Resolution map[string]any `json:"resolution,omitempty"`
	Version    int64          `json:"version"`
}

// InteractionResponseView 是可审计的人机响应，actor 与时间只由 Service 注入。
type InteractionResponseView struct {
	Action      string         `json:"action"`
	Resolution  map[string]any `json:"resolution"`
	RespondedBy string         `json:"respondedBy"`
	RespondedAt time.Time      `json:"respondedAt"`
}

// InteractionView 是控制台和 API 共用的 Interaction 读模型。
// Prompt 与 Description 同时保留，便于旧控制台逐步迁移而不猜测 payload 中的字段。
type InteractionView struct {
	ID              uuid.UUID                `json:"id"`
	TenantID        uuid.UUID                `json:"tenantId"`
	RunID           uuid.UUID                `json:"runId"`
	TaskID          *uuid.UUID               `json:"taskId,omitempty"`
	AttemptID       *uuid.UUID               `json:"attemptId,omitempty"`
	EffectID        *uuid.UUID               `json:"effectId,omitempty"`
	InteractionType interaction.Type         `json:"interactionType"`
	Status          interaction.Status       `json:"status"`
	Title           string                   `json:"title"`
	Prompt          string                   `json:"prompt,omitempty"`
	Description     string                   `json:"description,omitempty"`
	Payload         map[string]any           `json:"payload"`
	AllowedActions  []string                 `json:"allowedActions"`
	Response        *InteractionResponseView `json:"response,omitempty"`
	RiskLevel       effect.RiskLevel         `json:"riskLevel,omitempty"`
	RequestedBy     string                   `json:"requestedBy"`
	ResolvedBy      string                   `json:"resolvedBy,omitempty"`
	ExpiresAt       *time.Time               `json:"expiresAt,omitempty"`
	Version         int64                    `json:"version"`
	CreatedAt       time.Time                `json:"createdAt"`
	ResolvedAt      *time.Time               `json:"resolvedAt,omitempty"`
	UpdatedAt       time.Time                `json:"updatedAt"`
}

// ArtifactQuery 是某个 Run 的产物过滤条件。
type ArtifactQuery struct {
	RunID  uuid.UUID `json:"runId"`
	Status string    `json:"status,omitempty"`
	Limit  int       `json:"limit,omitempty"`
}

// ArtifactView 是不可把大对象内联进数据库/API 的产物描述符。
// ContentURI/ObjectKey 指向对象存储，ContentHash 与 VersionNo 共同固定可复现版本。
type ArtifactView struct {
	ID            uuid.UUID      `json:"id"`
	TenantID      uuid.UUID      `json:"tenantId"`
	RunID         uuid.UUID      `json:"runId"`
	TaskID        *uuid.UUID     `json:"taskId,omitempty"`
	AttemptID     *uuid.UUID     `json:"attemptId,omitempty"`
	Name          string         `json:"name"`
	ArtifactType  string         `json:"artifactType"`
	Status        string         `json:"status"`
	MediaType     string         `json:"mediaType,omitempty"`
	SchemaVersion string         `json:"schemaVersion,omitempty"`
	VersionNo     int64          `json:"versionNo"`
	ContentURI    string         `json:"contentUri,omitempty"`
	ObjectKey     string         `json:"objectKey,omitempty"`
	ContentHash   string         `json:"contentHash"`
	SizeBytes     int64          `json:"sizeBytes"`
	Metadata      map[string]any `json:"metadata"`
	Validation    map[string]any `json:"validation,omitempty"`
	ValidatedAt   *time.Time     `json:"validatedAt,omitempty"`
	CreatedAt     time.Time      `json:"createdAt"`
	UpdatedAt     time.Time      `json:"updatedAt"`
}

// ArtifactLineageQuery 控制血缘遍历方向和最大深度。Service 将深度限制在 1~20，防止无界图查询。
type ArtifactLineageQuery struct {
	Direction string `json:"direction,omitempty"`
	Depth     int    `json:"depth,omitempty"`
}

// ArtifactLineageEdgeView 是一条有向血缘边；Metadata 只能保存小型结构化说明。
type ArtifactLineageEdgeView struct {
	ID             uuid.UUID      `json:"id"`
	TenantID       uuid.UUID      `json:"tenantId"`
	FromArtifactID uuid.UUID      `json:"fromArtifactId"`
	ToArtifactID   uuid.UUID      `json:"toArtifactId"`
	RelationType   string         `json:"relationType"`
	Metadata       map[string]any `json:"metadata"`
	CreatedAt      time.Time      `json:"createdAt"`
}

// ArtifactLineageView 同时返回节点和边，客户端无需为每条边再次查询产物详情。
type ArtifactLineageView struct {
	RootArtifactID uuid.UUID                 `json:"rootArtifactId"`
	Artifacts      []ArtifactView            `json:"artifacts"`
	Edges          []ArtifactLineageEdgeView `json:"edges"`
}

// EffectQuery 是某个 Run 的 Effect 列表过滤条件。
type EffectQuery struct {
	RunID     uuid.UUID        `json:"runId"`
	Status    effect.Status    `json:"status,omitempty"`
	RiskLevel effect.RiskLevel `json:"riskLevel,omitempty"`
	Limit     int              `json:"limit,omitempty"`
}

// EffectCommandRequest 对 reconcile/compensate 同样执行乐观锁，避免控制器和人工操作相互覆盖。
type EffectCommandRequest struct {
	Version int64  `json:"version"`
	Reason  string `json:"reason,omitempty"`
}

// EffectView 是外部副作用的完整安全读模型。AuthorizationBlocked 表示审批已拒绝但领域状态仍按
// 设计基线保留为 PREPARED；任何 Store 实现都必须让该标记永久阻止后续授权。
type EffectView struct {
	ID                       uuid.UUID        `json:"id"`
	TenantID                 uuid.UUID        `json:"tenantId"`
	RunID                    uuid.UUID        `json:"runId"`
	TaskID                   *uuid.UUID       `json:"taskId,omitempty"`
	AttemptID                uuid.UUID        `json:"attemptId"`
	ToolCallID               *uuid.UUID       `json:"toolCallId,omitempty"`
	IdempotencyKey           string           `json:"idempotencyKey"`
	EffectType               string           `json:"effectType"`
	RiskLevel                effect.RiskLevel `json:"riskLevel"`
	Status                   effect.Status    `json:"status"`
	RequestHash              string           `json:"requestHash"`
	SanitizedRequest         map[string]any   `json:"sanitizedRequest"`
	SanitizedResult          map[string]any   `json:"sanitizedResult,omitempty"`
	ExternalRef              string           `json:"externalRef,omitempty"`
	ApprovalInteractionID    *uuid.UUID       `json:"approvalInteractionId,omitempty"`
	ApprovedBy               string           `json:"approvedBy,omitempty"`
	AuthorizationBlocked     bool             `json:"authorizationBlocked"`
	AuthorizationBlockReason string           `json:"authorizationBlockReason,omitempty"`
	ReconcileAfter           *time.Time       `json:"reconcileAfter,omitempty"`
	RetryCount               int              `json:"retryCount"`
	CompensationSpec         map[string]any   `json:"compensationSpec,omitempty"`
	ErrorCode                string           `json:"errorCode,omitempty"`
	ErrorMessage             string           `json:"errorMessage,omitempty"`
	FencingToken             int64            `json:"fencingToken"`
	Version                  int64            `json:"version"`
	PreparedAt               time.Time        `json:"preparedAt"`
	AuthorizedAt             *time.Time       `json:"authorizedAt,omitempty"`
	StartedAt                *time.Time       `json:"startedAt,omitempty"`
	FinishedAt               *time.Time       `json:"finishedAt,omitempty"`
	UpdatedAt                time.Time        `json:"updatedAt"`
}

// TimelineQuery 控制 Run Timeline 的读取窗口。
type TimelineQuery struct {
	RunID uuid.UUID `json:"runId"`
	Limit int       `json:"limit,omitempty"`
}

// TimelineEventView 是跨 Aggregate 的统一时间线投影。
type TimelineEventView struct {
	ID            uuid.UUID      `json:"id"`
	TenantID      uuid.UUID      `json:"tenantId"`
	RunID         uuid.UUID      `json:"runId"`
	TaskID        *uuid.UUID     `json:"taskId,omitempty"`
	AttemptID     *uuid.UUID     `json:"attemptId,omitempty"`
	EventType     string         `json:"eventType"`
	AggregateType string         `json:"aggregateType"`
	AggregateID   uuid.UUID      `json:"aggregateId"`
	Actor         string         `json:"actor,omitempty"`
	ActorType     string         `json:"actorType,omitempty"`
	Message       string         `json:"message"`
	Summary       string         `json:"summary,omitempty"`
	Status        string         `json:"status,omitempty"`
	Payload       map[string]any `json:"payload"`
	CorrelationID *uuid.UUID     `json:"correlationId,omitempty"`
	CausationID   *uuid.UUID     `json:"causationId,omitempty"`
	TraceID       string         `json:"traceId,omitempty"`
	CreatedAt     time.Time      `json:"createdAt"`
	PublishedAt   *time.Time     `json:"publishedAt,omitempty"`
}

// SchedulerDecisionQuery 控制调度决策的 Run 级列表。
type SchedulerDecisionQuery struct {
	RunID uuid.UUID `json:"runId"`
	Limit int       `json:"limit,omitempty"`
}

// SchedulerDecisionView 保留候选、过滤器和最终原因，以便解释“为什么是这个 Agent”。
type SchedulerDecisionView struct {
	ID              uuid.UUID        `json:"id"`
	TenantID        uuid.UUID        `json:"tenantId"`
	RunID           uuid.UUID        `json:"runId"`
	TaskID          uuid.UUID        `json:"taskId"`
	TaskName        string           `json:"taskName,omitempty"`
	SchedulerID     string           `json:"schedulerId"`
	TaskVersion     int64            `json:"taskVersion"`
	QueueScore      float64          `json:"queueScore"`
	SelectedAgentID *uuid.UUID       `json:"selectedAgentId,omitempty"`
	AgentID         *uuid.UUID       `json:"agentId,omitempty"`
	AgentName       string           `json:"agentName,omitempty"`
	SelectedScore   float64          `json:"selectedScore"`
	Outcome         string           `json:"outcome,omitempty"`
	Candidates      []map[string]any `json:"candidates"`
	Filters         []map[string]any `json:"filters"`
	Reason          string           `json:"reason"`
	Details         map[string]any   `json:"details,omitempty"`
	CreatedAt       time.Time        `json:"createdAt"`
}

// VerificationQuery 是 Run 级验证执行列表。
type VerificationQuery struct {
	RunID uuid.UUID `json:"runId"`
	Limit int       `json:"limit,omitempty"`
}

// AcceptanceGateView 固化验收门规格；Required=false 的失败仍展示，但不会单独阻止完成。
type AcceptanceGateView struct {
	ID        uuid.UUID      `json:"id"`
	Name      string         `json:"name"`
	GateType  string         `json:"gateType"`
	Required  bool           `json:"required"`
	Config    map[string]any `json:"config"`
	CreatedAt time.Time      `json:"createdAt"`
}

// GateResultView 是一次硬验证的证据索引，不内联大型测试日志。
type GateResultView struct {
	ID           uuid.UUID               `json:"id"`
	GateID       uuid.UUID               `json:"gateId"`
	GateName     string                  `json:"gateName"`
	Status       verification.GateStatus `json:"status"`
	EvidenceRefs []string                `json:"evidenceRefs"`
	Metrics      map[string]float64      `json:"metrics"`
	Message      string                  `json:"message,omitempty"`
	StartedAt    *time.Time              `json:"startedAt,omitempty"`
	FinishedAt   *time.Time              `json:"finishedAt,omitempty"`
}

// VerificationRunView 聚合一次 Verification 与其 Gate 结果。
type VerificationRunView struct {
	ID         uuid.UUID            `json:"id"`
	TenantID   uuid.UUID            `json:"tenantId"`
	RunID      uuid.UUID            `json:"runId"`
	TaskID     *uuid.UUID           `json:"taskId,omitempty"`
	AttemptID  *uuid.UUID           `json:"attemptId,omitempty"`
	Status     string               `json:"status"`
	Gates      []AcceptanceGateView `json:"gates"`
	Results    []GateResultView     `json:"results"`
	CreatedAt  time.Time            `json:"createdAt"`
	FinishedAt *time.Time           `json:"finishedAt,omitempty"`
}

// CompletionManifestView 是任务或 Run 宣告完成时的不可变证据清单。
type CompletionManifestView struct {
	ID             uuid.UUID                   `json:"id"`
	TenantID       uuid.UUID                   `json:"tenantId"`
	RunID          uuid.UUID                   `json:"runId"`
	TaskID         *uuid.UUID                  `json:"taskId,omitempty"`
	AttemptID      *uuid.UUID                  `json:"attemptId,omitempty"`
	VerificationID *uuid.UUID                  `json:"verificationId,omitempty"`
	Status         verification.ManifestStatus `json:"status"`
	Artifacts      []ArtifactView              `json:"artifacts"`
	Evidence       []GateResultView            `json:"evidence"`
	KnownIssues    []string                    `json:"knownIssues"`
	Assumptions    []string                    `json:"assumptions"`
	Summary        map[string]any              `json:"summary"`
	ContentHash    string                      `json:"contentHash"`
	CreatedAt      time.Time                   `json:"createdAt"`
}

// MutationMetadata 是 Store 落库每个写操作时必须同时保存的幂等与审计事实。
// 唯一键是 (tenantId, idempotencyKey)；同键异 hash 必须返回冲突，不能静默回放旧结果。
type MutationMetadata struct {
	TenantID        uuid.UUID
	Operation       string
	AggregateID     uuid.UUID
	ExpectedVersion int64
	IdempotencyKey  string
	RequestHash     string
	Actor           string
	OccurredAt      time.Time
}

// MutationResult 让 Service 在领域校验前完成幂等回放。
// Store 返回 nil 表示尚未执行；命中同键异 hash 时必须返回 ErrIdempotencyConflict。
type MutationResult struct {
	Operation   string
	Interaction *InteractionView
	Effect      *EffectView
}

// EffectApprovalBinding 要求创建审批 Interaction 与绑定 Effect 在同一事务中提交。
type EffectApprovalBinding struct {
	EffectID              uuid.UUID
	ExpectedEffectVersion int64
}

// CreateInteractionRecord 是已经完成领域校验的创建事实。
type CreateInteractionRecord struct {
	Interaction InteractionView
	Binding     *EffectApprovalBinding
	Mutation    MutationMetadata
}

// EffectAuthorization 描述 approve 命令附带的 Effect 授权 CAS。
// Store 必须先确认 approvalInteractionId 相等且 AuthorizationBlocked=false，再写 AUTHORIZED。
type EffectAuthorization struct {
	EffectID              uuid.UUID
	ExpectedEffectVersion int64
	ExpectedStatus        effect.Status
	NextStatus            effect.Status
	ApprovedBy            string
	ApprovedAt            time.Time
}

// EffectRejection 描述审批拒绝后的封存事实。Effect 继续保持 PREPARED，Store 应写审计事件和拒绝原因，
// 并让后续读取返回 AuthorizationBlocked=true，永久阻止该 Effect 被授权。
type EffectRejection struct {
	EffectID              uuid.UUID
	ExpectedEffectVersion int64
	ExpectedStatus        effect.Status
	RejectedBy            string
	RejectedAt            time.Time
	Reason                string
}

// ResolveInteractionRecord 要求 Interaction 响应、可选 Effect 授权/封存、审计和幂等结果同事务提交。
type ResolveInteractionRecord struct {
	InteractionID   uuid.UUID
	ExpectedStatus  interaction.Status
	ExpectedVersion int64
	Response        InteractionResponseView
	Authorization   *EffectAuthorization
	Rejection       *EffectRejection
	Mutation        MutationMetadata
}

// TransitionEffectRecord 是 reconcile/compensate 的领域校验结果。
type TransitionEffectRecord struct {
	EffectID        uuid.UUID
	ExpectedStatus  effect.Status
	NextStatus      effect.Status
	ExpectedVersion int64
	Reason          string
	Mutation        MutationMetadata
}

// Store 是安全控制面的持久化端口。所有方法都显式携带 tenantId；实现不得先按全局 ID 查询再在内存中过滤。
// 写方法还必须把业务变更、版本 CAS、幂等结果、Timeline/Outbox 审计事件放进同一 PostgreSQL 事务。
type Store interface {
	EnsureRunTenant(context.Context, uuid.UUID, uuid.UUID) error
	FindMutation(context.Context, uuid.UUID, string, string) (*MutationResult, error)

	ListInteractions(context.Context, uuid.UUID, InteractionQuery) ([]InteractionView, error)
	GetInteraction(context.Context, uuid.UUID, uuid.UUID) (*InteractionView, error)
	CreateInteraction(context.Context, CreateInteractionRecord) (*InteractionView, error)
	ResolveInteraction(context.Context, ResolveInteractionRecord) (*InteractionView, error)

	ListArtifacts(context.Context, uuid.UUID, ArtifactQuery) ([]ArtifactView, error)
	GetArtifact(context.Context, uuid.UUID, uuid.UUID) (*ArtifactView, error)
	GetArtifactLineage(context.Context, uuid.UUID, uuid.UUID, ArtifactLineageQuery) (*ArtifactLineageView, error)

	ListEffects(context.Context, uuid.UUID, EffectQuery) ([]EffectView, error)
	GetEffect(context.Context, uuid.UUID, uuid.UUID) (*EffectView, error)
	TransitionEffect(context.Context, TransitionEffectRecord) (*EffectView, error)

	ListTimeline(context.Context, uuid.UUID, TimelineQuery) ([]TimelineEventView, error)
	ListSchedulerDecisions(context.Context, uuid.UUID, SchedulerDecisionQuery) ([]SchedulerDecisionView, error)
	ListVerificationRuns(context.Context, uuid.UUID, VerificationQuery) ([]VerificationRunView, error)
	GetVerificationRun(context.Context, uuid.UUID, uuid.UUID) (*VerificationRunView, error)
	GetCompletionManifest(context.Context, uuid.UUID, uuid.UUID) (*CompletionManifestView, error)
}
