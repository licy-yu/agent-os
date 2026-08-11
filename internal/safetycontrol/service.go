package safetycontrol

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/effect"
	"github.com/licy-yu/agent-os/internal/interaction"
	"github.com/licy-yu/agent-os/internal/runcontrol"
)

var (
	// ErrInvalidRequest 表示请求在进入持久化层前已经违反安全控制面合同。
	ErrInvalidRequest = errors.New("安全控制请求不符合合同")
	// ErrForbidden 表示可信 Principal 缺少当前写操作所需的 scope 或 actor。
	ErrForbidden = errors.New("安全控制操作无权限")
	// ErrTenantBoundary 表示 Store 返回了请求租户之外的数据。该错误应映射为 404，避免泄漏资源存在性。
	ErrTenantBoundary = errors.New("资源不属于当前租户")
	// ErrVersionConflict 表示调用方提交的版本已过期，应映射为 HTTP 409 并刷新读模型。
	ErrVersionConflict = errors.New("资源版本冲突")
	// ErrIdempotencyRequired 表示写操作没有稳定的 Idempotency-Key。
	ErrIdempotencyRequired = errors.New("缺少有效的 Idempotency-Key")
	// ErrIdempotencyConflict 表示同一租户复用了幂等键，但请求内容已经不同。
	ErrIdempotencyConflict = errors.New("Idempotency-Key 请求冲突")
	// ErrApprovalRequired 表示 R3 Effect 缺少可验证的批准证据，或该授权已被拒绝封存。
	ErrApprovalRequired = errors.New("R3 Effect 尚未获得有效审批")
	// ErrInvalidStoreResult 是防御性错误：持久化端口返回了与命令合同不一致的结果。
	ErrInvalidStoreResult = errors.New("安全控制 Store 返回无效结果")
)

const (
	defaultListLimit   = 100
	maximumListLimit   = 200
	maximumPayloadSize = 64 * 1024
	maximumKeyLength   = 256

	operationInteractionCreate  = "interaction.create"
	operationInteractionApprove = "interaction.approve"
	operationInteractionReject  = "interaction.reject"
	operationInteractionResolve = "interaction.resolve"
	operationEffectReconcile    = "effect.reconcile"
	operationEffectCompensate   = "effect.compensate"
)

var validationInteractionID = uuid.MustParse("00000000-0000-0000-0000-000000000001")

// Service 组合租户边界、授权、领域状态机和幂等事务合同。
// clock/newID 作为依赖注入点，确保单元测试和 Durable Workflow 重放可以保持确定性。
type Service struct {
	store Store
	clock func() time.Time
	newID func() uuid.UUID
}

// NewService 创建安全控制面服务。Store 可以稍后由 PostgreSQL 适配器实现；nil Store 会在调用时明确失败。
func NewService(store Store) *Service {
	return &Service{
		store: store,
		clock: func() time.Time { return time.Now().UTC() },
		newID: uuid.New,
	}
}

// ListInteractions 返回当前租户的 Interaction；历史前端传入 PENDING 时会被归一化为 WAITING。
func (s *Service) ListInteractions(ctx context.Context, query InteractionQuery) ([]InteractionView, error) {
	principal, err := s.readPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	query.Status, err = normalizeInteractionStatus(query.Status)
	if err != nil {
		return nil, err
	}
	query.Limit = normalizeLimit(query.Limit)
	if query.RunID != uuid.Nil {
		if err := s.store.EnsureRunTenant(ctx, principal.TenantID, query.RunID); err != nil {
			return nil, err
		}
	}
	items, err := s.store.ListInteractions(ctx, principal.TenantID, query)
	if err != nil {
		return nil, err
	}
	for index := range items {
		if err := validateInteractionTenant(&items[index], principal.TenantID, query.RunID); err != nil {
			return nil, err
		}
	}
	return items, nil
}

// GetInteraction 以 tenant_id + id 查询并再次检查返回值，防止错误 Store 实现造成跨租户数据泄漏。
func (s *Service) GetInteraction(ctx context.Context, id uuid.UUID) (*InteractionView, error) {
	principal, err := s.readPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	if id == uuid.Nil {
		return nil, fmt.Errorf("%w: interaction id 不能为空", ErrInvalidRequest)
	}
	item, err := s.store.GetInteraction(ctx, principal.TenantID, id)
	if err != nil {
		return nil, err
	}
	if err := validateInteractionTenant(item, principal.TenantID, uuid.Nil); err != nil {
		return nil, err
	}
	return item, nil
}

// CreateInteraction 创建并立即发布一个 WAITING Interaction。
// Idempotency-Key 在生成新 ID 之前查询，因此 HTTP 超时后的同请求重试不会因随机 ID 不同而冲突。
func (s *Service) CreateInteraction(ctx context.Context, idempotencyKey string, request CreateInteractionRequest) (*InteractionView, error) {
	principal, err := s.writePrincipal(ctx, ScopeInteractionCreate)
	if err != nil {
		return nil, err
	}
	request, candidate, err := normalizeCreateInteraction(request)
	if err != nil {
		return nil, err
	}
	if request.ExpiresAt != nil {
		expires := request.ExpiresAt.UTC()
		if !expires.After(s.now()) {
			return nil, fmt.Errorf("%w: expiresAt 必须晚于当前时间", ErrInvalidRequest)
		}
		request.ExpiresAt = &expires
	}
	metadata, replay, err := s.prepareMutation(ctx, principal, operationInteractionCreate,
		uuid.Nil, 0, idempotencyKey, request)
	if err != nil {
		return nil, err
	}
	if replay != nil {
		return s.replayInteraction(replay, operationInteractionCreate, principal.TenantID)
	}
	if err := s.store.EnsureRunTenant(ctx, principal.TenantID, request.RunID); err != nil {
		return nil, err
	}

	var binding *EffectApprovalBinding
	risk := effect.RiskLevel("")
	if request.EffectID != nil {
		linked, getErr := s.store.GetEffect(ctx, principal.TenantID, *request.EffectID)
		if getErr != nil {
			return nil, getErr
		}
		if err := validateEffectTenant(linked, principal.TenantID, request.RunID); err != nil {
			return nil, err
		}
		if linked.Status != effect.StatusPrepared || linked.AuthorizationBlocked {
			return nil, fmt.Errorf("%w: 只有未封存的 PREPARED Effect 可以绑定新交互", ErrInvalidRequest)
		}
		if request.TaskID != nil && linked.TaskID != nil && *request.TaskID != *linked.TaskID {
			return nil, fmt.Errorf("%w: Interaction 与 Effect 的 taskId 不一致", ErrInvalidRequest)
		}
		if request.AttemptID != nil && *request.AttemptID != linked.AttemptID {
			return nil, fmt.Errorf("%w: Interaction 与 Effect 的 attemptId 不一致", ErrInvalidRequest)
		}
		risk = linked.RiskLevel
		if linked.RiskLevel == effect.RiskR3ProductionDestructive {
			if request.InteractionType != interaction.TypeApproval ||
				!containsAction(candidate.AllowedActions, "approve") || !containsAction(candidate.AllowedActions, "reject") {
				return nil, fmt.Errorf("%w: R3 Effect 必须绑定同时允许 approve/reject 的 APPROVAL Interaction", ErrApprovalRequired)
			}
		}
		if request.InteractionType == interaction.TypeApproval {
			if linked.ApprovalInteractionID != nil {
				return nil, fmt.Errorf("%w: Effect 已绑定审批 Interaction", ErrVersionConflict)
			}
			binding = &EffectApprovalBinding{EffectID: linked.ID, ExpectedEffectVersion: linked.Version}
		}
	}

	id := s.newID()
	domainInteraction, err := interaction.New(id, request.InteractionType, request.Title, request.Payload, request.AllowedActions)
	if err != nil {
		return nil, err
	}
	if err := domainInteraction.Transition(interaction.StatusWaiting); err != nil {
		return nil, err
	}
	now := s.now()
	view := InteractionView{
		ID: id, TenantID: principal.TenantID, RunID: request.RunID,
		TaskID: request.TaskID, AttemptID: request.AttemptID, EffectID: request.EffectID,
		InteractionType: domainInteraction.Type, Status: domainInteraction.Status,
		Title: domainInteraction.Title, Prompt: request.Description, Description: request.Description,
		Payload: domainInteraction.Payload, AllowedActions: domainInteraction.AllowedActions,
		RiskLevel: risk, RequestedBy: principal.Subject, ExpiresAt: request.ExpiresAt,
		Version: domainInteraction.Version, CreatedAt: now, UpdatedAt: now,
	}
	metadata.AggregateID = id
	created, err := s.store.CreateInteraction(ctx, CreateInteractionRecord{
		Interaction: view, Binding: binding, Mutation: metadata,
	})
	if err != nil {
		return nil, err
	}
	if err := validateInteractionTenant(created, principal.TenantID, request.RunID); err != nil {
		return nil, err
	}
	if created.Status != interaction.StatusWaiting {
		return nil, fmt.Errorf("%w: 创建结果状态必须为 WAITING", ErrInvalidStoreResult)
	}
	return created, nil
}

// Approve 只能处理 APPROVAL Interaction；若关联 Effect，则在同一 Store 事务内完成 PREPARED -> AUTHORIZED。
func (s *Service) Approve(ctx context.Context, id uuid.UUID, idempotencyKey string, request InteractionCommandRequest) (*InteractionView, error) {
	return s.resolveInteraction(ctx, id, idempotencyKey, request, "approve", operationInteractionApprove)
}

// Reject 原子解决审批并封存关联 Effect 的授权。Effect 按设计基线继续保持 PREPARED，不会被伪造为 FAILED。
func (s *Service) Reject(ctx context.Context, id uuid.UUID, idempotencyKey string, request InteractionCommandRequest) (*InteractionView, error) {
	return s.resolveInteraction(ctx, id, idempotencyKey, request, "reject", operationInteractionReject)
}

// Resolve 处理 INPUT/CHOICE/EDIT/AUTH/TAKEOVER；APPROVAL 必须走 Approve 或 Reject，避免通用入口绕过 R3 规则。
func (s *Service) Resolve(ctx context.Context, id uuid.UUID, idempotencyKey string, request InteractionCommandRequest) (*InteractionView, error) {
	return s.resolveInteraction(ctx, id, idempotencyKey, request, "", operationInteractionResolve)
}

func (s *Service) resolveInteraction(ctx context.Context, id uuid.UUID, idempotencyKey string,
	request InteractionCommandRequest, forcedAction, operation string,
) (*InteractionView, error) {
	principal, err := s.writePrincipal(ctx, ScopeInteractionResolve)
	if err != nil {
		return nil, err
	}
	if id == uuid.Nil || request.Version <= 0 {
		return nil, fmt.Errorf("%w: interaction id 和正版本号不能为空", ErrInvalidRequest)
	}
	if request.Resolution == nil {
		request.Resolution = map[string]any{}
	}
	if err := validateJSONPayload(request.Resolution); err != nil {
		return nil, err
	}
	request.Action = strings.TrimSpace(request.Action)
	if forcedAction != "" {
		if request.Action != "" && !strings.EqualFold(request.Action, forcedAction) {
			return nil, fmt.Errorf("%w: %s 端点不能提交 action=%q", ErrInvalidRequest, forcedAction, request.Action)
		}
		request.Action = forcedAction
	} else if request.Action == "" {
		return nil, fmt.Errorf("%w: resolve 必须提交 action", ErrInvalidRequest)
	}
	metadata, replay, err := s.prepareMutation(ctx, principal, operation, id, request.Version,
		idempotencyKey, request)
	if err != nil {
		return nil, err
	}
	if replay != nil {
		return s.replayInteraction(replay, operation, principal.TenantID)
	}

	current, err := s.store.GetInteraction(ctx, principal.TenantID, id)
	if err != nil {
		return nil, err
	}
	if err := validateInteractionTenant(current, principal.TenantID, uuid.Nil); err != nil {
		return nil, err
	}
	if current.Version != request.Version {
		return nil, fmt.Errorf("%w: expected=%d actual=%d", ErrVersionConflict, request.Version, current.Version)
	}
	if current.ExpiresAt != nil && !current.ExpiresAt.After(s.now()) {
		return nil, fmt.Errorf("%w: Interaction 已过期", interaction.ErrInvalidTransition)
	}
	if forcedAction != "" && current.InteractionType != interaction.TypeApproval {
		return nil, fmt.Errorf("%w: approve/reject 只接受 APPROVAL Interaction", ErrInvalidRequest)
	}
	if forcedAction == "" && current.InteractionType == interaction.TypeApproval {
		return nil, fmt.Errorf("%w: APPROVAL 必须使用 approve/reject 端点", ErrApprovalRequired)
	}
	if current.InteractionType == interaction.TypeAuth {
		if err := validateCredentialResolution(request.Resolution); err != nil {
			return nil, err
		}
	}

	now := s.now()
	aggregate := interaction.Interaction{
		ID: current.ID, Type: current.InteractionType, Status: current.Status,
		Title: current.Title, Payload: current.Payload, AllowedActions: current.AllowedActions,
		Version: current.Version,
	}
	domainResponse := interaction.Response{
		Action: request.Action, Payload: request.Resolution,
		RespondedBy: principal.Subject, RespondedAt: now,
	}
	if err := aggregate.Resolve(domainResponse); err != nil {
		return nil, err
	}
	record := ResolveInteractionRecord{
		InteractionID: id, ExpectedStatus: current.Status, ExpectedVersion: current.Version,
		Response: InteractionResponseView{
			Action: aggregate.Response.Action, Resolution: aggregate.Response.Payload,
			RespondedBy: aggregate.Response.RespondedBy, RespondedAt: aggregate.Response.RespondedAt,
		},
		Mutation: metadata,
	}

	if current.EffectID != nil && current.InteractionType == interaction.TypeApproval {
		linked, getErr := s.store.GetEffect(ctx, principal.TenantID, *current.EffectID)
		if getErr != nil {
			return nil, getErr
		}
		if err := validateEffectTenant(linked, principal.TenantID, current.RunID); err != nil {
			return nil, err
		}
		if linked.ApprovalInteractionID == nil || *linked.ApprovalInteractionID != current.ID {
			return nil, fmt.Errorf("%w: Effect 未绑定当前审批 Interaction", ErrApprovalRequired)
		}
		if linked.Status != effect.StatusPrepared {
			return nil, fmt.Errorf("%w: 审批时 Effect 必须仍为 PREPARED", effect.ErrInvalidTransition)
		}
		if forcedAction == "approve" {
			if linked.AuthorizationBlocked {
				return nil, fmt.Errorf("%w: %s", ErrApprovalRequired, linked.AuthorizationBlockReason)
			}
			effectAggregate := effect.Effect{
				ID: linked.ID, AttemptID: linked.AttemptID, IdempotencyKey: linked.IdempotencyKey,
				RequestHash: linked.RequestHash, RiskLevel: linked.RiskLevel,
				Status: linked.Status, ExternalRef: linked.ExternalRef, Version: linked.Version,
			}
			if err := effectAggregate.Transition(effect.StatusAuthorized); err != nil {
				return nil, err
			}
			record.Authorization = &EffectAuthorization{
				EffectID: linked.ID, ExpectedEffectVersion: linked.Version,
				ExpectedStatus: linked.Status, NextStatus: effectAggregate.Status,
				ApprovedBy: principal.Subject, ApprovedAt: now,
			}
		} else {
			record.Rejection = &EffectRejection{
				EffectID: linked.ID, ExpectedEffectVersion: linked.Version,
				ExpectedStatus: linked.Status, RejectedBy: principal.Subject,
				RejectedAt: now, Reason: rejectionReason(request.Resolution),
			}
		}
	}

	resolved, err := s.store.ResolveInteraction(ctx, record)
	if err != nil {
		return nil, err
	}
	if err := validateInteractionTenant(resolved, principal.TenantID, current.RunID); err != nil {
		return nil, err
	}
	if resolved.Status != interaction.StatusResolved || resolved.Response == nil {
		return nil, fmt.Errorf("%w: 响应结果必须为带证据的 RESOLVED", ErrInvalidStoreResult)
	}
	return resolved, nil
}

// ListArtifacts 返回某个 Run 的产物版本列表。
func (s *Service) ListArtifacts(ctx context.Context, query ArtifactQuery) ([]ArtifactView, error) {
	principal, err := s.readPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	if query.RunID == uuid.Nil {
		return nil, fmt.Errorf("%w: runId 不能为空", ErrInvalidRequest)
	}
	query.Status = strings.ToUpper(strings.TrimSpace(query.Status))
	if query.Status != "" && !validArtifactStatus(query.Status) {
		return nil, fmt.Errorf("%w: 未知 Artifact status %q", ErrInvalidRequest, query.Status)
	}
	query.Limit = normalizeLimit(query.Limit)
	if err := s.store.EnsureRunTenant(ctx, principal.TenantID, query.RunID); err != nil {
		return nil, err
	}
	items, err := s.store.ListArtifacts(ctx, principal.TenantID, query)
	if err != nil {
		return nil, err
	}
	for index := range items {
		if err := validateArtifactTenant(&items[index], principal.TenantID, query.RunID); err != nil {
			return nil, err
		}
	}
	return items, nil
}

// GetArtifact 返回单个不可变 Artifact 描述符。
func (s *Service) GetArtifact(ctx context.Context, id uuid.UUID) (*ArtifactView, error) {
	principal, err := s.readPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	if id == uuid.Nil {
		return nil, fmt.Errorf("%w: artifact id 不能为空", ErrInvalidRequest)
	}
	item, err := s.store.GetArtifact(ctx, principal.TenantID, id)
	if err != nil {
		return nil, err
	}
	if err := validateArtifactTenant(item, principal.TenantID, uuid.Nil); err != nil {
		return nil, err
	}
	return item, nil
}

// GetArtifactLineage 返回受深度限制的产物血缘子图。
func (s *Service) GetArtifactLineage(ctx context.Context, id uuid.UUID, query ArtifactLineageQuery) (*ArtifactLineageView, error) {
	principal, err := s.readPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	if id == uuid.Nil {
		return nil, fmt.Errorf("%w: artifact id 不能为空", ErrInvalidRequest)
	}
	query.Direction = strings.ToLower(strings.TrimSpace(query.Direction))
	if query.Direction == "" {
		query.Direction = "both"
	}
	if query.Direction != "ancestors" && query.Direction != "descendants" && query.Direction != "both" {
		return nil, fmt.Errorf("%w: lineage direction 只能为 ancestors/descendants/both", ErrInvalidRequest)
	}
	if query.Depth == 0 {
		query.Depth = 5
	}
	if query.Depth < 1 || query.Depth > 20 {
		return nil, fmt.Errorf("%w: lineage depth 必须在 1~20 之间", ErrInvalidRequest)
	}
	result, err := s.store.GetArtifactLineage(ctx, principal.TenantID, id, query)
	if err != nil {
		return nil, err
	}
	if result == nil || result.RootArtifactID != id {
		return nil, fmt.Errorf("%w: lineage root 不匹配", ErrInvalidStoreResult)
	}
	for index := range result.Artifacts {
		if err := validateArtifactTenant(&result.Artifacts[index], principal.TenantID, uuid.Nil); err != nil {
			return nil, err
		}
	}
	for index := range result.Edges {
		if result.Edges[index].TenantID != principal.TenantID {
			return nil, ErrTenantBoundary
		}
	}
	return result, nil
}

// ListEffects 返回某个 Run 的副作用账本。
func (s *Service) ListEffects(ctx context.Context, query EffectQuery) ([]EffectView, error) {
	principal, err := s.readPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	if query.RunID == uuid.Nil {
		return nil, fmt.Errorf("%w: runId 不能为空", ErrInvalidRequest)
	}
	if query.Status != "" && !query.Status.Valid() {
		return nil, fmt.Errorf("%w: 未知 Effect status %q", ErrInvalidRequest, query.Status)
	}
	if query.RiskLevel != "" && !query.RiskLevel.Valid() {
		return nil, fmt.Errorf("%w: 未知 Effect riskLevel %q", ErrInvalidRequest, query.RiskLevel)
	}
	query.Limit = normalizeLimit(query.Limit)
	if err := s.store.EnsureRunTenant(ctx, principal.TenantID, query.RunID); err != nil {
		return nil, err
	}
	items, err := s.store.ListEffects(ctx, principal.TenantID, query)
	if err != nil {
		return nil, err
	}
	for index := range items {
		if err := validateEffectTenant(&items[index], principal.TenantID, query.RunID); err != nil {
			return nil, err
		}
	}
	return items, nil
}

// GetEffect 返回单个 Effect 的安全读模型。
func (s *Service) GetEffect(ctx context.Context, id uuid.UUID) (*EffectView, error) {
	principal, err := s.readPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	if id == uuid.Nil {
		return nil, fmt.Errorf("%w: effect id 不能为空", ErrInvalidRequest)
	}
	item, err := s.store.GetEffect(ctx, principal.TenantID, id)
	if err != nil {
		return nil, err
	}
	if err := validateEffectTenant(item, principal.TenantID, uuid.Nil); err != nil {
		return nil, err
	}
	return item, nil
}

// Reconcile 严格执行 UNKNOWN -> RECONCILING；它只启动对账，绝不能重新调用原 Execute。
func (s *Service) Reconcile(ctx context.Context, id uuid.UUID, idempotencyKey string, request EffectCommandRequest) (*EffectView, error) {
	return s.transitionEffect(ctx, id, idempotencyKey, request, ScopeEffectReconcile,
		operationEffectReconcile, effect.StatusReconciling)
}

// Compensate 严格执行 SUCCEEDED -> COMPENSATING；最终 COMPENSATED 只能由外部补偿确认路径写入。
func (s *Service) Compensate(ctx context.Context, id uuid.UUID, idempotencyKey string, request EffectCommandRequest) (*EffectView, error) {
	return s.transitionEffect(ctx, id, idempotencyKey, request, ScopeEffectCompensate,
		operationEffectCompensate, effect.StatusCompensating)
}

func (s *Service) transitionEffect(ctx context.Context, id uuid.UUID, idempotencyKey string,
	request EffectCommandRequest, scope, operation string, next effect.Status,
) (*EffectView, error) {
	principal, err := s.writePrincipal(ctx, scope)
	if err != nil {
		return nil, err
	}
	request.Reason = strings.TrimSpace(request.Reason)
	if id == uuid.Nil || request.Version <= 0 {
		return nil, fmt.Errorf("%w: effect id 和正版本号不能为空", ErrInvalidRequest)
	}
	metadata, replay, err := s.prepareMutation(ctx, principal, operation, id, request.Version,
		idempotencyKey, request)
	if err != nil {
		return nil, err
	}
	if replay != nil {
		return s.replayEffect(replay, operation, principal.TenantID)
	}
	current, err := s.store.GetEffect(ctx, principal.TenantID, id)
	if err != nil {
		return nil, err
	}
	if err := validateEffectTenant(current, principal.TenantID, uuid.Nil); err != nil {
		return nil, err
	}
	if current.Version != request.Version {
		return nil, fmt.Errorf("%w: expected=%d actual=%d", ErrVersionConflict, request.Version, current.Version)
	}
	if current.AuthorizationBlocked {
		return nil, fmt.Errorf("%w: %s", ErrApprovalRequired, current.AuthorizationBlockReason)
	}
	if current.RiskLevel == effect.RiskR3ProductionDestructive &&
		(current.ApprovalInteractionID == nil || strings.TrimSpace(current.ApprovedBy) == "") {
		return nil, ErrApprovalRequired
	}
	aggregate := effect.Effect{
		ID: current.ID, AttemptID: current.AttemptID, IdempotencyKey: current.IdempotencyKey,
		RequestHash: current.RequestHash, RiskLevel: current.RiskLevel, Status: current.Status,
		ExternalRef: current.ExternalRef, Version: current.Version,
	}
	if err := aggregate.Transition(next); err != nil {
		return nil, err
	}
	updated, err := s.store.TransitionEffect(ctx, TransitionEffectRecord{
		EffectID: id, ExpectedStatus: current.Status, NextStatus: aggregate.Status,
		ExpectedVersion: current.Version, Reason: request.Reason, Mutation: metadata,
	})
	if err != nil {
		return nil, err
	}
	if err := validateEffectTenant(updated, principal.TenantID, current.RunID); err != nil {
		return nil, err
	}
	if updated.Status != next {
		return nil, fmt.Errorf("%w: Effect 结果状态不是 %s", ErrInvalidStoreResult, next)
	}
	return updated, nil
}

// ListTimeline 返回按发生时间排序的 Run 级审计投影；排序本身由 Store 的稳定游标查询保证。
func (s *Service) ListTimeline(ctx context.Context, query TimelineQuery) ([]TimelineEventView, error) {
	principal, err := s.readPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	if query.RunID == uuid.Nil {
		return nil, fmt.Errorf("%w: runId 不能为空", ErrInvalidRequest)
	}
	query.Limit = normalizeLimit(query.Limit)
	if err := s.store.EnsureRunTenant(ctx, principal.TenantID, query.RunID); err != nil {
		return nil, err
	}
	items, err := s.store.ListTimeline(ctx, principal.TenantID, query)
	if err != nil {
		return nil, err
	}
	for index := range items {
		if items[index].TenantID != principal.TenantID || items[index].RunID != query.RunID {
			return nil, ErrTenantBoundary
		}
	}
	return items, nil
}

// ListSchedulerDecisions 返回可解释的调度决策历史。
func (s *Service) ListSchedulerDecisions(ctx context.Context, query SchedulerDecisionQuery) ([]SchedulerDecisionView, error) {
	principal, err := s.readPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	if query.RunID == uuid.Nil {
		return nil, fmt.Errorf("%w: runId 不能为空", ErrInvalidRequest)
	}
	query.Limit = normalizeLimit(query.Limit)
	if err := s.store.EnsureRunTenant(ctx, principal.TenantID, query.RunID); err != nil {
		return nil, err
	}
	items, err := s.store.ListSchedulerDecisions(ctx, principal.TenantID, query)
	if err != nil {
		return nil, err
	}
	for index := range items {
		if items[index].TenantID != principal.TenantID || items[index].RunID != query.RunID {
			return nil, ErrTenantBoundary
		}
	}
	return items, nil
}

// ListVerificationRuns 返回硬验收执行及其证据索引。
func (s *Service) ListVerificationRuns(ctx context.Context, query VerificationQuery) ([]VerificationRunView, error) {
	principal, err := s.readPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	if query.RunID == uuid.Nil {
		return nil, fmt.Errorf("%w: runId 不能为空", ErrInvalidRequest)
	}
	query.Limit = normalizeLimit(query.Limit)
	if err := s.store.EnsureRunTenant(ctx, principal.TenantID, query.RunID); err != nil {
		return nil, err
	}
	items, err := s.store.ListVerificationRuns(ctx, principal.TenantID, query)
	if err != nil {
		return nil, err
	}
	for index := range items {
		if err := validateVerificationTenant(&items[index], principal.TenantID, query.RunID); err != nil {
			return nil, err
		}
	}
	return items, nil
}

// GetVerificationRun 返回单次 Verification 详情。
func (s *Service) GetVerificationRun(ctx context.Context, id uuid.UUID) (*VerificationRunView, error) {
	principal, err := s.readPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	if id == uuid.Nil {
		return nil, fmt.Errorf("%w: verification id 不能为空", ErrInvalidRequest)
	}
	item, err := s.store.GetVerificationRun(ctx, principal.TenantID, id)
	if err != nil {
		return nil, err
	}
	if err := validateVerificationTenant(item, principal.TenantID, uuid.Nil); err != nil {
		return nil, err
	}
	return item, nil
}

// GetCompletionManifest 返回不可变完成清单。Service 同时检查清单中内联 Artifact 的租户与 Run。
func (s *Service) GetCompletionManifest(ctx context.Context, id uuid.UUID) (*CompletionManifestView, error) {
	principal, err := s.readPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	if id == uuid.Nil {
		return nil, fmt.Errorf("%w: completion manifest id 不能为空", ErrInvalidRequest)
	}
	item, err := s.store.GetCompletionManifest(ctx, principal.TenantID, id)
	if err != nil {
		return nil, err
	}
	if item == nil || item.TenantID != principal.TenantID {
		return nil, ErrTenantBoundary
	}
	for index := range item.Artifacts {
		if err := validateArtifactTenant(&item.Artifacts[index], principal.TenantID, item.RunID); err != nil {
			return nil, err
		}
	}
	return item, nil
}

func (s *Service) readPrincipal(ctx context.Context) (runcontrol.Principal, error) {
	if s == nil || s.store == nil {
		return runcontrol.Principal{}, errors.New("SafetyControl Service 未初始化")
	}
	principal := runcontrol.PrincipalFromContext(ctx)
	if principal.TenantID == uuid.Nil {
		return runcontrol.Principal{}, ErrTenantBoundary
	}
	return principal, nil
}

func (s *Service) writePrincipal(ctx context.Context, scope string) (runcontrol.Principal, error) {
	principal, err := s.readPrincipal(ctx)
	if err != nil {
		return runcontrol.Principal{}, err
	}
	if strings.TrimSpace(principal.Subject) == "" ||
		!(principal.Scopes["*"] || principal.Scopes[scope]) {
		return runcontrol.Principal{}, fmt.Errorf("%w: 需要 scope %s", ErrForbidden, scope)
	}
	principal.Subject = strings.TrimSpace(principal.Subject)
	return principal, nil
}

func (s *Service) prepareMutation(ctx context.Context, principal runcontrol.Principal,
	operation string, aggregateID uuid.UUID, expectedVersion int64, idempotencyKey string, payload any,
) (MutationMetadata, *MutationResult, error) {
	key := strings.TrimSpace(idempotencyKey)
	if key == "" || len(key) > maximumKeyLength {
		return MutationMetadata{}, nil, fmt.Errorf("%w: 长度必须为 1~%d", ErrIdempotencyRequired, maximumKeyLength)
	}
	hash, err := hashMutation(principal.TenantID, principal.Subject, operation, aggregateID, expectedVersion, payload)
	if err != nil {
		return MutationMetadata{}, nil, err
	}
	replay, err := s.store.FindMutation(ctx, principal.TenantID, key, hash)
	if err != nil {
		return MutationMetadata{}, nil, err
	}
	metadata := MutationMetadata{
		TenantID: principal.TenantID, Operation: operation, AggregateID: aggregateID,
		ExpectedVersion: expectedVersion, IdempotencyKey: key, RequestHash: hash,
		Actor: principal.Subject, OccurredAt: s.now(),
	}
	return metadata, replay, nil
}

func (s *Service) replayInteraction(result *MutationResult, operation string, tenantID uuid.UUID) (*InteractionView, error) {
	if result == nil || (result.Operation != "" && result.Operation != operation) || result.Interaction == nil {
		return nil, fmt.Errorf("%w: 幂等回放缺少 Interaction 结果", ErrInvalidStoreResult)
	}
	if err := validateInteractionTenant(result.Interaction, tenantID, uuid.Nil); err != nil {
		return nil, err
	}
	return result.Interaction, nil
}

func (s *Service) replayEffect(result *MutationResult, operation string, tenantID uuid.UUID) (*EffectView, error) {
	if result == nil || (result.Operation != "" && result.Operation != operation) || result.Effect == nil {
		return nil, fmt.Errorf("%w: 幂等回放缺少 Effect 结果", ErrInvalidStoreResult)
	}
	if err := validateEffectTenant(result.Effect, tenantID, uuid.Nil); err != nil {
		return nil, err
	}
	return result.Effect, nil
}

func normalizeCreateInteraction(request CreateInteractionRequest) (CreateInteractionRequest, *interaction.Interaction, error) {
	if request.RunID == uuid.Nil {
		return request, nil, fmt.Errorf("%w: runId 不能为空", ErrInvalidRequest)
	}
	if (request.TaskID != nil && *request.TaskID == uuid.Nil) ||
		(request.AttemptID != nil && *request.AttemptID == uuid.Nil) ||
		(request.EffectID != nil && *request.EffectID == uuid.Nil) {
		return request, nil, fmt.Errorf("%w: 可选资源 ID 不能是 nil UUID", ErrInvalidRequest)
	}
	request.Title = strings.TrimSpace(request.Title)
	request.Description = strings.TrimSpace(request.Description)
	if request.AllowedActions == nil {
		if request.InteractionType == interaction.TypeApproval {
			request.AllowedActions = []string{"approve", "reject"}
		} else {
			request.AllowedActions = []string{"resolve"}
		}
	}
	if err := validateJSONPayload(request.Payload); err != nil {
		return request, nil, err
	}
	candidate, err := interaction.New(validationInteractionID, request.InteractionType,
		request.Title, request.Payload, request.AllowedActions)
	if err != nil {
		return request, nil, err
	}
	request.Title = candidate.Title
	request.Payload = candidate.Payload
	request.AllowedActions = candidate.AllowedActions
	return request, candidate, nil
}

func normalizeInteractionStatus(status interaction.Status) (interaction.Status, error) {
	if strings.EqualFold(strings.TrimSpace(string(status)), "PENDING") {
		return interaction.StatusWaiting, nil
	}
	if status != "" && !status.Valid() {
		return "", fmt.Errorf("%w: 未知 Interaction status %q", ErrInvalidRequest, status)
	}
	return status, nil
}

func validateJSONPayload(payload map[string]any) error {
	if payload == nil {
		return nil
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("%w: payload 必须能编码为 JSON: %v", ErrInvalidRequest, err)
	}
	if len(raw) > maximumPayloadSize {
		return fmt.Errorf("%w: payload 不能超过 %d bytes", ErrInvalidRequest, maximumPayloadSize)
	}
	return nil
}

func validateCredentialResolution(resolution map[string]any) error {
	if len(resolution) != 1 {
		return fmt.Errorf("%w: AUTH resolution 只能包含 credentialRef", ErrInvalidRequest)
	}
	value, ok := resolution["credentialRef"].(string)
	if !ok || strings.TrimSpace(value) == "" {
		return fmt.Errorf("%w: AUTH resolution 必须包含非空 credentialRef，不能包含明文 secret", ErrInvalidRequest)
	}
	return nil
}

func hashMutation(tenantID uuid.UUID, actor, operation string, aggregateID uuid.UUID,
	expectedVersion int64, payload any,
) (string, error) {
	envelope := struct {
		TenantID        uuid.UUID `json:"tenantId"`
		Actor           string    `json:"actor"`
		Operation       string    `json:"operation"`
		AggregateID     uuid.UUID `json:"aggregateId,omitempty"`
		ExpectedVersion int64     `json:"expectedVersion,omitempty"`
		Payload         any       `json:"payload"`
	}{
		TenantID: tenantID, Actor: actor, Operation: operation, AggregateID: aggregateID,
		ExpectedVersion: expectedVersion, Payload: payload,
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		return "", fmt.Errorf("%w: 幂等请求无法编码: %v", ErrInvalidRequest, err)
	}
	digest := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func validateInteractionTenant(item *InteractionView, tenantID, runID uuid.UUID) error {
	if item == nil {
		return fmt.Errorf("%w: Interaction 为空", ErrInvalidStoreResult)
	}
	if item.TenantID != tenantID || (runID != uuid.Nil && item.RunID != runID) {
		return ErrTenantBoundary
	}
	return nil
}

func validateArtifactTenant(item *ArtifactView, tenantID, runID uuid.UUID) error {
	if item == nil {
		return fmt.Errorf("%w: Artifact 为空", ErrInvalidStoreResult)
	}
	if item.TenantID != tenantID || (runID != uuid.Nil && item.RunID != runID) {
		return ErrTenantBoundary
	}
	return nil
}

func validateEffectTenant(item *EffectView, tenantID, runID uuid.UUID) error {
	if item == nil {
		return fmt.Errorf("%w: Effect 为空", ErrInvalidStoreResult)
	}
	if item.TenantID != tenantID || (runID != uuid.Nil && item.RunID != runID) {
		return ErrTenantBoundary
	}
	return nil
}

func validateVerificationTenant(item *VerificationRunView, tenantID, runID uuid.UUID) error {
	if item == nil {
		return fmt.Errorf("%w: Verification 为空", ErrInvalidStoreResult)
	}
	if item.TenantID != tenantID || (runID != uuid.Nil && item.RunID != runID) {
		return ErrTenantBoundary
	}
	return nil
}

func normalizeLimit(limit int) int {
	if limit <= 0 || limit > maximumListLimit {
		return defaultListLimit
	}
	return limit
}

func validArtifactStatus(status string) bool {
	switch status {
	case "DRAFT", "VALIDATING", "VALID", "INVALID", "SUPERSEDED", "ARCHIVED":
		return true
	default:
		return false
	}
}

func containsAction(actions []string, expected string) bool {
	for _, action := range actions {
		if strings.EqualFold(strings.TrimSpace(action), expected) {
			return true
		}
	}
	return false
}

func rejectionReason(resolution map[string]any) string {
	if value, ok := resolution["reason"].(string); ok && strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}
	return "approval rejected"
}

func (s *Service) now() time.Time {
	return s.clock().UTC()
}
