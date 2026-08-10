// Package service 实现 Kratos API 的用例编排。
//
// 传输层只负责协议解析；默认值、参数校验、领域对象创建和错误语义都集中在这里，
// 因此 HTTP 与 gRPC 会得到完全一致的行为。
package service

import (
	"bytes"
	"context"
	"encoding/json"
	stdErrors "errors"
	"fmt"
	"strings"
	"time"

	kratosErrors "github.com/go-kratos/kratos/v2/errors"
	"github.com/google/uuid"
	v1 "github.com/licy-yu/agent-os/api/controlplane/v1"
	"github.com/licy-yu/agent-os/internal/domain"
	"github.com/licy-yu/agent-os/internal/domain/agent"
	"github.com/licy-yu/agent-os/internal/domain/swarm"
	"github.com/licy-yu/agent-os/internal/domain/task"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const serviceVersion = "0.1.0"

// HealthRepository 只暴露健康检查需要的最小能力。
type HealthRepository interface {
	Ping(context.Context) error
}

// ControlPlaneService 实现生成的 gRPC 服务，同时供手写 HTTP 路由复用。
type ControlPlaneService struct {
	v1.UnimplementedControlPlaneServer
	health HealthRepository
	swarms swarm.Repository
	agents agent.Repository
	tasks  task.Repository
	clock  func() time.Time
}

// NewControlPlaneService 注入领域仓储。clock 可替换能力让时间相关测试保持确定性。
func NewControlPlaneService(health HealthRepository, swarms swarm.Repository, agents agent.Repository, tasks task.Repository) *ControlPlaneService {
	return &ControlPlaneService{
		health: health, swarms: swarms, agents: agents, tasks: tasks,
		clock: func() time.Time { return time.Now().UTC() },
	}
}

func (s *ControlPlaneService) Health(ctx context.Context, _ *emptypb.Empty) (*v1.HealthReply, error) {
	if err := s.health.Ping(ctx); err != nil {
		return nil, kratosErrors.ServiceUnavailable("DATABASE_UNAVAILABLE", "PostgreSQL 不可用")
	}
	return &v1.HealthReply{Status: "ok", Service: "swarmos-control-plane", Version: serviceVersion}, nil
}

func (s *ControlPlaneService) CreateSwarm(ctx context.Context, req *v1.CreateSwarmRequest) (*v1.Swarm, error) {
	if req == nil {
		return nil, kratosErrors.BadRequest("INVALID_REQUEST", "请求体不能为空")
	}
	name := strings.TrimSpace(req.Name)
	goal := strings.TrimSpace(req.Goal)
	if name == "" || len(name) > 128 {
		return nil, kratosErrors.BadRequest("INVALID_NAME", "name 必须为 1~128 个字符")
	}
	if goal == "" {
		return nil, kratosErrors.BadRequest("INVALID_GOAL", "goal 不能为空")
	}
	if req.BudgetTokens < 0 || req.BudgetCostMicros < 0 {
		return nil, kratosErrors.BadRequest("INVALID_BUDGET", "预算不能为负数")
	}
	maxAgents := req.MaxAgents
	if maxAgents == 0 {
		maxAgents = 8
	}
	if maxAgents < 1 || maxAgents > 1000 {
		return nil, kratosErrors.BadRequest("INVALID_MAX_AGENTS", "max_agents 必须在 1~1000 之间")
	}
	now := s.clock()
	value := &swarm.Swarm{
		ID: uuid.New(), Name: name, Goal: goal, Status: swarm.StatusPending,
		BudgetTokens: req.BudgetTokens, BudgetCostMicros: req.BudgetCostMicros,
		MaxAgents: maxAgents, Policy: structMap(req.Policy), Version: 1,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := s.swarms.Create(ctx, value); err != nil {
		return nil, translateError(err)
	}
	return swarmToProto(value)
}

func (s *ControlPlaneService) GetSwarm(ctx context.Context, req *v1.GetSwarmRequest) (*v1.Swarm, error) {
	id, err := parseID(req.GetId(), "swarm.id")
	if err != nil {
		return nil, err
	}
	value, repoErr := s.swarms.Get(ctx, id)
	if repoErr != nil {
		return nil, translateError(repoErr)
	}
	return swarmToProto(value)
}

func (s *ControlPlaneService) ListSwarms(ctx context.Context, req *v1.ListSwarmsRequest) (*v1.ListSwarmsReply, error) {
	page, err := parsePage(req.GetPage())
	if err != nil {
		return nil, err
	}
	items, repoErr := s.swarms.List(ctx, page)
	if repoErr != nil {
		return nil, translateError(repoErr)
	}
	next, items, err := swarmPage(items, page.Size)
	if err != nil {
		return nil, kratosErrors.InternalServer("CURSOR_ERROR", err.Error())
	}
	reply := &v1.ListSwarmsReply{NextPageToken: next, Items: make([]*v1.Swarm, 0, len(items))}
	for _, value := range items {
		item, convertErr := swarmToProto(value)
		if convertErr != nil {
			return nil, kratosErrors.InternalServer("SERIALIZE_ERROR", convertErr.Error())
		}
		reply.Items = append(reply.Items, item)
	}
	return reply, nil
}

func (s *ControlPlaneService) CreateAgentTemplate(ctx context.Context, req *v1.CreateAgentTemplateRequest) (*v1.AgentTemplate, error) {
	if req == nil {
		return nil, kratosErrors.BadRequest("INVALID_REQUEST", "请求体不能为空")
	}
	if strings.TrimSpace(req.Name) == "" || strings.TrimSpace(req.Role) == "" || strings.TrimSpace(req.Model) == "" {
		return nil, kratosErrors.BadRequest("INVALID_TEMPLATE", "name、role、model 不能为空")
	}
	for name, level := range req.Skills {
		if strings.TrimSpace(name) == "" || level < 0 || level > 1 {
			return nil, kratosErrors.BadRequest("INVALID_SKILL", "skill 名不能为空且熟练度必须在 0~1 之间")
		}
	}
	templateVersion := strings.TrimSpace(req.TemplateVersion)
	if templateVersion == "" {
		templateVersion = "1.0.0"
	}
	now := s.clock()
	value := &agent.Template{
		ID: uuid.New(), Name: strings.TrimSpace(req.Name), Role: strings.TrimSpace(req.Role),
		Prompt: req.Prompt, Model: strings.TrimSpace(req.Model), Skills: cloneSkills(req.Skills),
		Tools: cleanStrings(req.Tools), Permissions: cleanStrings(req.Permissions),
		TemplateVersion: templateVersion, Enabled: true, CreatedAt: now, UpdatedAt: now,
	}
	if err := s.agents.CreateTemplate(ctx, value); err != nil {
		return nil, translateError(err)
	}
	return templateToProto(value), nil
}

func (s *ControlPlaneService) ListAgentTemplates(ctx context.Context, req *v1.ListAgentTemplatesRequest) (*v1.ListAgentTemplatesReply, error) {
	page, err := parsePage(req.GetPage())
	if err != nil {
		return nil, err
	}
	items, repoErr := s.agents.ListTemplates(ctx, page)
	if repoErr != nil {
		return nil, translateError(repoErr)
	}
	next, trimmed, cursorErr := genericPage(items, page.Size, func(v *agent.Template) domain.Cursor {
		return domain.Cursor{CreatedAt: v.CreatedAt, ID: v.ID}
	})
	if cursorErr != nil {
		return nil, kratosErrors.InternalServer("CURSOR_ERROR", cursorErr.Error())
	}
	reply := &v1.ListAgentTemplatesReply{NextPageToken: next, Items: make([]*v1.AgentTemplate, 0, len(trimmed))}
	for _, value := range trimmed {
		reply.Items = append(reply.Items, templateToProto(value))
	}
	return reply, nil
}

func (s *ControlPlaneService) RegisterAgent(ctx context.Context, req *v1.RegisterAgentRequest) (*v1.AgentInstance, error) {
	templateID, err := parseID(req.GetTemplateId(), "template_id")
	if err != nil {
		return nil, err
	}
	if _, repoErr := s.agents.GetTemplate(ctx, templateID); repoErr != nil {
		return nil, translateError(repoErr)
	}
	var swarmID *uuid.UUID
	if req.GetSwarmId() != "" {
		parsed, parseErr := parseID(req.GetSwarmId(), "swarm_id")
		if parseErr != nil {
			return nil, parseErr
		}
		if _, repoErr := s.swarms.Get(ctx, parsed); repoErr != nil {
			return nil, translateError(repoErr)
		}
		swarmID = &parsed
	}
	name := strings.TrimSpace(req.GetName())
	if name == "" {
		name = "agent-" + uuid.NewString()[:8]
	}
	now := s.clock()
	value := &agent.Instance{
		ID: uuid.New(), TemplateID: templateID, SwarmID: swarmID, Name: name,
		Status: agent.StatusRegistered, HeartbeatAt: now, Version: 1,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := s.agents.CreateInstance(ctx, value); err != nil {
		return nil, translateError(err)
	}
	return instanceToProto(value), nil
}

func (s *ControlPlaneService) ListAgents(ctx context.Context, req *v1.ListAgentsRequest) (*v1.ListAgentsReply, error) {
	var swarmID *uuid.UUID
	if req.GetSwarmId() != "" {
		parsed, err := parseID(req.GetSwarmId(), "swarm_id")
		if err != nil {
			return nil, err
		}
		swarmID = &parsed
	}
	page, err := parsePage(req.GetPage())
	if err != nil {
		return nil, err
	}
	items, repoErr := s.agents.ListInstances(ctx, swarmID, page)
	if repoErr != nil {
		return nil, translateError(repoErr)
	}
	next, trimmed, cursorErr := genericPage(items, page.Size, func(v *agent.Instance) domain.Cursor {
		return domain.Cursor{CreatedAt: v.CreatedAt, ID: v.ID}
	})
	if cursorErr != nil {
		return nil, kratosErrors.InternalServer("CURSOR_ERROR", cursorErr.Error())
	}
	reply := &v1.ListAgentsReply{NextPageToken: next, Items: make([]*v1.AgentInstance, 0, len(trimmed))}
	for _, value := range trimmed {
		reply.Items = append(reply.Items, instanceToProto(value))
	}
	return reply, nil
}

func (s *ControlPlaneService) CreateTask(ctx context.Context, req *v1.CreateTaskRequest) (*v1.Task, error) {
	swarmID, err := parseID(req.GetSwarmId(), "swarm_id")
	if err != nil {
		return nil, err
	}
	if _, repoErr := s.swarms.Get(ctx, swarmID); repoErr != nil {
		return nil, translateError(repoErr)
	}
	name := strings.TrimSpace(req.GetName())
	goal := strings.TrimSpace(req.GetGoal())
	if name == "" || goal == "" {
		return nil, kratosErrors.BadRequest("INVALID_TASK", "name 和 goal 不能为空")
	}
	priority := req.GetPriority()
	if priority == 0 {
		priority = 50
	}
	if priority < 0 || priority > 1000 {
		return nil, kratosErrors.BadRequest("INVALID_PRIORITY", "priority 必须在 0~1000 之间")
	}
	parentID, err := optionalID(req.GetParentId(), "parent_id")
	if err != nil {
		return nil, err
	}
	dependencyIDs, err := parseIDs(req.GetDependencyIds(), "dependency_ids")
	if err != nil {
		return nil, err
	}

	requirements := task.Requirements{}
	if err := decodeStruct(req.GetRequirements(), &requirements); err != nil {
		return nil, kratosErrors.BadRequest("INVALID_REQUIREMENTS", err.Error())
	}
	acceptance := task.Acceptance{}
	if err := decodeStruct(req.GetAcceptance(), &acceptance); err != nil {
		return nil, kratosErrors.BadRequest("INVALID_ACCEPTANCE", err.Error())
	}
	policy := task.DefaultExecutionPolicy()
	if req.GetExecutionPolicy() != nil {
		if err := decodeStruct(req.GetExecutionPolicy(), &policy); err != nil {
			return nil, kratosErrors.BadRequest("INVALID_EXECUTION_POLICY", err.Error())
		}
	}
	if err := validateExecutionPolicy(policy); err != nil {
		return nil, kratosErrors.BadRequest("INVALID_EXECUTION_POLICY", err.Error())
	}

	now := s.clock()
	value := &task.Task{
		ID: uuid.New(), SwarmID: swarmID, ParentID: parentID, Name: name, Goal: goal,
		Status: task.StatusCreated, Priority: priority, Input: structMap(req.GetInput()),
		Requirements: requirements, Acceptance: acceptance, ExecutionPolicy: policy,
		DependencyIDs: dependencyIDs, AvailableAt: now, Version: 1,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := s.tasks.CreateTask(ctx, value); err != nil {
		return nil, translateError(err)
	}
	return taskToProto(value)
}

func (s *ControlPlaneService) GetTask(ctx context.Context, req *v1.GetTaskRequest) (*v1.Task, error) {
	id, err := parseID(req.GetId(), "task.id")
	if err != nil {
		return nil, err
	}
	value, repoErr := s.tasks.GetTask(ctx, id)
	if repoErr != nil {
		return nil, translateError(repoErr)
	}
	return taskToProto(value)
}

func (s *ControlPlaneService) ListTasks(ctx context.Context, req *v1.ListTasksRequest) (*v1.ListTasksReply, error) {
	swarmID, err := parseID(req.GetSwarmId(), "swarm_id")
	if err != nil {
		return nil, err
	}
	page, err := parsePage(req.GetPage())
	if err != nil {
		return nil, err
	}
	var status *task.Status
	if req.GetStatus() != "" {
		parsed := task.Status(strings.ToUpper(req.GetStatus()))
		if !validTaskStatus(parsed) {
			return nil, kratosErrors.BadRequest("INVALID_STATUS", "未知 task status")
		}
		status = &parsed
	}
	items, repoErr := s.tasks.ListTasks(ctx, task.ListFilter{SwarmID: swarmID, Status: status, Page: page})
	if repoErr != nil {
		return nil, translateError(repoErr)
	}
	next, trimmed, cursorErr := genericPage(items, page.Size, func(v *task.Task) domain.Cursor {
		return domain.Cursor{CreatedAt: v.CreatedAt, ID: v.ID}
	})
	if cursorErr != nil {
		return nil, kratosErrors.InternalServer("CURSOR_ERROR", cursorErr.Error())
	}
	reply := &v1.ListTasksReply{NextPageToken: next, Items: make([]*v1.Task, 0, len(trimmed))}
	for _, value := range trimmed {
		item, convertErr := taskToProto(value)
		if convertErr != nil {
			return nil, kratosErrors.InternalServer("SERIALIZE_ERROR", convertErr.Error())
		}
		reply.Items = append(reply.Items, item)
	}
	return reply, nil
}

func parsePage(req *v1.PageRequest) (domain.Page, error) {
	if req == nil {
		return domain.Page{Size: domain.DefaultPageSize}, nil
	}
	cursor, err := domain.DecodeCursor(req.GetPageToken())
	if err != nil {
		return domain.Page{}, kratosErrors.BadRequest("INVALID_PAGE_TOKEN", err.Error())
	}
	return domain.Page{Size: domain.NormalizePageSize(req.GetPageSize()), Cursor: cursor}, nil
}

func genericPage[T any](items []T, size int, cursor func(T) domain.Cursor) (string, []T, error) {
	if len(items) <= size {
		return "", items, nil
	}
	trimmed := items[:size]
	token, err := domain.EncodeCursor(cursor(trimmed[len(trimmed)-1]))
	return token, trimmed, err
}

func swarmPage(items []*swarm.Swarm, size int) (string, []*swarm.Swarm, error) {
	return genericPage(items, size, func(v *swarm.Swarm) domain.Cursor {
		return domain.Cursor{CreatedAt: v.CreatedAt, ID: v.ID}
	})
}

func parseID(raw, field string) (uuid.UUID, error) {
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, kratosErrors.BadRequest("INVALID_ID", fmt.Sprintf("%s 不是合法 UUID", field))
	}
	return id, nil
}

func optionalID(raw, field string) (*uuid.UUID, error) {
	if raw == "" {
		return nil, nil
	}
	id, err := parseID(raw, field)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

func parseIDs(raw []string, field string) ([]uuid.UUID, error) {
	result := make([]uuid.UUID, 0, len(raw))
	seen := make(map[uuid.UUID]struct{}, len(raw))
	for _, item := range raw {
		id, err := parseID(item, field)
		if err != nil {
			return nil, err
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}
	return result, nil
}

func decodeStruct(input *structpb.Struct, output any) error {
	if input == nil {
		return nil
	}
	// Struct 不知道内部字段的 protobuf JSON 名称。这里先把 lowerCamelCase 统一为
	// snake_case，随后拒绝未知字段，避免拼写错误被静默忽略后使用危险默认值。
	raw, err := json.Marshal(normalizeJSONKeys(input.AsMap()))
	if err != nil {
		return fmt.Errorf("序列化结构化字段: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return fmt.Errorf("结构化字段不符合合同: %w", err)
	}
	return nil
}

func normalizeJSONKeys(value any) any {
	switch current := value.(type) {
	case map[string]any:
		result := make(map[string]any, len(current))
		for key, child := range current {
			result[toSnakeCase(key)] = normalizeJSONKeys(child)
		}
		return result
	case []any:
		result := make([]any, len(current))
		for index, child := range current {
			result[index] = normalizeJSONKeys(child)
		}
		return result
	default:
		return value
	}
}

func toSnakeCase(value string) string {
	var builder strings.Builder
	for index, char := range value {
		if char >= 'A' && char <= 'Z' {
			if index > 0 {
				builder.WriteByte('_')
			}
			builder.WriteByte(byte(char - 'A' + 'a'))
			continue
		}
		builder.WriteRune(char)
	}
	return builder.String()
}

func validateExecutionPolicy(policy task.ExecutionPolicy) error {
	if policy.MaxAttempts < 1 || policy.MaxAttempts > 20 {
		return fmt.Errorf("max_attempts 必须在 1~20 之间")
	}
	if policy.MaxHandoffs < 0 || policy.MaxTokens < 1 || policy.MaxToolCalls < 0 || policy.MaxNoProgressRounds < 1 {
		return fmt.Errorf("handoff、token、tool call 和 no-progress 限额无效")
	}
	if policy.TimeoutSeconds < 1 || policy.TimeoutSeconds > int64((24*time.Hour).Seconds()) {
		return fmt.Errorf("timeout_seconds 必须在 1~86400 之间")
	}
	return nil
}

func translateError(err error) error {
	switch {
	case stdErrors.Is(err, domain.ErrNotFound):
		return kratosErrors.NotFound("NOT_FOUND", err.Error())
	case stdErrors.Is(err, domain.ErrConflict):
		return kratosErrors.Conflict("CONFLICT", err.Error())
	case stdErrors.Is(err, domain.ErrInvalidTransition):
		return kratosErrors.Conflict("INVALID_TRANSITION", err.Error())
	default:
		return kratosErrors.InternalServer("INTERNAL_ERROR", "内部服务错误")
	}
}

func structMap(value *structpb.Struct) map[string]any {
	if value == nil {
		return map[string]any{}
	}
	return value.AsMap()
}

func cleanStrings(values []string) []string {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func cloneSkills(input map[string]float64) map[string]float64 {
	result := make(map[string]float64, len(input))
	for name, value := range input {
		result[strings.TrimSpace(name)] = value
	}
	return result
}

func validTaskStatus(value task.Status) bool {
	switch value {
	case task.StatusCreated, task.StatusPlanning, task.StatusBlocked, task.StatusReady,
		task.StatusScheduling, task.StatusAssigned, task.StatusRunning, task.StatusWaitingTool,
		task.StatusWaitingInput, task.StatusReview, task.StatusRetryWait, task.StatusSucceeded,
		task.StatusFailed, task.StatusCanceled, task.StatusRejected:
		return true
	default:
		return false
	}
}

func swarmToProto(value *swarm.Swarm) (*v1.Swarm, error) {
	policy, err := structpb.NewStruct(value.Policy)
	if err != nil {
		return nil, fmt.Errorf("转换 swarm.policy: %w", err)
	}
	return &v1.Swarm{
		Id: value.ID.String(), Name: value.Name, Goal: value.Goal, Status: string(value.Status),
		BudgetTokens: value.BudgetTokens, BudgetCostMicros: value.BudgetCostMicros,
		MaxAgents: value.MaxAgents, Policy: policy, Version: value.Version,
		CreatedAt: timestamppb.New(value.CreatedAt), UpdatedAt: timestamppb.New(value.UpdatedAt),
	}, nil
}

func templateToProto(value *agent.Template) *v1.AgentTemplate {
	return &v1.AgentTemplate{
		Id: value.ID.String(), Name: value.Name, Role: value.Role, Prompt: value.Prompt,
		Model: value.Model, Skills: cloneSkills(value.Skills), Tools: append([]string(nil), value.Tools...),
		Permissions: append([]string(nil), value.Permissions...), TemplateVersion: value.TemplateVersion,
		Enabled: value.Enabled, CreatedAt: timestamppb.New(value.CreatedAt), UpdatedAt: timestamppb.New(value.UpdatedAt),
	}
}

func instanceToProto(value *agent.Instance) *v1.AgentInstance {
	result := &v1.AgentInstance{
		Id: value.ID.String(), TemplateId: value.TemplateID.String(), Name: value.Name,
		Status: string(value.Status), Load: value.Load, HeartbeatAt: timestamppb.New(value.HeartbeatAt),
		Version: value.Version, CreatedAt: timestamppb.New(value.CreatedAt), UpdatedAt: timestamppb.New(value.UpdatedAt),
	}
	if value.SwarmID != nil {
		result.SwarmId = value.SwarmID.String()
	}
	return result
}

func taskToProto(value *task.Task) (*v1.Task, error) {
	input, err := structpb.NewStruct(value.Input)
	if err != nil {
		return nil, fmt.Errorf("转换 task.input: %w", err)
	}
	requirements, err := toStruct(value.Requirements)
	if err != nil {
		return nil, err
	}
	acceptance, err := toStruct(value.Acceptance)
	if err != nil {
		return nil, err
	}
	policy, err := toStruct(value.ExecutionPolicy)
	if err != nil {
		return nil, err
	}
	result := &v1.Task{
		Id: value.ID.String(), SwarmId: value.SwarmID.String(), Name: value.Name, Goal: value.Goal,
		Status: string(value.Status), Priority: value.Priority, Input: input, Requirements: requirements,
		Acceptance: acceptance, ExecutionPolicy: policy, AttemptCount: value.AttemptCount,
		Version: value.Version, CreatedAt: timestamppb.New(value.CreatedAt), UpdatedAt: timestamppb.New(value.UpdatedAt),
		DependencyIds: make([]string, 0, len(value.DependencyIDs)),
	}
	if value.ParentID != nil {
		result.ParentId = value.ParentID.String()
	}
	if value.AssignedAgentID != nil {
		result.AssignedAgentId = value.AssignedAgentID.String()
	}
	for _, id := range value.DependencyIDs {
		result.DependencyIds = append(result.DependencyIds, id.String())
	}
	return result, nil
}

func toStruct(value any) (*structpb.Struct, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("序列化结构化字段: %w", err)
	}
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil {
		return nil, fmt.Errorf("反序列化结构化字段: %w", err)
	}
	return structpb.NewStruct(object)
}
