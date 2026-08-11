// Package toolgateway 实现模型工具调用的统一策略、配额和审计边界。
package toolgateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/effect"
	"github.com/licy-yu/agent-os/internal/execution"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var (
	// ErrDenied 表示调用被策略拒绝，属于可审计的业务结果，而非基础设施故障。
	ErrDenied = errors.New("工具调用被策略拒绝")
	// ErrApprovalPending 表示 Effect 已安全停在 PREPARED，等待人工审批。Worker 必须保留
	// Attempt 并从 Checkpoint 重投，不能把它当成普通执行失败提交 Reviewer。
	ErrApprovalPending = errors.New("Effect 等待人工审批")
	// ErrEffectReconcileRequired 表示外部调用结果不确定。原 Execute 绝不能直接重试，
	// 只能由 Reconciler 对账后把 Effect 收敛到 SUCCEEDED/FAILED。
	ErrEffectReconcileRequired = errors.New("Effect 必须先对账")
)

// EffectWaitError 在保留 errors.Is 哨兵语义的同时，携带真正触发等待的 Effect ID。
// Worker 把它传给持久化层，才能在“审批恰好先完成”的竞态窗口中检查同一条 Effect，
// 而不是猜测 Attempt 下最近更新的其它副作用。
type EffectWaitError struct {
	Cause         error
	EffectID      uuid.UUID
	InteractionID *uuid.UUID
	Status        effect.Status
	Detail        string
}

func (e *EffectWaitError) Error() string {
	if e == nil {
		return "Effect 等待状态未知"
	}
	return fmt.Sprintf("%v: effect=%s status=%s interaction=%v detail=%s",
		e.Cause, e.EffectID, e.Status, e.InteractionID, strings.TrimSpace(e.Detail))
}

func (e *EffectWaitError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// WaitingEffectID 从任意层级的包装错误中提取等待边界的 Effect ID。
func WaitingEffectID(err error) uuid.UUID {
	var wait *EffectWaitError
	if errors.As(err, &wait) && wait != nil {
		return wait.EffectID
	}
	return uuid.Nil
}

// Definition 是工具注册表的运行时视图。
type Definition struct {
	Name                string
	Description         string
	Adapter             string
	RequiredPermissions []string
	RiskLevel           string
	Enabled             bool
	Config              map[string]any
}

// CallRecord 由 Store 写入 tool_calls；失败和拒绝也必须保留。
type CallRecord struct {
	ID           uuid.UUID
	AttemptID    uuid.UUID
	TaskID       uuid.UUID
	AgentID      uuid.UUID
	ToolName     string
	Arguments    map[string]any
	Result       map[string]any
	Status       string
	RiskLevel    string
	ErrorMessage string
	StartedAt    time.Time
}

// EffectRequest 是 R2/R3 ToolCall 在触碰外部世界前提交给 Store 的无密钥事实。
type EffectRequest struct {
	ToolCallID     uuid.UUID
	IdempotencyKey string
	Request        effect.Request
	RequestHash    string
	RiskLevel      effect.RiskLevel
	EffectType     string
	RequestedAt    time.Time
}

// EffectDisposition 告诉 Gateway 当前是否拥有调用 Adapter 的持久化授权。
type EffectDisposition string

const (
	EffectExecute         EffectDisposition = "EXECUTE"
	EffectApprovalPending EffectDisposition = "APPROVAL_PENDING"
	EffectAlreadyDone     EffectDisposition = "ALREADY_DONE"
	EffectReconcileOnly   EffectDisposition = "RECONCILE_ONLY"
	EffectDenied          EffectDisposition = "DENIED"
	EffectTerminalFailure EffectDisposition = "TERMINAL_FAILURE"
)

type EffectPermit struct {
	EffectID      uuid.UUID
	InteractionID *uuid.UUID
	Disposition   EffectDisposition
	Status        effect.Status
	Version       int64
	Result        map[string]any
	Reason        string
}

// EffectCompletion 将 Adapter 返回转换为确定的 SUCCEEDED 或保守的 UNKNOWN。
type EffectCompletion struct {
	Status       effect.Status
	Result       map[string]any
	ExternalRef  string
	ErrorMessage string
	FinishedAt   time.Time
}

// Store 负责注册表读取、额度的原子预留以及审计记录完成。
type Store interface {
	// GetToolDefinition 必须同时校验 Attempt owner，并只返回该 Attempt 租户的工具定义。
	// 工具名不是跨租户授权凭据，绝不能仅凭模型传入的 name 查询全局注册表。
	GetToolDefinition(context.Context, execution.AttemptOwner, string) (*Definition, error)
	BeginToolCall(context.Context, execution.AttemptOwner, CallRecord, int32) error
	FinishToolCall(context.Context, execution.AttemptOwner, uuid.UUID, string, map[string]any, string) error
	PrepareToolEffect(context.Context, execution.AttemptOwner, EffectRequest) (*EffectPermit, error)
	BeginToolEffect(context.Context, execution.AttemptOwner, uuid.UUID, string) error
	FinishToolEffect(context.Context, execution.AttemptOwner, uuid.UUID, EffectCompletion) error
}

// Adapter 执行已经通过本地策略的调用。MCP 也只是一种 Adapter，不能绕过 Gateway。
type Adapter interface {
	Call(context.Context, *Definition, map[string]any) (map[string]any, error)
}

// Gateway 是单次 Attempt 的能力令牌：模板白名单、权限、风险区和调用上限都被固定。
type Gateway struct {
	store       Store
	owner       execution.AttemptOwner
	attemptID   uuid.UUID
	taskID      uuid.UUID
	runID       uuid.UUID
	agentID     uuid.UUID
	allowed     map[string]struct{}
	permissions map[string]struct{}
	riskZone    string
	maxCalls    int32
	adapters    map[string]Adapter
	mu          sync.Mutex
}

// New 创建一次执行专属网关。调用方传入模板声明，避免模型自行扩大权限。
func New(store Store, work *execution.Work, adapters map[string]Adapter) *Gateway {
	allowed := make(map[string]struct{}, len(work.Template.Tools))
	for _, name := range work.Template.Tools {
		allowed[strings.ToLower(name)] = struct{}{}
	}
	permissions := make(map[string]struct{}, len(work.Template.Permissions))
	for _, value := range work.Template.Permissions {
		permissions[strings.ToLower(value)] = struct{}{}
	}
	return &Gateway{
		store: store, owner: work.Attempt.Owner(), attemptID: work.Attempt.ID,
		taskID: work.Task.ID, runID: work.Task.SwarmID, agentID: work.Agent.ID,
		allowed: allowed, permissions: permissions, riskZone: work.Template.RiskZone,
		maxCalls: work.Task.ExecutionPolicy.MaxToolCalls, adapters: adapters,
	}
}

// Call 先完成全部本地判定和额度预留，再调用外部适配器，最后无条件结束审计记录。
func (g *Gateway) Call(ctx context.Context, name string, arguments map[string]any) (map[string]any, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	ctx, span := otel.Tracer("swarmos/tool-gateway").Start(ctx, "tool.call",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("swarmos.tool.name", name),
			attribute.String("swarmos.attempt.id", g.attemptID.String()),
			attribute.Int64("swarmos.attempt.fencing_token", g.owner.FencingToken),
		),
	)
	defer span.End()

	definition, err := g.store.GetToolDefinition(ctx, g.owner, name)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("读取工具 %s: %w", name, err)
	}
	span.SetAttributes(
		attribute.String("swarmos.tool.adapter", definition.Adapter),
		attribute.String("swarmos.tool.risk_level", definition.RiskLevel),
	)
	if arguments == nil {
		arguments = map[string]any{}
	}
	record := CallRecord{
		ID: uuid.New(), AttemptID: g.attemptID, TaskID: g.taskID, AgentID: g.agentID,
		ToolName: name, Arguments: arguments, Status: "STARTED", RiskLevel: definition.RiskLevel,
		StartedAt: time.Now().UTC(),
	}
	denial := g.denialReason(definition)
	if denial != "" {
		span.SetStatus(codes.Error, denial)
		record.Status, record.ErrorMessage = "DENIED", denial
		// 被拒绝的调用也占用调用额度，防止恶意模型通过重复试探制造无限循环。
		if beginErr := g.store.BeginToolCall(ctx, g.owner, record, g.maxCalls); beginErr != nil {
			return nil, beginErr
		}
		return nil, fmt.Errorf("%w: %s", ErrDenied, denial)
	}
	adapter, ok := g.adapters[strings.ToLower(definition.Adapter+":"+definition.Name)]
	if !ok {
		span.SetStatus(codes.Error, "adapter unavailable")
		// mcp:* 等通配适配器承载同一种传输的多个远端工具，具体端点仍来自受控注册表。
		adapter, ok = g.adapters[strings.ToLower(definition.Adapter+":*")]
	}
	if !ok {
		record.Status, record.ErrorMessage = "DENIED", "未安装对应工具适配器"
		if beginErr := g.store.BeginToolCall(ctx, g.owner, record, g.maxCalls); beginErr != nil {
			return nil, beginErr
		}
		return nil, fmt.Errorf("%w: 工具 %s 未安装适配器", ErrDenied, name)
	}

	risk, externalEffect := effectRiskLevel(definition.RiskLevel)
	var effectRequest EffectRequest
	if externalEffect {
		effectRequest, err = g.buildEffectRequest(definition, arguments, risk, record.StartedAt)
		if err != nil {
			record.Status, record.ErrorMessage = "DENIED", err.Error()
			if beginErr := g.store.BeginToolCall(ctx, g.owner, record, g.maxCalls); beginErr != nil {
				return nil, beginErr
			}
			return nil, err
		}
		// 同一 Effect 的至少一次重投必须命中同一 ToolCall 审计行，防止审批轮询消耗额度。
		record.ID = stableToolCallID(g.attemptID, effectRequest.IdempotencyKey, effectRequest.RequestHash)
		effectRequest.ToolCallID = record.ID
	}
	if err := g.store.BeginToolCall(ctx, g.owner, record, g.maxCalls); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	if externalEffect {
		permit, prepareErr := g.store.PrepareToolEffect(ctx, g.owner, effectRequest)
		if prepareErr != nil {
			_ = g.store.FinishToolCall(ctx, g.owner, record.ID, "DENIED", nil, prepareErr.Error())
			return nil, prepareErr
		}
		switch permit.Disposition {
		case EffectApprovalPending:
			return nil, &EffectWaitError{
				Cause: ErrApprovalPending, EffectID: permit.EffectID,
				InteractionID: permit.InteractionID, Status: permit.Status,
			}
		case EffectReconcileOnly:
			return nil, &EffectWaitError{
				Cause: ErrEffectReconcileRequired, EffectID: permit.EffectID, Status: permit.Status,
			}
		case EffectAlreadyDone:
			if err := g.store.FinishToolCall(ctx, g.owner, record.ID, "SUCCEEDED", permit.Result, ""); err != nil {
				return nil, err
			}
			return permit.Result, nil
		case EffectDenied:
			_ = g.store.FinishToolCall(ctx, g.owner, record.ID, "DENIED", nil, permit.Reason)
			return nil, fmt.Errorf("%w: %s", ErrDenied, permit.Reason)
		case EffectTerminalFailure:
			_ = g.store.FinishToolCall(ctx, g.owner, record.ID, "FAILED", nil, permit.Reason)
			return nil, errors.New(permit.Reason)
		case EffectExecute:
			if err := g.store.BeginToolEffect(ctx, g.owner, permit.EffectID, effectRequest.RequestHash); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("%w: 未知 Effect disposition %q", ErrDenied, permit.Disposition)
		}

		result, callErr := adapter.Call(ctx, definition, arguments)
		if callErr != nil {
			// Adapter 已被调用后，普通 error 无法证明外部系统没有落地副作用；按 UNKNOWN
			// 持久化并停止直接重试，是比误判 FAILED 更安全的默认语义。
			finishErr := g.store.FinishToolEffect(ctx, g.owner, permit.EffectID, EffectCompletion{
				Status: effect.StatusUnknown, ErrorMessage: callErr.Error(), FinishedAt: time.Now().UTC(),
			})
			// UNKNOWN 不是失败终态：稳定 ToolCall 必须保留 STARTED，待 Reconciler
			// 确认 SUCCEEDED 后才能改为 SUCCEEDED；若确认失败，由对账事务原子写 FAILED。
			// 原始错误已经保存在 Effect.error_message，不能为了日志便利破坏可恢复性。
			if finishErr != nil {
				return nil, &EffectWaitError{
					Cause: ErrEffectReconcileRequired, EffectID: permit.EffectID,
					Status: effect.StatusExecuting,
					Detail: fmt.Sprintf("adapter=%v; persist_unknown=%v", callErr, finishErr),
				}
			}
			return nil, &EffectWaitError{
				Cause: ErrEffectReconcileRequired, EffectID: permit.EffectID,
				Status: effect.StatusUnknown, Detail: callErr.Error(),
			}
		}
		externalRef, _ := result["external_ref"].(string)
		if err := g.store.FinishToolEffect(ctx, g.owner, permit.EffectID, EffectCompletion{
			Status: effect.StatusSucceeded, Result: result, ExternalRef: externalRef,
			FinishedAt: time.Now().UTC(),
		}); err != nil {
			return nil, err
		}
		if err := g.store.FinishToolCall(ctx, g.owner, record.ID, "SUCCEEDED", result, ""); err != nil {
			return nil, err
		}
		span.SetStatus(codes.Ok, "effect succeeded")
		return result, nil
	}
	result, callErr := adapter.Call(ctx, definition, arguments)
	if callErr != nil {
		span.RecordError(callErr)
		span.SetStatus(codes.Error, callErr.Error())
		finishErr := g.store.FinishToolCall(ctx, g.owner, record.ID, "FAILED", nil, callErr.Error())
		if finishErr != nil {
			return nil, fmt.Errorf("调用失败且结束审计失败: call=%v audit=%w", callErr, finishErr)
		}
		return nil, callErr
	}
	if err := g.store.FinishToolCall(ctx, g.owner, record.ID, "SUCCEEDED", result, ""); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.SetStatus(codes.Ok, "tool call succeeded")
	return result, nil
}

func (g *Gateway) buildEffectRequest(definition *Definition, arguments map[string]any,
	risk effect.RiskLevel, now time.Time,
) (EffectRequest, error) {
	operation, _ := definition.Config["operation"].(string)
	resource, _ := arguments["resource"].(string)
	credentialRef, _ := arguments["credential_ref"].(string)
	request := effect.Request{
		ToolName: definition.Name, Operation: operation, Resource: resource,
		Arguments: arguments, CredentialRef: credentialRef,
	}
	hash, err := request.Hash()
	if err != nil {
		return EffectRequest{}, err
	}
	key := firstString(arguments, "_swarmos_idempotency_key", "idempotency_key")
	if key == "" {
		// 无显式业务键时使用请求哈希形成稳定恢复键。调用方若需要以相同参数执行两次，
		// 必须显式提供不同 idempotency_key，消除“重试还是新操作”的歧义。
		key = fmt.Sprintf("run-%s/task-%s/%s/%s", g.runID, g.taskID,
			strings.ToLower(definition.Name), strings.TrimPrefix(hash, "sha256:")[:24])
	}
	effectType, _ := definition.Config["effect_type"].(string)
	if strings.TrimSpace(effectType) == "" {
		effectType = definition.Name
	}
	return EffectRequest{
		IdempotencyKey: key, Request: request, RequestHash: hash, RiskLevel: risk,
		EffectType: effectType, RequestedAt: now,
	}, nil
}

func firstString(values map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := values[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func stableToolCallID(attemptID uuid.UUID, key, requestHash string) uuid.UUID {
	// Attempt ID 隐含租户边界；同一租户/Attempt 的重放稳定，两个租户即使复用相同
	// 业务幂等键也不会争用 tool_calls 的全局主键。
	return uuid.NewSHA1(uuid.NameSpaceURL,
		[]byte("swarmos/tool-call/"+attemptID.String()+"/"+key+"/"+requestHash))
}

func effectRiskLevel(value string) (effect.RiskLevel, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "trusted":
		return effect.RiskR2ExternalReversible, true
	case "production":
		return effect.RiskR3ProductionDestructive, true
	default:
		return "", false
	}
}

func (g *Gateway) denialReason(definition *Definition) string {
	if !definition.Enabled {
		return "工具已被管理员禁用"
	}
	if _, ok := g.allowed[strings.ToLower(definition.Name)]; !ok {
		return "工具不在 AgentTemplate 白名单"
	}
	for _, required := range definition.RequiredPermissions {
		if _, ok := g.permissions[strings.ToLower(required)]; !ok {
			return "缺少权限 " + required
		}
	}
	if riskLevelRank(definition.RiskLevel) > riskZoneRank(g.riskZone) {
		return fmt.Sprintf("工具风险级别 %s 超过 Agent 风险区 %s", definition.RiskLevel, g.riskZone)
	}
	return ""
}

func riskLevelRank(value string) int {
	switch strings.ToLower(value) {
	case "read-only":
		return 0
	case "sandbox":
		return 1
	case "trusted":
		return 2
	case "production":
		return 3
	default:
		// 未知工具风险级别按最高风险处理，遵循 fail-closed 原则。
		return 100
	}
}

func riskZoneRank(value string) int {
	rank := riskLevelRank(value)
	if rank == 100 {
		// 未知 Agent 风险区代表没有已知授权能力，不能反向当成“最高权限”。
		return -1
	}
	return rank
}

// EchoAdapter 是无副作用的内置适配器，主要用于部署冒烟和 Tool Gateway 契约测试。
type EchoAdapter struct{}

func (EchoAdapter) Call(_ context.Context, _ *Definition, arguments map[string]any) (map[string]any, error) {
	return map[string]any{"echo": arguments}, nil
}

// MCPAdapter 实现 MCP 2026-07-28 的无状态 Streamable HTTP tools/call。
// endpoint 和可选 bearer_token_env 来自管理员维护的 tool_registry.config，真实令牌只读环境变量。
type MCPAdapter struct{ client *http.Client }

func NewMCPAdapter(client *http.Client) *MCPAdapter {
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	return &MCPAdapter{client: client}
}

type mcpRequest struct {
	JSONRPC string         `json:"jsonrpc"`
	ID      string         `json:"id"`
	Method  string         `json:"method"`
	Params  map[string]any `json:"params"`
}

type mcpResponse struct {
	JSONRPC string `json:"jsonrpc"`
	ID      any    `json:"id"`
	Result  struct {
		Content           []map[string]any `json:"content"`
		StructuredContent map[string]any   `json:"structuredContent"`
		IsError           bool             `json:"isError"`
	} `json:"result"`
	Error *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (a *MCPAdapter) Call(ctx context.Context, definition *Definition, arguments map[string]any) (map[string]any, error) {
	endpoint, _ := definition.Config["endpoint"].(string)
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "https" && parsed.Scheme != "http") {
		return nil, errors.New("MCP endpoint 必须是合法的 http/https URL")
	}
	remoteName, _ := definition.Config["remote_tool"].(string)
	if remoteName == "" {
		remoteName = definition.Name
	}
	requestID := uuid.NewString()
	payload := mcpRequest{
		JSONRPC: "2.0", ID: requestID, Method: "tools/call",
		Params: map[string]any{
			"name": remoteName, "arguments": arguments,
			"_meta": map[string]any{"io.modelcontextprotocol/clientInfo": map[string]any{
				"name": "swarmos-tool-gateway", "version": "1.0.0",
			}},
		},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("序列化 MCP 请求: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("创建 MCP 请求: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json, text/event-stream")
	request.Header.Set("MCP-Protocol-Version", "2026-07-28")
	request.Header.Set("Mcp-Method", "tools/call")
	request.Header.Set("Mcp-Name", remoteName)
	if envName, _ := definition.Config["bearer_token_env"].(string); envName != "" {
		if token := os.Getenv(envName); token != "" {
			request.Header.Set("Authorization", "Bearer "+token)
		}
	}
	response, err := a.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("调用 MCP %s: %w", remoteName, err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		return nil, fmt.Errorf("MCP HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	decoded, err := decodeMCPResponse(response)
	if err != nil {
		return nil, err
	}
	if decoded.Error != nil {
		return nil, fmt.Errorf("MCP JSON-RPC %d: %s", decoded.Error.Code, decoded.Error.Message)
	}
	if decoded.Result.IsError {
		return nil, fmt.Errorf("MCP 工具 %s 返回业务错误: %v", remoteName, decoded.Result.Content)
	}
	if decoded.Result.StructuredContent != nil {
		return decoded.Result.StructuredContent, nil
	}
	return map[string]any{"content": decoded.Result.Content}, nil
}

func decodeMCPResponse(response *http.Response) (*mcpResponse, error) {
	contentType := strings.ToLower(response.Header.Get("Content-Type"))
	decoder := json.NewDecoder(io.LimitReader(response.Body, 4<<20))
	if !strings.Contains(contentType, "text/event-stream") {
		var value mcpResponse
		if err := decoder.Decode(&value); err != nil {
			return nil, fmt.Errorf("解析 MCP JSON 响应: %w", err)
		}
		return &value, nil
	}
	// 兼容仍以 SSE 返回单次 JSON-RPC 响应的 Streamable HTTP 服务。
	scanner := bufio.NewScanner(io.LimitReader(response.Body, 4<<20))
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		var value mcpResponse
		if err := json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &value); err != nil {
			continue
		}
		if value.Result.Content != nil || value.Result.StructuredContent != nil || value.Error != nil {
			return &value, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("读取 MCP SSE: %w", err)
	}
	return nil, errors.New("MCP SSE 未返回对应 JSON-RPC 结果")
}
