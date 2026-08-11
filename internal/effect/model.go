// Package effect 定义外部副作用的领域模型。
//
// Effect 与普通 ToolCall 的关键区别是：ToolCall 描述一次调用尝试，Effect 描述外部世界中
// 最终只能发生一次的业务副作用。即使 Worker 崩溃或消息重复投递，同一个幂等键也必须绑定
// 到完全相同的请求；外部结果不确定时则必须先对账，不能直接重试。
package effect

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

var (
	// ErrInvalidTransition 表示调用方试图绕过 Effect 的受控生命周期。
	ErrInvalidTransition = errors.New("非法 Effect 状态转换")
	// ErrInvalidRequest 表示请求无法形成稳定、可审计的幂等指纹。
	ErrInvalidRequest = errors.New("非法 Effect 请求")
	// ErrIdempotencyConflict 表示同一个幂等键被复用于不同请求。
	ErrIdempotencyConflict = errors.New("Effect 幂等请求冲突")
)

// Status 是一个外部副作用从准备、授权、执行到最终确认或补偿的状态。
type Status string

const (
	StatusPrepared     Status = "PREPARED"
	StatusAuthorized   Status = "AUTHORIZED"
	StatusExecuting    Status = "EXECUTING"
	StatusSucceeded    Status = "SUCCEEDED"
	StatusFailed       Status = "FAILED"
	StatusUnknown      Status = "UNKNOWN"
	StatusReconciling  Status = "RECONCILING"
	StatusCompensating Status = "COMPENSATING"
	StatusCompensated  Status = "COMPENSATED"
)

// RiskLevel 使用文档中的 R0~R3 风险分级。审批策略应根据该值在 Effect 进入
// AUTHORIZED 前执行，而不是依赖模型自行判断操作是否危险。
type RiskLevel string

const (
	RiskR0ReadOnly              RiskLevel = "R0_READ_ONLY"
	RiskR1SandboxWrite          RiskLevel = "R1_SANDBOX_WRITE"
	RiskR2ExternalReversible    RiskLevel = "R2_EXTERNAL_REVERSIBLE"
	RiskR3ProductionDestructive RiskLevel = "R3_PRODUCTION_DESTRUCTIVE"
)

// Request 是参与幂等判定的完整、无密钥调用合同。
// CredentialRef 只是 Credential Broker 中的引用；真实 Token 或密码不得进入该结构。
type Request struct {
	ToolName      string         `json:"tool_name"`
	Operation     string         `json:"operation,omitempty"`
	Resource      string         `json:"resource,omitempty"`
	Arguments     map[string]any `json:"arguments"`
	CredentialRef string         `json:"credential_ref,omitempty"`
}

// Effect 保存一次不可重复副作用的最小领域事实。RequestHash 与 IdempotencyKey 必须同时
// 持久化：幂等键负责命中历史记录，请求哈希负责拒绝“同键异参”。
type Effect struct {
	ID             uuid.UUID
	AttemptID      uuid.UUID
	IdempotencyKey string
	RequestHash    string
	RiskLevel      RiskLevel
	Status         Status
	ExternalRef    string
	Version        int64
}

var allowedTransitions = map[Status]map[Status]bool{
	StatusPrepared: {
		StatusAuthorized: true,
	},
	StatusAuthorized: {
		StatusExecuting: true,
	},
	StatusExecuting: {
		StatusSucceeded: true,
		StatusFailed:    true,
		StatusUnknown:   true,
	},
	StatusUnknown: {
		StatusReconciling: true,
	},
	StatusReconciling: {
		StatusSucceeded: true,
		StatusFailed:    true,
		// 外部系统仍不能给出确定答案时返回 UNKNOWN，等待下一次有界对账。
		StatusUnknown: true,
	},
	StatusSucceeded: {
		StatusCompensating: true,
	},
	StatusCompensating: {
		StatusCompensated: true,
	},
}

// Prepare 创建处于 PREPARED 的 Effect，并立即固化请求哈希。调用方应在同一个数据库事务
// 中持久化返回值和业务状态，随后再进入授权流程。
func Prepare(id, attemptID uuid.UUID, idempotencyKey string, risk RiskLevel, request Request) (*Effect, error) {
	if id == uuid.Nil || attemptID == uuid.Nil {
		return nil, fmt.Errorf("%w: id 和 attempt_id 不能为空", ErrInvalidRequest)
	}
	key := strings.TrimSpace(idempotencyKey)
	if key == "" {
		return nil, fmt.Errorf("%w: idempotency_key 不能为空", ErrInvalidRequest)
	}
	if !risk.Valid() {
		return nil, fmt.Errorf("%w: 未知风险等级 %q", ErrInvalidRequest, risk)
	}
	hash, err := request.Hash()
	if err != nil {
		return nil, err
	}
	return &Effect{
		ID: id, AttemptID: attemptID, IdempotencyKey: key, RequestHash: hash,
		RiskLevel: risk, Status: StatusPrepared, Version: 1,
	}, nil
}

// Hash 对规范 JSON 计算带算法前缀的 SHA-256。encoding/json 会稳定排序字符串 Map Key，
// 因而字段插入顺序不同但语义相同的请求会得到同一哈希；无法 JSON 编码的值会被拒绝。
func (r Request) Hash() (string, error) {
	r.ToolName = strings.TrimSpace(r.ToolName)
	if r.ToolName == "" {
		return "", fmt.Errorf("%w: tool_name 不能为空", ErrInvalidRequest)
	}
	// nil 和空对象对 Tool Gateway 具有相同语义，先归一化以避免不必要的幂等冲突。
	if r.Arguments == nil {
		r.Arguments = map[string]any{}
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return "", fmt.Errorf("%w: 请求不能编码为 JSON: %v", ErrInvalidRequest, err)
	}
	digest := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

// MatchesRequest 验证重试请求是否与已持久化 Effect 完全一致。返回
// ErrIdempotencyConflict 时，调用方必须停止执行并记录安全事件。
func (e Effect) MatchesRequest(request Request) error {
	hash, err := request.Hash()
	if err != nil {
		return err
	}
	if e.RequestHash == "" || e.RequestHash != hash {
		return fmt.Errorf("%w: stored=%q incoming=%q", ErrIdempotencyConflict, e.RequestHash, hash)
	}
	return nil
}

// Transition 执行严格状态转换。UNKNOWN 只能先进入 RECONCILING；这样恢复控制器不会把
// “外部可能已成功”误当成普通失败并重复执行。
func (e *Effect) Transition(next Status) error {
	if e == nil {
		return fmt.Errorf("%w: Effect 不能为空", ErrInvalidTransition)
	}
	if !e.Status.Valid() || !next.Valid() || !allowedTransitions[e.Status][next] {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, e.Status, next)
	}
	e.Status = next
	return nil
}

// Valid 报告状态是否属于当前领域协议。
func (s Status) Valid() bool {
	switch s {
	case StatusPrepared, StatusAuthorized, StatusExecuting, StatusSucceeded, StatusFailed,
		StatusUnknown, StatusReconciling, StatusCompensating, StatusCompensated:
		return true
	default:
		return false
	}
}

// Valid 报告风险等级是否属于 R0~R3 的封闭集合。
func (r RiskLevel) Valid() bool {
	switch r {
	case RiskR0ReadOnly, RiskR1SandboxWrite, RiskR2ExternalReversible, RiskR3ProductionDestructive:
		return true
	default:
		return false
	}
}
