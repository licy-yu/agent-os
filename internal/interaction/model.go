// Package interaction 定义 Agent 等待用户输入、审批或接管时的结构化领域模型。
//
// Interaction 本身保存用户请求与响应；Task/Agent 的 WAITING 状态由编排层在同一事务中
// 推进。领域状态机禁止已解决、已过期或已取消的请求被重新打开，避免迟到响应覆盖新决策。
package interaction

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	// ErrInvalidInteraction 表示 Interaction 初始合同不完整或包含歧义动作。
	ErrInvalidInteraction = errors.New("非法 Interaction")
	// ErrInvalidTransition 表示 Interaction 生命周期被非法跳过或重新打开。
	ErrInvalidTransition = errors.New("非法 Interaction 状态转换")
	// ErrInvalidResponse 表示用户动作不在服务端固化的允许动作集合中。
	ErrInvalidResponse = errors.New("非法 Interaction 响应")
)

// Type 描述需要用户参与的原因。类型是策略输入，不应依赖 title 文本猜测。
type Type string

const (
	TypeInput    Type = "INPUT"
	TypeApproval Type = "APPROVAL"
	TypeChoice   Type = "CHOICE"
	TypeEdit     Type = "EDIT"
	TypeAuth     Type = "AUTH"
	TypeTakeover Type = "TAKEOVER"
)

// Status 描述结构化交互从创建、对用户可见到结束的生命周期。
type Status string

const (
	StatusCreated  Status = "CREATED"
	StatusWaiting  Status = "WAITING"
	StatusResolved Status = "RESOLVED"
	StatusExpired  Status = "EXPIRED"
	StatusCanceled Status = "CANCELED"
)

// Response 是一次最终用户决策。Payload 可保存编辑后的参数，但凭据正文不应写入其中；
// AUTH 类型应只保存 Credential Broker 返回的 credential_ref。
type Response struct {
	Action      string
	Payload     map[string]any
	RespondedBy string
	RespondedAt time.Time
}

// Interaction 是可审计的人机协作合同。AllowedActions 在创建时固化，后续模型不能自行增加
// “approve”之类高权限动作。
type Interaction struct {
	ID             uuid.UUID
	Type           Type
	Status         Status
	Title          string
	Payload        map[string]any
	AllowedActions []string
	Response       *Response
	Version        int64
}

var allowedTransitions = map[Status]map[Status]bool{
	StatusCreated: {
		StatusWaiting:  true,
		StatusCanceled: true,
	},
	StatusWaiting: {
		StatusResolved: true,
		StatusExpired:  true,
		StatusCanceled: true,
	},
}

// New 创建尚未发布给用户的 Interaction。调用方应先持久化 CREATED，再与 Task/Agent 的
// WAITING 状态一起推进为 WAITING，确保用户不会响应一个尚未建立等待边界的请求。
func New(id uuid.UUID, interactionType Type, title string, payload map[string]any, actions []string) (*Interaction, error) {
	if id == uuid.Nil {
		return nil, fmt.Errorf("%w: id 不能为空", ErrInvalidInteraction)
	}
	if !interactionType.Valid() {
		return nil, fmt.Errorf("%w: 未知类型 %q", ErrInvalidInteraction, interactionType)
	}
	if strings.TrimSpace(title) == "" {
		return nil, fmt.Errorf("%w: title 不能为空", ErrInvalidInteraction)
	}
	cleanActions := make([]string, 0, len(actions))
	seen := make(map[string]struct{}, len(actions))
	for _, action := range actions {
		clean := strings.TrimSpace(action)
		if clean == "" {
			return nil, fmt.Errorf("%w: allowed action 不能为空", ErrInvalidInteraction)
		}
		key := strings.ToLower(clean)
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("%w: allowed action %q 重复", ErrInvalidInteraction, clean)
		}
		seen[key] = struct{}{}
		cleanActions = append(cleanActions, clean)
	}
	if len(cleanActions) == 0 {
		return nil, fmt.Errorf("%w: 至少需要一个 allowed action", ErrInvalidInteraction)
	}
	if payload == nil {
		payload = map[string]any{}
	}
	return &Interaction{
		ID: id, Type: interactionType, Status: StatusCreated, Title: strings.TrimSpace(title),
		Payload: payload, AllowedActions: cleanActions, Version: 1,
	}, nil
}

// Transition 执行不携带用户响应的状态转换。RESOLVED 必须通过 Resolve 完成，以保证
// Interaction 不会出现“状态已解决但没有决策证据”的不一致记录。
func (i *Interaction) Transition(next Status) error {
	if i == nil {
		return fmt.Errorf("%w: Interaction 不能为空", ErrInvalidTransition)
	}
	if next == StatusResolved {
		return fmt.Errorf("%w: RESOLVED 必须通过 Resolve 写入响应", ErrInvalidTransition)
	}
	return i.transition(next)
}

// Resolve 校验用户动作并原子更新领域对象。存储层仍需以 status + version 做 CAS，防止两个
// 浏览器标签页同时响应时后写覆盖先写。
func (i *Interaction) Resolve(response Response) error {
	if i == nil {
		return fmt.Errorf("%w: Interaction 不能为空", ErrInvalidResponse)
	}
	action := strings.TrimSpace(response.Action)
	if action == "" || !i.actionAllowed(action) {
		return fmt.Errorf("%w: action %q 不在允许集合", ErrInvalidResponse, response.Action)
	}
	if strings.TrimSpace(response.RespondedBy) == "" {
		return fmt.Errorf("%w: responded_by 不能为空", ErrInvalidResponse)
	}
	if response.RespondedAt.IsZero() {
		return fmt.Errorf("%w: responded_at 不能为空", ErrInvalidResponse)
	}
	if response.Payload == nil {
		response.Payload = map[string]any{}
	}
	response.Action = action
	if err := i.transition(StatusResolved); err != nil {
		return err
	}
	i.Response = &response
	return nil
}

func (i *Interaction) transition(next Status) error {
	if !i.Status.Valid() || !next.Valid() || !allowedTransitions[i.Status][next] {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, i.Status, next)
	}
	i.Status = next
	return nil
}

func (i Interaction) actionAllowed(action string) bool {
	for _, allowed := range i.AllowedActions {
		if strings.EqualFold(allowed, action) {
			return true
		}
	}
	return false
}

// Terminal 报告 Interaction 是否已经不可再响应。
func (i Interaction) Terminal() bool {
	return i.Status == StatusResolved || i.Status == StatusExpired || i.Status == StatusCanceled
}

// Valid 报告类型是否属于受支持的人机协作类型。
func (t Type) Valid() bool {
	switch t {
	case TypeInput, TypeApproval, TypeChoice, TypeEdit, TypeAuth, TypeTakeover:
		return true
	default:
		return false
	}
}

// Valid 报告状态是否属于当前领域协议。
func (s Status) Valid() bool {
	switch s {
	case StatusCreated, StatusWaiting, StatusResolved, StatusExpired, StatusCanceled:
		return true
	default:
		return false
	}
}
