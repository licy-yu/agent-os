// Package planning 定义 Goal 经过 Planner 后、进入执行层之前的强类型计划合同。
//
// Planner 可以使用模型生成 PlanCandidate，但本包的校验和编译全部是确定性代码。
// 只有通过 PlanCompiler 的候选计划才能成为 ExecutablePlan。
package planning

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ResourceBudget 是 Task 或 Run 的硬资源上限。0 表示该维度不设上限；负数永远非法。
type ResourceBudget struct {
	MaxTokens          int64 `json:"max_tokens"`
	MaxCostMicros      int64 `json:"max_cost_micros"`
	MaxDurationSeconds int64 `json:"max_duration_seconds"`
}

// InputSpec 描述 Task 输入。Source 推荐使用 artifact://、workspace:// 或 memory:// URI，
// 但 Compiler 不解释具体协议，解析和授权由 Context Engine 完成。
type InputSpec struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Source   string `json:"source,omitempty"`
	Required bool   `json:"required"`
}

// ArtifactSpec 描述 Task 必须交付的结构化产物，而不是模型自然语言中的“已完成”。
type ArtifactSpec struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// OutputSpec 是 Task 输出合同。Schema 必须只包含可 JSON 序列化的数据。
type OutputSpec struct {
	Type      string         `json:"type"`
	Schema    map[string]any `json:"schema,omitempty"`
	Artifacts []ArtifactSpec `json:"artifacts,omitempty"`
}

// Requirements 是 Scheduler Filter 的硬约束输入。
type Requirements struct {
	Capabilities map[string]float64 `json:"capabilities"`
	Tools        []string           `json:"tools,omitempty"`
	Permissions  []string           `json:"permissions,omitempty"`
	Models       []string           `json:"models,omitempty"`
	RiskZone     string             `json:"risk_zone,omitempty"`
}

// ContextPolicy 限制 Context Engine 可装载的来源和 Token 数。
type ContextPolicy struct {
	MaxTokens              int64 `json:"max_tokens"`
	IncludeProjectMemory   bool  `json:"include_project_memory"`
	IncludeParentArtifacts bool  `json:"include_parent_artifacts"`
	IncludePreviousFailure bool  `json:"include_previous_failure"`
}

// SideEffectPolicy 限制 Task 可产生的真实外部副作用。
type SideEffectPolicy struct {
	MaxRisk         string   `json:"max_risk"`
	AllowedEffects  []string `json:"allowed_effects,omitempty"`
	RequireApproval bool     `json:"require_approval"`
}

// AcceptanceCriterion 是 AcceptanceGate 必须独立验证的一项条件。
type AcceptanceCriterion struct {
	Name   string         `json:"name"`
	Type   string         `json:"type"`
	Config map[string]any `json:"config,omitempty"`
}

// RetryPolicy 决定逻辑 Task 可创建多少次物理 Attempt。
type RetryPolicy struct {
	MaxAttempts           int32 `json:"max_attempts"`
	InitialBackoffSeconds int64 `json:"initial_backoff_seconds"`
	MaxBackoffSeconds     int64 `json:"max_backoff_seconds"`
}

// DefaultRetryPolicy 提供保守的默认重试上限。
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 3, InitialBackoffSeconds: 5, MaxBackoffSeconds: 300}
}

// RuntimeGuard 是单次 Attempt 的死循环保险丝。MaxToolCalls 可以为 0，表示纯模型任务；
// 其它计数型上限必须为正数。
type RuntimeGuard struct {
	MaxTurns            int32 `json:"max_turns"`
	MaxModelCalls       int32 `json:"max_model_calls"`
	MaxToolCalls        int32 `json:"max_tool_calls"`
	MaxHandoffs         int32 `json:"max_handoffs"`
	MaxNoProgressRounds int32 `json:"max_no_progress_rounds"`
}

// DefaultRuntimeGuard 给 Planner 未显式设置的任务提供可生产使用的安全上限。
func DefaultRuntimeGuard() RuntimeGuard {
	return RuntimeGuard{
		MaxTurns: 64, MaxModelCalls: 32, MaxToolCalls: 100,
		MaxHandoffs: 10, MaxNoProgressRounds: 3,
	}
}

// TaskContract 是可执行 Task 的完整合同。Inputs 使用非 nil 空切片表示“已声明无输入”，
// 从而能够区分 Planner 忘记填写 inputs 和真正的 DAG 根任务。
type TaskContract struct {
	ID                      uuid.UUID             `json:"id"`
	RunID                   uuid.UUID             `json:"run_id"`
	Name                    string                `json:"name"`
	Goal                    string                `json:"goal"`
	Inputs                  []InputSpec           `json:"inputs"`
	Output                  OutputSpec            `json:"output"`
	Requirements            Requirements          `json:"requirements"`
	ContextPolicy           ContextPolicy         `json:"context_policy"`
	SideEffectPolicy        SideEffectPolicy      `json:"side_effect_policy"`
	Acceptance              []AcceptanceCriterion `json:"acceptance"`
	RetryPolicy             RetryPolicy           `json:"retry_policy"`
	Budget                  ResourceBudget        `json:"budget"`
	RuntimeGuard            RuntimeGuard          `json:"runtime_guard"`
	Priority                int32                 `json:"priority"`
	Deadline                *time.Time            `json:"deadline,omitempty"`
	ExpectedDurationSeconds int64                 `json:"expected_duration_seconds"`
}

// ContractViolation 是单个 Task Contract 中可供 Planner 修复的精确错误。
type ContractViolation struct {
	Field   string
	Message string
}

func (v ContractViolation) Error() string {
	return fmt.Sprintf("%s: %s", v.Field, v.Message)
}

// ValidateContract 对 Task 做纯函数校验，返回顺序稳定的全部问题，而不是遇到第一个问题就退出。
func ValidateContract(value TaskContract) []ContractViolation {
	violations := make([]ContractViolation, 0)
	add := func(field, message string) {
		violations = append(violations, ContractViolation{Field: field, Message: message})
	}

	if value.ID == uuid.Nil {
		add("id", "不能为空")
	}
	if value.RunID == uuid.Nil {
		add("run_id", "不能为空")
	}
	if strings.TrimSpace(value.Name) == "" {
		add("name", "不能为空")
	}
	if strings.TrimSpace(value.Goal) == "" {
		add("goal", "不能为空")
	}
	if value.Priority < 0 || value.Priority > 1_000 {
		add("priority", "必须在 0~1000 范围内")
	}
	if value.Inputs == nil {
		add("inputs", "必须显式声明；无输入时请使用空数组")
	}
	for index, input := range value.Inputs {
		if strings.TrimSpace(input.Name) == "" {
			add(fmt.Sprintf("inputs[%d].name", index), "不能为空")
		}
		if strings.TrimSpace(input.Type) == "" {
			add(fmt.Sprintf("inputs[%d].type", index), "不能为空")
		}
		if input.Required && strings.TrimSpace(input.Source) == "" {
			add(fmt.Sprintf("inputs[%d].source", index), "必需输入必须声明来源")
		}
	}
	if strings.TrimSpace(value.Output.Type) == "" {
		add("output.type", "不能为空")
	}
	for index, artifact := range value.Output.Artifacts {
		if strings.TrimSpace(artifact.Name) == "" || strings.TrimSpace(artifact.Type) == "" {
			add(fmt.Sprintf("output.artifacts[%d]", index), "name 和 type 均不能为空")
		}
	}

	capabilityNames := sortedFloatKeys(value.Requirements.Capabilities)
	if len(capabilityNames) == 0 {
		add("requirements.capabilities", "至少需要一项能力要求")
	}
	for _, name := range capabilityNames {
		level := value.Requirements.Capabilities[name]
		if strings.TrimSpace(name) == "" {
			add("requirements.capabilities", "能力名不能为空")
		}
		if level <= 0 || level > 1 {
			add("requirements.capabilities."+name, "能力阈值必须在 (0,1] 范围内")
		}
	}
	seenCapabilities := make(map[string]string, len(capabilityNames))
	for _, name := range capabilityNames {
		normalized := strings.ToLower(strings.TrimSpace(name))
		if previous, exists := seenCapabilities[normalized]; normalized != "" && exists && previous != name {
			add("requirements.capabilities."+name, "能力名与 "+previous+" 仅大小写不同，属于重复声明")
		}
		seenCapabilities[normalized] = name
	}
	validateNames := func(field string, values []string) {
		seen := make(map[string]struct{}, len(values))
		for index, raw := range values {
			name := strings.ToLower(strings.TrimSpace(raw))
			if name == "" {
				add(fmt.Sprintf("%s[%d]", field, index), "不能为空")
				continue
			}
			if _, exists := seen[name]; exists {
				add(fmt.Sprintf("%s[%d]", field, index), "不能重复")
			}
			seen[name] = struct{}{}
		}
	}
	validateNames("requirements.tools", value.Requirements.Tools)
	validateNames("requirements.permissions", value.Requirements.Permissions)
	validateNames("requirements.models", value.Requirements.Models)

	if value.ContextPolicy.MaxTokens < 1 {
		add("context_policy.max_tokens", "必须大于等于 1")
	}
	if !validRiskLevel(value.SideEffectPolicy.MaxRisk) {
		add("side_effect_policy.max_risk", "必须是 R0~R3 或 read-only/sandbox/trusted/production")
	}
	validateNames("side_effect_policy.allowed_effects", value.SideEffectPolicy.AllowedEffects)
	if len(value.Acceptance) == 0 {
		add("acceptance", "至少需要一项可验证验收条件")
	}
	for index, criterion := range value.Acceptance {
		if strings.TrimSpace(criterion.Name) == "" {
			add(fmt.Sprintf("acceptance[%d].name", index), "不能为空")
		}
		if strings.TrimSpace(criterion.Type) == "" {
			add(fmt.Sprintf("acceptance[%d].type", index), "不能为空")
		}
	}
	if value.RetryPolicy.MaxAttempts < 1 {
		add("retry_policy.max_attempts", "必须大于等于 1")
	}
	if value.RetryPolicy.InitialBackoffSeconds < 0 || value.RetryPolicy.MaxBackoffSeconds < 0 {
		add("retry_policy", "退避时间不能为负数")
	}
	if value.RetryPolicy.MaxBackoffSeconds > 0 &&
		value.RetryPolicy.InitialBackoffSeconds > value.RetryPolicy.MaxBackoffSeconds {
		add("retry_policy", "初始退避不能大于最大退避")
	}
	if value.Budget.MaxTokens < 1 {
		add("budget.max_tokens", "必须大于等于 1")
	}
	if value.Budget.MaxCostMicros < 0 || value.Budget.MaxDurationSeconds < 1 {
		add("budget", "成本不能为负数且最长执行时间必须大于等于 1 秒")
	}
	guard := value.RuntimeGuard
	if guard.MaxTurns < 1 || guard.MaxModelCalls < 1 || guard.MaxToolCalls < 0 ||
		guard.MaxHandoffs < 0 || guard.MaxNoProgressRounds < 1 {
		add("runtime_guard", "turn/model/no-progress 上限必须为正，tool/handoff 上限不能为负")
	}
	if value.ExpectedDurationSeconds < 1 {
		add("expected_duration_seconds", "必须大于等于 1")
	}

	sort.SliceStable(violations, func(i, j int) bool {
		if violations[i].Field == violations[j].Field {
			return violations[i].Message < violations[j].Message
		}
		return violations[i].Field < violations[j].Field
	})
	return violations
}

func sortedFloatKeys(values map[string]float64) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func validRiskLevel(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "r0", "r1", "r2", "r3",
		"r0_read_only", "r1_sandbox_write", "r2_external_reversible", "r3_production_destructive",
		"read-only", "sandbox", "trusted", "production":
		return true
	default:
		return false
	}
}
