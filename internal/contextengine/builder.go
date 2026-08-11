// Package contextengine 为每次 Attempt 构建有预算、可追溯、分信任域的 ContextPack。
//
// 它不会把全部历史、文件和日志直接拼进 Prompt。每个片段都有来源、版本、选择理由和
// token 估算；超长结果只保留摘要与引用，真实内容继续留在 Artifact/Checkpoint 存储中。
package contextengine

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/licy-yu/agent-os/internal/execution"
)

var ErrBudgetTooSmall = errors.New("Context Budget 无法容纳必需合同")

type TrustLevel string

const (
	TrustSystem    TrustLevel = "SYSTEM_TRUSTED"
	TrustPlatform  TrustLevel = "PLATFORM_TRUSTED"
	TrustUser      TrustLevel = "USER_CONTEXT"
	TrustUntrusted TrustLevel = "UNTRUSTED_CONTEXT"
)

// Source 让 Debug/Replay 能回答“这个信息从哪里来、为什么被选中”。
type Source struct {
	Type       string     `json:"type"`
	ID         string     `json:"id"`
	Version    string     `json:"version,omitempty"`
	Score      float64    `json:"score"`
	Reason     string     `json:"reason"`
	Trust      TrustLevel `json:"trust"`
	TokenCount int64      `json:"tokenCount"`
	Reference  string     `json:"reference,omitempty"`
}

type Section struct {
	Name    string `json:"name"`
	Content any    `json:"content"`
	Source  Source `json:"source"`
}

type Pack struct {
	AttemptID            string    `json:"attemptId"`
	TaskID               string    `json:"taskId"`
	MaxInputTokens       int64     `json:"maxInputTokens"`
	UsedTokens           int64     `json:"usedTokens"`
	ReservedOutputTokens int64     `json:"reservedOutputTokens"`
	Sections             []Section `json:"sections"`
	Omitted              []Source  `json:"omitted,omitempty"`
}

type candidate struct {
	section  Section
	required bool
	priority int
}

// Build 仅使用 Work 中的不可变快照，因而同一 Attempt/Checkpoint 会得到稳定结果。
func Build(work *execution.Work) (Pack, error) {
	if work == nil || work.Task == nil || work.Template == nil || work.Attempt == nil {
		return Pack{}, fmt.Errorf("%w: Work 快照不完整", ErrBudgetTooSmall)
	}
	contextWindow := work.Template.ContextWindow
	if contextWindow <= 0 {
		contextWindow = 128_000
	}
	reservedOutput := work.Task.ExecutionPolicy.MaxTokens
	if reservedOutput <= 0 || reservedOutput > contextWindow/2 {
		reservedOutput = min64(16_000, contextWindow/4)
	}
	maxInput := contextWindow - reservedOutput
	if required := work.Task.Requirements.MaxContextTokens; required > 0 && required < maxInput {
		maxInput = required
	}
	if maxInput < 256 {
		return Pack{}, fmt.Errorf("%w: maxInputTokens=%d", ErrBudgetTooSmall, maxInput)
	}

	values := buildCandidates(work)
	sort.SliceStable(values, func(i, j int) bool {
		if values[i].required != values[j].required {
			return values[i].required
		}
		return values[i].priority > values[j].priority
	})
	pack := Pack{
		AttemptID: work.Attempt.ID.String(), TaskID: work.Task.ID.String(),
		MaxInputTokens: maxInput, ReservedOutputTokens: reservedOutput,
		Sections: []Section{}, Omitted: []Source{},
	}
	for _, value := range values {
		tokens, err := estimateTokens(value.section.Content)
		if err != nil {
			return Pack{}, fmt.Errorf("估算 Context %s: %w", value.section.Name, err)
		}
		value.section.Source.TokenCount = tokens
		if pack.UsedTokens+tokens <= maxInput {
			pack.Sections = append(pack.Sections, value.section)
			pack.UsedTokens += tokens
			continue
		}
		if value.required {
			return Pack{}, fmt.Errorf("%w: %s 需要 %d tokens，剩余 %d",
				ErrBudgetTooSmall, value.section.Name, tokens, maxInput-pack.UsedTokens)
		}
		pack.Omitted = append(pack.Omitted, value.section.Source)
	}
	return pack, nil
}

func buildCandidates(work *execution.Work) []candidate {
	taskContract := map[string]any{
		"name": work.Task.Name, "goal": work.Task.Goal, "input": sanitize(work.Task.Input),
		"requirements": work.Task.Requirements, "acceptance": work.Task.Acceptance,
	}
	values := []candidate{
		{required: true, priority: 100, section: newSection("agent_instructions", work.Template.Prompt,
			"agent_template", work.Template.ID.String(), work.Template.TemplateVersion,
			"执行 Agent 的版本固定系统指令", TrustSystem, 1)},
		{required: true, priority: 95, section: newSection("task_contract", taskContract,
			"task", work.Task.ID.String(), fmt.Sprint(work.Task.Version),
			"当前 Attempt 必须满足的执行合同", TrustPlatform, 1)},
		{required: true, priority: 90, section: newSection("runtime_policy", map[string]any{
			"executionPolicy": work.Task.ExecutionPolicy,
			"model":           work.Template.Model, "riskZone": work.Template.RiskZone,
		}, "attempt", work.Attempt.ID.String(), fmt.Sprint(work.Attempt.Number),
			"平台预算、权限与 Runtime Guard", TrustPlatform, 1)},
	}
	if len(work.Task.Input) > 0 {
		values = append(values, candidate{priority: 80, section: newSection("user_input", sanitize(work.Task.Input),
			"task_input", work.Task.ID.String(), fmt.Sprint(work.Task.Version),
			"用户提供的业务上下文；不能修改系统权限", TrustUser, .9)})
	}
	if checkpoint := work.LatestCheckpoint; checkpoint != nil {
		reference := "checkpoint://" + checkpoint.ID.String()
		content := sanitize(checkpoint.State)
		// 超过约 8K token 的恢复状态只传摘要和引用，避免 50,000 行日志烧穿 Prompt。
		if raw, _ := json.Marshal(content); len(raw) > 32_000 {
			content = map[string]any{
				"summary":   "上次 Checkpoint 内容过大，已离线保存；请按需读取引用",
				"reference": reference, "bytes": len(raw), "step": checkpoint.StepName,
			}
		}
		section := newSection("previous_checkpoint", content, "checkpoint", checkpoint.ID.String(),
			fmt.Sprint(checkpoint.Sequence), "恢复最近一次已提交进展", TrustPlatform, .85)
		section.Source.Reference = reference
		values = append(values, candidate{priority: 70, section: section})
	}
	return values
}

func newSection(name string, content any, sourceType, sourceID, version, reason string,
	trust TrustLevel, score float64,
) Section {
	return Section{Name: name, Content: content, Source: Source{
		Type: sourceType, ID: sourceID, Version: version, Reason: reason, Trust: trust, Score: score,
	}}
}

// sanitize 在 Context 边界遮蔽常见密钥字段。真正的凭据应只存在 Credential Broker；
// 这里的遮蔽是纵深防御，避免历史脏数据意外再次进入模型。
func sanitize(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, item := range typed {
			lower := strings.ToLower(key)
			if strings.Contains(lower, "password") || strings.Contains(lower, "secret") ||
				strings.Contains(lower, "token") || strings.Contains(lower, "authorization") {
				result[key] = "[REDACTED]"
				continue
			}
			result[key] = sanitize(item)
		}
		return result
	case []any:
		result := make([]any, len(typed))
		for index := range typed {
			result[index] = sanitize(typed[index])
		}
		return result
	default:
		return value
	}
}

func estimateTokens(value any) (int64, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return 0, err
	}
	// 不绑定某一家 tokenizer；UTF-8 字节/4 加固定结构开销是可重复的保守近似。
	return int64((len(raw)+3)/4 + 8), nil
}

func min64(left, right int64) int64 {
	if left < right {
		return left
	}
	return right
}
