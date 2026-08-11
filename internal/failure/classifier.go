// Package failure 把运行时错误归一化为稳定的失败类别和恢复策略。
//
// 模型供应商、MCP、数据库和策略层返回的原始错误文本都不稳定，不能直接作为
// “是否重试”的判断条件。本包把判断集中在一个纯函数中：调用方可以把 Decision
// 持久化到 Attempt/Timeline，也可以按 RetryLevel 选择 Step、Attempt、Agent 或 Plan
// 级恢复。纯函数没有时间、网络和随机数依赖，适合在重放与单元测试中保持确定性。
package failure

import (
	"context"
	"errors"
	"net"
	"strings"
)

// Class 是设计文档定义的稳定失败分类。新增分类时必须同时补充策略和测试；未知错误
// 会保守落到 MODEL_PROVIDER_ERROR，绝不因为没有识别出来就无限快速重试。
type Class string

const (
	NetworkTemporary   Class = "NETWORK_TEMPORARY"
	RateLimit          Class = "RATE_LIMIT"
	ModelProviderError Class = "MODEL_PROVIDER_ERROR"
	ModelOutputInvalid Class = "MODEL_OUTPUT_INVALID"
	ToolTemporary      Class = "TOOL_TEMPORARY"
	ToolAuthRequired   Class = "TOOL_AUTH_REQUIRED"
	ToolUnknownEffect  Class = "TOOL_UNKNOWN_EFFECT"
	WorkerLost         Class = "WORKER_LOST"
	ContextOverflow    Class = "CONTEXT_OVERFLOW"
	NoProgress         Class = "NO_PROGRESS"
	VerificationFailed Class = "VERIFICATION_FAILED"
	PlanInvalid        Class = "PLAN_INVALID"
	CapabilityMismatch Class = "CAPABILITY_MISMATCH"
	BudgetExceeded     Class = "BUDGET_EXCEEDED"
	PolicyViolation    Class = "POLICY_VIOLATION"
	UserRejected       Class = "USER_REJECTED"
)

// Action 是控制器应采取的第一恢复动作。它不是日志文案；业务层可以据此进入
// WAITING_USER、触发 Effect Reconcile、切换模型或停止执行。
type Action string

const (
	ActionBackoffRetry     Action = "BACKOFF_RETRY"
	ActionProviderFallback Action = "PROVIDER_FALLBACK"
	ActionRepairPrompt     Action = "REPAIR_PROMPT"
	ActionToolRetry        Action = "TOOL_RETRY"
	ActionWaitUser         Action = "WAITING_USER"
	ActionReconcile        Action = "RECONCILE_EFFECT"
	ActionResume           Action = "RESUME_CHECKPOINT"
	ActionOffloadContext   Action = "OFFLOAD_CONTEXT"
	ActionSelfCheck        Action = "SELF_CHECK"
	ActionCorrect          Action = "CORRECTION_ATTEMPT"
	ActionReplan           Action = "REPLAN"
	ActionReschedule       Action = "RESCHEDULE"
	ActionPause            Action = "PAUSE_FOR_APPROVAL"
	ActionStop             Action = "STOP"
)

// RetryLevel 明确恢复发生在哪一层，防止一个 Tool 暂时错误升级成整个 Run 重跑，或把
// Plan 错误误当成同一 Step 的可重试错误。
type RetryLevel string

const (
	RetryNone    RetryLevel = "NONE"
	RetryStep    RetryLevel = "STEP"
	RetryAttempt RetryLevel = "ATTEMPT"
	RetryAgent   RetryLevel = "AGENT_SWITCH"
	RetryModel   RetryLevel = "MODEL_SWITCH"
	RetryTask    RetryLevel = "TASK_REPLAN"
	RetryRun     RetryLevel = "RUN_REPLAN"
)

// Decision 是可持久化的分类结果。Retryable 只表示系统可以自动继续；需要用户输入、
// 审批或人工 Reconcile 的动作会返回 false，避免消息队列立即重投造成风暴。
type Decision struct {
	Class      Class      `json:"class"`
	Action     Action     `json:"action"`
	RetryLevel RetryLevel `json:"retryLevel"`
	Retryable  bool       `json:"retryable"`
}

// Hint 是结构化分类提示。应优先由 Tool Gateway、Verification 或预算守卫提供，只有
// 旧适配器没有结构化错误时才回退到文本/网络错误识别。
type Hint struct {
	Class Class
}

// Error 允许下游返回结构化失败，同时保留原始 cause 供日志和 Trace 使用。
type Error struct {
	FailureClass Class
	Cause        error
}

func (e *Error) Error() string {
	if e == nil || e.Cause == nil {
		return string(e.FailureClass)
	}
	return string(e.FailureClass) + ": " + e.Cause.Error()
}

func (e *Error) Unwrap() error { return e.Cause }

// Classify 先读取显式 Class，再识别 context/net 标准错误，最后兼容常见 Provider/MCP
// 错误码。文本识别只作为迁移期兜底，并全部转为有限、可审计的策略。
func Classify(err error, hint Hint) Decision {
	class := hint.Class
	if class == "" {
		var structured *Error
		if errors.As(err, &structured) && structured.FailureClass != "" {
			class = structured.FailureClass
		}
	}
	if class == "" {
		class = infer(err)
	}
	return policy(class)
}

func infer(err error) Class {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return NetworkTemporary
	}
	var networkError net.Error
	if errors.As(err, &networkError) && (networkError.Timeout() || networkError.Temporary()) {
		return NetworkTemporary
	}
	message := strings.ToLower(errorText(err))
	switch {
	case containsAny(message, "429", "rate limit", "too many requests", "resource_exhausted"):
		return RateLimit
	case containsAny(message, "unknown effect", "effect_unknown", "outcome unknown"):
		return ToolUnknownEffect
	case containsAny(message, "credential", "unauthorized", "authentication required", "auth required"):
		return ToolAuthRequired
	case containsAny(message, "context length", "context window", "maximum context", "token limit"):
		return ContextOverflow
	case containsAny(message, "no progress", "repeated state", "stagnation"):
		return NoProgress
	case containsAny(message, "verification failed", "gate rejected", "acceptance failed"):
		return VerificationFailed
	case containsAny(message, "budget exceeded", "quota budget", "cost limit"):
		return BudgetExceeded
	case containsAny(message, "policy denied", "policy violation", "permission denied", "forbidden"):
		return PolicyViolation
	case containsAny(message, "invalid plan", "plan compile", "cycle detected"):
		return PlanInvalid
	case containsAny(message, "capability mismatch", "no eligible agent", "unsupported capability"):
		return CapabilityMismatch
	case containsAny(message, "invalid model output", "schema mismatch", "malformed response"):
		return ModelOutputInvalid
	case containsAny(message, "user rejected", "approval rejected"):
		return UserRejected
	case containsAny(message, "worker lost", "heartbeat expired", "lease expired"):
		return WorkerLost
	case containsAny(message, "tool timeout", "tool temporary", "mcp unavailable"):
		return ToolTemporary
	default:
		return ModelProviderError
	}
}

func policy(class Class) Decision {
	switch class {
	case NetworkTemporary:
		return Decision{class, ActionBackoffRetry, RetryAttempt, true}
	case RateLimit:
		return Decision{class, ActionProviderFallback, RetryModel, true}
	case ModelProviderError:
		return Decision{class, ActionProviderFallback, RetryModel, true}
	case ModelOutputInvalid:
		return Decision{class, ActionRepairPrompt, RetryStep, true}
	case ToolTemporary:
		return Decision{class, ActionToolRetry, RetryStep, true}
	case ToolAuthRequired:
		return Decision{class, ActionWaitUser, RetryNone, false}
	case ToolUnknownEffect:
		return Decision{class, ActionReconcile, RetryNone, false}
	case WorkerLost:
		return Decision{class, ActionResume, RetryAttempt, true}
	case ContextOverflow:
		return Decision{class, ActionOffloadContext, RetryStep, true}
	case NoProgress:
		return Decision{class, ActionSelfCheck, RetryAgent, true}
	case VerificationFailed:
		return Decision{class, ActionCorrect, RetryAttempt, true}
	case PlanInvalid:
		return Decision{class, ActionReplan, RetryTask, true}
	case CapabilityMismatch:
		return Decision{class, ActionReschedule, RetryAgent, true}
	case BudgetExceeded:
		return Decision{class, ActionPause, RetryNone, false}
	case PolicyViolation, UserRejected:
		return Decision{class, ActionStop, RetryNone, false}
	default:
		return Decision{ModelProviderError, ActionProviderFallback, RetryModel, true}
	}
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func containsAny(value string, candidates ...string) bool {
	for _, candidate := range candidates {
		if strings.Contains(value, candidate) {
			return true
		}
	}
	return false
}
