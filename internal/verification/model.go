// Package verification 定义硬验收门、验收证据和完成清单。
//
// Verification 只回答“是否满足任务合同中的硬条件”。它不等同于可包含 LLM 的质量 Reviewer，
// 也不等同于用于统计和进化的 Evaluation。只有 CompletionManifest 对全部必需 Gate 给出可审计
// 的 PASS 结果，任务才可以进入成功状态。
package verification

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// GateStatus 是单个 Acceptance Gate 的确定性执行结果。
type GateStatus string

const (
	GatePending GateStatus = "PENDING"
	GatePass    GateStatus = "PASS"
	GateFail    GateStatus = "FAIL"
	GateError   GateStatus = "ERROR"
	GateSkipped GateStatus = "SKIPPED"
)

// ManifestStatus 是完成清单的最终判定。
type ManifestStatus string

const (
	ManifestAccepted   ManifestStatus = "ACCEPTED"
	ManifestRejected   ManifestStatus = "REJECTED"
	ManifestIncomplete ManifestStatus = "INCOMPLETE"
	ManifestInvalid    ManifestStatus = "INVALID"
)

// GateInput 是 AcceptanceGate 的协议无关输入。ArtifactRefs 指向受版本和哈希保护的产物，
// Values 只保存小型结构化参数，大日志或文件必须先 offload 为 Artifact。
type GateInput struct {
	TaskID       string
	AttemptID    string
	ArtifactRefs []string
	Values       map[string]any
}

// GateResult 是一次 Gate 执行的持久化证据。EvidenceRefs 可引用测试报告、安全报告或人工审批，
// Metrics 保存 coverage 等机器可比较数值。
type GateResult struct {
	GateName     string
	Status       GateStatus
	EvidenceRefs []string
	Metrics      map[string]float64
	Message      string
}

// AcceptanceGate 隔离具体构建、测试、安全扫描和人工审批实现。Gate 实现只产生证据，不能直接
// 修改 Task 状态；最终状态由编排层根据 CompletionManifest.Decide 的结果推进。
type AcceptanceGate interface {
	Name() string
	Verify(context.Context, GateInput) (GateResult, error)
}

// GateSpec 固化任务要求的 Gate 名称及是否为硬条件。可选 Gate 的失败仍会留在清单中供 Reviewer
// 使用，但不会单独阻止任务完成。
type GateSpec struct {
	Name     string
	Required bool
}

// CompletionManifest 是任务声明完成时提交的不可变摘要。Artifacts 与 Evidence 应保存 URI 或 ID，
// 不直接嵌入大对象；KnownIssues 和 Assumptions 用于向用户显式披露残余风险。
type CompletionManifest struct {
	Artifacts   []string
	Evidence    []GateResult
	KnownIssues []string
	Assumptions []string
}

// ManifestDecision 保存可解释的验收结论。调用方可直接向控制台展示缺失、失败和非法项，
// 不需要再次从自然语言中推断失败原因。
type ManifestDecision struct {
	Status         ManifestStatus
	MissingGates   []string
	FailedGates    []string
	ErrorGates     []string
	InvalidReasons []string
}

// Accepted 报告全部硬验收条件是否已通过。
func (d ManifestDecision) Accepted() bool { return d.Status == ManifestAccepted }

// Decide 对完成清单进行纯确定性判定。优先级依次为 INVALID、REJECTED、INCOMPLETE、ACCEPTED：
// 协议本身有歧义时不能伪装成普通失败；已明确 FAIL/ERROR 时也不能被缺失证据掩盖。
func (m CompletionManifest) Decide(specs []GateSpec) ManifestDecision {
	decision := ManifestDecision{}
	required := make(map[string]string)
	allSpecs := make(map[string]struct{})
	for _, spec := range specs {
		name := strings.TrimSpace(spec.Name)
		key := strings.ToLower(name)
		switch {
		case name == "":
			decision.InvalidReasons = append(decision.InvalidReasons, "GateSpec.name 不能为空")
		case hasKey(allSpecs, key):
			decision.InvalidReasons = append(decision.InvalidReasons, fmt.Sprintf("GateSpec %q 重复", name))
		default:
			allSpecs[key] = struct{}{}
			if spec.Required {
				required[key] = name
			}
		}
	}

	results := make(map[string]GateResult)
	for _, result := range m.Evidence {
		name := strings.TrimSpace(result.GateName)
		key := strings.ToLower(name)
		switch {
		case name == "":
			decision.InvalidReasons = append(decision.InvalidReasons, "GateResult.gate_name 不能为空")
		case !result.Status.Valid():
			decision.InvalidReasons = append(decision.InvalidReasons,
				fmt.Sprintf("GateResult %q 状态 %q 非法", name, result.Status))
		case hasResult(results, key):
			decision.InvalidReasons = append(decision.InvalidReasons,
				fmt.Sprintf("GateResult %q 重复", name))
		default:
			results[key] = result
		}
	}

	for key, displayName := range required {
		result, exists := results[key]
		if !exists || result.Status == GatePending || result.Status == GateSkipped {
			decision.MissingGates = append(decision.MissingGates, displayName)
			continue
		}
		switch result.Status {
		case GateFail:
			decision.FailedGates = append(decision.FailedGates, displayName)
		case GateError:
			decision.ErrorGates = append(decision.ErrorGates, displayName)
		}
	}

	// Map 遍历顺序不稳定，排序保证 API、日志和测试输出可重复。
	sort.Strings(decision.MissingGates)
	sort.Strings(decision.FailedGates)
	sort.Strings(decision.ErrorGates)
	sort.Strings(decision.InvalidReasons)
	switch {
	case len(decision.InvalidReasons) > 0:
		decision.Status = ManifestInvalid
	case len(decision.FailedGates) > 0 || len(decision.ErrorGates) > 0:
		decision.Status = ManifestRejected
	case len(decision.MissingGates) > 0:
		decision.Status = ManifestIncomplete
	default:
		decision.Status = ManifestAccepted
	}
	return decision
}

// Valid 报告 Gate 状态是否属于当前验收协议。
func (s GateStatus) Valid() bool {
	switch s {
	case GatePending, GatePass, GateFail, GateError, GateSkipped:
		return true
	default:
		return false
	}
}

func hasKey(values map[string]struct{}, key string) bool {
	_, ok := values[key]
	return ok
}

func hasResult(values map[string]GateResult, key string) bool {
	_, ok := values[key]
	return ok
}
