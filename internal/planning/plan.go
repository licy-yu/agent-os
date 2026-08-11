package planning

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// DependencyType 表达 Task 之间不同的就绪条件；当前 Compiler 负责图结构校验，
// DATA/APPROVAL/TEMPORAL 的运行时满足条件由 Task Engine 判断。
type DependencyType string

const (
	DependencyHard     DependencyType = "HARD"
	DependencySoft     DependencyType = "SOFT"
	DependencyData     DependencyType = "DATA"
	DependencyApproval DependencyType = "APPROVAL"
	DependencyTemporal DependencyType = "TEMPORAL"
)

func (d DependencyType) valid() bool {
	switch d {
	case DependencyHard, DependencySoft, DependencyData, DependencyApproval, DependencyTemporal:
		return true
	default:
		return false
	}
}

// Dependency 是一条“TaskID 等待 DependsOnID”的有向边。
type Dependency struct {
	TaskID      uuid.UUID      `json:"task_id"`
	DependsOnID uuid.UUID      `json:"depends_on_id"`
	Type        DependencyType `json:"type"`
	Artifact    string         `json:"artifact,omitempty"`
}

// Deliverable 把用户要求的最终交付物映射到负责生产它的 Task。Compiler 通过该映射
// 判断 Task 是否处于通向最终结果的路径上，而不是武断地把所有并行根任务视为孤儿。
type Deliverable struct {
	Name   string    `json:"name"`
	Type   string    `json:"type"`
	TaskID uuid.UUID `json:"task_id"`
}

// PlanCandidate 是 Planner 的输出，尚不可信，必须经过 PlanCompiler 才可执行。
type PlanCandidate struct {
	Tasks           []TaskContract `json:"tasks"`
	Dependencies    []Dependency   `json:"dependencies"`
	Deliverables    []Deliverable  `json:"deliverables"`
	Assumptions     []string       `json:"assumptions,omitempty"`
	Risks           []string       `json:"risks,omitempty"`
	Unknowns        []string       `json:"unknowns,omitempty"`
	EstimatedBudget ResourceBudget `json:"estimated_budget"`
}

// PlanStatus 是 PlanVersion 的发布生命周期。
type PlanStatus string

const (
	PlanDraft      PlanStatus = "DRAFT"
	PlanValidating PlanStatus = "VALIDATING"
	// PlanValid 表示确定性 Compiler 已通过、但尚未被 RunController 原子激活。
	// 将“校验通过”和“成为当前版本”拆开，可避免数据库事务失败时出现两个 ACTIVE 版本。
	PlanValid      PlanStatus = "VALID"
	PlanActive     PlanStatus = "ACTIVE"
	PlanSuperseded PlanStatus = "SUPERSEDED"
	PlanRejected   PlanStatus = "REJECTED"
)

func (s PlanStatus) valid() bool {
	switch s {
	case PlanDraft, PlanValidating, PlanValid, PlanActive, PlanSuperseded, PlanRejected:
		return true
	default:
		return false
	}
}

var planStatusTransitions = map[PlanStatus]map[PlanStatus]bool{
	PlanDraft:      {PlanValidating: true, PlanRejected: true},
	PlanValidating: {PlanValid: true, PlanRejected: true},
	PlanValid:      {PlanActive: true, PlanRejected: true},
	PlanActive:     {PlanSuperseded: true},
}

// PlanVersionParams 是构造不可变 PlanVersion 所需的完整输入。ID 必须由调用方提供，
// 避免在 Temporal Workflow 的确定性代码路径中隐式生成随机数。
type PlanVersionParams struct {
	ID              uuid.UUID
	RunID           uuid.UUID
	Version         int32
	ParentVersionID *uuid.UUID
	Status          PlanStatus
	Reason          string
	PlannerVersion  string
	Candidate       PlanCandidate
	CreatedAt       time.Time
}

// PlanVersion 的字段全部私有。构造时深拷贝 Candidate，读取时再返回深拷贝；调用方无法
// 原地修改已持久化计划内容。状态发布也通过 WithStatus 返回新值。
type PlanVersion struct {
	id              uuid.UUID
	runID           uuid.UUID
	version         int32
	parentVersionID *uuid.UUID
	status          PlanStatus
	reason          string
	plannerVersion  string
	graphHash       string
	candidate       PlanCandidate
	createdAt       time.Time
}

// NewPlanVersion 验证版本链元数据并创建不可变快照。候选计划允许暂时不合法，
// 因为 DRAFT 正是 Plan Repair Loop 的输入；是否可执行由 PlanCompiler 单独裁决。
func NewPlanVersion(params PlanVersionParams) (*PlanVersion, error) {
	if params.ID == uuid.Nil || params.RunID == uuid.Nil {
		return nil, fmt.Errorf("plan version id 和 run id 不能为空")
	}
	if params.Version < 1 {
		return nil, fmt.Errorf("plan version 必须大于等于 1")
	}
	if params.Version == 1 && params.ParentVersionID != nil {
		return nil, fmt.Errorf("首个 plan version 不能声明 parent")
	}
	if params.Version > 1 && (params.ParentVersionID == nil || *params.ParentVersionID == uuid.Nil) {
		return nil, fmt.Errorf("非首个 plan version 必须声明 parent")
	}
	if params.ParentVersionID != nil && *params.ParentVersionID == params.ID {
		return nil, fmt.Errorf("plan version 不能以自身为 parent")
	}
	if !params.Status.valid() {
		return nil, fmt.Errorf("未知 plan status %q", params.Status)
	}
	if strings.TrimSpace(params.PlannerVersion) == "" {
		return nil, fmt.Errorf("planner version 不能为空")
	}
	if params.CreatedAt.IsZero() {
		return nil, fmt.Errorf("created_at 不能为空")
	}
	candidate, err := cloneCandidate(params.Candidate)
	if err != nil {
		return nil, fmt.Errorf("plan candidate 必须可 JSON 序列化: %w", err)
	}
	graphHash, err := hashCandidate(candidate)
	if err != nil {
		return nil, err
	}
	return &PlanVersion{
		id: params.ID, runID: params.RunID, version: params.Version,
		parentVersionID: cloneUUIDPointer(params.ParentVersionID), status: params.Status,
		reason: params.Reason, plannerVersion: params.PlannerVersion,
		graphHash: graphHash, candidate: candidate, createdAt: params.CreatedAt,
	}, nil
}

// WithStatus 返回状态变化后的新快照，原 PlanVersion 永远不被修改。
func (p *PlanVersion) WithStatus(next PlanStatus, reason string) (*PlanVersion, error) {
	if p == nil {
		return nil, fmt.Errorf("plan version 不能为空")
	}
	if !planStatusTransitions[p.status][next] {
		return nil, fmt.Errorf("plan version %s 不能从 %s 变为 %s", p.id, p.status, next)
	}
	copyValue := *p
	copyValue.status = next
	copyValue.reason = reason
	copyValue.parentVersionID = cloneUUIDPointer(p.parentVersionID)
	copyValue.candidate, _ = cloneCandidate(p.candidate)
	return &copyValue, nil
}

func (p *PlanVersion) ID() uuid.UUID               { return p.id }
func (p *PlanVersion) RunID() uuid.UUID            { return p.runID }
func (p *PlanVersion) Version() int32              { return p.version }
func (p *PlanVersion) Status() PlanStatus          { return p.status }
func (p *PlanVersion) Reason() string              { return p.reason }
func (p *PlanVersion) PlannerVersion() string      { return p.plannerVersion }
func (p *PlanVersion) GraphHash() string           { return p.graphHash }
func (p *PlanVersion) CreatedAt() time.Time        { return p.createdAt }
func (p *PlanVersion) ParentVersionID() *uuid.UUID { return cloneUUIDPointer(p.parentVersionID) }

// Candidate 返回深拷贝，调用者修改返回值不会改变版本快照或 GraphHash。
func (p *PlanVersion) Candidate() PlanCandidate {
	if p == nil {
		return PlanCandidate{}
	}
	value, _ := cloneCandidate(p.candidate)
	return value
}

func cloneUUIDPointer(value *uuid.UUID) *uuid.UUID {
	if value == nil {
		return nil
	}
	copyValue := *value
	return &copyValue
}

// cloneCandidate 使用 JSON 作为领域快照边界。计划最终要持久化为 JSONB，因此在构造
// PlanVersion 时尽早拒绝 channel/function 等不可持久化对象，比运行中失败更安全。
func cloneCandidate(value PlanCandidate) (PlanCandidate, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return PlanCandidate{}, err
	}
	var result PlanCandidate
	if err := json.Unmarshal(raw, &result); err != nil {
		return PlanCandidate{}, err
	}
	return result, nil
}

// hashCandidate 对无语义顺序的集合做稳定排序，再计算 SHA-256。相同计划即使由 Planner
// 以不同 Task/Dependency 顺序输出，也会得到同一个 hash。
func hashCandidate(value PlanCandidate) (string, error) {
	canonical, err := cloneCandidate(value)
	if err != nil {
		return "", fmt.Errorf("复制 plan candidate: %w", err)
	}
	sort.SliceStable(canonical.Tasks, func(i, j int) bool {
		return canonical.Tasks[i].ID.String() < canonical.Tasks[j].ID.String()
	})
	for index := range canonical.Tasks {
		canonicalizeTask(&canonical.Tasks[index])
	}
	sort.SliceStable(canonical.Dependencies, func(i, j int) bool {
		return dependencyKey(canonical.Dependencies[i]) < dependencyKey(canonical.Dependencies[j])
	})
	sort.SliceStable(canonical.Deliverables, func(i, j int) bool {
		left := canonical.Deliverables[i].TaskID.String() + "\x00" + canonical.Deliverables[i].Name
		right := canonical.Deliverables[j].TaskID.String() + "\x00" + canonical.Deliverables[j].Name
		return left < right
	})
	sort.Strings(canonical.Assumptions)
	sort.Strings(canonical.Risks)
	sort.Strings(canonical.Unknowns)
	raw, err := json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("序列化 canonical plan: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func canonicalizeTask(value *TaskContract) {
	sort.SliceStable(value.Inputs, func(i, j int) bool { return value.Inputs[i].Name < value.Inputs[j].Name })
	sort.SliceStable(value.Output.Artifacts, func(i, j int) bool {
		return value.Output.Artifacts[i].Name < value.Output.Artifacts[j].Name
	})
	sort.Strings(value.Requirements.Tools)
	sort.Strings(value.Requirements.Permissions)
	sort.Strings(value.Requirements.Models)
	sort.Strings(value.SideEffectPolicy.AllowedEffects)
	sort.SliceStable(value.Acceptance, func(i, j int) bool {
		return value.Acceptance[i].Name < value.Acceptance[j].Name
	})
}

func dependencyKey(value Dependency) string {
	return value.TaskID.String() + "\x00" + value.DependsOnID.String() + "\x00" + string(value.Type) + "\x00" + value.Artifact
}
