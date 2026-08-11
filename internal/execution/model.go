// Package execution 定义 TaskAttempt、Checkpoint、ToolCall 与 Reviewer 的执行合同。
//
// 该包只包含领域数据和端口，不依赖 PostgreSQL、NATS 或具体模型 SDK，便于用内存实现
// 做确定性测试，也让 Worker Runtime 可以替换执行器而不改变控制面状态机。
package execution

import (
	"context"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/domain/agent"
	"github.com/licy-yu/agent-os/internal/domain/task"
)

// AttemptStatus 是一次物理执行的状态；与可重试的逻辑 Task 生命周期分离。
type AttemptStatus string

const (
	AttemptCreated AttemptStatus = "CREATED"
	AttemptRunning AttemptStatus = "RUNNING"
	// AttemptWaiting 表示执行权已经主动归还，等待审批或外部对账。
	// 它不是失败，也不应被心跳超时回收；恢复时沿用同一 Attempt，并签发新的 fencing token。
	AttemptWaiting   AttemptStatus = "WAITING"
	AttemptReview    AttemptStatus = "REVIEW"
	AttemptSucceeded AttemptStatus = "SUCCEEDED"
	AttemptFailed    AttemptStatus = "FAILED"
	AttemptAborted   AttemptStatus = "ABORTED"
)

// AttemptWaitReason 区分“等人批准”和“等外部系统确认”。持久化层据此选择
// Task 的 WAITING_APPROVAL/WAITING_EXTERNAL 状态，避免用一条模糊的 RUNNING
// 记录长时间占住 Worker 与 JetStream 投递次数。
type AttemptWaitReason string

const (
	WaitForApproval AttemptWaitReason = "APPROVAL"
	WaitForExternal AttemptWaitReason = "EXTERNAL"
)

func (r AttemptWaitReason) Valid() bool {
	return r == WaitForApproval || r == WaitForExternal
}

// AttemptWait 精确指向触发等待的 Effect。禁止按“最新 Effect”猜测，因为同一 Attempt
// 可能已产生多条副作用；猜错会把一条审批结论应用到另一条外部操作。
type AttemptWait struct {
	Reason   AttemptWaitReason
	EffectID uuid.UUID
}

func (w AttemptWait) Valid() bool { return w.Reason.Valid() && w.EffectID != uuid.Nil }

// Attempt 保存一次执行所需的最小审计信息。Input/Output 是执行当时的不可变快照。
type Attempt struct {
	ID             uuid.UUID
	TaskID         uuid.UUID
	AgentID        uuid.UUID
	Number         int32
	Status         AttemptStatus
	InputSnapshot  map[string]any
	OutputSnapshot map[string]any
	Model          string
	PromptVersion  string
	WorkerID       string
	// FencingToken 是数据库签发的单调所有权凭据。WorkerID 只标识执行者名称，
	// token 才能阻止已经失去所有权的旧进程继续提交 Step、Tool 或最终结果。
	FencingToken int64
	// ResumeCheckpointID 指向前一个 Attempt 的恢复点；它只作为只读输入，
	// 新 Attempt 仍拥有独立的 ID、worker 和 fence。
	ResumeCheckpointID *uuid.UUID
	StartedAt          time.Time
	FinishedAt         *time.Time
	HeartbeatAt        time.Time
	TokensIn           int64
	TokensOut          int64
	CostMicros         int64
	StepCount          int32
	ToolCallCount      int32
	NoProgressRounds   int32
	ErrorCode          string
	ErrorMessage       string
}

// AttemptOwner 是每次有状态写操作必须携带的执行所有权证明。
// AttemptID、WorkerID 和 FencingToken 必须作为整体校验，不能只检查其中一项。
type AttemptOwner struct {
	AttemptID    uuid.UUID
	WorkerID     string
	FencingToken int64
}

// Owner 返回当前 Attempt 的不可变所有权快照，供 Worker、CheckpointWriter 和
// Tool Gateway 贯穿传递同一个 fencing token。
func (a Attempt) Owner() AttemptOwner {
	return AttemptOwner{AttemptID: a.ID, WorkerID: a.WorkerID, FencingToken: a.FencingToken}
}

// OwnedBy 用完整三元组判断一次数据库写入是否仍属于当前 Attempt。
func (a Attempt) OwnedBy(owner AttemptOwner) bool {
	return owner.Valid() && a.ID == owner.AttemptID && a.WorkerID == owner.WorkerID &&
		a.FencingToken == owner.FencingToken
}

// CanReplay 只允许签发该活跃 Attempt 的同一 Worker 消费重复 assignment 消息。
// 不同 Worker 即使知道 Attempt ID 也不能热接管；接管必须先终止旧 Attempt 再创建新 Attempt。
func (a Attempt) CanReplay(workerID string) bool {
	return a.Status == AttemptRunning && a.FencingToken > 0 &&
		a.WorkerID == strings.TrimSpace(workerID)
}

// Valid 在进入数据库前拒绝零值所有权。fence 从 1 开始，0 仅用于迁移旧数据，
// 不能授权新的执行写操作。
func (o AttemptOwner) Valid() bool {
	return o.AttemptID != uuid.Nil && strings.TrimSpace(o.WorkerID) != "" && o.FencingToken > 0
}

// Work 是 Worker 领取后的完整、版本固定的执行上下文。
// 模板提示词和模型随 Attempt 一起快照化，后续修改模板不会改变正在运行的任务。
type Work struct {
	Task     *task.Task
	Agent    *agent.Instance
	Template *agent.Template
	Attempt  *Attempt
	// LatestCheckpoint 是 ClaimWork 在同一事务快照中读取的最近恢复点。
	// 它既可能属于同一 Attempt（至少一次消息重放），也可能属于上一 Attempt（失败恢复）；
	// 没有任何已持久化恢复边界时才为 nil。
	LatestCheckpoint *Checkpoint
}

// Checkpoint 是 Agent Loop 的可恢复边界。State 必须只含可 JSON 序列化的数据。
type Checkpoint struct {
	ID           uuid.UUID
	AttemptID    uuid.UUID
	Sequence     int32
	StepName     string
	State        map[string]any
	ArtifactRefs []string
	FencingToken int64
	CreatedAt    time.Time
}

// StepType 是 task_steps 表允许的封闭类型集合。
type StepType string

const (
	StepPlan       StepType = "PLAN"
	StepModel      StepType = "MODEL"
	StepTool       StepType = "TOOL"
	StepObserve    StepType = "OBSERVE"
	StepWrite      StepType = "WRITE"
	StepVerify     StepType = "VERIFY"
	StepCheckpoint StepType = "CHECKPOINT"
	StepHandoff    StepType = "HANDOFF"
	StepHuman      StepType = "HUMAN"
)

// StepStatus 是单个 typed Step 的执行状态。
type StepStatus string

const (
	StepPending   StepStatus = "PENDING"
	StepRunning   StepStatus = "RUNNING"
	StepWaiting   StepStatus = "WAITING"
	StepSucceeded StepStatus = "SUCCEEDED"
	StepFailed    StepStatus = "FAILED"
	StepSkipped   StepStatus = "SKIPPED"
	StepCanceled  StepStatus = "CANCELED"
)

// TaskStep 是 Attempt 内不可覆盖的最小审计单元。当前阶段在每次 Checkpoint 时创建
// SUCCEEDED/CHECKPOINT Step；后续 Eino Runtime 可复用同一类型记录 MODEL、TOOL 等步骤。
type TaskStep struct {
	ID           uuid.UUID
	TenantID     uuid.UUID
	RunID        uuid.UUID
	TaskID       uuid.UUID
	AttemptID    uuid.UUID
	Sequence     int32
	Type         StepType
	Name         string
	Status       StepStatus
	Input        map[string]any
	Output       map[string]any
	Usage        map[string]any
	ModelCallID  string
	ToolCallID   *uuid.UUID
	CheckpointID *uuid.UUID
	ErrorCode    string
	ErrorMessage string
	StartedAt    *time.Time
	FinishedAt   *time.Time
	CreatedAt    time.Time
}

// ExecutionResult 是执行器交给 Reviewer 的证据，而不是直接宣告任务成功。
type ExecutionResult struct {
	Output           map[string]any
	Checks           map[string]bool
	PolicyViolations []string
	QualityScore     float64
	TokensIn         int64
	TokensOut        int64
	CostMicros       int64
}

// ReviewDecision 明确区分通过、可恢复失败和不可恢复拒绝。
type ReviewDecision string

const (
	DecisionAccept ReviewDecision = "ACCEPT"
	DecisionRetry  ReviewDecision = "RETRY"
	DecisionReject ReviewDecision = "REJECT"
)

// Evaluation 保存 Reviewer 的四类证据，便于事后解释自动决策。
type Evaluation struct {
	ID           uuid.UUID
	TaskID       uuid.UUID
	AttemptID    uuid.UUID
	Reviewer     string
	MachinePass  bool
	PolicyPass   bool
	QualityScore float64
	Decision     ReviewDecision
	Findings     []string
	CreatedAt    time.Time
}

// Store 聚合 Worker、Reviewer 与 Recovery 所需的原子持久化操作。
// ClaimWork 和 ApplyReview 是状态机边界，必须在单个数据库事务内同时写入 Outbox。
type Store interface {
	ClaimWork(context.Context, uuid.UUID, uuid.UUID, string) (*Work, error)
	SuspendAttempt(context.Context, AttemptOwner, AttemptWait) error
	SaveCheckpoint(context.Context, AttemptOwner, Checkpoint) error
	HeartbeatAttempt(context.Context, AttemptOwner) error
	CompleteAttempt(context.Context, AttemptOwner, ExecutionResult) error
	ListReviewWork(context.Context, int) ([]*Work, error)
	ApplyReview(context.Context, *Work, Evaluation, time.Time) error
	// ExpireWaitingInteractions 收敛已超过 expires_at 的持久化人工等待。实现必须使用
	// 数据库行锁与 CAS，让自动过期和并发人工审批至多只有一方成功。
	ExpireWaitingInteractions(context.Context, time.Time, int) (int, error)
	RecoverTimedOut(context.Context, time.Time, int) (int, error)
}

// Executor 是可替换的 Agent 执行后端。生产可使用 OpenAI，测试使用确定性执行器。
type Executor interface {
	Execute(context.Context, *Work, CheckpointWriter, ToolCaller) (ExecutionResult, error)
}

// CheckpointWriter 让执行器在关键步骤持久化状态，但不直接接触数据库实现。
type CheckpointWriter interface {
	Save(context.Context, string, map[string]any, []string) error
}

// ToolCaller 是模型与外部系统之间的唯一出口；所有调用都必须经过策略和审计。
type ToolCaller interface {
	Call(context.Context, string, map[string]any) (map[string]any, error)
}
