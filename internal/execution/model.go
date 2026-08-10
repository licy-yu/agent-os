// Package execution 定义 TaskAttempt、Checkpoint、ToolCall 与 Reviewer 的执行合同。
//
// 该包只包含领域数据和端口，不依赖 PostgreSQL、NATS 或具体模型 SDK，便于用内存实现
// 做确定性测试，也让 Worker Runtime 可以替换执行器而不改变控制面状态机。
package execution

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/domain/agent"
	"github.com/licy-yu/agent-os/internal/domain/task"
)

// AttemptStatus 是一次物理执行的状态；与可重试的逻辑 Task 生命周期分离。
type AttemptStatus string

const (
	AttemptCreated   AttemptStatus = "CREATED"
	AttemptRunning   AttemptStatus = "RUNNING"
	AttemptReview    AttemptStatus = "REVIEW"
	AttemptSucceeded AttemptStatus = "SUCCEEDED"
	AttemptFailed    AttemptStatus = "FAILED"
	AttemptAborted   AttemptStatus = "ABORTED"
)

// Attempt 保存一次执行所需的最小审计信息。Input/Output 是执行当时的不可变快照。
type Attempt struct {
	ID               uuid.UUID
	TaskID           uuid.UUID
	AgentID          uuid.UUID
	Number           int32
	Status           AttemptStatus
	InputSnapshot    map[string]any
	OutputSnapshot   map[string]any
	Model            string
	PromptVersion    string
	WorkerID         string
	StartedAt        time.Time
	FinishedAt       *time.Time
	HeartbeatAt      time.Time
	TokensIn         int64
	TokensOut        int64
	CostMicros       int64
	StepCount        int32
	ToolCallCount    int32
	NoProgressRounds int32
	ErrorCode        string
	ErrorMessage     string
}

// Work 是 Worker 领取后的完整、版本固定的执行上下文。
// 模板提示词和模型随 Attempt 一起快照化，后续修改模板不会改变正在运行的任务。
type Work struct {
	Task     *task.Task
	Agent    *agent.Instance
	Template *agent.Template
	Attempt  *Attempt
}

// Checkpoint 是 Agent Loop 的可恢复边界。State 必须只含可 JSON 序列化的数据。
type Checkpoint struct {
	ID           uuid.UUID
	AttemptID    uuid.UUID
	Sequence     int32
	StepName     string
	State        map[string]any
	ArtifactRefs []string
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
	SaveCheckpoint(context.Context, Checkpoint) error
	HeartbeatAttempt(context.Context, uuid.UUID, string) error
	CompleteAttempt(context.Context, uuid.UUID, ExecutionResult) error
	ListReviewWork(context.Context, int) ([]*Work, error)
	ApplyReview(context.Context, *Work, Evaluation, time.Time) error
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
