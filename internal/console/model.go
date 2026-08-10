// Package console 提供只读运维视图，避免 Web Console 直接拼接领域表。
package console

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Overview 是蜂群运行态摘要。状态计数使用 map，新增状态时前端无需升级协议。
type Overview struct {
	SwarmID          uuid.UUID        `json:"swarm_id"`
	TaskStatuses     map[string]int64 `json:"task_statuses"`
	AgentStatuses    map[string]int64 `json:"agent_statuses"`
	TotalAttempts    int64            `json:"total_attempts"`
	RunningAttempts  int64            `json:"running_attempts"`
	PendingOutbox    int64            `json:"pending_outbox"`
	SpentTokens      int64            `json:"spent_tokens"`
	BudgetTokens     int64            `json:"budget_tokens"`
	SpentCostMicros  int64            `json:"spent_cost_micros"`
	BudgetCostMicros int64            `json:"budget_cost_micros"`
	UpdatedAt        time.Time        `json:"updated_at"`
}

// EvaluationView 是 Reviewer 对某次 Attempt 的最终裁决证据。
type EvaluationView struct {
	Reviewer     string    `json:"reviewer"`
	MachinePass  bool      `json:"machine_pass"`
	PolicyPass   bool      `json:"policy_pass"`
	QualityScore float64   `json:"quality_score"`
	Decision     string    `json:"decision"`
	Findings     []string  `json:"findings"`
	CreatedAt    time.Time `json:"created_at"`
}

// AttemptView 聚合执行、检查点和工具审计数量，详情输出仍保留结构化 JSON。
type AttemptView struct {
	ID             uuid.UUID        `json:"id"`
	TaskID         uuid.UUID        `json:"task_id"`
	AgentID        uuid.UUID        `json:"agent_id"`
	AttemptNo      int32            `json:"attempt_no"`
	Status         string           `json:"status"`
	Model          string           `json:"model"`
	WorkerID       string           `json:"worker_id"`
	Output         map[string]any   `json:"output"`
	TokensIn       int64            `json:"tokens_in"`
	TokensOut      int64            `json:"tokens_out"`
	CostMicros     int64            `json:"cost_micros"`
	StepCount      int32            `json:"step_count"`
	ToolCallCount  int32            `json:"tool_call_count"`
	StartedAt      *time.Time       `json:"started_at,omitempty"`
	FinishedAt     *time.Time       `json:"finished_at,omitempty"`
	HeartbeatAt    *time.Time       `json:"heartbeat_at,omitempty"`
	ErrorCode      string           `json:"error_code,omitempty"`
	ErrorMessage   string           `json:"error_message,omitempty"`
	Evaluation     *EvaluationView  `json:"evaluation,omitempty"`
	CheckpointList []CheckpointView `json:"checkpoints"`
	ToolCalls      []ToolCallView   `json:"tool_calls"`
}

type CheckpointView struct {
	Sequence  int32          `json:"sequence"`
	StepName  string         `json:"step_name"`
	State     map[string]any `json:"state"`
	CreatedAt time.Time      `json:"created_at"`
}

type ToolCallView struct {
	ID           uuid.UUID      `json:"id"`
	ToolName     string         `json:"tool_name"`
	Status       string         `json:"status"`
	RiskLevel    string         `json:"risk_level"`
	Arguments    map[string]any `json:"arguments"`
	Result       map[string]any `json:"result"`
	ErrorMessage string         `json:"error_message,omitempty"`
	StartedAt    time.Time      `json:"started_at"`
	FinishedAt   *time.Time     `json:"finished_at,omitempty"`
}

// EventView 是 Transactional Outbox 的可观察视图，包含尚未发布和已发布事件。
type EventView struct {
	ID               uuid.UUID      `json:"id"`
	AggregateType    string         `json:"aggregate_type"`
	AggregateID      uuid.UUID      `json:"aggregate_id"`
	EventType        string         `json:"event_type"`
	AggregateVersion int64          `json:"aggregate_version"`
	Payload          map[string]any `json:"payload"`
	Attempts         int32          `json:"attempts"`
	PublishedAt      *time.Time     `json:"published_at,omitempty"`
	LastError        string         `json:"last_error,omitempty"`
	CreatedAt        time.Time      `json:"created_at"`
}

type Store interface {
	ConsoleOverview(context.Context, uuid.UUID) (*Overview, error)
	ListConsoleAttempts(context.Context, uuid.UUID, int) ([]AttemptView, error)
	ListConsoleEvents(context.Context, uuid.UUID, int) ([]EventView, error)
}

// Service 是轻量只读用例层，集中限制单次查询规模。
type Service struct{ store Store }

func NewService(store Store) *Service { return &Service{store: store} }

func (s *Service) Overview(ctx context.Context, swarmID uuid.UUID) (*Overview, error) {
	return s.store.ConsoleOverview(ctx, swarmID)
}

func (s *Service) Attempts(ctx context.Context, taskID uuid.UUID) ([]AttemptView, error) {
	return s.store.ListConsoleAttempts(ctx, taskID, 20)
}

func (s *Service) Events(ctx context.Context, swarmID uuid.UUID) ([]EventView, error) {
	return s.store.ListConsoleEvents(ctx, swarmID, 200)
}
