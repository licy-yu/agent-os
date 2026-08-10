package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	consoleview "github.com/licy-yu/agent-os/internal/console"
)

// ConsoleOverview 用少量聚合查询构造 dashboard 摘要，不把高基数明细塞入 metrics 标签。
func (r *Repository) ConsoleOverview(ctx context.Context, swarmID uuid.UUID) (*consoleview.Overview, error) {
	value := &consoleview.Overview{
		SwarmID: swarmID, TaskStatuses: map[string]int64{}, AgentStatuses: map[string]int64{},
		UpdatedAt: time.Now().UTC(),
	}
	err := r.pool.QueryRow(ctx, `
		SELECT spent_tokens,budget_tokens,spent_cost_micros,budget_cost_micros
		FROM swarms WHERE id=$1`, swarmID).Scan(
		&value.SpentTokens, &value.BudgetTokens, &value.SpentCostMicros, &value.BudgetCostMicros)
	if err != nil {
		return nil, mapReadError("读取 console swarm overview", err)
	}
	rows, err := r.pool.Query(ctx, `SELECT status,count(*) FROM tasks WHERE swarm_id=$1 GROUP BY status`, swarmID)
	if err != nil {
		return nil, fmt.Errorf("聚合 task statuses: %w", err)
	}
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			rows.Close()
			return nil, err
		}
		value.TaskStatuses[status] = count
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	rows, err = r.pool.Query(ctx, `SELECT status,count(*) FROM agent_instances WHERE swarm_id=$1 GROUP BY status`, swarmID)
	if err != nil {
		return nil, fmt.Errorf("聚合 agent statuses: %w", err)
	}
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			rows.Close()
			return nil, err
		}
		value.AgentStatuses[status] = count
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	if err := r.pool.QueryRow(ctx, `
		SELECT count(*),count(*) FILTER (WHERE a.status='RUNNING')
		FROM task_attempts a JOIN tasks t ON t.id=a.task_id WHERE t.swarm_id=$1`, swarmID).Scan(
		&value.TotalAttempts, &value.RunningAttempts); err != nil {
		return nil, fmt.Errorf("聚合 attempts: %w", err)
	}
	if err := r.pool.QueryRow(ctx, `
		SELECT count(*) FROM event_outbox e
		WHERE e.published_at IS NULL AND (
			e.aggregate_id=$1 OR
			e.aggregate_id IN (SELECT id FROM tasks WHERE swarm_id=$1) OR
			e.aggregate_id IN (SELECT id FROM agent_instances WHERE swarm_id=$1) OR
			e.aggregate_id IN (SELECT a.id FROM task_attempts a JOIN tasks t ON t.id=a.task_id WHERE t.swarm_id=$1) OR
			e.aggregate_id IN (SELECT v.id FROM evaluations v JOIN tasks t ON t.id=v.task_id WHERE t.swarm_id=$1)
		)`, swarmID).Scan(&value.PendingOutbox); err != nil {
		return nil, fmt.Errorf("聚合 pending outbox: %w", err)
	}
	return value, nil
}

func (r *Repository) ListConsoleAttempts(ctx context.Context, taskID uuid.UUID, limit int) ([]consoleview.AttemptView, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT a.id,a.task_id,a.agent_id,a.attempt_no,a.status,COALESCE(a.model,''),COALESCE(a.worker_id,''),
		       a.output_snapshot,a.tokens_in,a.tokens_out,a.cost_micros,a.step_count,a.tool_call_count,
		       a.started_at,a.finished_at,a.heartbeat_at,COALESCE(a.error_code,''),COALESCE(a.error_message,''),
		       e.reviewer,e.machine_pass,e.policy_pass,e.quality_score,e.decision,e.findings,e.created_at
		FROM task_attempts a LEFT JOIN evaluations e ON e.attempt_id=a.id
		WHERE a.task_id=$1 ORDER BY a.attempt_no DESC LIMIT $2`, taskID, limit)
	if err != nil {
		return nil, fmt.Errorf("列出 console attempts: %w", err)
	}
	items := make([]consoleview.AttemptView, 0, limit)
	for rows.Next() {
		var value consoleview.AttemptView
		var output []byte
		var reviewer, decision *string
		var machinePass, policyPass *bool
		var quality *float64
		var findings []byte
		var evaluationAt *time.Time
		if err := rows.Scan(
			&value.ID, &value.TaskID, &value.AgentID, &value.AttemptNo, &value.Status,
			&value.Model, &value.WorkerID, &output, &value.TokensIn, &value.TokensOut,
			&value.CostMicros, &value.StepCount, &value.ToolCallCount, &value.StartedAt,
			&value.FinishedAt, &value.HeartbeatAt, &value.ErrorCode, &value.ErrorMessage,
			&reviewer, &machinePass, &policyPass, &quality, &decision, &findings, &evaluationAt,
		); err != nil {
			rows.Close()
			return nil, fmt.Errorf("扫描 console attempt: %w", err)
		}
		if err := json.Unmarshal(output, &value.Output); err != nil {
			rows.Close()
			return nil, fmt.Errorf("解析 attempt output: %w", err)
		}
		if reviewer != nil {
			evaluation := &consoleview.EvaluationView{
				Reviewer: *reviewer, MachinePass: *machinePass, PolicyPass: *policyPass,
				QualityScore: *quality, Decision: *decision, CreatedAt: *evaluationAt,
			}
			if err := json.Unmarshal(findings, &evaluation.Findings); err != nil {
				rows.Close()
				return nil, fmt.Errorf("解析 evaluation findings: %w", err)
			}
			value.Evaluation = evaluation
		}
		items = append(items, value)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	for index := range items {
		checkpoints, err := r.listConsoleCheckpoints(ctx, items[index].ID)
		if err != nil {
			return nil, err
		}
		toolCalls, err := r.listConsoleToolCalls(ctx, items[index].ID)
		if err != nil {
			return nil, err
		}
		items[index].CheckpointList = checkpoints
		items[index].ToolCalls = toolCalls
	}
	return items, nil
}

func (r *Repository) listConsoleCheckpoints(ctx context.Context, attemptID uuid.UUID) ([]consoleview.CheckpointView, error) {
	rows, err := r.pool.Query(ctx, `SELECT sequence,step_name,state,created_at FROM checkpoints WHERE attempt_id=$1 ORDER BY sequence`, attemptID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []consoleview.CheckpointView{}
	for rows.Next() {
		var value consoleview.CheckpointView
		var state []byte
		if err := rows.Scan(&value.Sequence, &value.StepName, &state, &value.CreatedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(state, &value.State); err != nil {
			return nil, err
		}
		items = append(items, value)
	}
	return items, rows.Err()
}

func (r *Repository) listConsoleToolCalls(ctx context.Context, attemptID uuid.UUID) ([]consoleview.ToolCallView, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id,tool_name,status,risk_level,arguments,result,COALESCE(error_message,''),started_at,finished_at
		FROM tool_calls WHERE attempt_id=$1 ORDER BY started_at`, attemptID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []consoleview.ToolCallView{}
	for rows.Next() {
		var value consoleview.ToolCallView
		var arguments, result []byte
		if err := rows.Scan(&value.ID, &value.ToolName, &value.Status, &value.RiskLevel,
			&arguments, &result, &value.ErrorMessage, &value.StartedAt, &value.FinishedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(arguments, &value.Arguments); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(result, &value.Result); err != nil {
			return nil, err
		}
		items = append(items, value)
	}
	return items, rows.Err()
}

func (r *Repository) ListConsoleEvents(ctx context.Context, swarmID uuid.UUID, limit int) ([]consoleview.EventView, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT e.id,e.aggregate_type,e.aggregate_id,e.event_type,e.aggregate_version,e.payload,
		       e.attempts,e.published_at,COALESCE(e.last_error,''),e.created_at
		FROM event_outbox e WHERE e.aggregate_id=$1 OR e.aggregate_id IN (
			SELECT id FROM tasks WHERE swarm_id=$1
			UNION SELECT id FROM agent_instances WHERE swarm_id=$1
			UNION SELECT a.id FROM task_attempts a JOIN tasks t ON t.id=a.task_id WHERE t.swarm_id=$1
			UNION SELECT v.id FROM evaluations v JOIN tasks t ON t.id=v.task_id WHERE t.swarm_id=$1
		)
		ORDER BY e.created_at DESC,e.id DESC LIMIT $2`, swarmID, limit)
	if err != nil {
		return nil, fmt.Errorf("列出 console events: %w", err)
	}
	defer rows.Close()
	items := make([]consoleview.EventView, 0, limit)
	for rows.Next() {
		var value consoleview.EventView
		var payload []byte
		if err := rows.Scan(&value.ID, &value.AggregateType, &value.AggregateID, &value.EventType,
			&value.AggregateVersion, &payload, &value.Attempts, &value.PublishedAt,
			&value.LastError, &value.CreatedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(payload, &value.Payload); err != nil {
			return nil, err
		}
		items = append(items, value)
	}
	return items, rows.Err()
}
