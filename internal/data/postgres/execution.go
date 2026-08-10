package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/licy-yu/agent-os/internal/domain"
	"github.com/licy-yu/agent-os/internal/domain/agent"
	"github.com/licy-yu/agent-os/internal/domain/task"
	"github.com/licy-yu/agent-os/internal/execution"
	"github.com/licy-yu/agent-os/internal/toolgateway"
)

// ClaimWork 把 ASSIGNED Task、RESERVED Agent 和新 Attempt 在一个事务内切到 RUNNING。
// JetStream 至少一次投递可能重复消息；若同一任务已经 RUNNING，则返回原 Attempt，
// Worker 会从最新检查点继续，而不会创建第二条执行历史。
func (r *Repository) ClaimWork(ctx context.Context, taskID, agentID uuid.UUID, workerID string) (*execution.Work, error) {
	var claimed *execution.Work
	err := r.withTx(ctx, func(tx pgx.Tx) error {
		value, err := scanTask(tx.QueryRow(ctx, taskSelect+" WHERE t.id=$1 FOR UPDATE", taskID))
		if err != nil {
			return mapReadError("领取 task", err)
		}
		if value.AssignedAgentID == nil || *value.AssignedAgentID != agentID {
			// 陈旧的 task.assigned 消息不应影响任务的新绑定。
			return nil
		}
		instance, err := scanInstance(tx.QueryRow(ctx, `
			SELECT id,template_id,swarm_id,name,status,current_task_id,load,heartbeat_at,
			       version,created_at,updated_at
			FROM agent_instances WHERE id=$1 FOR UPDATE`, agentID))
		if err != nil {
			return mapReadError("领取 agent", err)
		}
		template, err := scanTemplate(tx.QueryRow(ctx, `
			SELECT id,name,role,prompt,model,skills,tools,permissions,template_version,
			       context_window,risk_zone,cost_per_1k_tokens_micros,enabled,created_at,updated_at
			FROM agent_templates WHERE id=$1`, instance.TemplateID))
		if err != nil {
			return mapReadError("读取执行模板", err)
		}

		switch value.Status {
		case task.StatusAssigned:
			if instance.Status != agent.StatusReserved || instance.CurrentTaskID == nil || *instance.CurrentTaskID != value.ID {
				return fmt.Errorf("%w: task %s 与 agent %s 的预留关系不一致", domain.ErrConflict, value.ID, agentID)
			}
			attempt := &execution.Attempt{
				ID: uuid.New(), TaskID: value.ID, AgentID: agentID, Number: value.AttemptCount + 1,
				Status: execution.AttemptRunning, InputSnapshot: value.Input, Model: template.Model,
				PromptVersion: template.TemplateVersion, WorkerID: workerID,
				StartedAt: time.Now().UTC(), HeartbeatAt: time.Now().UTC(), OutputSnapshot: map[string]any{},
			}
			input, err := marshalJSON(attempt.InputSnapshot, "attempt.input_snapshot")
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `
				INSERT INTO task_attempts(
					id,task_id,agent_id,attempt_no,status,input_snapshot,output_snapshot,
					model,prompt_version,worker_id,started_at,heartbeat_at
				) VALUES($1,$2,$3,$4,'RUNNING',$5,'{}'::jsonb,$6,$7,$8,$9,$10)`,
				attempt.ID, attempt.TaskID, attempt.AgentID, attempt.Number, input,
				attempt.Model, attempt.PromptVersion, workerID, attempt.StartedAt, attempt.HeartbeatAt)
			if err != nil {
				return mapWriteError("创建 task attempt", err)
			}
			var taskVersion, agentVersion int64
			err = tx.QueryRow(ctx, `
				UPDATE tasks SET status='RUNNING',attempt_count=$3,version=version+1,updated_at=now()
				WHERE id=$1 AND version=$2 AND status='ASSIGNED' RETURNING version`,
				value.ID, value.Version, attempt.Number).Scan(&taskVersion)
			if err != nil {
				return fmt.Errorf("启动 task: %w", err)
			}
			err = tx.QueryRow(ctx, `
				UPDATE agent_instances
				SET status='RUNNING',heartbeat_at=now(),version=version+1,updated_at=now()
				WHERE id=$1 AND version=$2 AND status='RESERVED' RETURNING version,heartbeat_at`,
				agentID, instance.Version).Scan(&agentVersion, &instance.HeartbeatAt)
			if err != nil {
				return fmt.Errorf("启动 agent: %w", err)
			}
			if err := insertOutbox(ctx, tx, "task", value.ID, "task.running", taskVersion, map[string]any{
				"id": value.ID, "agent_id": agentID, "attempt_id": attempt.ID, "version": taskVersion,
			}); err != nil {
				return err
			}
			if err := insertOutbox(ctx, tx, "attempt", attempt.ID, "attempt.running", 1, map[string]any{
				"id": attempt.ID, "task_id": value.ID, "agent_id": agentID, "attempt_no": attempt.Number,
			}); err != nil {
				return err
			}
			value.Status, value.Version, value.AttemptCount = task.StatusRunning, taskVersion, attempt.Number
			instance.Status, instance.Version = agent.StatusRunning, agentVersion
			claimed = &execution.Work{Task: value, Agent: instance, Template: template, Attempt: attempt}
		case task.StatusRunning:
			attempt, err := scanAttempt(tx.QueryRow(ctx, attemptSelect+`
				WHERE a.task_id=$1 AND a.agent_id=$2 AND a.status='RUNNING'
				ORDER BY a.attempt_no DESC LIMIT 1`, taskID, agentID))
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("%w: RUNNING task %s 没有活动 Attempt", domain.ErrConflict, taskID)
			}
			if err != nil {
				return fmt.Errorf("读取活动 attempt: %w", err)
			}
			claimed = &execution.Work{Task: value, Agent: instance, Template: template, Attempt: attempt}
		default:
			// REVIEW 或终态意味着此前投递已经处理完成，调用方可以安全 ACK。
			return nil
		}
		return nil
	})
	return claimed, err
}

// SaveCheckpoint 使用 (attempt_id,sequence) 唯一键实现重放幂等，并刷新心跳。
func (r *Repository) SaveCheckpoint(ctx context.Context, value execution.Checkpoint) error {
	state, err := marshalJSON(value.State, "checkpoint.state")
	if err != nil {
		return err
	}
	artifacts, err := marshalJSON(value.ArtifactRefs, "checkpoint.artifact_refs")
	if err != nil {
		return err
	}
	return r.withTx(ctx, func(tx pgx.Tx) error {
		result, err := tx.Exec(ctx, `
			INSERT INTO checkpoints(id,attempt_id,sequence,step_name,state,artifact_refs)
			SELECT $1,$2,$3,$4,$5,$6
			WHERE EXISTS (SELECT 1 FROM task_attempts WHERE id=$2 AND status='RUNNING')
			ON CONFLICT(attempt_id,sequence) DO NOTHING`,
			value.ID, value.AttemptID, value.Sequence, value.StepName, state, artifacts)
		if err != nil {
			return mapWriteError("保存 checkpoint", err)
		}
		if result.RowsAffected() == 0 {
			// 相同 sequence 已存在是消息重放；Attempt 不在 RUNNING 则属于陈旧 Worker。
			var exists bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS(
				SELECT 1 FROM checkpoints WHERE attempt_id=$1 AND sequence=$2)`,
				value.AttemptID, value.Sequence).Scan(&exists); err != nil {
				return err
			}
			if exists {
				return nil
			}
			return fmt.Errorf("%w: attempt %s 已不允许写 checkpoint", domain.ErrConflict, value.AttemptID)
		}
		_, err = tx.Exec(ctx, `
			UPDATE task_attempts SET step_count=GREATEST(step_count,$2),heartbeat_at=now(),updated_at=now()
			WHERE id=$1 AND status='RUNNING'`, value.AttemptID, value.Sequence)
		return err
	})
}

// HeartbeatAttempt 只允许当前 worker 刷新自己的 RUNNING Attempt，隔离已经失效的旧进程。
func (r *Repository) HeartbeatAttempt(ctx context.Context, attemptID uuid.UUID, workerID string) error {
	result, err := r.pool.Exec(ctx, `
		UPDATE task_attempts a SET heartbeat_at=now(),updated_at=now()
		FROM agent_instances ai
		WHERE a.id=$1 AND a.worker_id=$2 AND a.status='RUNNING'
		  AND ai.id=a.agent_id AND ai.current_task_id=a.task_id`, attemptID, workerID)
	if err != nil {
		return fmt.Errorf("刷新 attempt 心跳: %w", err)
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("%w: attempt %s 已不属于 worker %s", domain.ErrConflict, attemptID, workerID)
	}
	return nil
}

// CompleteAttempt 将执行结果交给 REVIEW，并立即释放 Agent；Reviewer 只判定证据，不占用执行槽。
func (r *Repository) CompleteAttempt(ctx context.Context, attemptID uuid.UUID, result execution.ExecutionResult) error {
	output := map[string]any{
		"output": result.Output, "checks": result.Checks,
		"policy_violations": result.PolicyViolations, "quality_score": result.QualityScore,
	}
	raw, err := marshalJSON(output, "attempt.output_snapshot")
	if err != nil {
		return err
	}
	return r.withTx(ctx, func(tx pgx.Tx) error {
		attempt, err := scanAttempt(tx.QueryRow(ctx, attemptSelect+" WHERE a.id=$1 FOR UPDATE", attemptID))
		if err != nil {
			return mapReadError("完成 attempt", err)
		}
		if attempt.Status == execution.AttemptReview || attempt.Status == execution.AttemptSucceeded {
			return nil
		}
		if attempt.Status != execution.AttemptRunning {
			return fmt.Errorf("%w: attempt %s 状态为 %s", domain.ErrConflict, attemptID, attempt.Status)
		}
		var taskVersion, agentVersion int64
		err = tx.QueryRow(ctx, `
			UPDATE tasks SET status='REVIEW',version=version+1,updated_at=now()
			WHERE id=$1 AND assigned_agent_id=$2 AND status='RUNNING' RETURNING version`,
			attempt.TaskID, attempt.AgentID).Scan(&taskVersion)
		if err != nil {
			return fmt.Errorf("task 进入 REVIEW: %w", err)
		}
		_, err = tx.Exec(ctx, `
			UPDATE task_attempts
			SET status='REVIEW',output_snapshot=$2,tokens_in=$3,tokens_out=$4,cost_micros=$5,
			    finished_at=now(),heartbeat_at=now(),updated_at=now()
			WHERE id=$1 AND status='RUNNING'`, attemptID, raw, result.TokensIn, result.TokensOut, result.CostMicros)
		if err != nil {
			return fmt.Errorf("attempt 进入 REVIEW: %w", err)
		}
		err = tx.QueryRow(ctx, `
			UPDATE agent_instances
			SET status='IDLE',current_task_id=NULL,load=0,heartbeat_at=now(),version=version+1,updated_at=now()
			WHERE id=$1 AND current_task_id=$2 AND status='RUNNING' RETURNING version`,
			attempt.AgentID, attempt.TaskID).Scan(&agentVersion)
		if err != nil {
			return fmt.Errorf("释放 agent: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE swarms s SET spent_tokens=s.spent_tokens+$2+$3,spent_cost_micros=s.spent_cost_micros+$4,
			       version=s.version+1,updated_at=now()
			FROM tasks t WHERE t.id=$1 AND s.id=t.swarm_id`,
			attempt.TaskID, result.TokensIn, result.TokensOut, result.CostMicros); err != nil {
			return fmt.Errorf("记录 swarm 消耗: %w", err)
		}
		if err := insertOutbox(ctx, tx, "attempt", attempt.ID, "attempt.review", 2, map[string]any{
			"id": attempt.ID, "task_id": attempt.TaskID, "tokens_in": result.TokensIn,
			"tokens_out": result.TokensOut, "cost_micros": result.CostMicros,
		}); err != nil {
			return err
		}
		if err := insertOutbox(ctx, tx, "task", attempt.TaskID, "task.review", taskVersion, map[string]any{
			"id": attempt.TaskID, "attempt_id": attempt.ID, "version": taskVersion,
		}); err != nil {
			return err
		}
		return insertOutbox(ctx, tx, "agent_instance", attempt.AgentID, "agent.idle", agentVersion, map[string]any{
			"id": attempt.AgentID, "completed_task_id": attempt.TaskID, "version": agentVersion,
		})
	})
}

// ListReviewWork 返回待判定的证据快照；ApplyReview 仍会用行锁/CAS 防止多副本重复裁决。
func (r *Repository) ListReviewWork(ctx context.Context, limit int) ([]*execution.Work, error) {
	rows, err := r.pool.Query(ctx, attemptSelect+`
		WHERE a.status='REVIEW' ORDER BY a.finished_at,a.id LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("列出 REVIEW attempts: %w", err)
	}
	attempts := make([]*execution.Attempt, 0, limit)
	for rows.Next() {
		value, scanErr := scanAttempt(rows)
		if scanErr != nil {
			rows.Close()
			return nil, fmt.Errorf("扫描 REVIEW attempt: %w", scanErr)
		}
		attempts = append(attempts, value)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	works := make([]*execution.Work, 0, len(attempts))
	for _, attempt := range attempts {
		taskValue, err := r.GetTask(ctx, attempt.TaskID)
		if err != nil {
			return nil, err
		}
		agentValue, err := r.GetInstance(ctx, attempt.AgentID)
		if err != nil {
			return nil, err
		}
		template, err := r.GetTemplate(ctx, agentValue.TemplateID)
		if err != nil {
			return nil, err
		}
		works = append(works, &execution.Work{Task: taskValue, Agent: agentValue, Template: template, Attempt: attempt})
	}
	return works, nil
}

// ApplyReview 持久化 Evaluation，并把 Attempt/Task 一起推进到最终状态或 RETRY_WAIT。
func (r *Repository) ApplyReview(ctx context.Context, work *execution.Work, evaluation execution.Evaluation, retryAt time.Time) error {
	findings, err := marshalJSON(evaluation.Findings, "evaluation.findings")
	if err != nil {
		return err
	}
	return r.withTx(ctx, func(tx pgx.Tx) error {
		var attemptStatus execution.AttemptStatus
		if err := tx.QueryRow(ctx, `SELECT status FROM task_attempts WHERE id=$1 FOR UPDATE`, work.Attempt.ID).Scan(&attemptStatus); err != nil {
			return mapReadError("锁定 REVIEW attempt", err)
		}
		var taskStatus task.Status
		var taskVersion int64
		if err := tx.QueryRow(ctx, `SELECT status,version FROM tasks WHERE id=$1 FOR UPDATE`, work.Task.ID).Scan(&taskStatus, &taskVersion); err != nil {
			return mapReadError("锁定 REVIEW task", err)
		}
		if attemptStatus != execution.AttemptReview || taskStatus != task.StatusReview {
			return fmt.Errorf("%w: task/attempt 已被其他 Reviewer 裁决", domain.ErrConflict)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO evaluations(id,task_id,attempt_id,reviewer,machine_pass,policy_pass,
			                        quality_score,decision,findings)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			evaluation.ID, work.Task.ID, work.Attempt.ID, evaluation.Reviewer,
			evaluation.MachinePass, evaluation.PolicyPass, evaluation.QualityScore,
			evaluation.Decision, findings); err != nil {
			return mapWriteError("写入 evaluation", err)
		}

		var nextTask task.Status
		var nextAttempt execution.AttemptStatus
		var eventType string
		switch evaluation.Decision {
		case execution.DecisionAccept:
			nextTask, nextAttempt, eventType = task.StatusSucceeded, execution.AttemptSucceeded, "task.succeeded"
		case execution.DecisionRetry:
			nextTask, nextAttempt, eventType = task.StatusRetryWait, execution.AttemptFailed, "task.retry_wait"
		case execution.DecisionReject:
			nextTask, nextAttempt, eventType = task.StatusFailed, execution.AttemptFailed, "task.failed"
		default:
			return fmt.Errorf("未知 Reviewer 决策 %q", evaluation.Decision)
		}
		if _, err := tx.Exec(ctx, `UPDATE task_attempts SET status=$2,updated_at=now() WHERE id=$1`, work.Attempt.ID, nextAttempt); err != nil {
			return err
		}
		var nextVersion int64
		err := tx.QueryRow(ctx, `
			UPDATE tasks
			SET status=$2,assigned_agent_id=NULL,available_at=$3,version=version+1,updated_at=now()
			WHERE id=$1 AND version=$4 AND status='REVIEW' RETURNING version`,
			work.Task.ID, nextTask, retryAt, taskVersion).Scan(&nextVersion)
		if err != nil {
			return fmt.Errorf("应用 Reviewer 决策: %w", err)
		}
		if err := insertOutbox(ctx, tx, "evaluation", evaluation.ID, "evaluation.completed", 1, map[string]any{
			"id": evaluation.ID, "task_id": work.Task.ID, "attempt_id": work.Attempt.ID,
			"decision": evaluation.Decision, "quality_score": evaluation.QualityScore,
		}); err != nil {
			return err
		}
		return insertOutbox(ctx, tx, "task", work.Task.ID, eventType, nextVersion, map[string]any{
			"id": work.Task.ID, "attempt_id": work.Attempt.ID, "version": nextVersion,
			"decision": evaluation.Decision,
		})
	})
}

// RecoverTimedOut 回收心跳超时的 RUNNING Attempt。行锁加 SKIP LOCKED 支持控制面横向扩展。
func (r *Repository) RecoverTimedOut(ctx context.Context, cutoff time.Time, limit int) (int, error) {
	recovered := 0
	err := r.withTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT a.id,a.task_id,a.agent_id,t.execution_policy,t.attempt_count,t.version
			FROM task_attempts a JOIN tasks t ON t.id=a.task_id
			WHERE a.status='RUNNING' AND a.heartbeat_at < $1 AND t.status='RUNNING'
			ORDER BY a.heartbeat_at FOR UPDATE OF a,t SKIP LOCKED LIMIT $2`, cutoff, limit)
		if err != nil {
			return err
		}
		type stale struct {
			attemptID, taskID, agentID uuid.UUID
			policy                     task.ExecutionPolicy
			attemptCount               int32
			version                    int64
		}
		items := make([]stale, 0, limit)
		for rows.Next() {
			var value stale
			var raw []byte
			if err := rows.Scan(&value.attemptID, &value.taskID, &value.agentID, &raw, &value.attemptCount, &value.version); err != nil {
				rows.Close()
				return err
			}
			if err := json.Unmarshal(raw, &value.policy); err != nil {
				rows.Close()
				return fmt.Errorf("解析超时任务策略: %w", err)
			}
			items = append(items, value)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		for _, value := range items {
			next, eventType := task.StatusRetryWait, "task.retry_wait"
			if value.attemptCount >= value.policy.MaxAttempts {
				next, eventType = task.StatusFailed, "task.failed"
			}
			if _, err := tx.Exec(ctx, `
				UPDATE task_attempts SET status='ABORTED',finished_at=now(),error_code='HEARTBEAT_TIMEOUT',
				       error_message='Worker 心跳超时，由 RecoveryController 回收',updated_at=now()
				WHERE id=$1 AND status='RUNNING'`, value.attemptID); err != nil {
				return err
			}
			var nextVersion int64
			err := tx.QueryRow(ctx, `
				UPDATE tasks SET status=$2,assigned_agent_id=NULL,available_at=now()+interval '5 seconds',
				       version=version+1,updated_at=now()
				WHERE id=$1 AND version=$3 AND status='RUNNING' RETURNING version`,
				value.taskID, next, value.version).Scan(&nextVersion)
			if err != nil {
				return err
			}
			var agentVersion int64
			err = tx.QueryRow(ctx, `
				UPDATE agent_instances SET status='OFFLINE',current_task_id=NULL,load=0,
				       version=version+1,updated_at=now()
				WHERE id=$1 AND current_task_id=$2 RETURNING version`, value.agentID, value.taskID).Scan(&agentVersion)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
			if err := insertOutbox(ctx, tx, "attempt", value.attemptID, "attempt.aborted", 2, map[string]any{
				"id": value.attemptID, "task_id": value.taskID, "reason": "heartbeat_timeout",
			}); err != nil {
				return err
			}
			if err := insertOutbox(ctx, tx, "task", value.taskID, eventType, nextVersion, map[string]any{
				"id": value.taskID, "attempt_id": value.attemptID, "version": nextVersion,
				"reason": "heartbeat_timeout",
			}); err != nil {
				return err
			}
			if err == nil {
				if err := insertOutbox(ctx, tx, "agent_instance", value.agentID, "agent.offline", agentVersion, map[string]any{
					"id": value.agentID, "reason": "heartbeat_timeout", "version": agentVersion,
				}); err != nil {
					return err
				}
			}
			recovered++
		}
		return nil
	})
	return recovered, err
}

// GetToolDefinition 从声明式注册表读取工具能力。
func (r *Repository) GetToolDefinition(ctx context.Context, name string) (*toolgateway.Definition, error) {
	value := new(toolgateway.Definition)
	var permissions, config []byte
	err := r.pool.QueryRow(ctx, `
		SELECT name,description,adapter,required_permissions,risk_level,enabled,config
		FROM tool_registry WHERE lower(name)=lower($1)`, name).Scan(
		&value.Name, &value.Description, &value.Adapter, &permissions, &value.RiskLevel, &value.Enabled, &config)
	if err != nil {
		return nil, mapReadError("读取 tool registry", err)
	}
	if err := json.Unmarshal(permissions, &value.RequiredPermissions); err != nil {
		return nil, fmt.Errorf("解析工具权限: %w", err)
	}
	if err := json.Unmarshal(config, &value.Config); err != nil {
		return nil, fmt.Errorf("解析工具 config: %w", err)
	}
	return value, nil
}

// BeginToolCall 在 Attempt 行锁下原子检查并扣减调用额度，避免并发调用突破 MaxToolCalls。
func (r *Repository) BeginToolCall(ctx context.Context, record toolgateway.CallRecord, maxCalls int32) error {
	arguments, err := marshalJSON(record.Arguments, "tool_call.arguments")
	if err != nil {
		return err
	}
	deniedByLimit := false
	err = r.withTx(ctx, func(tx pgx.Tx) error {
		var count int32
		var status execution.AttemptStatus
		if err := tx.QueryRow(ctx, `SELECT tool_call_count,status FROM task_attempts WHERE id=$1 FOR UPDATE`, record.AttemptID).Scan(&count, &status); err != nil {
			return mapReadError("锁定 tool call attempt", err)
		}
		if status != execution.AttemptRunning {
			return fmt.Errorf("%w: attempt %s 已不在 RUNNING", domain.ErrConflict, record.AttemptID)
		}
		if maxCalls >= 0 && count >= maxCalls {
			// 达到上限本身也是重要的安全事件：写 DENIED 审计，但不再增加计数。
			_, err := tx.Exec(ctx, `
				INSERT INTO tool_calls(id,attempt_id,task_id,agent_id,tool_name,arguments,status,
				                       risk_level,error_message,started_at,finished_at)
				VALUES($1,$2,$3,$4,$5,$6,'DENIED',$7,$8,$9,$9)`,
				record.ID, record.AttemptID, record.TaskID, record.AgentID, record.ToolName,
				arguments, record.RiskLevel, fmt.Sprintf("已达到工具调用上限 %d", maxCalls), record.StartedAt)
			if err != nil {
				return mapWriteError("记录超额 tool call", err)
			}
			deniedByLimit = true
			return nil
		}
		finishedAt := any(nil)
		if record.Status == "DENIED" {
			finishedAt = record.StartedAt
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO tool_calls(id,attempt_id,task_id,agent_id,tool_name,arguments,status,
			                       risk_level,error_message,started_at,finished_at)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,NULLIF($9,''),$10,$11)`,
			record.ID, record.AttemptID, record.TaskID, record.AgentID, record.ToolName,
			arguments, record.Status, record.RiskLevel, record.ErrorMessage, record.StartedAt, finishedAt)
		if err != nil {
			return mapWriteError("开始 tool call", err)
		}
		_, err = tx.Exec(ctx, `
			UPDATE task_attempts SET tool_call_count=tool_call_count+1,heartbeat_at=now(),updated_at=now()
			WHERE id=$1`, record.AttemptID)
		return err
	})
	if err != nil {
		return err
	}
	if deniedByLimit {
		return fmt.Errorf("%w: 已达到工具调用上限 %d", toolgateway.ErrDenied, maxCalls)
	}
	return nil
}

// FinishToolCall 填入结果或错误，STARTED 以外的记录不能被再次结束。
func (r *Repository) FinishToolCall(ctx context.Context, id uuid.UUID, status string, result map[string]any, errorMessage string) error {
	if result == nil {
		result = map[string]any{}
	}
	raw, err := marshalJSON(result, "tool_call.result")
	if err != nil {
		return err
	}
	command, err := r.pool.Exec(ctx, `
		UPDATE tool_calls SET status=$2,result=$3,error_message=NULLIF($4,''),finished_at=now()
		WHERE id=$1 AND status='STARTED'`, id, status, raw, errorMessage)
	if err != nil {
		return fmt.Errorf("结束 tool call: %w", err)
	}
	if command.RowsAffected() != 1 {
		return fmt.Errorf("%w: tool call %s 已结束或不存在", domain.ErrConflict, id)
	}
	return nil
}

const attemptSelect = `
	SELECT a.id,a.task_id,a.agent_id,a.attempt_no,a.status,a.input_snapshot,a.output_snapshot,
	       COALESCE(a.model,''),COALESCE(a.prompt_version,''),COALESCE(a.worker_id,''),
	       a.started_at,a.finished_at,COALESCE(a.heartbeat_at,a.created_at),
	       a.tokens_in,a.tokens_out,a.cost_micros,a.step_count,a.tool_call_count,
	       a.no_progress_rounds,COALESCE(a.error_code,''),COALESCE(a.error_message,'')
	FROM task_attempts a`

func scanAttempt(row rowScanner) (*execution.Attempt, error) {
	value := new(execution.Attempt)
	var input, output []byte
	err := row.Scan(&value.ID, &value.TaskID, &value.AgentID, &value.Number, &value.Status,
		&input, &output, &value.Model, &value.PromptVersion, &value.WorkerID,
		&value.StartedAt, &value.FinishedAt, &value.HeartbeatAt, &value.TokensIn,
		&value.TokensOut, &value.CostMicros, &value.StepCount, &value.ToolCallCount,
		&value.NoProgressRounds, &value.ErrorCode, &value.ErrorMessage)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(input, &value.InputSnapshot); err != nil {
		return nil, fmt.Errorf("解析 attempt.input_snapshot: %w", err)
	}
	if err := json.Unmarshal(output, &value.OutputSnapshot); err != nil {
		return nil, fmt.Errorf("解析 attempt.output_snapshot: %w", err)
	}
	return value, nil
}
