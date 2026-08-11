package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
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
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return nil, fmt.Errorf("%w: worker_id 不能为空", domain.ErrConflict)
	}
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
			latest, err := loadLatestTaskCheckpoint(ctx, tx, value.ID)
			if err != nil {
				return err
			}
			var resumeCheckpointID *uuid.UUID
			if latest != nil {
				resumeCheckpointID = &latest.ID
			}
			attempt := &execution.Attempt{
				ID: uuid.New(), TaskID: value.ID, AgentID: agentID, Number: value.AttemptCount + 1,
				Status: execution.AttemptRunning, InputSnapshot: value.Input, Model: template.Model,
				PromptVersion: template.TemplateVersion, WorkerID: workerID, FencingToken: 1,
				ResumeCheckpointID: resumeCheckpointID,
				StartedAt:          time.Now().UTC(), HeartbeatAt: time.Now().UTC(), OutputSnapshot: map[string]any{},
			}
			input, err := marshalJSON(attempt.InputSnapshot, "attempt.input_snapshot")
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `
				INSERT INTO task_attempts(
					id,task_id,agent_id,attempt_no,status,input_snapshot,output_snapshot,
					model,prompt_version,worker_id,fencing_token,resume_checkpoint_id,started_at,heartbeat_at
				) VALUES($1,$2,$3,$4,'RUNNING',$5,'{}'::jsonb,$6,$7,$8,$9,$10,$11,$12)`,
				attempt.ID, attempt.TaskID, attempt.AgentID, attempt.Number, input,
				attempt.Model, attempt.PromptVersion, workerID, attempt.FencingToken,
				attempt.ResumeCheckpointID, attempt.StartedAt, attempt.HeartbeatAt)
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
				"id": attempt.ID, "task_id": value.ID, "agent_id": agentID,
				"attempt_no": attempt.Number, "worker_id": workerID, "fencing_token": attempt.FencingToken,
			}); err != nil {
				return err
			}
			value.Status, value.Version, value.AttemptCount = task.StatusRunning, taskVersion, attempt.Number
			instance.Status, instance.Version = agent.StatusRunning, agentVersion
			claimed = &execution.Work{Task: value, Agent: instance, Template: template,
				Attempt: attempt, LatestCheckpoint: latest}
		case task.StatusRunning:
			if instance.Status != agent.StatusRunning || instance.CurrentTaskID == nil || *instance.CurrentTaskID != value.ID {
				return fmt.Errorf("%w: RUNNING task %s 与 agent %s 所有权不一致", domain.ErrConflict, value.ID, agentID)
			}
			attempt, err := scanAttempt(tx.QueryRow(ctx, attemptSelect+`
				WHERE a.task_id=$1 AND a.agent_id=$2 AND a.status='RUNNING'
				ORDER BY a.attempt_no DESC LIMIT 1`, taskID, agentID))
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("%w: RUNNING task %s 没有活动 Attempt", domain.ErrConflict, taskID)
			}
			if err != nil {
				return fmt.Errorf("读取活动 attempt: %w", err)
			}
			// 至少一次消息只能由原 Worker 重放。不同 Worker 必须等待 RecoveryController
			// 先 ABORT 旧 Attempt，再通过新的调度创建新 Attempt，禁止无 fencing 的热接管。
			if !attempt.CanReplay(workerID) {
				return fmt.Errorf("%w: attempt %s 当前属于 worker %s/fence %d，worker %s 不能接管",
					domain.ErrConflict, attempt.ID, attempt.WorkerID, attempt.FencingToken, workerID)
			}
			latest, err := loadLatestCheckpoint(ctx, tx, attempt.ID)
			if err != nil {
				return err
			}
			claimed = &execution.Work{Task: value, Agent: instance, Template: template,
				Attempt: attempt, LatestCheckpoint: latest}
		default:
			// REVIEW 或终态意味着此前投递已经处理完成，调用方可以安全 ACK。
			return nil
		}
		return nil
	})
	return claimed, err
}

// SaveCheckpoint 使用 (attempt_id,sequence) 唯一键实现重放幂等，并在同一事务中写入
// typed CHECKPOINT Step。任何写入前都锁定 Attempt 并校验 worker_id + fencing_token，
// 因此 Zombie Worker 即使碰巧重放了已有 sequence，也不能借“幂等”绕过所有权检查。
func (r *Repository) SaveCheckpoint(ctx context.Context, owner execution.AttemptOwner, value execution.Checkpoint) error {
	if !owner.Valid() || value.AttemptID != owner.AttemptID ||
		value.FencingToken != owner.FencingToken || value.Sequence < 1 {
		return fmt.Errorf("%w: checkpoint 的 attempt owner 或 sequence 非法", domain.ErrConflict)
	}
	if value.ID == uuid.Nil {
		return fmt.Errorf("%w: checkpoint id 不能为空", domain.ErrConflict)
	}
	if value.CreatedAt.IsZero() {
		value.CreatedAt = time.Now().UTC()
	}
	state, err := marshalJSON(value.State, "checkpoint.state")
	if err != nil {
		return err
	}
	artifacts, err := marshalJSON(value.ArtifactRefs, "checkpoint.artifact_refs")
	if err != nil {
		return err
	}
	// PostgreSQL JSONB 读取数字时会解码为 float64；先把期望值也经过同一 JSON
	// 边界归一化，避免 int(1) 与 float64(1) 让合法消息重放被误判为分叉。
	var expectedState map[string]any
	if err := json.Unmarshal(state, &expectedState); err != nil {
		return fmt.Errorf("归一化 checkpoint.state: %w", err)
	}
	var expectedArtifacts []string
	if err := json.Unmarshal(artifacts, &expectedArtifacts); err != nil {
		return fmt.Errorf("归一化 checkpoint.artifact_refs: %w", err)
	}
	return r.withTx(ctx, func(tx pgx.Tx) error {
		attempt, err := lockOwnedAttempt(ctx, tx, owner)
		if err != nil {
			return err
		}
		if attempt.Status != execution.AttemptRunning {
			return fmt.Errorf("%w: attempt %s 已不在 RUNNING", domain.ErrConflict, owner.AttemptID)
		}
		var tenantID, runID uuid.UUID
		if err := tx.QueryRow(ctx, `SELECT tenant_id,swarm_id FROM tasks WHERE id=$1`, attempt.TaskID).
			Scan(&tenantID, &runID); err != nil {
			return mapReadError("读取 checkpoint task 归属", err)
		}

		persistedID := value.ID
		result, err := tx.Exec(ctx, `
			INSERT INTO checkpoints(id,attempt_id,sequence,step_name,state,artifact_refs,fencing_token,created_at)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8)
			ON CONFLICT(attempt_id,sequence) DO NOTHING`,
			value.ID, value.AttemptID, value.Sequence, value.StepName, state, artifacts,
			owner.FencingToken, value.CreatedAt)
		if err != nil {
			return mapWriteError("保存 checkpoint", err)
		}
		if result.RowsAffected() == 0 {
			// 相同 sequence 只有在 fence 和完整 payload 都相同时才是合法重放；
			// 不同内容使用同一 sequence 表示 Runtime 已经发生分叉，必须显式失败。
			var existing execution.Checkpoint
			var rawState, rawArtifacts []byte
			if err := tx.QueryRow(ctx, `
				SELECT id,attempt_id,sequence,step_name,state,artifact_refs,fencing_token,created_at
				FROM checkpoints WHERE attempt_id=$1 AND sequence=$2`,
				value.AttemptID, value.Sequence).Scan(
				&existing.ID, &existing.AttemptID, &existing.Sequence, &existing.StepName,
				&rawState, &rawArtifacts, &existing.FencingToken, &existing.CreatedAt,
			); err != nil {
				return mapReadError("读取重放 checkpoint", err)
			}
			if err := json.Unmarshal(rawState, &existing.State); err != nil {
				return fmt.Errorf("解析 checkpoint.state: %w", err)
			}
			if err := json.Unmarshal(rawArtifacts, &existing.ArtifactRefs); err != nil {
				return fmt.Errorf("解析 checkpoint.artifact_refs: %w", err)
			}
			if existing.FencingToken != owner.FencingToken || existing.StepName != value.StepName ||
				!reflect.DeepEqual(existing.State, expectedState) || !reflect.DeepEqual(existing.ArtifactRefs, expectedArtifacts) {
				return fmt.Errorf("%w: checkpoint sequence %d 已由不同 fence 或 payload 使用", domain.ErrConflict, value.Sequence)
			}
			persistedID = existing.ID
		}

		stepOutput, err := marshalJSON(map[string]any{
			"checkpoint_id": persistedID.String(), "artifact_refs": value.ArtifactRefs,
		}, "checkpoint.step.output")
		if err != nil {
			return err
		}
		stepID := uuid.New()
		stepResult, err := tx.Exec(ctx, `
			INSERT INTO task_steps(
				id,tenant_id,run_id,task_id,attempt_id,sequence,step_type,name,status,
				input,output,usage,checkpoint_id,started_at,finished_at,created_at
			) VALUES($1,$2,$3,$4,$5,$6,'CHECKPOINT',$7,'SUCCEEDED',
			         '{}'::jsonb,$8,'{}'::jsonb,$9,$10,$10,$10)
			ON CONFLICT(attempt_id,sequence) DO NOTHING`,
			stepID, tenantID, runID, attempt.TaskID, owner.AttemptID, value.Sequence,
			value.StepName, stepOutput, persistedID, value.CreatedAt)
		if err != nil {
			return mapWriteError("保存 CHECKPOINT task step", err)
		}
		if stepResult.RowsAffected() == 0 {
			var stepType execution.StepType
			var stepStatus execution.StepStatus
			var checkpointID *uuid.UUID
			var stepName string
			if err := tx.QueryRow(ctx, `
				SELECT step_type,status,checkpoint_id,name FROM task_steps
				WHERE attempt_id=$1 AND sequence=$2`, owner.AttemptID, value.Sequence).
				Scan(&stepType, &stepStatus, &checkpointID, &stepName); err != nil {
				return mapReadError("读取重放 checkpoint step", err)
			}
			if stepType != execution.StepCheckpoint || stepStatus != execution.StepSucceeded ||
				checkpointID == nil || *checkpointID != persistedID || stepName != value.StepName {
				return fmt.Errorf("%w: task step sequence %d 已被其它 Step 使用", domain.ErrConflict, value.Sequence)
			}
		}

		command, err := tx.Exec(ctx, `
			UPDATE task_attempts SET step_count=GREATEST(step_count,$4),heartbeat_at=now(),updated_at=now()
			WHERE id=$1 AND worker_id=$2 AND fencing_token=$3 AND status='RUNNING'`,
			owner.AttemptID, owner.WorkerID, owner.FencingToken, value.Sequence)
		if err != nil {
			return fmt.Errorf("刷新 checkpoint attempt: %w", err)
		}
		if command.RowsAffected() != 1 {
			return fmt.Errorf("%w: attempt %s 在 checkpoint 事务中失去所有权", domain.ErrConflict, owner.AttemptID)
		}
		return nil
	})
}

// HeartbeatAttempt 只允许当前 worker + fence 刷新自己的 RUNNING Attempt。
func (r *Repository) HeartbeatAttempt(ctx context.Context, owner execution.AttemptOwner) error {
	if !owner.Valid() {
		return fmt.Errorf("%w: attempt owner 非法", domain.ErrConflict)
	}
	result, err := r.pool.Exec(ctx, `
		UPDATE task_attempts a SET heartbeat_at=now(),updated_at=now()
		FROM agent_instances ai
		WHERE a.id=$1 AND a.worker_id=$2 AND a.fencing_token=$3 AND a.status='RUNNING'
		  AND ai.id=a.agent_id AND ai.current_task_id=a.task_id AND ai.status='RUNNING'`,
		owner.AttemptID, owner.WorkerID, owner.FencingToken)
	if err != nil {
		return fmt.Errorf("刷新 attempt 心跳: %w", err)
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("%w: attempt %s 已不属于 worker %s/fence %d",
			domain.ErrConflict, owner.AttemptID, owner.WorkerID, owner.FencingToken)
	}
	return nil
}

// CompleteAttempt 将执行结果交给 REVIEW，并立即释放 Agent；提交前必须验证完整 owner。
// 已由同一 owner 提交到 REVIEW/SUCCEEDED 的重放返回成功，错误 owner 即使看到终态也会被拒绝。
func (r *Repository) CompleteAttempt(ctx context.Context, owner execution.AttemptOwner, result execution.ExecutionResult) error {
	if !owner.Valid() {
		return fmt.Errorf("%w: attempt owner 非法", domain.ErrConflict)
	}
	output := map[string]any{
		"output": result.Output, "checks": result.Checks,
		"policy_violations": result.PolicyViolations, "quality_score": result.QualityScore,
	}
	raw, err := marshalJSON(output, "attempt.output_snapshot")
	if err != nil {
		return err
	}
	return r.withTx(ctx, func(tx pgx.Tx) error {
		attempt, err := lockOwnedAttempt(ctx, tx, owner)
		if err != nil {
			return err
		}
		if attempt.Status == execution.AttemptReview || attempt.Status == execution.AttemptSucceeded {
			return nil
		}
		if attempt.Status != execution.AttemptRunning {
			return fmt.Errorf("%w: attempt %s 状态为 %s", domain.ErrConflict, owner.AttemptID, attempt.Status)
		}
		// 候选输出、独立 VerificationRun 和 GateResult 必须先于 REVIEW 状态在同一事务落库。
		// 因此 Reviewer 永远不会观察到“有 REVIEW Task、却没有机器验收事实”的中间态。
		closure, err := persistCandidateVerification(ctx, tx, attempt, result, time.Now().UTC())
		if err != nil {
			return err
		}
		var taskVersion, agentVersion int64
		err = tx.QueryRow(ctx, `
			UPDATE tasks SET status='REVIEW',version=version+1,updated_at=now()
			WHERE id=$1 AND assigned_agent_id=$2 AND status='RUNNING' RETURNING version`,
			attempt.TaskID, attempt.AgentID).Scan(&taskVersion)
		if err != nil {
			return fmt.Errorf("task 进入 REVIEW: %w", err)
		}
		command, err := tx.Exec(ctx, `
			UPDATE task_attempts
			SET status='REVIEW',output_snapshot=$2,tokens_in=$3,tokens_out=$4,cost_micros=$5,
			    finished_at=now(),heartbeat_at=now(),updated_at=now()
			WHERE id=$1 AND status='RUNNING' AND worker_id=$6 AND fencing_token=$7`,
			owner.AttemptID, raw, result.TokensIn, result.TokensOut, result.CostMicros,
			owner.WorkerID, owner.FencingToken)
		if err != nil {
			return fmt.Errorf("attempt 进入 REVIEW: %w", err)
		}
		if command.RowsAffected() != 1 {
			return fmt.Errorf("%w: attempt %s 在完成事务中失去所有权", domain.ErrConflict, owner.AttemptID)
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
		if err := insertTenantOutbox(ctx, tx, closure.TenantID, "attempt", attempt.ID, "attempt.review", 2, map[string]any{
			"id": attempt.ID, "task_id": attempt.TaskID, "tokens_in": result.TokensIn,
			"tokens_out": result.TokensOut, "cost_micros": result.CostMicros,
			"worker_id": owner.WorkerID, "fencing_token": owner.FencingToken,
			"artifact_id": closure.ArtifactID, "verification_run_id": closure.VerificationID,
			"required_gates_passed": closure.RequiredPassed,
		}); err != nil {
			return err
		}
		if err := insertTenantOutbox(ctx, tx, closure.TenantID, "task", attempt.TaskID, "task.review", taskVersion, map[string]any{
			"id": attempt.TaskID, "attempt_id": attempt.ID, "version": taskVersion,
			"verification_run_id": closure.VerificationID,
		}); err != nil {
			return err
		}
		return insertTenantOutbox(ctx, tx, closure.TenantID, "agent_instance", attempt.AgentID, "agent.idle", agentVersion, map[string]any{
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
// Reviewer 的 ACCEPT 只是建议；数据库中全部 required GateResult 均为 PASSED 才能成功。
func (r *Repository) ApplyReview(ctx context.Context, work *execution.Work, evaluation execution.Evaluation, retryAt time.Time) error {
	if work == nil || work.Task == nil || work.Attempt == nil || evaluation.ID == uuid.Nil ||
		evaluation.TaskID != work.Task.ID || evaluation.AttemptID != work.Attempt.ID {
		return fmt.Errorf("%w: Review Work/Evaluation 标识不一致", domain.ErrConflict)
	}
	return r.withTx(ctx, func(tx pgx.Tx) error {
		var attemptStatus execution.AttemptStatus
		var attemptNo int32
		var persistedTaskID uuid.UUID
		if err := tx.QueryRow(ctx, `SELECT status,attempt_no,task_id FROM task_attempts WHERE id=$1 FOR UPDATE`, work.Attempt.ID).
			Scan(&attemptStatus, &attemptNo, &persistedTaskID); err != nil {
			return mapReadError("锁定 REVIEW attempt", err)
		}
		if persistedTaskID != work.Task.ID {
			return fmt.Errorf("%w: REVIEW attempt 不属于指定 Task", domain.ErrConflict)
		}
		var taskStatus task.Status
		var taskVersion int64
		var tenantID, runID uuid.UUID
		var executionPolicyRaw []byte
		if err := tx.QueryRow(ctx, `
			SELECT status,version,tenant_id,swarm_id,execution_policy
			FROM tasks WHERE id=$1 FOR UPDATE`, work.Task.ID).
			Scan(&taskStatus, &taskVersion, &tenantID, &runID, &executionPolicyRaw); err != nil {
			return mapReadError("锁定 REVIEW task", err)
		}
		if attemptStatus != execution.AttemptReview || taskStatus != task.StatusReview {
			// Reviewer 消息也可能至少一次重放；同一 Evaluation 已持久化即表示事务完整提交。
			var replay bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS(
				SELECT 1 FROM evaluations WHERE id=$1 AND attempt_id=$2)`, evaluation.ID, work.Attempt.ID).
				Scan(&replay); err != nil {
				return err
			}
			if replay {
				return nil
			}
			return fmt.Errorf("%w: task/attempt 已被其他 Reviewer 裁决", domain.ErrConflict)
		}
		var policy task.ExecutionPolicy
		if err := json.Unmarshal(executionPolicyRaw, &policy); err != nil {
			return fmt.Errorf("解析 Review execution_policy: %w", err)
		}
		closure, err := loadVerificationClosure(ctx, tx, tenantID, work.Task.ID, work.Attempt.ID)
		if err != nil {
			return err
		}
		if closure.RunID != runID {
			return fmt.Errorf("%w: VerificationRun 不属于 Task Run", domain.ErrConflict)
		}
		evaluation = enforceRequiredGates(evaluation, closure, attemptNo, policy.MaxAttempts)
		findings, err := marshalJSON(evaluation.Findings, "evaluation.findings")
		if err != nil {
			return err
		}

		// 所有成功 Task 都按同一顺序取得 Run 行锁，串行化“最后一个 Task”判定，
		// 避免并发完成时两个事务都看见对方尚未提交而漏建 Run Manifest。
		runCanComplete := false
		if evaluation.Decision == execution.DecisionAccept {
			var runStatus string
			if err := tx.QueryRow(ctx, `SELECT status FROM swarms WHERE id=$1 FOR UPDATE`, runID).
				Scan(&runStatus); err != nil {
				return mapReadError("锁定待完成 Run", err)
			}
			runCanComplete = runStatus != "COMPLETED" && runStatus != "FAILED" &&
				runStatus != "CANCELED" && runStatus != "EXPIRED"
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO evaluations(id,tenant_id,task_id,attempt_id,reviewer,machine_pass,policy_pass,
			                        quality_score,decision,findings)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
			evaluation.ID, tenantID, work.Task.ID, work.Attempt.ID, evaluation.Reviewer,
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
		now := time.Now().UTC()
		var taskManifestID uuid.UUID
		if nextTask == task.StatusSucceeded {
			taskManifestID, err = createTaskCompletionManifest(ctx, tx, closure, work.Task.ID,
				work.Attempt.ID, evaluation, now)
			if err != nil {
				return err
			}
		} else if err := invalidateCandidateArtifact(ctx, tx, closure, evaluation.Decision, now); err != nil {
			return fmt.Errorf("标记候选 Artifact INVALID: %w", err)
		}
		command, err := tx.Exec(ctx, `
			UPDATE task_attempts SET status=$2,updated_at=$3 WHERE id=$1 AND status='REVIEW'`,
			work.Attempt.ID, nextAttempt, now)
		if err != nil {
			return err
		}
		if command.RowsAffected() != 1 {
			return fmt.Errorf("%w: REVIEW attempt 已被并发裁决", domain.ErrConflict)
		}
		var nextVersion int64
		err = tx.QueryRow(ctx, `
			UPDATE tasks
			SET status=$2,assigned_agent_id=NULL,available_at=$3,version=version+1,updated_at=$5
			WHERE id=$1 AND version=$4 AND status='REVIEW' RETURNING version`,
			work.Task.ID, nextTask, retryAt, taskVersion, now).Scan(&nextVersion)
		if err != nil {
			return fmt.Errorf("应用 Reviewer 决策: %w", err)
		}
		if err := insertTenantOutbox(ctx, tx, tenantID, "evaluation", evaluation.ID, "evaluation.completed", 1, map[string]any{
			"id": evaluation.ID, "task_id": work.Task.ID, "attempt_id": work.Attempt.ID,
			"decision": evaluation.Decision, "quality_score": evaluation.QualityScore,
			"verification_run_id": closure.VerificationID,
		}); err != nil {
			return err
		}
		if err := insertTenantOutbox(ctx, tx, tenantID, "task", work.Task.ID, eventType, nextVersion, map[string]any{
			"id": work.Task.ID, "attempt_id": work.Attempt.ID, "version": nextVersion,
			"decision": evaluation.Decision, "completion_manifest_id": taskManifestID,
		}); err != nil {
			return err
		}
		if nextTask == task.StatusSucceeded && runCanComplete {
			if _, err := finalizeRunIfComplete(ctx, tx, tenantID, runID, work.Task.ID,
				work.Attempt.ID, evaluation.Reviewer, now); err != nil {
				return err
			}
		}
		return nil
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

// BeginToolCall 在 Attempt 行锁下同时验证 owner、检查额度并写审计，避免并发调用突破
// MaxToolCalls，也避免已失效 Worker 在外部调用前取得新的审计凭据。
func (r *Repository) BeginToolCall(ctx context.Context, owner execution.AttemptOwner, record toolgateway.CallRecord, maxCalls int32) error {
	if !owner.Valid() || record.AttemptID != owner.AttemptID {
		return fmt.Errorf("%w: tool call attempt owner 非法", domain.ErrConflict)
	}
	arguments, err := marshalJSON(record.Arguments, "tool_call.arguments")
	if err != nil {
		return err
	}
	deniedByLimit := false
	err = r.withTx(ctx, func(tx pgx.Tx) error {
		attempt, err := lockOwnedAttempt(ctx, tx, owner)
		if err != nil {
			return err
		}
		if attempt.Status != execution.AttemptRunning {
			return fmt.Errorf("%w: attempt %s 已不在 RUNNING", domain.ErrConflict, record.AttemptID)
		}
		if attempt.TaskID != record.TaskID || attempt.AgentID != record.AgentID {
			return fmt.Errorf("%w: tool call 的 task/agent 与 attempt 不一致", domain.ErrConflict)
		}
		// R2/R3 在审批轮询和 Checkpoint Resume 时使用稳定 ToolCall ID。已存在且请求完全
		// 相同即为合法重放，不重复扣减额度；同 ID 异参必须显式冲突。
		var existingAttemptID, existingTaskID, existingAgentID uuid.UUID
		var existingTool string
		var existingArguments []byte
		replayErr := tx.QueryRow(ctx, `
			SELECT attempt_id,task_id,agent_id,tool_name,arguments
			FROM tool_calls WHERE id=$1`, record.ID).Scan(
			&existingAttemptID, &existingTaskID, &existingAgentID, &existingTool,
			&existingArguments,
		)
		if replayErr == nil {
			var expected, persisted map[string]any
			_ = json.Unmarshal(arguments, &expected)
			_ = json.Unmarshal(existingArguments, &persisted)
			if existingAttemptID != record.AttemptID || existingTaskID != record.TaskID ||
				existingAgentID != record.AgentID || !strings.EqualFold(existingTool, record.ToolName) ||
				!reflect.DeepEqual(expected, persisted) {
				return fmt.Errorf("%w: ToolCall 幂等 ID 被不同请求占用", domain.ErrConflict)
			}
			return nil
		}
		if !errors.Is(replayErr, pgx.ErrNoRows) {
			return mapReadError("读取幂等 ToolCall", replayErr)
		}
		count := attempt.ToolCallCount
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
		_, err = tx.Exec(ctx, `
			INSERT INTO tool_calls(id,attempt_id,task_id,agent_id,tool_name,arguments,status,
			                       risk_level,error_message,started_at,finished_at)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,NULLIF($9,''),$10,$11)`,
			record.ID, record.AttemptID, record.TaskID, record.AgentID, record.ToolName,
			arguments, record.Status, record.RiskLevel, record.ErrorMessage, record.StartedAt, finishedAt)
		if err != nil {
			return mapWriteError("开始 tool call", err)
		}
		command, err := tx.Exec(ctx, `
			UPDATE task_attempts SET tool_call_count=tool_call_count+1,heartbeat_at=now(),updated_at=now()
			WHERE id=$1 AND worker_id=$2 AND fencing_token=$3 AND status='RUNNING'`,
			owner.AttemptID, owner.WorkerID, owner.FencingToken)
		if err != nil {
			return err
		}
		if command.RowsAffected() != 1 {
			return fmt.Errorf("%w: attempt %s 在 tool call 事务中失去所有权", domain.ErrConflict, owner.AttemptID)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if deniedByLimit {
		return fmt.Errorf("%w: 已达到工具调用上限 %d", toolgateway.ErrDenied, maxCalls)
	}
	return nil
}

// FinishToolCall 填入结果或错误。即使 Tool Adapter 已返回，也必须再次验证 owner；
// 若期间发生 Recovery，旧 Worker 不能把外部结果提交到新的执行历史中。
func (r *Repository) FinishToolCall(ctx context.Context, owner execution.AttemptOwner, id uuid.UUID,
	status string, result map[string]any, errorMessage string,
) error {
	if !owner.Valid() {
		return fmt.Errorf("%w: tool call attempt owner 非法", domain.ErrConflict)
	}
	if result == nil {
		result = map[string]any{}
	}
	raw, err := marshalJSON(result, "tool_call.result")
	if err != nil {
		return err
	}
	return r.withTx(ctx, func(tx pgx.Tx) error {
		attempt, err := lockOwnedAttempt(ctx, tx, owner)
		if err != nil {
			return err
		}
		if attempt.Status != execution.AttemptRunning {
			return fmt.Errorf("%w: attempt %s 已不在 RUNNING", domain.ErrConflict, owner.AttemptID)
		}
		command, err := tx.Exec(ctx, `
			UPDATE tool_calls SET status=$3,result=$4,error_message=NULLIF($5,''),finished_at=now()
			WHERE id=$1 AND attempt_id=$2 AND status='STARTED'`,
			id, owner.AttemptID, status, raw, errorMessage)
		if err != nil {
			return fmt.Errorf("结束 tool call: %w", err)
		}
		if command.RowsAffected() != 1 {
			var persistedStatus, persistedError string
			var persistedRaw []byte
			if err := tx.QueryRow(ctx, `
				SELECT status,result,COALESCE(error_message,'') FROM tool_calls
				WHERE id=$1 AND attempt_id=$2`, id, owner.AttemptID).
				Scan(&persistedStatus, &persistedRaw, &persistedError); err != nil {
				return fmt.Errorf("%w: tool call %s 不存在或不属于当前 attempt", domain.ErrConflict, id)
			}
			var expected, persisted map[string]any
			_ = json.Unmarshal(raw, &expected)
			_ = json.Unmarshal(persistedRaw, &persisted)
			if persistedStatus == status && persistedError == errorMessage && reflect.DeepEqual(expected, persisted) {
				return nil
			}
			return fmt.Errorf("%w: tool call %s 已由不同结果结束", domain.ErrConflict, id)
		}
		return nil
	})
}

const attemptSelect = `
	SELECT a.id,a.task_id,a.agent_id,a.attempt_no,a.status,a.input_snapshot,a.output_snapshot,
	       COALESCE(a.model,''),COALESCE(a.prompt_version,''),COALESCE(a.worker_id,''),
	       a.fencing_token,a.resume_checkpoint_id,a.started_at,a.finished_at,COALESCE(a.heartbeat_at,a.created_at),
	       a.tokens_in,a.tokens_out,a.cost_micros,a.step_count,a.tool_call_count,
	       a.no_progress_rounds,COALESCE(a.error_code,''),COALESCE(a.error_message,'')
	FROM task_attempts a`

func scanAttempt(row rowScanner) (*execution.Attempt, error) {
	value := new(execution.Attempt)
	var input, output []byte
	err := row.Scan(&value.ID, &value.TaskID, &value.AgentID, &value.Number, &value.Status,
		&input, &output, &value.Model, &value.PromptVersion, &value.WorkerID,
		&value.FencingToken, &value.ResumeCheckpointID, &value.StartedAt, &value.FinishedAt,
		&value.HeartbeatAt, &value.TokensIn,
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

// lockOwnedAttempt 在事务内锁定 Attempt 并验证完整 owner。状态由调用者根据操作语义判断：
// Complete 需要允许同一 owner 的 REVIEW 重放，而 Checkpoint/Tool 只接受 RUNNING。
func lockOwnedAttempt(ctx context.Context, tx pgx.Tx, owner execution.AttemptOwner) (*execution.Attempt, error) {
	if !owner.Valid() {
		return nil, fmt.Errorf("%w: attempt owner 非法", domain.ErrConflict)
	}
	value, err := scanAttempt(tx.QueryRow(ctx, attemptSelect+" WHERE a.id=$1 FOR UPDATE", owner.AttemptID))
	if err != nil {
		return nil, mapReadError("锁定 attempt owner", err)
	}
	if !value.OwnedBy(owner) {
		return nil, fmt.Errorf("%w: attempt %s owner 不匹配，当前为 %s/fence %d",
			domain.ErrConflict, owner.AttemptID, value.WorkerID, value.FencingToken)
	}
	return value, nil
}

// loadLatestCheckpoint 返回同一数据库快照中的最近恢复点；没有 Checkpoint 不是错误。
func loadLatestCheckpoint(ctx context.Context, tx pgx.Tx, attemptID uuid.UUID) (*execution.Checkpoint, error) {
	value := new(execution.Checkpoint)
	var state, artifacts []byte
	err := tx.QueryRow(ctx, `
		SELECT id,attempt_id,sequence,step_name,state,artifact_refs,fencing_token,created_at
		FROM checkpoints WHERE attempt_id=$1 ORDER BY sequence DESC LIMIT 1`, attemptID).Scan(
		&value.ID, &value.AttemptID, &value.Sequence, &value.StepName,
		&state, &artifacts, &value.FencingToken, &value.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, mapReadError("读取 latest checkpoint", err)
	}
	if err := json.Unmarshal(state, &value.State); err != nil {
		return nil, fmt.Errorf("解析 checkpoint.state: %w", err)
	}
	if err := json.Unmarshal(artifacts, &value.ArtifactRefs); err != nil {
		return nil, fmt.Errorf("解析 checkpoint.artifact_refs: %w", err)
	}
	return value, nil
}

// loadLatestTaskCheckpoint 为新 Attempt 查找上一 Attempt 的最近恢复点。它只在旧 Attempt
// 已经离开活跃执行、Task 再次 ASSIGNED 后调用，因此不会形成跨 Worker 热接管。
func loadLatestTaskCheckpoint(ctx context.Context, tx pgx.Tx, taskID uuid.UUID) (*execution.Checkpoint, error) {
	value := new(execution.Checkpoint)
	var state, artifacts []byte
	err := tx.QueryRow(ctx, `
		SELECT c.id,c.attempt_id,c.sequence,c.step_name,c.state,c.artifact_refs,c.fencing_token,c.created_at
		FROM checkpoints c
		JOIN task_attempts a ON a.id=c.attempt_id
		WHERE a.task_id=$1
		ORDER BY a.attempt_no DESC,c.sequence DESC LIMIT 1`, taskID).Scan(
		&value.ID, &value.AttemptID, &value.Sequence, &value.StepName,
		&state, &artifacts, &value.FencingToken, &value.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, mapReadError("读取 task latest checkpoint", err)
	}
	if err := json.Unmarshal(state, &value.State); err != nil {
		return nil, fmt.Errorf("解析 checkpoint.state: %w", err)
	}
	if err := json.Unmarshal(artifacts, &value.ArtifactRefs); err != nil {
		return nil, fmt.Errorf("解析 checkpoint.artifact_refs: %w", err)
	}
	return value, nil
}
