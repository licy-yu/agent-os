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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/licy-yu/agent-os/internal/domain"
	"github.com/licy-yu/agent-os/internal/domain/agent"
	"github.com/licy-yu/agent-os/internal/domain/task"
	"github.com/licy-yu/agent-os/internal/effect"
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
		var tenantID uuid.UUID
		if err := tx.QueryRow(ctx, `SELECT tenant_id FROM tasks WHERE id=$1`, value.ID).
			Scan(&tenantID); err != nil {
			return mapReadError("读取 ClaimWork Task 租户", err)
		}
		instance, err := scanInstance(tx.QueryRow(ctx, `
			SELECT id,template_id,swarm_id,name,status,current_task_id,load,heartbeat_at,
			       version,created_at,updated_at
			FROM agent_instances WHERE id=$1 AND tenant_id=$2 FOR UPDATE`, agentID, tenantID))
		if err != nil {
			return mapReadError("领取 agent", err)
		}
		template, err := scanTemplate(tx.QueryRow(ctx, `
			SELECT id,name,role,prompt,model,skills,tools,permissions,template_version,
			       context_window,risk_zone,cost_per_1k_tokens_micros,enabled,created_at,updated_at
			FROM agent_templates WHERE id=$1 AND tenant_id=$2`, instance.TemplateID, tenantID))
		if err != nil {
			return mapReadError("读取执行模板", err)
		}

		switch value.Status {
		case task.StatusAssigned:
			if instance.Status != agent.StatusReserved || instance.CurrentTaskID == nil || *instance.CurrentTaskID != value.ID {
				return fmt.Errorf("%w: task %s 与 agent %s 的预留关系不一致", domain.ErrConflict, value.ID, agentID)
			}
			// 审批/对账恢复沿用原 Attempt，而不是增加 attempt_count。重新签发 fence 后，
			// 旧 Worker 即使稍后苏醒也无法再写 Checkpoint、ToolCall 或最终结果。
			waitingAttempt, waitingErr := scanAttempt(tx.QueryRow(ctx, attemptSelect+`
				WHERE a.task_id=$1 AND a.agent_id=$2 AND a.status='WAITING'
				ORDER BY a.attempt_no DESC LIMIT 1 FOR UPDATE`, taskID, agentID))
			if waitingErr == nil {
				latest, err := loadLatestCheckpoint(ctx, tx, waitingAttempt.ID)
				if err != nil {
					return err
				}
				now := time.Now().UTC()
				newFence := waitingAttempt.FencingToken + 1
				var resumeCheckpointID *uuid.UUID
				if latest != nil {
					resumeCheckpointID = &latest.ID
				}
				command, err := tx.Exec(ctx, `
					UPDATE task_attempts
					SET status='RUNNING',worker_id=$2,fencing_token=$3,resume_checkpoint_id=$4,
					    heartbeat_at=$5,updated_at=$5
					WHERE id=$1 AND status='WAITING'`, waitingAttempt.ID, workerID,
					newFence, resumeCheckpointID, now)
				if err != nil {
					return mapWriteError("恢复 WAITING Attempt", err)
				}
				if command.RowsAffected() != 1 {
					return fmt.Errorf("%w: WAITING Attempt 已被其它 Worker 接管", domain.ErrConflict)
				}
				// AUTHORIZED Effect 仍绑定旧 fence；只有同一 Attempt 的恢复事务可以把它
				// 提升到新 fence。SUCCEEDED/UNKNOWN 等事实状态绝不在这里改写。
				if _, err := tx.Exec(ctx, `
					UPDATE effects SET fencing_token=$2,updated_at=$4
					WHERE attempt_id=$1 AND status='AUTHORIZED' AND fencing_token=$3`,
					waitingAttempt.ID, newFence, waitingAttempt.FencingToken, now); err != nil {
					return mapWriteError("续签已授权 Effect fence", err)
				}

				var taskVersion, agentVersion int64
				if err := tx.QueryRow(ctx, `
					UPDATE tasks SET status='RUNNING',version=version+1,updated_at=$3
					WHERE id=$1 AND version=$2 AND status='ASSIGNED' RETURNING version`,
					value.ID, value.Version, now).Scan(&taskVersion); err != nil {
					return mapWriteError("恢复 WAITING Task", err)
				}
				if err := tx.QueryRow(ctx, `
					UPDATE agent_instances
					SET status='RUNNING',heartbeat_at=$3,version=version+1,updated_at=$3
					WHERE id=$1 AND version=$2 AND status='RESERVED' RETURNING version`,
					agentID, instance.Version, now).Scan(&agentVersion); err != nil {
					return mapWriteError("恢复 WAITING Agent", err)
				}
				if err := insertTenantOutbox(ctx, tx, tenantID, "task", value.ID,
					"task.running", taskVersion, map[string]any{
						"id": value.ID, "run_id": value.SwarmID, "agent_id": agentID,
						"attempt_id": waitingAttempt.ID, "resumed": true, "version": taskVersion,
					}); err != nil {
					return err
				}
				if err := insertTenantOutbox(ctx, tx, tenantID, "attempt", waitingAttempt.ID,
					"attempt.resumed", newFence, map[string]any{
						"id": waitingAttempt.ID, "run_id": value.SwarmID, "task_id": value.ID,
						"agent_id": agentID, "worker_id": workerID, "fencing_token": newFence,
					}); err != nil {
					return err
				}

				waitingAttempt.Status = execution.AttemptRunning
				waitingAttempt.WorkerID = workerID
				waitingAttempt.FencingToken = newFence
				waitingAttempt.ResumeCheckpointID = resumeCheckpointID
				waitingAttempt.HeartbeatAt = now
				value.Status, value.Version = task.StatusRunning, taskVersion
				instance.Status, instance.Version, instance.HeartbeatAt = agent.StatusRunning, agentVersion, now
				claimed = &execution.Work{Task: value, Agent: instance, Template: template,
					Attempt: waitingAttempt, LatestCheckpoint: latest}
				return nil
			}
			if !errors.Is(waitingErr, pgx.ErrNoRows) {
				return fmt.Errorf("读取 WAITING Attempt: %w", waitingErr)
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
					id,tenant_id,task_id,agent_id,attempt_no,status,input_snapshot,output_snapshot,
					model,prompt_version,worker_id,fencing_token,resume_checkpoint_id,started_at,heartbeat_at
				) VALUES($1,$2,$3,$4,$5,'RUNNING',$6,'{}'::jsonb,$7,$8,$9,$10,$11,$12,$13)`,
				attempt.ID, tenantID, attempt.TaskID, attempt.AgentID, attempt.Number, input,
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
			if err := insertTenantOutbox(ctx, tx, tenantID, "task", value.ID, "task.running", taskVersion, map[string]any{
				"id": value.ID, "agent_id": agentID, "attempt_id": attempt.ID, "version": taskVersion,
			}); err != nil {
				return err
			}
			if err := insertTenantOutbox(ctx, tx, tenantID, "attempt", attempt.ID, "attempt.running", 1, map[string]any{
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

// SuspendAttempt 在工具网关命中人工审批或 UNKNOWN 对账边界时，原子释放 Worker
// 执行权。WAITING Attempt 不再刷新心跳，也不会被 RecoveryController 当成崩溃；
// 当前 JetStream 消息随后可安全 ACK，恢复消息由审批/对账事务重新写入 Outbox。
func (r *Repository) SuspendAttempt(ctx context.Context, owner execution.AttemptOwner,
	wait execution.AttemptWait,
) error {
	if !owner.Valid() || !wait.Valid() {
		return fmt.Errorf("%w: attempt owner 或等待原因非法", domain.ErrConflict)
	}
	taskStatus, taskEvent := task.StatusWaitingApproval, "task.waiting_approval"
	if wait.Reason == execution.WaitForExternal {
		taskStatus, taskEvent = task.StatusWaitingExternal, "task.waiting_external"
	}
	return r.withTx(ctx, func(tx pgx.Tx) error {
		// 所有会同时触碰 Effect 与 Attempt 的事务统一按 Effect -> Attempt -> Task
		// 加锁；Resolve/Reconcile 也是该顺序，避免审批与 Worker 恰好并发时死锁。
		waitEffect, err := lockAttemptWaitEffect(ctx, tx, owner.AttemptID, wait.EffectID)
		if err != nil {
			return err
		}
		attempt, err := lockOwnedAttempt(ctx, tx, owner)
		if err != nil {
			return err
		}
		if attempt.Status != execution.AttemptRunning {
			return fmt.Errorf("%w: attempt %s 已不在 RUNNING", domain.ErrConflict, owner.AttemptID)
		}

		var tenantID, runID uuid.UUID
		var taskVersion int64
		var assignedAgentID *uuid.UUID
		var currentTaskStatus task.Status
		if err := tx.QueryRow(ctx, `
			SELECT tenant_id,swarm_id,status,version,assigned_agent_id
			FROM tasks WHERE id=$1 FOR UPDATE`, attempt.TaskID).
			Scan(&tenantID, &runID, &currentTaskStatus, &taskVersion, &assignedAgentID); err != nil {
			return mapReadError("读取待挂起 Task", err)
		}
		if currentTaskStatus != task.StatusRunning || assignedAgentID == nil ||
			*assignedAgentID != attempt.AgentID {
			return fmt.Errorf("%w: Task 与 Attempt 的运行所有权不一致", domain.ErrConflict)
		}
		if waitEffect.TenantID != tenantID || waitEffect.TaskID != attempt.TaskID {
			return fmt.Errorf("%w: 等待 Effect 与 Task 租户/归属不一致", domain.ErrConflict)
		}
		if wait.Reason == execution.WaitForExternal && waitEffect.Status == effect.StatusExecuting {
			// Adapter 已被调用但 UNKNOWN 持久化失败时，绝不能把普通 error 送 Reviewer。
			// 既然进程已无法证明外部结果，就在当前恢复事务中 fail-closed 为 UNKNOWN。
			var effectVersion int64
			if err := tx.QueryRow(ctx, `
				UPDATE effects
				SET status='UNKNOWN',error_code='EXTERNAL_RESULT_UNKNOWN',
				    error_message='Adapter 已调用，但原 UNKNOWN 持久化未确认',
				    reconcile_after=now()+interval '30 seconds',version=version+1,updated_at=now()
				WHERE tenant_id=$1 AND id=$2 AND status='EXECUTING' AND version=$3
				RETURNING version`, tenantID, waitEffect.ID, waitEffect.Version).Scan(&effectVersion); err != nil {
				return mapWriteError("收敛失联 EXECUTING Effect", err)
			}
			waitEffect.Status, waitEffect.Version = effect.StatusUnknown, effectVersion
			if err := insertTenantOutbox(ctx, tx, tenantID, "effect", waitEffect.ID,
				"effect.unknown", effectVersion, map[string]any{
					"id": waitEffect.ID, "run_id": runID, "task_id": attempt.TaskID,
					"reason": "unknown_persist_recovery",
				}); err != nil {
				return err
			}
		}
		disposition, failureReason := classifyPersistedWait(wait.Reason, waitEffect)
		now := time.Now().UTC()

		if disposition == waitAlreadyFailed {
			command, err := tx.Exec(ctx, `
				UPDATE task_attempts
				SET status='ABORTED',finished_at=$4,error_code='WAIT_RESOLVED_FAILED',
				    error_message=$5,updated_at=$4
				WHERE id=$1 AND worker_id=$2 AND fencing_token=$3 AND status='RUNNING'`,
				owner.AttemptID, owner.WorkerID, owner.FencingToken, now, failureReason)
			if err != nil {
				return mapWriteError("终止已失败等待 Attempt", err)
			}
			if command.RowsAffected() != 1 {
				return fmt.Errorf("%w: Attempt 所有权已变化", domain.ErrConflict)
			}
			var nextTaskVersion int64
			if err := tx.QueryRow(ctx, `
				UPDATE tasks SET status='FAILED',assigned_agent_id=NULL,
				       version=version+1,updated_at=$4
				WHERE id=$1 AND version=$2 AND status='RUNNING' AND tenant_id=$3
				RETURNING version`, attempt.TaskID, taskVersion, tenantID, now).
				Scan(&nextTaskVersion); err != nil {
				return mapWriteError("终止已失败等待 Task", err)
			}
			var agentVersion int64
			agentErr := tx.QueryRow(ctx, `
				UPDATE agent_instances
				SET status='IDLE',current_task_id=NULL,load=0,version=version+1,updated_at=$3
				WHERE id=$1 AND status='RUNNING' AND current_task_id=$2
				RETURNING version`, attempt.AgentID, attempt.TaskID, now).Scan(&agentVersion)
			if agentErr != nil && !errors.Is(agentErr, pgx.ErrNoRows) {
				return mapWriteError("释放已失败等待 Agent", agentErr)
			}
			if err := insertTenantOutbox(ctx, tx, tenantID, "attempt", attempt.ID,
				"attempt.aborted", owner.FencingToken+1, map[string]any{
					"id": attempt.ID, "run_id": runID, "task_id": attempt.TaskID,
					"effect_id": waitEffect.ID, "reason": failureReason,
				}); err != nil {
				return err
			}
			if err := insertTenantOutbox(ctx, tx, tenantID, "task", attempt.TaskID,
				"task.failed", nextTaskVersion, map[string]any{
					"id": attempt.TaskID, "run_id": runID, "attempt_id": attempt.ID,
					"effect_id": waitEffect.ID, "reason": failureReason,
					"version": nextTaskVersion,
				}); err != nil {
				return err
			}
			if agentErr == nil {
				if err := insertTenantOutbox(ctx, tx, tenantID, "agent_instance", attempt.AgentID,
					"agent.idle", agentVersion, map[string]any{
						"id": attempt.AgentID, "run_id": runID,
						"reason": "wait_resolved_failed", "version": agentVersion,
					}); err != nil {
					return err
				}
			}
			_, err = failRunForTask(ctx, tx, tenantID, runID, attempt.TaskID, attempt.ID,
				"WAIT_RESOLVED_FAILED", failureReason, now)
			return err
		}

		command, err := tx.Exec(ctx, `
			UPDATE task_attempts SET status='WAITING',updated_at=$4
			WHERE id=$1 AND worker_id=$2 AND fencing_token=$3 AND status='RUNNING'`,
			owner.AttemptID, owner.WorkerID, owner.FencingToken, now)
		if err != nil {
			return mapWriteError("挂起 Attempt", err)
		}
		if command.RowsAffected() != 1 {
			return fmt.Errorf("%w: Attempt 所有权已变化", domain.ErrConflict)
		}

		if err := insertTenantOutbox(ctx, tx, tenantID, "attempt", attempt.ID,
			"attempt.waiting", owner.FencingToken, map[string]any{
				"id": attempt.ID, "run_id": runID, "task_id": attempt.TaskID,
				"effect_id": waitEffect.ID, "reason": wait.Reason,
				"pre_resolved":  disposition == waitAlreadySucceeded,
				"fencing_token": owner.FencingToken,
			}); err != nil {
			return err
		}

		var nextTaskVersion, agentVersion int64
		if disposition == waitAlreadySucceeded {
			if err := tx.QueryRow(ctx, `
				UPDATE tasks SET status='ASSIGNED',version=version+1,updated_at=$4
				WHERE id=$1 AND version=$2 AND status='RUNNING' AND tenant_id=$3
				RETURNING version`, attempt.TaskID, taskVersion, tenantID, now).
				Scan(&nextTaskVersion); err != nil {
				return mapWriteError("重新分配已完成等待 Task", err)
			}
			if err := tx.QueryRow(ctx, `
				UPDATE agent_instances
				SET status='RESERVED',version=version+1,updated_at=$3
				WHERE id=$1 AND status='RUNNING' AND current_task_id=$2
				RETURNING version`, attempt.AgentID, attempt.TaskID, now).Scan(&agentVersion); err != nil {
				return mapWriteError("重新预留已完成等待 Agent", err)
			}
			if err := insertTenantOutbox(ctx, tx, tenantID, "task", attempt.TaskID,
				"task.assigned", nextTaskVersion, map[string]any{
					"id": attempt.TaskID, "run_id": runID, "attempt_id": attempt.ID,
					"agent_id": attempt.AgentID, "effect_id": waitEffect.ID,
					"resumed": true, "version": nextTaskVersion,
				}); err != nil {
				return err
			}
			return insertTenantOutbox(ctx, tx, tenantID, "agent_instance", attempt.AgentID,
				"agent.reserved", agentVersion, map[string]any{
					"id": attempt.AgentID, "run_id": runID, "task_id": attempt.TaskID,
					"resumed": true, "version": agentVersion,
				})
		}

		if err := tx.QueryRow(ctx, `
			UPDATE tasks SET status=$2,version=version+1,updated_at=$4
			WHERE id=$1 AND version=$3 AND status='RUNNING' RETURNING version`,
			attempt.TaskID, taskStatus, taskVersion, now).Scan(&nextTaskVersion); err != nil {
			return mapWriteError("挂起 Task", err)
		}
		if err := tx.QueryRow(ctx, `
			UPDATE agent_instances
			SET status='WAITING_TOOL',version=version+1,updated_at=$3
			WHERE id=$1 AND status='RUNNING' AND current_task_id=$2
			RETURNING version`, attempt.AgentID, attempt.TaskID, now).Scan(&agentVersion); err != nil {
			return mapWriteError("挂起 Agent", err)
		}
		if err := insertTenantOutbox(ctx, tx, tenantID, "task", attempt.TaskID,
			taskEvent, nextTaskVersion, map[string]any{
				"id": attempt.TaskID, "run_id": runID, "attempt_id": attempt.ID,
				"agent_id": attempt.AgentID, "effect_id": waitEffect.ID,
				"reason": wait.Reason, "version": nextTaskVersion,
			}); err != nil {
			return err
		}
		return insertTenantOutbox(ctx, tx, tenantID, "agent_instance", attempt.AgentID,
			"agent.waiting_tool", agentVersion, map[string]any{
				"id": attempt.AgentID, "run_id": runID, "task_id": attempt.TaskID,
				"effect_id": waitEffect.ID, "reason": wait.Reason, "version": agentVersion,
			})
	})
}

type persistedWaitDisposition int

const (
	waitStillPending persistedWaitDisposition = iota
	waitAlreadySucceeded
	waitAlreadyFailed
)

type persistedWaitEffect struct {
	ID                   uuid.UUID
	TenantID             uuid.UUID
	TaskID               uuid.UUID
	Status               effect.Status
	AuthorizationBlocked bool
	BlockReason          string
	InteractionStatus    string
	Version              int64
}

func lockAttemptWaitEffect(ctx context.Context, tx pgx.Tx, attemptID, effectID uuid.UUID,
) (*persistedWaitEffect, error) {
	value := new(persistedWaitEffect)
	err := tx.QueryRow(ctx, `
		SELECT e.id,e.tenant_id,e.task_id,e.status,
		       (COALESCE(e.sanitized_result #>> '{_authorization,blocked}','false')='true'),
		       COALESCE(e.sanitized_result #>> '{_authorization,reason}',''),
		       COALESCE(i.status,''),e.version
		FROM effects e
		LEFT JOIN interactions i ON i.tenant_id=e.tenant_id AND i.id=e.approval_interaction_id
		WHERE e.id=$1 AND e.attempt_id=$2
		FOR UPDATE OF e`, effectID, attemptID).Scan(&value.ID, &value.TenantID,
		&value.TaskID, &value.Status,
		&value.AuthorizationBlocked, &value.BlockReason, &value.InteractionStatus,
		&value.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: 等待边界没有关联 Effect", domain.ErrConflict)
	}
	if err != nil {
		return nil, mapReadError("读取等待 Effect", err)
	}
	return value, nil
}

func classifyPersistedWait(reason execution.AttemptWaitReason,
	value *persistedWaitEffect,
) (persistedWaitDisposition, string) {
	if value == nil {
		return waitAlreadyFailed, "等待 Effect 不存在"
	}
	if reason == execution.WaitForApproval {
		switch {
		case value.Status == effect.StatusAuthorized || value.Status == effect.StatusSucceeded:
			return waitAlreadySucceeded, ""
		case value.AuthorizationBlocked:
			if strings.TrimSpace(value.BlockReason) != "" {
				return waitAlreadyFailed, value.BlockReason
			}
			return waitAlreadyFailed, "人工审批已拒绝"
		case value.Status == effect.StatusPrepared && value.InteractionStatus == "WAITING":
			return waitStillPending, ""
		default:
			return waitAlreadyFailed, "审批 Interaction 已关闭但 Effect 未获授权"
		}
	}
	switch value.Status {
	case effect.StatusSucceeded:
		return waitAlreadySucceeded, ""
	case effect.StatusFailed, effect.StatusCompensated:
		return waitAlreadyFailed, "外部对账确认 Effect 失败"
	case effect.StatusUnknown, effect.StatusReconciling:
		return waitStillPending, ""
	default:
		return waitAlreadyFailed, "Effect 已离开可对账等待状态"
	}
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

		// 所有终结 Task 都按同一顺序取得 Run 行锁，串行化“最后一个成功 Task”与
		// “任一不可恢复失败 Task”之间的竞争。这样并发 ACCEPT/REJECT 不会分别把
		// 同一个 Run 写成 COMPLETED 和 FAILED。
		runCanComplete, runCanFail := false, false
		if evaluation.Decision == execution.DecisionAccept || evaluation.Decision == execution.DecisionReject {
			var runStatus string
			if err := tx.QueryRow(ctx, `SELECT status FROM swarms WHERE id=$1 FOR UPDATE`, runID).
				Scan(&runStatus); err != nil {
				return mapReadError("锁定待终结 Run", err)
			}
			active := runStatus != "SUCCEEDED" && runStatus != "COMPLETED" &&
				runStatus != "FAILED" && runStatus != "CANCELED" && runStatus != "EXPIRED"
			runCanComplete = active && evaluation.Decision == execution.DecisionAccept
			runCanFail = active && evaluation.Decision == execution.DecisionReject
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
		if nextTask == task.StatusFailed && runCanFail {
			reason := strings.TrimSpace(strings.Join(evaluation.Findings, "; "))
			if reason == "" {
				reason = "Task 被 Reviewer 最终拒绝"
			}
			if _, err := failRunForTask(ctx, tx, tenantID, runID, work.Task.ID,
				work.Attempt.ID, "TASK_REJECTED", reason, now); err != nil {
				return err
			}
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

// failRunForTask 把不可恢复的 Task 失败与 Run 终态、run.failed Outbox 放在调用方
// 已开启的同一个事务中。Run 已经处于任一终态时按幂等重放处理，绝不把 CANCELED、
// COMPLETED 或旧 V1 的 SUCCEEDED 覆盖成 FAILED。
func failRunForTask(ctx context.Context, tx pgx.Tx, tenantID, runID, taskID, attemptID uuid.UUID,
	failureCode, reason string, now time.Time,
) (bool, error) {
	var runVersion int64
	err := tx.QueryRow(ctx, `
		UPDATE swarms
		SET status='FAILED',version=version+1,updated_at=$3
		WHERE id=$1 AND tenant_id=$2
		  AND status NOT IN ('SUCCEEDED','COMPLETED','FAILED','CANCELED','EXPIRED')
		RETURNING version`, runID, tenantID, now).Scan(&runVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("收敛失败 Run: %w", err)
	}
	if err := insertTenantOutbox(ctx, tx, tenantID, "swarm", runID, "run.failed", runVersion,
		map[string]any{
			"id": runID, "task_id": taskID, "attempt_id": attemptID,
			"failure_code": strings.TrimSpace(failureCode), "reason": strings.TrimSpace(reason),
			"version": runVersion,
		}); err != nil {
		return false, err
	}
	return true, nil
}

// timedOutEffectDisposition 描述 RecoveryController 在发现“Effect 已经持久化、但 Worker
// 尚未来得及调用 SuspendAttempt”这一故障窗口时应采取的动作。只要 Attempt 关联过
// Effect，就不能走普通的 ABORT + 新 Attempt 路径，否则新 Attempt 无法合法复用旧
// Effect，而且最坏情况下会对外部系统重复执行一次副作用。
type timedOutEffectDisposition uint8

const (
	timedOutWithoutEffect timedOutEffectDisposition = iota
	timedOutWaitApproval
	timedOutWaitExternal
	timedOutReassignSameAttempt
	timedOutFailSameAttempt
)

type timedOutRecoveryEffect struct {
	ID                   uuid.UUID
	TenantID             uuid.UUID
	RunID                uuid.UUID
	TaskID               uuid.UUID
	Status               effect.Status
	AuthorizationBlocked bool
	BlockReason          string
	InteractionStatus    string
	ErrorMessage         string
	Version              int64
}

type timedOutRecoveryPlan struct {
	Disposition timedOutEffectDisposition
	EffectID    uuid.UUID
	FailureCode string
	Reason      string
}

type timedOutRecoveryAttempt struct {
	AttemptID    uuid.UUID
	TaskID       uuid.UUID
	AgentID      uuid.UUID
	TenantID     uuid.UUID
	RunID        uuid.UUID
	WorkerID     string
	Fence        int64
	Policy       task.ExecutionPolicy
	AttemptCount int32
	TaskVersion  int64
}

// classifyTimedOutEffects 只基于已持久化事实做决定。优先级是：永久封存/失败 >
// 外部结果不确定 > 等待审批 > 已可继续。这样一个 Attempt 即使历史上有多个 Effect，
// 也不会因为较早的 SUCCEEDED 掩盖当前 UNKNOWN 或被拒绝的 Effect。
func classifyTimedOutEffects(values []timedOutRecoveryEffect) timedOutRecoveryPlan {
	if len(values) == 0 {
		return timedOutRecoveryPlan{Disposition: timedOutWithoutEffect}
	}

	for _, value := range values {
		if value.AuthorizationBlocked {
			reason := strings.TrimSpace(value.BlockReason)
			if reason == "" {
				reason = "Effect 授权已被永久封存"
			}
			return timedOutRecoveryPlan{
				Disposition: timedOutFailSameAttempt, EffectID: value.ID,
				FailureCode: "EFFECT_AUTHORIZATION_BLOCKED", Reason: reason,
			}
		}
		switch value.Status {
		case effect.StatusFailed, effect.StatusCompensated:
			reason := strings.TrimSpace(value.ErrorMessage)
			if reason == "" {
				reason = "Effect 已持久化为失败终态"
			}
			return timedOutRecoveryPlan{
				Disposition: timedOutFailSameAttempt, EffectID: value.ID,
				FailureCode: "EFFECT_TERMINAL_FAILURE", Reason: reason,
			}
		}
	}

	for _, value := range values {
		switch value.Status {
		case effect.StatusExecuting, effect.StatusUnknown, effect.StatusReconciling,
			effect.StatusCompensating:
			return timedOutRecoveryPlan{
				Disposition: timedOutWaitExternal, EffectID: value.ID,
				Reason: "Effect 外部结果尚未确认，必须先完成对账",
			}
		}
	}

	for _, value := range values {
		if value.Status == effect.StatusPrepared && value.InteractionStatus == "WAITING" {
			return timedOutRecoveryPlan{
				Disposition: timedOutWaitApproval, EffectID: value.ID,
				Reason: "Effect 正在等待人工审批",
			}
		}
	}

	for _, value := range values {
		if value.Status == effect.StatusPrepared {
			// R2 无审批路径会在创建 Effect 的同一事务内进入 AUTHORIZED；因此没有
			// WAITING Interaction 的 PREPARED 不是一个可安全猜测的中间态。
			return timedOutRecoveryPlan{
				Disposition: timedOutFailSameAttempt, EffectID: value.ID,
				FailureCode: "EFFECT_AUTHORIZATION_INCOMPLETE",
				Reason:      "Effect 停留在 PREPARED，但没有有效的 WAITING 审批",
			}
		}
	}

	// AUTHORIZED 表示 Adapter 尚未取得执行权；SUCCEEDED 表示 Adapter 结果已经落库。
	// 两者都可重新投递，但必须先把原 Attempt 置为 WAITING，让 ClaimWork 对同一个
	// Attempt 续签 fence，而不是增加 attempt_count。
	return timedOutRecoveryPlan{
		Disposition: timedOutReassignSameAttempt, EffectID: values[0].ID,
		Reason: "Effect 已持久化，可沿用原 Attempt 恢复",
	}
}

// RecoverTimedOut 回收心跳超时的 RUNNING Attempt。
//
// 候选扫描故意不预先锁 Attempt/Task；逐条恢复时先锁 Effect，再锁 Attempt、Task，
// 与审批、对账和 SuspendAttempt 的 Effect -> Attempt -> Task 顺序保持一致。并发的
// RecoveryController 即使读到同一候选，也会在取得锁后重新检查 RUNNING 与 heartbeat，
// 只有一个事务能够真正改变状态。
func (r *Repository) RecoverTimedOut(ctx context.Context, cutoff time.Time, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	recovered := 0
	err := r.withTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT a.id
			FROM task_attempts a JOIN tasks t ON t.id=a.task_id
			WHERE a.status='RUNNING' AND a.heartbeat_at < $1 AND t.status='RUNNING'
			ORDER BY a.heartbeat_at,a.id
			LIMIT $2`, cutoff, limit)
		if err != nil {
			return fmt.Errorf("扫描心跳超时 Attempt: %w", err)
		}
		attemptIDs := make([]uuid.UUID, 0, limit)
		for rows.Next() {
			var attemptID uuid.UUID
			if err := rows.Scan(&attemptID); err != nil {
				rows.Close()
				return fmt.Errorf("扫描心跳超时 Attempt ID: %w", err)
			}
			attemptIDs = append(attemptIDs, attemptID)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("遍历心跳超时 Attempt: %w", err)
		}
		rows.Close()

		for _, attemptID := range attemptIDs {
			changed, err := r.recoverTimedOutAttempt(ctx, tx, attemptID, cutoff)
			if err != nil {
				return err
			}
			if changed {
				recovered++
			}
		}
		return nil
	})
	return recovered, err
}

func (r *Repository) recoverTimedOutAttempt(ctx context.Context, tx pgx.Tx, attemptID uuid.UUID,
	cutoff time.Time,
) (bool, error) {
	// 第一步只锁 Effect。不能先锁 Attempt，否则会和审批/对账事务形成
	// Attempt -> Effect / Effect -> Attempt 的环形等待。
	effects, err := lockTimedOutAttemptEffects(ctx, tx, attemptID)
	if err != nil {
		return false, err
	}

	value := timedOutRecoveryAttempt{AttemptID: attemptID}
	attemptLockSQL := `
		SELECT task_id,agent_id,COALESCE(worker_id,''),fencing_token
		FROM task_attempts
		WHERE id=$1 AND status='RUNNING' AND heartbeat_at < $2
		FOR UPDATE`
	if len(effects) == 0 {
		// 首次查询没有 Effect 时不能阻塞等待 Attempt：PrepareToolEffect 的顺序是先锁
		// Attempt、再插入 Effect。若这里等待它提交，我们手里的空结果就会过期，随后
		// 错把“刚提交的 Effect”按无 Effect 路径 ABORT。NOWAIT 让本轮直接跳过，下一轮
		// 会从 Effect -> Attempt 的正常顺序接管。
		attemptLockSQL += " NOWAIT"
		// PostgreSQL 的 NOWAIT 失败会把当前事务标记为 aborted；用 pgx 嵌套事务
		// （底层 SAVEPOINT）隔离该错误，Rollback 后外层批次仍可继续处理其它候选。
		lockTx, beginErr := tx.Begin(ctx)
		if beginErr != nil {
			return false, fmt.Errorf("创建 Attempt NOWAIT savepoint: %w", beginErr)
		}
		lockErr := lockTx.QueryRow(ctx, attemptLockSQL, attemptID, cutoff).
			Scan(&value.TaskID, &value.AgentID, &value.WorkerID, &value.Fence)
		if lockErr != nil {
			_ = lockTx.Rollback(ctx)
			var postgresError *pgconn.PgError
			if errors.As(lockErr, &postgresError) && postgresError.Code == "55P03" {
				return false, nil
			}
			if errors.Is(lockErr, pgx.ErrNoRows) {
				return false, nil
			}
			return false, mapReadError("NOWAIT 锁定心跳超时 Attempt", lockErr)
		}
		if err := lockTx.Commit(ctx); err != nil {
			return false, fmt.Errorf("释放 Attempt NOWAIT savepoint: %w", err)
		}
	} else {
		err = tx.QueryRow(ctx, attemptLockSQL, attemptID, cutoff).
			Scan(&value.TaskID, &value.AgentID, &value.WorkerID, &value.Fence)
		if errors.Is(err, pgx.ErrNoRows) {
			// Worker 在候选扫描后刷新了心跳，或另一个 RecoveryController 已经完成回收。
			return false, nil
		}
		if err != nil {
			return false, mapReadError("锁定心跳超时 Attempt", err)
		}
	}
	if len(effects) == 0 {
		// 覆盖另一种更窄的 TOCTOU：Prepare 恰好在首次 Effect 查询后、Attempt NOWAIT
		// 前提交。此时我们已经持有 Attempt，新的 Prepare 无法再进入；只做无锁 EXISTS
		// 复核并让本轮提交释放 Attempt，下一轮再按 Effect 优先顺序加锁。
		var effectCommitted bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(
			SELECT 1 FROM effects WHERE attempt_id=$1)`, attemptID).Scan(&effectCommitted); err != nil {
			return false, mapReadError("复核超时 Attempt Effect", err)
		}
		if effectCommitted {
			return false, nil
		}
	}

	var rawPolicy []byte
	var currentStatus task.Status
	var assignedAgentID *uuid.UUID
	err = tx.QueryRow(ctx, `
		SELECT tenant_id,swarm_id,execution_policy,attempt_count,version,status,assigned_agent_id
		FROM tasks WHERE id=$1 FOR UPDATE`, value.TaskID).
		Scan(&value.TenantID, &value.RunID, &rawPolicy, &value.AttemptCount,
			&value.TaskVersion, &currentStatus, &assignedAgentID)
	if err != nil {
		return false, mapReadError("锁定心跳超时 Task", err)
	}
	if currentStatus != task.StatusRunning {
		return false, nil
	}
	if assignedAgentID == nil || *assignedAgentID != value.AgentID {
		return false, fmt.Errorf("%w: 超时 Attempt 与 Task 的 Agent 绑定不一致", domain.ErrConflict)
	}
	if err := json.Unmarshal(rawPolicy, &value.Policy); err != nil {
		return false, fmt.Errorf("解析超时任务策略: %w", err)
	}
	for _, current := range effects {
		if current.TenantID != value.TenantID || current.RunID != value.RunID ||
			current.TaskID != value.TaskID {
			return false, fmt.Errorf("%w: Effect 与超时 Attempt 的租户/Run/Task 归属不一致", domain.ErrConflict)
		}
	}

	plan := classifyTimedOutEffects(effects)
	now := time.Now().UTC()
	// EXECUTING 代表 Adapter 已经取得执行权。Worker 一旦失联，外部结果就不可证明；
	// 在挂起 Attempt 之前先原子落成 UNKNOWN，绝不重新授予 Adapter 调用权。
	for index := range effects {
		if effects[index].Status != effect.StatusExecuting {
			continue
		}
		var nextVersion int64
		err := tx.QueryRow(ctx, `
			UPDATE effects
			SET status='UNKNOWN',error_code='EXECUTION_LOST',
			    error_message='Worker 心跳超时，外部执行结果无法确认',
			    reconcile_after=$4::timestamptz+interval '30 seconds',
			    finished_at=COALESCE(finished_at,$4::timestamptz),
			    version=version+1,updated_at=$4
			WHERE id=$1 AND tenant_id=$2 AND version=$3 AND status='EXECUTING'
			RETURNING version`, effects[index].ID, value.TenantID, effects[index].Version, now).
			Scan(&nextVersion)
		if err != nil {
			return false, mapWriteError("收敛失联 EXECUTING Effect", err)
		}
		effects[index].Status, effects[index].Version = effect.StatusUnknown, nextVersion
		if err := insertTenantOutbox(ctx, tx, value.TenantID, "effect", effects[index].ID,
			"effect.unknown", nextVersion, map[string]any{
				"id": effects[index].ID, "run_id": value.RunID, "task_id": value.TaskID,
				"attempt_id": value.AttemptID, "status": effect.StatusUnknown,
				"reason": "worker_heartbeat_timeout",
			}); err != nil {
			return false, err
		}
	}

	switch plan.Disposition {
	case timedOutWaitApproval:
		return true, parkTimedOutEffectAttempt(ctx, tx, value, plan, task.StatusWaitingApproval,
			"task.waiting_approval", now)
	case timedOutWaitExternal:
		return true, parkTimedOutEffectAttempt(ctx, tx, value, plan, task.StatusWaitingExternal,
			"task.waiting_external", now)
	case timedOutReassignSameAttempt:
		return true, reassignTimedOutEffectAttempt(ctx, tx, value, plan, now)
	case timedOutFailSameAttempt:
		return true, failTimedOutEffectAttempt(ctx, tx, value, plan, now)
	case timedOutWithoutEffect:
		return true, recoverTimedOutAttemptWithoutEffect(ctx, tx, value, now)
	default:
		return false, fmt.Errorf("%w: 未知超时恢复决策 %d", domain.ErrConflict, plan.Disposition)
	}
}

func lockTimedOutAttemptEffects(ctx context.Context, tx pgx.Tx, attemptID uuid.UUID,
) ([]timedOutRecoveryEffect, error) {
	rows, err := tx.Query(ctx, `
		SELECT e.id,e.tenant_id,e.run_id,e.task_id,e.status,
		       (COALESCE(e.sanitized_result #>> '{_authorization,blocked}','false')='true'),
		       COALESCE(e.sanitized_result #>> '{_authorization,reason}',''),
		       COALESCE(i.status,''),COALESCE(e.error_message,''),e.version
		FROM effects e
		LEFT JOIN interactions i ON i.tenant_id=e.tenant_id AND i.id=e.approval_interaction_id
		WHERE e.attempt_id=$1
		ORDER BY e.prepared_at DESC,e.id DESC
		FOR UPDATE OF e`, attemptID)
	if err != nil {
		return nil, fmt.Errorf("锁定超时 Attempt Effects: %w", err)
	}
	defer rows.Close()
	values := make([]timedOutRecoveryEffect, 0, 1)
	for rows.Next() {
		var value timedOutRecoveryEffect
		if err := rows.Scan(&value.ID, &value.TenantID, &value.RunID, &value.TaskID,
			&value.Status, &value.AuthorizationBlocked, &value.BlockReason,
			&value.InteractionStatus, &value.ErrorMessage, &value.Version); err != nil {
			return nil, fmt.Errorf("扫描超时 Attempt Effect: %w", err)
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历超时 Attempt Effects: %w", err)
	}
	return values, nil
}

func parkTimedOutEffectAttempt(ctx context.Context, tx pgx.Tx, value timedOutRecoveryAttempt,
	plan timedOutRecoveryPlan, nextTaskStatus task.Status, taskEvent string, now time.Time,
) error {
	command, err := tx.Exec(ctx, `
		UPDATE task_attempts SET status='WAITING',updated_at=$2
		WHERE id=$1 AND status='RUNNING'`, value.AttemptID, now)
	if err != nil {
		return mapWriteError("持久挂起超时 Effect Attempt", err)
	}
	if command.RowsAffected() != 1 {
		return fmt.Errorf("%w: 超时 Effect Attempt 已变化", domain.ErrConflict)
	}
	var taskVersion int64
	err = tx.QueryRow(ctx, `
		UPDATE tasks SET status=$2,version=version+1,updated_at=$5
		WHERE tenant_id=$1 AND id=$3 AND version=$4 AND status='RUNNING'
		RETURNING version`, value.TenantID, nextTaskStatus, value.TaskID, value.TaskVersion, now).
		Scan(&taskVersion)
	if err != nil {
		return mapWriteError("持久挂起超时 Effect Task", err)
	}
	var agentVersion int64
	err = tx.QueryRow(ctx, `
		UPDATE agent_instances
		SET status='WAITING_TOOL',version=version+1,updated_at=$4
		WHERE tenant_id=$1 AND id=$2 AND status='RUNNING' AND current_task_id=$3
		RETURNING version`, value.TenantID, value.AgentID, value.TaskID, now).Scan(&agentVersion)
	if err != nil {
		return mapWriteError("持久挂起超时 Effect Agent", err)
	}
	if err := insertTenantOutbox(ctx, tx, value.TenantID, "attempt", value.AttemptID,
		"attempt.waiting", value.Fence, map[string]any{
			"id": value.AttemptID, "run_id": value.RunID, "task_id": value.TaskID,
			"agent_id": value.AgentID, "effect_id": plan.EffectID,
			"reason": "effect_commit_recovery", "fencing_token": value.Fence,
		}); err != nil {
		return err
	}
	if err := insertTenantOutbox(ctx, tx, value.TenantID, "task", value.TaskID,
		taskEvent, taskVersion, map[string]any{
			"id": value.TaskID, "run_id": value.RunID, "attempt_id": value.AttemptID,
			"agent_id": value.AgentID, "effect_id": plan.EffectID,
			"reason": plan.Reason, "version": taskVersion,
		}); err != nil {
		return err
	}
	return insertTenantOutbox(ctx, tx, value.TenantID, "agent_instance", value.AgentID,
		"agent.waiting_tool", agentVersion, map[string]any{
			"id": value.AgentID, "run_id": value.RunID, "task_id": value.TaskID,
			"effect_id": plan.EffectID, "reason": plan.Reason, "version": agentVersion,
		})
}

func reassignTimedOutEffectAttempt(ctx context.Context, tx pgx.Tx, value timedOutRecoveryAttempt,
	plan timedOutRecoveryPlan, now time.Time,
) error {
	command, err := tx.Exec(ctx, `
		UPDATE task_attempts SET status='WAITING',updated_at=$2
		WHERE id=$1 AND status='RUNNING'`, value.AttemptID, now)
	if err != nil {
		return mapWriteError("准备重投超时 Effect Attempt", err)
	}
	if command.RowsAffected() != 1 {
		return fmt.Errorf("%w: 超时 Effect Attempt 已变化", domain.ErrConflict)
	}
	var taskVersion int64
	err = tx.QueryRow(ctx, `
		UPDATE tasks SET status='ASSIGNED',available_at=$5,version=version+1,updated_at=$5
		WHERE tenant_id=$1 AND id=$2 AND version=$3 AND status='RUNNING'
		  AND assigned_agent_id=$4
		RETURNING version`, value.TenantID, value.TaskID, value.TaskVersion, value.AgentID, now).
		Scan(&taskVersion)
	if err != nil {
		return mapWriteError("重投超时 Effect Task", err)
	}
	var agentVersion int64
	err = tx.QueryRow(ctx, `
		UPDATE agent_instances
		SET status='RESERVED',version=version+1,updated_at=$4
		WHERE tenant_id=$1 AND id=$2 AND status='RUNNING' AND current_task_id=$3
		RETURNING version`, value.TenantID, value.AgentID, value.TaskID, now).Scan(&agentVersion)
	if err != nil {
		return mapWriteError("重新预留超时 Effect Agent", err)
	}
	if err := insertTenantOutbox(ctx, tx, value.TenantID, "attempt", value.AttemptID,
		"attempt.waiting", value.Fence, map[string]any{
			"id": value.AttemptID, "run_id": value.RunID, "task_id": value.TaskID,
			"agent_id": value.AgentID, "effect_id": plan.EffectID,
			"reason": "effect_commit_recovery", "pre_resolved": true,
			"fencing_token": value.Fence,
		}); err != nil {
		return err
	}
	if err := insertTenantOutbox(ctx, tx, value.TenantID, "task", value.TaskID,
		"task.assigned", taskVersion, map[string]any{
			"id": value.TaskID, "run_id": value.RunID, "attempt_id": value.AttemptID,
			"agent_id": value.AgentID, "effect_id": plan.EffectID,
			"resumed": true, "recovery": true, "version": taskVersion,
		}); err != nil {
		return err
	}
	return insertTenantOutbox(ctx, tx, value.TenantID, "agent_instance", value.AgentID,
		"agent.reserved", agentVersion, map[string]any{
			"id": value.AgentID, "run_id": value.RunID, "task_id": value.TaskID,
			"resumed": true, "recovery": true, "version": agentVersion,
		})
}

func failTimedOutEffectAttempt(ctx context.Context, tx pgx.Tx, value timedOutRecoveryAttempt,
	plan timedOutRecoveryPlan, now time.Time,
) error {
	reason := strings.TrimSpace(plan.Reason)
	if reason == "" {
		reason = "Effect 已进入不可恢复状态"
	}
	command, err := tx.Exec(ctx, `
		UPDATE task_attempts
		SET status='ABORTED',finished_at=$2,error_code=$3,error_message=$4,updated_at=$2
		WHERE id=$1 AND status='RUNNING'`, value.AttemptID, now, plan.FailureCode, reason)
	if err != nil {
		return mapWriteError("终止超时 Effect Attempt", err)
	}
	if command.RowsAffected() != 1 {
		return fmt.Errorf("%w: 超时 Effect Attempt 已变化", domain.ErrConflict)
	}
	var taskVersion int64
	err = tx.QueryRow(ctx, `
		UPDATE tasks
		SET status='FAILED',assigned_agent_id=NULL,version=version+1,updated_at=$5
		WHERE tenant_id=$1 AND id=$2 AND version=$3 AND status='RUNNING'
		  AND assigned_agent_id=$4
		RETURNING version`, value.TenantID, value.TaskID, value.TaskVersion, value.AgentID, now).
		Scan(&taskVersion)
	if err != nil {
		return mapWriteError("终止超时 Effect Task", err)
	}
	var agentVersion int64
	agentErr := tx.QueryRow(ctx, `
		UPDATE agent_instances
		SET status='IDLE',current_task_id=NULL,load=0,version=version+1,updated_at=$4
		WHERE tenant_id=$1 AND id=$2 AND status='RUNNING' AND current_task_id=$3
		RETURNING version`, value.TenantID, value.AgentID, value.TaskID, now).Scan(&agentVersion)
	if agentErr != nil && !errors.Is(agentErr, pgx.ErrNoRows) {
		return mapWriteError("释放失败超时 Effect Agent", agentErr)
	}
	if err := insertTenantOutbox(ctx, tx, value.TenantID, "attempt", value.AttemptID,
		"attempt.aborted", value.Fence+1, map[string]any{
			"id": value.AttemptID, "run_id": value.RunID, "task_id": value.TaskID,
			"effect_id": plan.EffectID, "reason": reason, "failure_code": plan.FailureCode,
		}); err != nil {
		return err
	}
	if err := insertTenantOutbox(ctx, tx, value.TenantID, "task", value.TaskID,
		"task.failed", taskVersion, map[string]any{
			"id": value.TaskID, "run_id": value.RunID, "attempt_id": value.AttemptID,
			"effect_id": plan.EffectID, "reason": reason, "failure_code": plan.FailureCode,
			"version": taskVersion,
		}); err != nil {
		return err
	}
	if agentErr == nil {
		if err := insertTenantOutbox(ctx, tx, value.TenantID, "agent_instance", value.AgentID,
			"agent.idle", agentVersion, map[string]any{
				"id": value.AgentID, "run_id": value.RunID, "task_id": value.TaskID,
				"reason": "effect_recovery_failed", "version": agentVersion,
			}); err != nil {
			return err
		}
	}
	_, err = failRunForTask(ctx, tx, value.TenantID, value.RunID, value.TaskID,
		value.AttemptID, plan.FailureCode, reason, now)
	return err
}

// recoverTimedOutAttemptWithoutEffect 保留 V1 原有的普通执行恢复语义：未触及外部世界
// 的 Attempt 可以 ABORT，未达上限则进入 RETRY_WAIT；达到上限时 Task 与 Run 一起失败。
func recoverTimedOutAttemptWithoutEffect(ctx context.Context, tx pgx.Tx,
	value timedOutRecoveryAttempt, now time.Time,
) error {
	next, eventType := task.StatusRetryWait, "task.retry_wait"
	if value.AttemptCount >= value.Policy.MaxAttempts {
		next, eventType = task.StatusFailed, "task.failed"
	}
	command, err := tx.Exec(ctx, `
		UPDATE task_attempts
		SET status='ABORTED',finished_at=$2,error_code='HEARTBEAT_TIMEOUT',
		    error_message='Worker 心跳超时，由 RecoveryController 回收',updated_at=$2
		WHERE id=$1 AND status='RUNNING'`, value.AttemptID, now)
	if err != nil {
		return mapWriteError("回收无 Effect 超时 Attempt", err)
	}
	if command.RowsAffected() != 1 {
		return fmt.Errorf("%w: 无 Effect 超时 Attempt 已变化", domain.ErrConflict)
	}
	var taskVersion int64
	err = tx.QueryRow(ctx, `
		UPDATE tasks
		SET status=$2,assigned_agent_id=NULL,
		    available_at=$5::timestamptz+interval '5 seconds',
		    version=version+1,updated_at=$5::timestamptz
		WHERE tenant_id=$1 AND id=$3 AND version=$4 AND status='RUNNING'
		RETURNING version`, value.TenantID, next, value.TaskID, value.TaskVersion, now).
		Scan(&taskVersion)
	if err != nil {
		return mapWriteError("回收无 Effect 超时 Task", err)
	}
	var agentVersion int64
	agentErr := tx.QueryRow(ctx, `
		UPDATE agent_instances
		SET status='OFFLINE',current_task_id=NULL,load=0,version=version+1,updated_at=$4
		WHERE tenant_id=$1 AND id=$2 AND current_task_id=$3
		RETURNING version`, value.TenantID, value.AgentID, value.TaskID, now).Scan(&agentVersion)
	if agentErr != nil && !errors.Is(agentErr, pgx.ErrNoRows) {
		return mapWriteError("下线无 Effect 超时 Agent", agentErr)
	}
	if err := insertTenantOutbox(ctx, tx, value.TenantID, "attempt", value.AttemptID,
		"attempt.aborted", value.Fence+1, map[string]any{
			"id": value.AttemptID, "run_id": value.RunID, "task_id": value.TaskID,
			"reason": "heartbeat_timeout",
		}); err != nil {
		return err
	}
	if err := insertTenantOutbox(ctx, tx, value.TenantID, "task", value.TaskID,
		eventType, taskVersion, map[string]any{
			"id": value.TaskID, "run_id": value.RunID, "attempt_id": value.AttemptID,
			"version": taskVersion, "reason": "heartbeat_timeout",
		}); err != nil {
		return err
	}
	if next == task.StatusFailed {
		if _, err := failRunForTask(ctx, tx, value.TenantID, value.RunID, value.TaskID,
			value.AttemptID, "WORKER_HEARTBEAT_TIMEOUT",
			"Worker 心跳超时且已达到最大 Attempt 次数", now); err != nil {
			return err
		}
	}
	if agentErr == nil {
		if err := insertTenantOutbox(ctx, tx, value.TenantID, "agent_instance", value.AgentID,
			"agent.offline", agentVersion, map[string]any{
				"id": value.AgentID, "run_id": value.RunID, "task_id": value.TaskID,
				"reason": "heartbeat_timeout", "version": agentVersion,
			}); err != nil {
			return err
		}
	}
	return nil
}

// GetToolDefinition 从声明式注册表读取当前 Attempt 租户的工具能力。
//
// name 由模型输出，不能作为租户边界。查询从仍然有效的 Attempt owner 反查 tenant_id，
// 再与 tool_registry 做等租户连接；旧 Worker、伪造名称或另一租户的同名工具都会得到
// NotFound/Conflict，并且随后 BeginToolCall 还会在行锁下再次校验 fence。
func (r *Repository) GetToolDefinition(ctx context.Context, owner execution.AttemptOwner, name string) (*toolgateway.Definition, error) {
	if !owner.Valid() {
		return nil, fmt.Errorf("%w: tool definition attempt owner 非法", domain.ErrConflict)
	}
	value := new(toolgateway.Definition)
	var permissions, config []byte
	err := r.pool.QueryRow(ctx, `
		SELECT registry.name,registry.description,registry.adapter,registry.required_permissions,
		       registry.risk_level,registry.enabled,registry.config
		FROM task_attempts AS attempt
		JOIN tool_registry AS registry ON registry.tenant_id=attempt.tenant_id
		WHERE attempt.id=$1 AND attempt.worker_id=$2 AND attempt.fencing_token=$3
		  AND attempt.status='RUNNING' AND lower(registry.name)=lower($4)`,
		owner.AttemptID, owner.WorkerID, owner.FencingToken, name).Scan(
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
		var tenantID uuid.UUID
		if err := tx.QueryRow(ctx, `SELECT tenant_id FROM tasks WHERE id=$1`, attempt.TaskID).
			Scan(&tenantID); err != nil {
			return mapReadError("读取 ToolCall Task 租户", err)
		}
		// R2/R3 在审批轮询和 Checkpoint Resume 时使用稳定 ToolCall ID。已存在且请求完全
		// 相同即为合法重放，不重复扣减额度；同 ID 异参必须显式冲突。
		var existingTenantID, existingAttemptID, existingTaskID, existingAgentID uuid.UUID
		var existingTool string
		var existingArguments []byte
		replayErr := tx.QueryRow(ctx, `
			SELECT tenant_id,attempt_id,task_id,agent_id,tool_name,arguments
			FROM tool_calls WHERE id=$1`, record.ID).Scan(
			&existingTenantID, &existingAttemptID, &existingTaskID, &existingAgentID, &existingTool,
			&existingArguments,
		)
		if replayErr == nil {
			var expected, persisted map[string]any
			_ = json.Unmarshal(arguments, &expected)
			_ = json.Unmarshal(existingArguments, &persisted)
			if existingTenantID != tenantID || existingAttemptID != record.AttemptID || existingTaskID != record.TaskID ||
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
				INSERT INTO tool_calls(id,tenant_id,attempt_id,task_id,agent_id,tool_name,arguments,status,
				                       risk_level,error_message,started_at,finished_at)
				VALUES($1,$2,$3,$4,$5,$6,$7,'DENIED',$8,$9,$10,$10)`,
				record.ID, tenantID, record.AttemptID, record.TaskID, record.AgentID, record.ToolName,
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
			INSERT INTO tool_calls(id,tenant_id,attempt_id,task_id,agent_id,tool_name,arguments,status,
			                       risk_level,error_message,started_at,finished_at)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,NULLIF($10,''),$11,$12)`,
			record.ID, tenantID, record.AttemptID, record.TaskID, record.AgentID, record.ToolName,
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
	       a.fencing_token,a.resume_checkpoint_id,COALESCE(a.started_at,a.created_at),a.finished_at,
	       COALESCE(a.heartbeat_at,a.created_at),
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
