package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/licy-yu/agent-os/internal/domain"
	"github.com/licy-yu/agent-os/internal/domain/agent"
	"github.com/licy-yu/agent-os/internal/domain/task"
	"github.com/licy-yu/agent-os/internal/orchestrator"
)

// ListReconcileTasks 只读取 Controller 有权处理的非终态任务。
func (r *Repository) ListReconcileTasks(ctx context.Context, limit int) ([]*task.Task, error) {
	rows, err := r.pool.Query(ctx, taskSelect+`
		JOIN swarms run_scope ON run_scope.id=t.swarm_id
		WHERE t.status IN ('CREATED','PLANNING','BLOCKED','RETRY_WAIT')
		  AND run_scope.desired_state='RUNNING'
		  AND run_scope.status IN ('PENDING','RUNNING')
		ORDER BY t.updated_at,t.id
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("列出 reconcile tasks: %w", err)
	}
	defer rows.Close()
	items := make([]*task.Task, 0, limit)
	for rows.Next() {
		value, scanErr := scanTask(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("扫描 reconcile task: %w", scanErr)
		}
		items = append(items, value)
	}
	return items, rows.Err()
}

// DependenciesSatisfied 检查所有 required 依赖是否成功；SOFT 依赖不阻塞 READY。
// DATA/APPROVAL/TEMPORAL 还会在后续专用 Controller 中检查 Artifact、Interaction 与时间条件，
// 在专用条件满足前，其前置 Task 至少必须已经成功，不能被当作普通 SOFT 边跳过。
func (r *Repository) DependenciesSatisfied(ctx context.Context, taskID uuid.UUID) (bool, error) {
	var ready bool
	err := r.pool.QueryRow(ctx, `
		SELECT NOT EXISTS (
			SELECT 1
			FROM task_dependencies d
			JOIN tasks dependency ON dependency.id=d.depends_on_id
			WHERE d.task_id=$1
			  AND d.required=true
			  AND d.dependency_type<>'SOFT'
			  AND dependency.status <> 'SUCCEEDED'
		)`, taskID).Scan(&ready)
	if err != nil {
		return false, fmt.Errorf("检查 task dependencies: %w", err)
	}
	return ready, nil
}

// TransitionTask 以 status + version 双条件做 CAS，并在同一事务写入状态事件。
func (r *Repository) TransitionTask(ctx context.Context, id uuid.UUID, expectedVersion int64, from, to task.Status, eventType string) (int64, error) {
	var nextVersion int64
	err := r.withTx(ctx, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			UPDATE tasks
			SET status=$4,version=version+1,updated_at=now()
			WHERE id=$1 AND version=$2 AND status=$3
			RETURNING version`, id, expectedVersion, from, to).Scan(&nextVersion)
		if err == pgx.ErrNoRows {
			return fmt.Errorf("%w: task %s 已被其他控制器修改", domain.ErrConflict, id)
		}
		if err != nil {
			return fmt.Errorf("更新 task 状态: %w", err)
		}
		return insertOutbox(ctx, tx, "task", id, eventType, nextVersion, map[string]any{
			"id": id, "from": from, "to": to, "version": nextVersion,
		})
	})
	return nextVersion, err
}

// ActivateRegisteredAgents 完成 REGISTERED -> IDLE，并为每个实例产生独立事件。
func (r *Repository) ActivateRegisteredAgents(ctx context.Context, limit int) (int, error) {
	count := 0
	err := r.withTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id,version FROM agent_instances
			WHERE status='REGISTERED'
			ORDER BY created_at,id
			FOR UPDATE SKIP LOCKED
			LIMIT $1`, limit)
		if err != nil {
			return fmt.Errorf("锁定 REGISTERED agents: %w", err)
		}
		type lockedAgent struct {
			id      uuid.UUID
			version int64
		}
		locked := make([]lockedAgent, 0, limit)
		for rows.Next() {
			var value lockedAgent
			if err := rows.Scan(&value.id, &value.version); err != nil {
				rows.Close()
				return err
			}
			locked = append(locked, value)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()

		for _, value := range locked {
			result, err := tx.Exec(ctx, `
				UPDATE agent_instances
				SET status='IDLE',version=version+1,updated_at=now()
				WHERE id=$1 AND version=$2 AND status='REGISTERED'`, value.id, value.version)
			if err != nil {
				return fmt.Errorf("激活 agent %s: %w", value.id, err)
			}
			if result.RowsAffected() != 1 {
				continue
			}
			if err := insertOutbox(ctx, tx, "agent_instance", value.id, "agent.idle", value.version+1, map[string]any{
				"id": value.id, "from": agent.StatusRegistered, "to": agent.StatusIdle,
			}); err != nil {
				return err
			}
			count++
		}
		return nil
	})
	return count, err
}

// ListQueuedTasks 返回 READY 队列及其尚未完成的直接下游数量。
func (r *Repository) ListQueuedTasks(ctx context.Context, limit int) ([]orchestrator.QueuedTask, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT t.id,t.swarm_id,t.parent_id,t.name,t.goal,t.status,t.priority,t.input,
		       t.requirements,t.acceptance,t.execution_policy,t.assigned_agent_id,
		       t.attempt_count,t.available_at,t.deadline,t.version,t.created_at,t.updated_at,
		       (SELECT count(*) FROM task_dependencies d
		          JOIN tasks child ON child.id=d.task_id
		         WHERE d.depends_on_id=t.id
		           AND child.status NOT IN ('SUCCEEDED','FAILED','CANCELED','REJECTED')) AS blocked_children
		FROM tasks t JOIN swarms run_scope ON run_scope.id=t.swarm_id
		WHERE t.status='READY' AND t.available_at <= now()
		  AND run_scope.desired_state='RUNNING'
		  AND run_scope.status IN ('PENDING','RUNNING')
		ORDER BY t.priority DESC,t.created_at
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("列出 scheduler queue: %w", err)
	}
	defer rows.Close()
	items := make([]orchestrator.QueuedTask, 0, limit)
	for rows.Next() {
		var blocked int32
		value, scanErr := scanTaskExtra(rows, &blocked)
		if scanErr != nil {
			return nil, fmt.Errorf("扫描 scheduler queue: %w", scanErr)
		}
		items = append(items, orchestrator.QueuedTask{Task: value, BlockedChildren: blocked})
	}
	return items, rows.Err()
}

// ListSchedulerCandidates 查询 IDLE 实例、模板能力和 Swarm 剩余预算。
func (r *Repository) ListSchedulerCandidates(ctx context.Context, swarmID uuid.UUID, taskMaxTokens int64) ([]orchestrator.Candidate, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT ai.id,ai.template_id,ai.swarm_id,ai.name,ai.status,ai.current_task_id,
		       ai.load,ai.heartbeat_at,ai.version,ai.created_at,ai.updated_at,
		       at.id,at.name,at.role,at.prompt,at.model,at.skills,at.tools,at.permissions,
		       at.template_version,at.context_window,at.risk_zone,at.cost_per_1k_tokens_micros,
		       at.enabled,at.created_at,at.updated_at,
		       CASE WHEN s.budget_tokens=0 THEN true
		            ELSE s.spent_tokens + $2 <= s.budget_tokens END AS budget_allowed
		FROM agent_instances ai
		JOIN agent_templates at ON at.id=ai.template_id
		JOIN swarms s ON s.id=$1
		WHERE (ai.swarm_id IS NULL OR ai.swarm_id=$1)
		ORDER BY ai.load,ai.created_at`, swarmID, taskMaxTokens)
	if err != nil {
		return nil, fmt.Errorf("查询 scheduler candidates: %w", err)
	}
	defer rows.Close()
	items := make([]orchestrator.Candidate, 0)
	for rows.Next() {
		instance := new(agent.Instance)
		template := new(agent.Template)
		var skills, tools, permissions []byte
		var budgetAllowed bool
		if err := rows.Scan(
			&instance.ID, &instance.TemplateID, &instance.SwarmID, &instance.Name, &instance.Status,
			&instance.CurrentTaskID, &instance.Load, &instance.HeartbeatAt, &instance.Version,
			&instance.CreatedAt, &instance.UpdatedAt,
			&template.ID, &template.Name, &template.Role, &template.Prompt, &template.Model,
			&skills, &tools, &permissions, &template.TemplateVersion, &template.ContextWindow,
			&template.RiskZone, &template.CostPer1KTokensMicros, &template.Enabled,
			&template.CreatedAt, &template.UpdatedAt, &budgetAllowed,
		); err != nil {
			return nil, fmt.Errorf("扫描 scheduler candidate: %w", err)
		}
		if err := json.Unmarshal(skills, &template.Skills); err != nil {
			return nil, fmt.Errorf("解析 candidate skills: %w", err)
		}
		if err := json.Unmarshal(tools, &template.Tools); err != nil {
			return nil, fmt.Errorf("解析 candidate tools: %w", err)
		}
		if err := json.Unmarshal(permissions, &template.Permissions); err != nil {
			return nil, fmt.Errorf("解析 candidate permissions: %w", err)
		}
		contextAffinity := 0.2
		if instance.SwarmID != nil && *instance.SwarmID == swarmID {
			contextAffinity = 1
		}
		items = append(items, orchestrator.Candidate{
			Instance: instance, Template: template, BudgetAllowed: budgetAllowed,
			HistorySuccess: .5, ContextAffinity: contextAffinity, QualityScore: .5, LatencyScore: .5,
		})
	}
	return items, rows.Err()
}

// RecordSchedulerDecision 保存完整 Filter/Score 证据。task_id 反查 tenant/run，调用方不能
// 通过请求体伪造租户归属。
func (r *Repository) RecordSchedulerDecision(ctx context.Context, decision orchestrator.SchedulerDecision) error {
	candidatesRaw, err := marshalJSON(decision.Candidates, "scheduler.candidates")
	if err != nil {
		return err
	}
	filters := make([]orchestrator.CandidateDecision, 0)
	for _, candidate := range decision.Candidates {
		if !candidate.Accepted {
			filters = append(filters, candidate)
		}
	}
	filtersRaw, err := marshalJSON(filters, "scheduler.filters")
	if err != nil {
		return err
	}
	command, err := r.pool.Exec(ctx, `
		INSERT INTO scheduler_decisions(
			id,tenant_id,run_id,task_id,scheduler_id,task_version,queue_score,
			selected_agent_id,selected_score,candidates,filters,reason
		)
		SELECT $2,t.tenant_id,t.swarm_id,t.id,$3,$4,$5,$6,$7,$8,$9,$10
		FROM tasks t WHERE t.id=$1`, decision.TaskID, uuid.New(), decision.SchedulerID,
		decision.TaskVersion, decision.QueueScore, decision.SelectedAgentID, decision.SelectedScore,
		candidatesRaw, filtersRaw, decision.Reason)
	if err != nil {
		return mapWriteError("写入 Scheduler Explain", err)
	}
	if command.RowsAffected() != 1 {
		return fmt.Errorf("%w: scheduler task %s", domain.ErrNotFound, decision.TaskID)
	}
	return nil
}

// BindTask 原子地把 Task 和 Agent 互相绑定；任一 CAS 失败都会整体回滚。
func (r *Repository) BindTask(ctx context.Context, taskID uuid.UUID, taskVersion int64, agentID uuid.UUID, agentVersion int64) error {
	return r.withTx(ctx, func(tx pgx.Tx) error {
		var nextTaskVersion int64
		err := tx.QueryRow(ctx, `
			UPDATE tasks
			SET status='ASSIGNED',assigned_agent_id=$3,version=version+1,updated_at=now()
			WHERE id=$1 AND version=$2 AND status='SCHEDULING'
			RETURNING version`, taskID, taskVersion, agentID).Scan(&nextTaskVersion)
		if err == pgx.ErrNoRows {
			return fmt.Errorf("%w: task %s 已不在 SCHEDULING", domain.ErrConflict, taskID)
		}
		if err != nil {
			return fmt.Errorf("CAS bind task: %w", err)
		}

		var nextAgentVersion int64
		err = tx.QueryRow(ctx, `
			UPDATE agent_instances
			SET status='RESERVED',current_task_id=$3,version=version+1,updated_at=now()
			WHERE id=$1 AND version=$2 AND status='IDLE'
			RETURNING version`, agentID, agentVersion, taskID).Scan(&nextAgentVersion)
		if err == pgx.ErrNoRows {
			return fmt.Errorf("%w: agent %s 已不在 IDLE", domain.ErrConflict, agentID)
		}
		if err != nil {
			return fmt.Errorf("CAS reserve agent: %w", err)
		}

		if err := insertOutbox(ctx, tx, "task", taskID, "task.assigned", nextTaskVersion, map[string]any{
			"id": taskID, "agent_id": agentID, "version": nextTaskVersion,
		}); err != nil {
			return err
		}
		return insertOutbox(ctx, tx, "agent_instance", agentID, "agent.reserved", nextAgentVersion, map[string]any{
			"id": agentID, "task_id": taskID, "version": nextAgentVersion,
		})
	})
}
