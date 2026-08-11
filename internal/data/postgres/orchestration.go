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
	"github.com/licy-yu/agent-os/internal/orchestrator"
)

// ListReconcileTasks 读取 Controller 有权处理的非终态任务。SCHEDULING 必须先按
// updated_at 截止时间过滤再做 LIMIT；否则大量正在正常 Bind 的新鲜任务会占满批次，
// 让真正因 Scheduler SIGKILL 遗留的孤儿状态永远得不到恢复。
func (r *Repository) ListReconcileTasks(ctx context.Context, staleSchedulingBefore time.Time, limit int) ([]*task.Task, error) {
	rows, err := r.pool.Query(ctx, taskSelect+`
		JOIN swarms run_scope ON run_scope.id=t.swarm_id
		WHERE (t.status IN ('CREATED','PLANNING','BLOCKED','RETRY_WAIT')
		       OR (t.status='SCHEDULING' AND t.updated_at <= $1))
		  AND t.tenant_id=run_scope.tenant_id
		  AND run_scope.desired_state='RUNNING'
		  AND run_scope.status IN ('PENDING','RUNNING')
		ORDER BY t.updated_at,t.id
		LIMIT $2`, staleSchedulingBefore, limit)
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
		var tenantID, runID uuid.UUID
		err := tx.QueryRow(ctx, `
			UPDATE tasks
			SET status=$4,version=version+1,updated_at=now()
			WHERE id=$1 AND version=$2 AND status=$3
			RETURNING version,tenant_id,swarm_id`, id, expectedVersion, from, to).
			Scan(&nextVersion, &tenantID, &runID)
		if err == pgx.ErrNoRows {
			return fmt.Errorf("%w: task %s 已被其他控制器修改", domain.ErrConflict, id)
		}
		if err != nil {
			return fmt.Errorf("更新 task 状态: %w", err)
		}
		return insertTenantOutbox(ctx, tx, tenantID, "task", id, eventType, nextVersion, map[string]any{
			"id": id, "run_id": runID, "from": from, "to": to, "version": nextVersion,
		})
	})
	return nextVersion, err
}

// ActivateRegisteredAgents 完成 REGISTERED -> IDLE，并为每个实例产生独立事件。
func (r *Repository) ActivateRegisteredAgents(ctx context.Context, limit int) (int, error) {
	count := 0
	err := r.withTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id,tenant_id,version FROM agent_instances
			WHERE status='REGISTERED'
			ORDER BY created_at,id
			FOR UPDATE SKIP LOCKED
			LIMIT $1`, limit)
		if err != nil {
			return fmt.Errorf("锁定 REGISTERED agents: %w", err)
		}
		type lockedAgent struct {
			id, tenantID uuid.UUID
			version      int64
		}
		locked := make([]lockedAgent, 0, limit)
		for rows.Next() {
			var value lockedAgent
			if err := rows.Scan(&value.id, &value.tenantID, &value.version); err != nil {
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
			if err := insertTenantOutbox(ctx, tx, value.tenantID, "agent_instance", value.id, "agent.idle", value.version+1, map[string]any{
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
		  AND t.tenant_id=run_scope.tenant_id
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
		WHERE ai.tenant_id=s.tenant_id AND at.tenant_id=s.tenant_id
		  AND (ai.swarm_id IS NULL OR ai.swarm_id=$1)
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
//
// “未选中 Agent”是 READY Task 在某一版本上的当前调度解释，而不是每秒都发生一次的
// 领域事件。因此同一 task_id + task_version 最多保留一条未选中记录：证据不变时零写入，
// 候选集合或拒绝原因变化时原地刷新。成功选中的决策仍逐次追加，完整保留真正的 Reserve/
// Bind 审计轨迹。这个约束在仓储事务中实现，不依赖单进程内存，能覆盖多副本 Scheduler。
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
	decisionID := uuid.New()
	return r.withTx(ctx, func(tx pgx.Tx) error {
		if decision.SelectedAgentID == nil {
			// PostgreSQL advisory transaction lock 以 Task UUID 的稳定哈希作为锁键。
			// 它只串行同一 Task 的 Explain 合并，不会阻塞其他 Task；事务结束自动释放，
			// 即使 Scheduler 进程崩溃也不会遗留锁。仅靠 SELECT ... FOR UPDATE 无法保护
			// “尚无记录”这一空集合，两个副本首次写入时仍可能各插一行。
			if _, err := tx.Exec(ctx, `
				SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0::bigint))`,
				decision.TaskID.String()); err != nil {
				return fmt.Errorf("锁定 Scheduler Explain 合并键: %w", err)
			}

			var existingID uuid.UUID
			err := tx.QueryRow(ctx, `
				SELECT id
				FROM scheduler_decisions
				WHERE task_id=$1 AND task_version=$2 AND selected_agent_id IS NULL
				ORDER BY created_at DESC,id DESC
				LIMIT 1
				FOR UPDATE`, decision.TaskID, decision.TaskVersion).Scan(&existingID)
			switch {
			case err == nil:
				// QueueScore 会随等待时间连续变化，不能把它作为去重条件，否则即使候选
				// 完全不变仍会每秒产生一次 MVCC/WAL 写入。只有候选证据或结论变化才
				// 刷新整行，此时顺便记录最新分数和实际观察到变化的 Scheduler。
				_, updateErr := tx.Exec(ctx, `
					UPDATE scheduler_decisions
					SET scheduler_id=$2,queue_score=$3,candidates=$4::jsonb,
					    filters=$5::jsonb,reason=$6,created_at=now()
					WHERE id=$1
					  AND (candidates,filters,reason) IS DISTINCT FROM
					      ($4::jsonb,$5::jsonb,$6::text)`,
					existingID, decision.SchedulerID, decision.QueueScore,
					candidatesRaw, filtersRaw, decision.Reason)
				if updateErr != nil {
					return mapWriteError("合并 Scheduler Explain", updateErr)
				}
				return nil
			case !errors.Is(err, pgx.ErrNoRows):
				return fmt.Errorf("查询待合并 Scheduler Explain: %w", err)
			}
		}

		command, err := tx.Exec(ctx, `
			INSERT INTO scheduler_decisions(
				id,tenant_id,run_id,task_id,scheduler_id,task_version,queue_score,
				selected_agent_id,selected_score,candidates,filters,reason
			)
			SELECT $2,t.tenant_id,t.swarm_id,t.id,$3,$4,$5,$6,$7,$8,$9,$10
			FROM tasks t WHERE t.id=$1`, decision.TaskID, decisionID, decision.SchedulerID,
			decision.TaskVersion, decision.QueueScore, decision.SelectedAgentID, decision.SelectedScore,
			candidatesRaw, filtersRaw, decision.Reason)
		if err != nil {
			return mapWriteError("写入 Scheduler Explain", err)
		}
		if command.RowsAffected() != 1 {
			return fmt.Errorf("%w: scheduler task %s", domain.ErrNotFound, decision.TaskID)
		}
		return nil
	})
}

// BindTask 原子地把 Task 和 Agent 互相绑定；任一 CAS 失败都会整体回滚。
func (r *Repository) BindTask(ctx context.Context, taskID uuid.UUID, taskVersion int64, agentID uuid.UUID, agentVersion int64) error {
	return r.withTx(ctx, func(tx pgx.Tx) error {
		var tenantID, runID uuid.UUID
		if err := tx.QueryRow(ctx, `
			SELECT tenant_id,swarm_id FROM tasks WHERE id=$1 AND version=$2 FOR UPDATE`,
			taskID, taskVersion).Scan(&tenantID, &runID); err != nil {
			return mapReadError("锁定 BindTask 租户", err)
		}
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
			WHERE id=$1 AND version=$2 AND status='IDLE' AND tenant_id=$4
			RETURNING version`, agentID, agentVersion, taskID, tenantID).Scan(&nextAgentVersion)
		if err == pgx.ErrNoRows {
			return fmt.Errorf("%w: agent %s 已不在 IDLE", domain.ErrConflict, agentID)
		}
		if err != nil {
			return fmt.Errorf("CAS reserve agent: %w", err)
		}

		if err := insertTenantOutbox(ctx, tx, tenantID, "task", taskID, "task.assigned", nextTaskVersion, map[string]any{
			"id": taskID, "run_id": runID, "agent_id": agentID, "version": nextTaskVersion,
		}); err != nil {
			return err
		}
		return insertTenantOutbox(ctx, tx, tenantID, "agent_instance", agentID, "agent.reserved", nextAgentVersion, map[string]any{
			"id": agentID, "run_id": runID, "task_id": taskID, "version": nextAgentVersion,
		})
	})
}
