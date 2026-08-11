package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/licy-yu/agent-os/internal/domain"
	"github.com/licy-yu/agent-os/internal/domain/run"
	"github.com/licy-yu/agent-os/internal/planning"
	"github.com/licy-yu/agent-os/internal/runcontrol"
)

// PlanningCatalog 从管理员已经启用的 AgentTemplate/Tool Registry 汇总 PlanCompiler 事实。
// Planner 自己生成的 capability/tool 名不会进入此集合，因而不能借自然语言扩大权限。
func (r *Repository) PlanningCatalog(ctx context.Context, tenantID uuid.UUID) (runcontrol.Catalog, error) {
	result := runcontrol.Catalog{
		Capabilities: map[string]float64{}, Tools: map[string]bool{},
		Models: map[string]bool{}, Permissions: map[string]bool{},
	}
	rows, err := r.pool.Query(ctx, `
		SELECT skills,model,permissions
		FROM agent_templates
		WHERE tenant_id=$1 AND enabled=true`, tenantID)
	if err != nil {
		return result, fmt.Errorf("读取 Agent Catalog: %w", err)
	}
	for rows.Next() {
		var skillsRaw, permissionsRaw []byte
		var model string
		if err := rows.Scan(&skillsRaw, &model, &permissionsRaw); err != nil {
			rows.Close()
			return result, err
		}
		var skills map[string]float64
		var permissions []string
		if err := json.Unmarshal(skillsRaw, &skills); err != nil {
			rows.Close()
			return result, fmt.Errorf("解析 Agent skills: %w", err)
		}
		if err := json.Unmarshal(permissionsRaw, &permissions); err != nil {
			rows.Close()
			return result, fmt.Errorf("解析 Agent permissions: %w", err)
		}
		for name, level := range skills {
			key := strings.ToLower(strings.TrimSpace(name))
			if level > result.Capabilities[key] {
				result.Capabilities[key] = level
			}
		}
		if value := strings.ToLower(strings.TrimSpace(model)); value != "" {
			result.Models[value] = true
		}
		for _, permission := range permissions {
			if value := strings.ToLower(strings.TrimSpace(permission)); value != "" {
				result.Permissions[value] = true
			}
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return result, err
	}
	rows.Close()

	rows, err = r.pool.Query(ctx, `
		SELECT name FROM tool_registry WHERE tenant_id=$1 AND enabled=true`, tenantID)
	if err != nil {
		return result, fmt.Errorf("读取 Tool Catalog: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return result, err
		}
		result.Tools[strings.ToLower(name)] = true
	}
	return result, rows.Err()
}

// CreateCompiledRun 在单事务中写入 Run、Plan、Task、Dependency、Gate 和 Outbox。
// 任何一个合同不能持久化时，整个 Run 都不会以“半创建”状态出现在控制台。
func (r *Repository) CreateCompiledRun(ctx context.Context, record runcontrol.CreateRecord) (*runcontrol.RunView, error) {
	err := r.withTx(ctx, func(tx pgx.Tx) error {
		if err := r.insertRun(ctx, tx, record); err != nil {
			return err
		}
		if err := r.insertPlanAndTasks(ctx, tx, record); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE swarms
			SET current_plan_version_id=$2,status='RUNNING',admitted_at=$3,version=version+1,updated_at=$3
			WHERE id=$1 AND tenant_id=$4`, record.ID, record.PlanVersionID, record.CreatedAt, record.TenantID); err != nil {
			return fmt.Errorf("激活首个 PlanVersion: %w", err)
		}
		return insertTenantOutbox(ctx, tx, record.TenantID, "swarm", record.ID, "run.started", 2, map[string]any{
			"id": record.ID, "plan_version_id": record.PlanVersionID, "execution_engine": record.ExecutionEngine,
		})
	})
	if err != nil {
		return nil, err
	}
	return r.GetRun(ctx, record.TenantID, record.ID)
}

func (r *Repository) insertRun(ctx context.Context, tx pgx.Tx, record runcontrol.CreateRecord) error {
	normalizedGoal, err := marshalJSON(record.NormalizedGoal, "run.normalized_goal")
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO swarms(
			id,tenant_id,project_id,name,goal,normalized_goal,status,desired_state,
			execution_engine,budget_tokens,budget_cost_micros,max_agents,priority,policy,
			deadline,version,created_at,updated_at
		) VALUES($1,$2,$3,$4,$5,$6,'CREATED','RUNNING',$7,$8,$9,$10,$11,'{}'::jsonb,$12,1,$13,$13)`,
		record.ID, record.TenantID, record.ProjectID, record.Name, record.Goal, normalizedGoal,
		record.ExecutionEngine, record.BudgetTokens, record.BudgetCostMicros, record.MaxAgents,
		record.Priority, record.Deadline, record.CreatedAt)
	if err != nil {
		return mapWriteError("创建 Run", err)
	}
	return insertTenantOutbox(ctx, tx, record.TenantID, "swarm", record.ID, "run.created", 1, map[string]any{
		"id": record.ID, "name": record.Name, "goal": record.Goal, "status": "CREATED",
	})
}

func (r *Repository) insertPlanAndTasks(ctx context.Context, tx pgx.Tx, record runcontrol.CreateRecord) error {
	candidateRaw, err := marshalJSON(record.PlanCandidate, "plan.candidate")
	if err != nil {
		return err
	}
	compiledRaw, err := marshalJSON(record.ExecutablePlan, "plan.compiled")
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO plan_versions(
			id,tenant_id,run_id,version_no,source,status,candidate_spec,compiled_spec,
			validation_errors,diff_from_previous,compiler_version,content_hash,created_by,
			activated_at,created_at
		) VALUES($1,$2,$3,$4,$5,'ACTIVE',$6,$7,'[]'::jsonb,'{}'::jsonb,$8,$9,$10,$11,$11)`,
		record.PlanVersionID, record.TenantID, record.ID, record.PlanVersion, record.PlanSource,
		candidateRaw, compiledRaw, record.CompilerVersion, record.ExecutablePlan.GraphHash,
		record.CreatedBy, record.CreatedAt)
	if err != nil {
		return mapWriteError("创建 PlanVersion", err)
	}
	if err := insertTenantOutbox(ctx, tx, record.TenantID, "plan_version", record.PlanVersionID,
		"plan.activated", int64(record.PlanVersion), map[string]any{
			"id": record.PlanVersionID, "run_id": record.ID, "version": record.PlanVersion,
			"graph_hash": record.ExecutablePlan.GraphHash,
		}); err != nil {
		return err
	}

	blocking := make(map[uuid.UUID]int)
	for _, dependency := range record.ExecutablePlan.Dependencies {
		if dependency.Type != planning.DependencySoft {
			blocking[dependency.TaskID]++
		}
	}
	critical := make(map[uuid.UUID]float64, len(record.ExecutablePlan.CriticalPath))
	for index, id := range record.ExecutablePlan.CriticalPath {
		critical[id] = float64(len(record.ExecutablePlan.CriticalPath) - index)
	}

	for _, contract := range record.ExecutablePlan.Tasks {
		status := "READY"
		if blocking[contract.ID] > 0 {
			status = "BLOCKED"
		}
		inputRaw, _ := marshalJSON(map[string]any{"inputs": contract.Inputs}, "task.input")
		requirementsRaw, _ := marshalJSON(map[string]any{
			"skills": contract.Requirements.Capabilities, "tools": contract.Requirements.Tools,
			"permissions": contract.Requirements.Permissions, "models": contract.Requirements.Models,
			"max_context_tokens": contract.ContextPolicy.MaxTokens, "risk_zone": contract.Requirements.RiskZone,
		}, "task.requirements")
		checkNames := make([]string, 0, len(contract.Acceptance))
		for _, criterion := range contract.Acceptance {
			checkNames = append(checkNames, criterion.Name)
		}
		acceptanceRaw, _ := marshalJSON(map[string]any{"required_checks": checkNames}, "task.acceptance")
		executionPolicyRaw, _ := marshalJSON(map[string]any{
			"max_attempts":           contract.RetryPolicy.MaxAttempts,
			"max_handoffs":           contract.RuntimeGuard.MaxHandoffs,
			"max_tokens":             contract.Budget.MaxTokens,
			"max_tool_calls":         contract.RuntimeGuard.MaxToolCalls,
			"max_no_progress_rounds": contract.RuntimeGuard.MaxNoProgressRounds,
			"timeout_seconds":        contract.Budget.MaxDurationSeconds,
		}, "task.execution_policy")
		outputRaw, _ := marshalJSON(contract.Output, "task.output_spec")
		artifactRaw, _ := marshalJSON(contract.Output.Artifacts, "task.artifact_contract")
		contextRaw, _ := marshalJSON(contract.ContextPolicy, "task.context_policy")
		effectRaw, _ := marshalJSON(contract.SideEffectPolicy, "task.side_effect_policy")
		retryRaw, _ := marshalJSON(contract.RetryPolicy, "task.retry_policy")
		_, err := tx.Exec(ctx, `
			INSERT INTO tasks(
				id,tenant_id,swarm_id,plan_version_id,logical_key,task_type,name,goal,status,
				priority,input,requirements,acceptance,execution_policy,output_spec,artifact_contract,
				context_policy,side_effect_policy,retry_policy,critical_path_weight,available_at,
				deadline,version,created_at,updated_at
			) VALUES(
				$1,$2,$3,$4,$5,'GENERAL',$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,
				$19,$20,$21,1,$22,$22
			)`,
			contract.ID, record.TenantID, record.ID, record.PlanVersionID, contract.ID.String(),
			contract.Name, contract.Goal, status, contract.Priority, inputRaw, requirementsRaw,
			acceptanceRaw, executionPolicyRaw, outputRaw, artifactRaw, contextRaw, effectRaw,
			retryRaw, critical[contract.ID], record.CreatedAt, contract.Deadline, record.CreatedAt)
		if err != nil {
			return mapWriteError("创建编译后 Task", err)
		}
		for _, criterion := range contract.Acceptance {
			configRaw, _ := marshalJSON(criterion.Config, "acceptance_gate.config")
			_, err := tx.Exec(ctx, `
				INSERT INTO acceptance_gates(id,tenant_id,task_id,gate_key,gate_type,config,required,version)
				VALUES($1,$2,$3,$4,$5,$6,true,1)`, uuid.New(), record.TenantID, contract.ID,
				criterion.Name, normalizeGateType(criterion.Type), configRaw)
			if err != nil {
				return mapWriteError("创建 AcceptanceGate", err)
			}
		}
		if err := insertTenantOutbox(ctx, tx, record.TenantID, "task", contract.ID, "task.created", 1, map[string]any{
			"id": contract.ID, "run_id": record.ID, "plan_version_id": record.PlanVersionID,
			"status": status,
		}); err != nil {
			return err
		}
	}

	for _, dependency := range record.ExecutablePlan.Dependencies {
		_, err := tx.Exec(ctx, `
			INSERT INTO task_dependencies(task_id,depends_on_id,dependency_type,artifact_type,required)
			VALUES($1,$2,$3,NULLIF($4,''),$5)`, dependency.TaskID, dependency.DependsOnID,
			dependency.Type, dependency.Artifact, dependency.Type != planning.DependencySoft)
		if err != nil {
			return mapWriteError("创建编译后 TaskDependency", err)
		}
	}
	return nil
}

func normalizeGateType(value string) string {
	switch normalized := strings.ToUpper(strings.TrimSpace(value)); normalized {
	case "JSON_SCHEMA", "COMMAND", "UNIT_TEST", "COVERAGE", "FILE_EXISTS", "SECURITY",
		"POLICY", "ARTIFACT", "LLM_JUDGE", "HUMAN_APPROVAL":
		return normalized
	default:
		// 未知扩展 Gate 以 JSON_SCHEMA 保存并在 config 中交给插件解释，避免写入非法枚举。
		return "JSON_SCHEMA"
	}
}

// TransitionRun 以 tenant + version + status 做 CAS。Cancel 同事务隔离所有未完成 Worker，
// 迟到结果随后会因 Task/Attempt 状态和 fencing token 不匹配被拒绝。
func (r *Repository) TransitionRun(ctx context.Context, tenantID, runID uuid.UUID, expectedVersion int64,
	next run.Status, desired, reason string,
) (*runcontrol.RunView, error) {
	err := r.withTx(ctx, func(tx pgx.Tx) error {
		var current run.Status
		if err := tx.QueryRow(ctx, `
			SELECT status FROM swarms WHERE id=$1 AND tenant_id=$2 AND version=$3 FOR UPDATE`,
			runID, tenantID, expectedVersion).Scan(&current); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("%w: Run 已被其他控制器修改", domain.ErrConflict)
			}
			return err
		}
		command, err := tx.Exec(ctx, `
			UPDATE swarms SET status=$4,desired_state=$5,
				paused_at=CASE WHEN $4='PAUSED' THEN now() ELSE paused_at END,
				canceled_at=CASE WHEN $4='CANCELED' THEN now() ELSE canceled_at END,
				version=version+1,updated_at=now()
			WHERE id=$1 AND tenant_id=$2 AND version=$3 AND status=$6`,
			runID, tenantID, expectedVersion, next, desired, current)
		if err != nil || command.RowsAffected() != 1 {
			return fmt.Errorf("%w: Run 状态 CAS 失败", domain.ErrConflict)
		}
		if next == run.StatusCanceled {
			if err := r.cancelRunWork(ctx, tx, tenantID, runID); err != nil {
				return err
			}
		}
		return insertTenantOutbox(ctx, tx, tenantID, "swarm", runID,
			"run."+strings.ToLower(string(next)), expectedVersion+1, map[string]any{
				"id": runID, "from": current, "to": next, "reason": reason,
			})
	})
	if err != nil {
		return nil, err
	}
	return r.GetRun(ctx, tenantID, runID)
}

func (r *Repository) cancelRunWork(ctx context.Context, tx pgx.Tx, tenantID, runID uuid.UUID) error {
	if _, err := tx.Exec(ctx, `
		UPDATE task_attempts a SET status='ABORTED',finished_at=now(),
			error_code='RUN_CANCELED',error_message='Run 被用户取消',updated_at=now()
		FROM tasks t WHERE a.task_id=t.id AND t.tenant_id=$1 AND t.swarm_id=$2
		  AND a.status IN ('CREATED','RUNNING','REVIEW')`, tenantID, runID); err != nil {
		return fmt.Errorf("中止 Run Attempts: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE agent_instances ai SET status='IDLE',current_task_id=NULL,load=0,
			version=version+1,updated_at=now()
		FROM tasks t WHERE ai.current_task_id=t.id AND t.tenant_id=$1 AND t.swarm_id=$2`,
		tenantID, runID); err != nil {
		return fmt.Errorf("释放 Run Agents: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE tasks SET status='CANCELED',assigned_agent_id=NULL,version=version+1,updated_at=now()
		WHERE tenant_id=$1 AND swarm_id=$2
		  AND status NOT IN ('SUCCEEDED','FAILED','CANCELED','REJECTED','SKIPPED')`, tenantID, runID); err != nil {
		return fmt.Errorf("取消 Run Tasks: %w", err)
	}
	return nil
}

// ActivateReplan 保留旧计划和终态 Task，只取消未终结工作并激活新 PlanVersion。
func (r *Repository) ActivateReplan(ctx context.Context, tenantID, runID uuid.UUID, expectedVersion int64,
	record runcontrol.CreateRecord, reason string,
) (*runcontrol.RunView, error) {
	err := r.withTx(ctx, func(tx pgx.Tx) error {
		var currentPlanID *uuid.UUID
		if err := tx.QueryRow(ctx, `
			SELECT current_plan_version_id FROM swarms
			WHERE id=$1 AND tenant_id=$2 AND version=$3 FOR UPDATE`, runID, tenantID, expectedVersion).
			Scan(&currentPlanID); err != nil {
			return mapReadError("锁定 Replan Run", err)
		}
		if currentPlanID != nil {
			if _, err := tx.Exec(ctx, `
				UPDATE plan_versions SET status='SUPERSEDED'
				WHERE id=$1 AND tenant_id=$2 AND status='ACTIVE'`, *currentPlanID, tenantID); err != nil {
				return err
			}
		}
		var activeEffects bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS(
				SELECT 1 FROM effects
				WHERE tenant_id=$1 AND run_id=$2
				  AND status IN ('EXECUTING','UNKNOWN','RECONCILING','COMPENSATING')
			)`, tenantID, runID).Scan(&activeEffects); err != nil {
			return fmt.Errorf("检查 Replan Effect 安全点: %w", err)
		}
		if activeEffects {
			return fmt.Errorf("%w: 仍有未决 Effect，必须先完成对账或补偿", domain.ErrConflict)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE task_attempts a SET status='ABORTED',finished_at=now(),
				error_code='PLAN_SUPERSEDED',error_message='Replan 已激活新计划',updated_at=now()
			FROM tasks t WHERE a.task_id=t.id AND t.tenant_id=$1 AND t.swarm_id=$2
			  AND a.status IN ('CREATED','RUNNING')`, tenantID, runID); err != nil {
			return fmt.Errorf("隔离旧 Plan Attempts: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE agent_instances ai SET status='IDLE',current_task_id=NULL,load=0,
				version=version+1,updated_at=now()
			FROM tasks t WHERE ai.current_task_id=t.id AND t.tenant_id=$1 AND t.swarm_id=$2`,
			tenantID, runID); err != nil {
			return fmt.Errorf("释放旧 Plan Agents: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE tasks SET status='CANCELED',assigned_agent_id=NULL,version=version+1,updated_at=now()
			WHERE tenant_id=$1 AND swarm_id=$2
			  AND status NOT IN ('SUCCEEDED','FAILED','CANCELED','REJECTED','SKIPPED')`, tenantID, runID); err != nil {
			return fmt.Errorf("取消被替代 Plan Tasks: %w", err)
		}
		if err := r.insertPlanAndTasks(ctx, tx, record); err != nil {
			return err
		}
		normalizedGoal, err := marshalJSON(record.NormalizedGoal, "run.normalized_goal")
		if err != nil {
			return err
		}
		command, err := tx.Exec(ctx, `
			UPDATE swarms SET goal=$4,normalized_goal=$5,current_plan_version_id=$6,
				status='RUNNING',desired_state='RUNNING',version=version+1,updated_at=now()
			WHERE id=$1 AND tenant_id=$2 AND version=$3`, runID, tenantID, expectedVersion,
			record.Goal, normalizedGoal, record.PlanVersionID)
		if err != nil || command.RowsAffected() != 1 {
			return fmt.Errorf("%w: 激活 Replan 时 Run 已变化", domain.ErrConflict)
		}
		return insertTenantOutbox(ctx, tx, tenantID, "swarm", runID, "run.replanned", expectedVersion+1,
			map[string]any{"id": runID, "plan_version_id": record.PlanVersionID, "reason": reason})
	})
	if err != nil {
		return nil, err
	}
	return r.GetRun(ctx, tenantID, runID)
}

func (r *Repository) GetRun(ctx context.Context, tenantID, runID uuid.UUID) (*runcontrol.RunView, error) {
	value, err := scanRunView(r.pool.QueryRow(ctx, runViewSelect+` WHERE s.id=$1 AND s.tenant_id=$2`, runID, tenantID))
	if err != nil {
		return nil, mapReadError("读取 Run", err)
	}
	return value, nil
}

func (r *Repository) ListRuns(ctx context.Context, tenantID uuid.UUID, limit int) ([]runcontrol.RunView, error) {
	rows, err := r.pool.Query(ctx, runViewSelect+`
		WHERE s.tenant_id=$1 ORDER BY s.created_at DESC,s.id DESC LIMIT $2`, tenantID, limit)
	if err != nil {
		return nil, fmt.Errorf("列出 Runs: %w", err)
	}
	defer rows.Close()
	items := make([]runcontrol.RunView, 0, limit)
	for rows.Next() {
		value, err := scanRunView(rows)
		if err != nil {
			return nil, fmt.Errorf("扫描 Run: %w", err)
		}
		items = append(items, *value)
	}
	return items, rows.Err()
}

// AttachWorkflow 只允许首次绑定，或幂等写入完全相同的 Workflow 身份。同一个业务 Run
// 若出现不同 WorkflowID/RunID，说明发生了双启动风险，必须返回冲突而不是覆盖证据。
func (r *Repository) AttachWorkflow(ctx context.Context, tenantID, runID uuid.UUID, ref runcontrol.WorkflowRef) error {
	if strings.TrimSpace(ref.WorkflowID) == "" || strings.TrimSpace(ref.RunID) == "" {
		return fmt.Errorf("%w: Temporal Workflow 身份不能为空", runcontrol.ErrInvalidRequest)
	}
	return r.withTx(ctx, func(tx pgx.Tx) error {
		command, err := tx.Exec(ctx, `
			UPDATE swarms
			SET temporal_workflow_id=$3,temporal_run_id=$4,runtime_attached_at=now(),updated_at=now()
			WHERE id=$1 AND tenant_id=$2 AND execution_engine='TEMPORAL'
			  AND (temporal_workflow_id IS NULL OR temporal_workflow_id=$3)
			  AND (temporal_run_id IS NULL OR temporal_run_id=$4)`,
			runID, tenantID, strings.TrimSpace(ref.WorkflowID), strings.TrimSpace(ref.RunID))
		if err != nil {
			return mapWriteError("绑定 Temporal Workflow", err)
		}
		if command.RowsAffected() != 1 {
			return fmt.Errorf("%w: Run 不存在、不是 TEMPORAL 或已绑定其他 Workflow", domain.ErrConflict)
		}
		return insertTenantOutbox(ctx, tx, tenantID, "swarm", runID, "run.workflow_attached", 1,
			map[string]any{"workflow_id": ref.WorkflowID, "temporal_run_id": ref.RunID})
	})
}

const runViewSelect = `
	SELECT s.id,s.tenant_id,s.project_id,s.name,s.goal,s.normalized_goal,s.status,
	       s.desired_state,s.execution_engine,s.budget_tokens,s.budget_cost_micros,
	       s.spent_tokens,s.spent_cost_micros,s.max_agents,s.priority,
	       s.current_plan_version_id,COALESCE(p.version_no,0),
	       COALESCE(s.temporal_workflow_id,''),COALESCE(s.temporal_run_id,''),
	       COALESCE((SELECT jsonb_object_agg(x.status,x.count) FROM (
	           SELECT status,count(*) FROM tasks WHERE swarm_id=s.id GROUP BY status
	       ) x),'{}'::jsonb),
	       (SELECT count(*) FROM interactions i WHERE i.run_id=s.id AND i.status='WAITING'),
	       s.deadline,s.version,s.created_at,s.updated_at
	FROM swarms s LEFT JOIN plan_versions p ON p.id=s.current_plan_version_id`

func scanRunView(row rowScanner) (*runcontrol.RunView, error) {
	value := new(runcontrol.RunView)
	var normalized, taskStatuses []byte
	err := row.Scan(&value.ID, &value.TenantID, &value.ProjectID, &value.Name, &value.Goal,
		&normalized, &value.Status, &value.DesiredState, &value.ExecutionEngine,
		&value.BudgetTokens, &value.BudgetCostMicros, &value.SpentTokens, &value.SpentCostMicros,
		&value.MaxAgents, &value.Priority, &value.CurrentPlanVersionID, &value.CurrentPlanVersion,
		&value.TemporalWorkflowID, &value.TemporalRunID,
		&taskStatuses, &value.PendingInteractions, &value.Deadline, &value.Version,
		&value.CreatedAt, &value.UpdatedAt)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(normalized, &value.NormalizedGoal); err != nil {
		return nil, fmt.Errorf("解析 run.normalized_goal: %w", err)
	}
	if err := json.Unmarshal(taskStatuses, &value.TaskStatuses); err != nil {
		return nil, fmt.Errorf("解析 run.task_statuses: %w", err)
	}
	return value, nil
}

func (r *Repository) GetPlan(ctx context.Context, tenantID, runID, planID uuid.UUID) (*runcontrol.PlanView, error) {
	value := new(runcontrol.PlanView)
	var candidate, compiled, errorsRaw, diff []byte
	err := r.pool.QueryRow(ctx, `
		SELECT id,run_id,version_no,source,status,candidate_spec,compiled_spec,
		       validation_errors,diff_from_previous,compiler_version,content_hash,created_by,created_at
		FROM plan_versions WHERE id=$1 AND run_id=$2 AND tenant_id=$3`, planID, runID, tenantID).Scan(
		&value.ID, &value.RunID, &value.Version, &value.Source, &value.Status, &candidate,
		&compiled, &errorsRaw, &diff, &value.CompilerVersion, &value.ContentHash,
		&value.CreatedBy, &value.CreatedAt)
	if err != nil {
		return nil, mapReadError("读取 PlanVersion", err)
	}
	if err := json.Unmarshal(candidate, &value.Candidate); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(compiled, &value.Compiled); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(errorsRaw, &value.ValidationErrors); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(diff, &value.Diff); err != nil {
		return nil, err
	}
	return value, nil
}

// insertTenantOutbox 是 V1.5 写路径的租户感知版本。旧 V1 helper 继续服务默认租户，
// 新代码必须显式传入 tenant，防止 Timeline/审计事件落入错误租户。
func insertTenantOutbox(ctx context.Context, tx pgx.Tx, tenantID uuid.UUID, aggregateType string,
	aggregateID uuid.UUID, eventType string, version int64, payload any,
) error {
	raw, err := marshalJSON(payload, "outbox.payload")
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO event_outbox(
			id,tenant_id,aggregate_type,aggregate_id,event_type,aggregate_version,payload
		) VALUES($1,$2,$3,$4,$5,$6,$7)`, uuid.New(), tenantID, aggregateType,
		aggregateID, eventType, version, raw)
	if err != nil {
		return mapWriteError("写入 tenant outbox", err)
	}
	return nil
}
