// Package postgres 实现 SwarmOS 控制面的 PostgreSQL 仓储。
//
// 写操作会在同一事务中同时提交领域数据和 Outbox 事件；任何一步失败都会回滚，
// 从根源上避免“资源创建成功但下游永远收不到事件”的双写问题。
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/licy-yu/agent-os/internal/data/migrate"
	"github.com/licy-yu/agent-os/internal/domain"
	"github.com/licy-yu/agent-os/internal/domain/agent"
	"github.com/licy-yu/agent-os/internal/domain/swarm"
	"github.com/licy-yu/agent-os/internal/domain/task"
)

// Repository 同时实现 swarm、agent 和 task 三个领域仓储端口。
// 阶段 2 会在同一类型上增加调度所需的 CAS/Lease 数据操作。
type Repository struct {
	pool *pgxpool.Pool
}

// New 创建连接池并立即 Ping，确保错误在进程启动阶段暴露。
func New(ctx context.Context, dsn string) (*Repository, error) {
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("解析 PostgreSQL DSN: %w", err)
	}
	poolConfig.MaxConns = 20
	poolConfig.MinConns = 2
	poolConfig.MaxConnLifetime = time.Hour
	poolConfig.MaxConnIdleTime = 15 * time.Minute
	poolConfig.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("创建 PostgreSQL 连接池: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("连接 PostgreSQL: %w", err)
	}
	return &Repository{pool: pool}, nil
}

// Migrate 应用二进制内嵌的数据库迁移。
func (r *Repository) Migrate(ctx context.Context) error {
	return migrate.Up(ctx, r.pool)
}

// Ping 供健康检查判断数据库是否可用。
func (r *Repository) Ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

// Close 释放连接池。pgxpool.Close 会等待已借出的连接归还。
func (r *Repository) Close() { r.pool.Close() }

// Create 创建蜂群，并写入 swarm.created 事件。
func (r *Repository) Create(ctx context.Context, value *swarm.Swarm) error {
	return r.withTx(ctx, func(tx pgx.Tx) error {
		policy, err := marshalJSON(value.Policy, "swarm.policy")
		if err != nil {
			return err
		}
		err = tx.QueryRow(ctx, `
			INSERT INTO swarms (
				id,name,goal,status,budget_tokens,budget_cost_micros,spent_tokens,
				spent_cost_micros,max_agents,policy,version,created_at,updated_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
			RETURNING created_at,updated_at`,
			value.ID, value.Name, value.Goal, value.Status, value.BudgetTokens,
			value.BudgetCostMicros, value.SpentTokens, value.SpentCostMicros,
			value.MaxAgents, policy, value.Version, value.CreatedAt, value.UpdatedAt,
		).Scan(&value.CreatedAt, &value.UpdatedAt)
		if err != nil {
			return mapWriteError("创建 swarm", err)
		}
		return insertOutbox(ctx, tx, "swarm", value.ID, "swarm.created", value.Version, map[string]any{
			"id": value.ID, "name": value.Name, "goal": value.Goal, "status": value.Status,
		})
	})
}

// Get 读取单个蜂群。
func (r *Repository) Get(ctx context.Context, id uuid.UUID) (*swarm.Swarm, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT id,name,goal,status,budget_tokens,budget_cost_micros,spent_tokens,
		       spent_cost_micros,max_agents,policy,version,created_at,updated_at
		FROM swarms WHERE id=$1`, id)
	value, err := scanSwarm(row)
	if err != nil {
		return nil, mapReadError("读取 swarm", err)
	}
	return value, nil
}

// List 使用 keyset pagination 稳定列出蜂群。
func (r *Repository) List(ctx context.Context, page domain.Page) ([]*swarm.Swarm, error) {
	query := `
		SELECT id,name,goal,status,budget_tokens,budget_cost_micros,spent_tokens,
		       spent_cost_micros,max_agents,policy,version,created_at,updated_at
		FROM swarms`
	args := make([]any, 0, 3)
	if page.Cursor != nil {
		query += ` WHERE (created_at,id) < ($1,$2)`
		args = append(args, page.Cursor.CreatedAt, page.Cursor.ID)
	}
	query += fmt.Sprintf(" ORDER BY created_at DESC,id DESC LIMIT $%d", len(args)+1)
	args = append(args, page.Size+1)

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("列出 swarm: %w", err)
	}
	defer rows.Close()
	items := make([]*swarm.Swarm, 0, page.Size+1)
	for rows.Next() {
		value, err := scanSwarm(rows)
		if err != nil {
			return nil, fmt.Errorf("扫描 swarm: %w", err)
		}
		items = append(items, value)
	}
	return items, rows.Err()
}

// CreateTemplate 创建不可变版本的 Agent 模板。
func (r *Repository) CreateTemplate(ctx context.Context, value *agent.Template) error {
	return r.withTx(ctx, func(tx pgx.Tx) error {
		skills, err := marshalJSON(value.Skills, "agent_template.skills")
		if err != nil {
			return err
		}
		tools, err := marshalJSON(value.Tools, "agent_template.tools")
		if err != nil {
			return err
		}
		permissions, err := marshalJSON(value.Permissions, "agent_template.permissions")
		if err != nil {
			return err
		}
		err = tx.QueryRow(ctx, `
			INSERT INTO agent_templates (
				id,name,role,prompt,model,skills,tools,permissions,template_version,
				context_window,risk_zone,cost_per_1k_tokens_micros,enabled,created_at,updated_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
			RETURNING created_at,updated_at`,
			value.ID, value.Name, value.Role, value.Prompt, value.Model, skills,
			tools, permissions, value.TemplateVersion, value.ContextWindow, value.RiskZone,
			value.CostPer1KTokensMicros, value.Enabled, value.CreatedAt, value.UpdatedAt,
		).Scan(&value.CreatedAt, &value.UpdatedAt)
		if err != nil {
			return mapWriteError("创建 agent template", err)
		}
		return insertOutbox(ctx, tx, "agent_template", value.ID, "agent_template.created", 1, map[string]any{
			"id": value.ID, "name": value.Name, "template_version": value.TemplateVersion,
		})
	})
}

func (r *Repository) GetTemplate(ctx context.Context, id uuid.UUID) (*agent.Template, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT id,name,role,prompt,model,skills,tools,permissions,template_version,
		       context_window,risk_zone,cost_per_1k_tokens_micros,enabled,created_at,updated_at
		FROM agent_templates WHERE id=$1`, id)
	value, err := scanTemplate(row)
	if err != nil {
		return nil, mapReadError("读取 agent template", err)
	}
	return value, nil
}

func (r *Repository) ListTemplates(ctx context.Context, page domain.Page) ([]*agent.Template, error) {
	query := `SELECT id,name,role,prompt,model,skills,tools,permissions,template_version,
	                 context_window,risk_zone,cost_per_1k_tokens_micros,enabled,created_at,updated_at FROM agent_templates`
	args := make([]any, 0, 3)
	if page.Cursor != nil {
		query += ` WHERE (created_at,id) < ($1,$2)`
		args = append(args, page.Cursor.CreatedAt, page.Cursor.ID)
	}
	query += fmt.Sprintf(" ORDER BY created_at DESC,id DESC LIMIT $%d", len(args)+1)
	args = append(args, page.Size+1)
	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("列出 agent template: %w", err)
	}
	defer rows.Close()
	items := make([]*agent.Template, 0, page.Size+1)
	for rows.Next() {
		value, err := scanTemplate(rows)
		if err != nil {
			return nil, fmt.Errorf("扫描 agent template: %w", err)
		}
		items = append(items, value)
	}
	return items, rows.Err()
}

// CreateInstance 注册 Agent 实例；实例最初为 REGISTERED，完成 Worker 握手后才进入 IDLE。
func (r *Repository) CreateInstance(ctx context.Context, value *agent.Instance) error {
	return r.withTx(ctx, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			INSERT INTO agent_instances (
				id,template_id,swarm_id,name,status,current_task_id,load,heartbeat_at,
				version,created_at,updated_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
			RETURNING created_at,updated_at`,
			value.ID, value.TemplateID, value.SwarmID, value.Name, value.Status,
			value.CurrentTaskID, value.Load, value.HeartbeatAt, value.Version,
			value.CreatedAt, value.UpdatedAt,
		).Scan(&value.CreatedAt, &value.UpdatedAt)
		if err != nil {
			return mapWriteError("注册 agent instance", err)
		}
		return insertOutbox(ctx, tx, "agent_instance", value.ID, "agent.registered", value.Version, map[string]any{
			"id": value.ID, "template_id": value.TemplateID, "swarm_id": value.SwarmID, "status": value.Status,
		})
	})
}

func (r *Repository) GetInstance(ctx context.Context, id uuid.UUID) (*agent.Instance, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT id,template_id,swarm_id,name,status,current_task_id,load,heartbeat_at,
		       version,created_at,updated_at
		FROM agent_instances WHERE id=$1`, id)
	value, err := scanInstance(row)
	if err != nil {
		return nil, mapReadError("读取 agent instance", err)
	}
	return value, nil
}

func (r *Repository) ListInstances(ctx context.Context, swarmID *uuid.UUID, page domain.Page) ([]*agent.Instance, error) {
	query := `SELECT id,template_id,swarm_id,name,status,current_task_id,load,heartbeat_at,
	                 version,created_at,updated_at FROM agent_instances`
	args := make([]any, 0, 4)
	conditions := make([]string, 0, 2)
	if swarmID != nil {
		args = append(args, *swarmID)
		conditions = append(conditions, fmt.Sprintf("swarm_id=$%d", len(args)))
	}
	if page.Cursor != nil {
		args = append(args, page.Cursor.CreatedAt, page.Cursor.ID)
		conditions = append(conditions, fmt.Sprintf("(created_at,id)<($%d,$%d)", len(args)-1, len(args)))
	}
	if len(conditions) > 0 {
		query += " WHERE " + joinConditions(conditions)
	}
	query += fmt.Sprintf(" ORDER BY created_at DESC,id DESC LIMIT $%d", len(args)+1)
	args = append(args, page.Size+1)

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("列出 agent instance: %w", err)
	}
	defer rows.Close()
	items := make([]*agent.Instance, 0, page.Size+1)
	for rows.Next() {
		value, err := scanInstance(rows)
		if err != nil {
			return nil, fmt.Errorf("扫描 agent instance: %w", err)
		}
		items = append(items, value)
	}
	return items, rows.Err()
}

// Create 在一个事务中创建 Task、DAG 边和 task.created 事件。
func (r *Repository) CreateTask(ctx context.Context, value *task.Task) error {
	return r.withTx(ctx, func(tx pgx.Tx) error {
		if err := validateTaskRelations(ctx, tx, value); err != nil {
			return err
		}
		input, err := marshalJSON(value.Input, "task.input")
		if err != nil {
			return err
		}
		requirements, err := marshalJSON(value.Requirements, "task.requirements")
		if err != nil {
			return err
		}
		acceptance, err := marshalJSON(value.Acceptance, "task.acceptance")
		if err != nil {
			return err
		}
		policy, err := marshalJSON(value.ExecutionPolicy, "task.execution_policy")
		if err != nil {
			return err
		}
		err = tx.QueryRow(ctx, `
			INSERT INTO tasks (
				id,swarm_id,parent_id,name,goal,status,priority,input,requirements,
				acceptance,execution_policy,assigned_agent_id,attempt_count,available_at,
				deadline,version,created_at,updated_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
			RETURNING created_at,updated_at`,
			value.ID, value.SwarmID, value.ParentID, value.Name, value.Goal, value.Status,
			value.Priority, input, requirements, acceptance, policy, value.AssignedAgentID,
			value.AttemptCount, value.AvailableAt, value.Deadline, value.Version,
			value.CreatedAt, value.UpdatedAt,
		).Scan(&value.CreatedAt, &value.UpdatedAt)
		if err != nil {
			return mapWriteError("创建 task", err)
		}
		for _, dependencyID := range value.DependencyIDs {
			if _, err := tx.Exec(ctx, `INSERT INTO task_dependencies(task_id,depends_on_id) VALUES($1,$2)`, value.ID, dependencyID); err != nil {
				return mapWriteError("创建 task dependency", err)
			}
		}
		return insertOutbox(ctx, tx, "task", value.ID, "task.created", value.Version, map[string]any{
			"id": value.ID, "swarm_id": value.SwarmID, "status": value.Status,
			"dependency_ids": value.DependencyIDs,
		})
	})
}

// GetTask 读取任务及其依赖边。
func (r *Repository) GetTask(ctx context.Context, id uuid.UUID) (*task.Task, error) {
	row := r.pool.QueryRow(ctx, taskSelect+` WHERE t.id=$1`, id)
	value, err := scanTask(row)
	if err != nil {
		return nil, mapReadError("读取 task", err)
	}
	if err := r.loadDependencies(ctx, []*task.Task{value}); err != nil {
		return nil, err
	}
	return value, nil
}

// ListTasks 按蜂群、可选状态和 keyset 游标查询任务。
func (r *Repository) ListTasks(ctx context.Context, filter task.ListFilter) ([]*task.Task, error) {
	query := taskSelect + ` WHERE t.swarm_id=$1`
	args := []any{filter.SwarmID}
	if filter.Status != nil {
		args = append(args, *filter.Status)
		query += fmt.Sprintf(" AND t.status=$%d", len(args))
	}
	if filter.Page.Cursor != nil {
		args = append(args, filter.Page.Cursor.CreatedAt, filter.Page.Cursor.ID)
		query += fmt.Sprintf(" AND (t.created_at,t.id)<($%d,$%d)", len(args)-1, len(args))
	}
	query += fmt.Sprintf(" ORDER BY t.created_at DESC,t.id DESC LIMIT $%d", len(args)+1)
	args = append(args, filter.Page.Size+1)

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("列出 task: %w", err)
	}
	defer rows.Close()
	items := make([]*task.Task, 0, filter.Page.Size+1)
	for rows.Next() {
		value, err := scanTask(rows)
		if err != nil {
			return nil, fmt.Errorf("扫描 task: %w", err)
		}
		items = append(items, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历 task: %w", err)
	}
	if err := r.loadDependencies(ctx, items); err != nil {
		return nil, err
	}
	return items, nil
}

const taskSelect = `
	SELECT t.id,t.swarm_id,t.parent_id,t.name,t.goal,t.status,t.priority,t.input,
	       t.requirements,t.acceptance,t.execution_policy,t.assigned_agent_id,
	       t.attempt_count,t.available_at,t.deadline,t.version,t.created_at,t.updated_at
	FROM tasks t`

func validateTaskRelations(ctx context.Context, tx pgx.Tx, value *task.Task) error {
	if value.ParentID != nil {
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM tasks WHERE id=$1 AND swarm_id=$2)`, *value.ParentID, value.SwarmID).Scan(&exists); err != nil {
			return fmt.Errorf("校验 parent task: %w", err)
		}
		if !exists {
			return fmt.Errorf("%w: parent task 不存在或不属于同一 swarm", domain.ErrConflict)
		}
	}
	if len(value.DependencyIDs) == 0 {
		return nil
	}
	var count int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE swarm_id=$1 AND id=ANY($2)`, value.SwarmID, value.DependencyIDs).Scan(&count); err != nil {
		return fmt.Errorf("校验 task dependencies: %w", err)
	}
	if count != len(value.DependencyIDs) {
		return fmt.Errorf("%w: 依赖任务不存在或不属于同一 swarm", domain.ErrConflict)
	}
	return nil
}

func (r *Repository) loadDependencies(ctx context.Context, tasks []*task.Task) error {
	if len(tasks) == 0 {
		return nil
	}
	ids := make([]uuid.UUID, 0, len(tasks))
	index := make(map[uuid.UUID]*task.Task, len(tasks))
	for _, value := range tasks {
		ids = append(ids, value.ID)
		index[value.ID] = value
		value.DependencyIDs = []uuid.UUID{}
	}
	rows, err := r.pool.Query(ctx, `SELECT task_id,depends_on_id FROM task_dependencies WHERE task_id=ANY($1) ORDER BY created_at`, ids)
	if err != nil {
		return fmt.Errorf("读取 task dependencies: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var taskID, dependencyID uuid.UUID
		if err := rows.Scan(&taskID, &dependencyID); err != nil {
			return fmt.Errorf("扫描 task dependency: %w", err)
		}
		index[taskID].DependencyIDs = append(index[taskID].DependencyIDs, dependencyID)
	}
	return rows.Err()
}

func (r *Repository) withTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("开始数据库事务: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("提交数据库事务: %w", err)
	}
	return nil
}

func insertOutbox(ctx context.Context, tx pgx.Tx, aggregateType string, aggregateID uuid.UUID, eventType string, version int64, payload any) error {
	raw, err := marshalJSON(payload, "outbox.payload")
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO event_outbox(id,aggregate_type,aggregate_id,event_type,aggregate_version,payload)
		VALUES($1,$2,$3,$4,$5,$6)`, uuid.New(), aggregateType, aggregateID, eventType, version, raw); err != nil {
		return mapWriteError("写入 outbox", err)
	}
	return nil
}

func marshalJSON(value any, field string) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("序列化 %s: %w", field, err)
	}
	return raw, nil
}

func mapReadError(action string, err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: %s", domain.ErrNotFound, action)
	}
	return fmt.Errorf("%s: %w", action, err)
}

func mapWriteError(action string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && (pgErr.Code == "23505" || pgErr.Code == "23503" || pgErr.Code == "23514") {
		return fmt.Errorf("%w: %s (%s)", domain.ErrConflict, action, pgErr.ConstraintName)
	}
	return fmt.Errorf("%s: %w", action, err)
}

func joinConditions(parts []string) string {
	result := ""
	for i, part := range parts {
		if i > 0 {
			result += " AND "
		}
		result += part
	}
	return result
}

// rowScanner 同时兼容 pgx.Row 与 pgx.Rows，减少扫描代码重复。
type rowScanner interface {
	Scan(...any) error
}

func scanSwarm(row rowScanner) (*swarm.Swarm, error) {
	value := new(swarm.Swarm)
	var policy []byte
	err := row.Scan(&value.ID, &value.Name, &value.Goal, &value.Status, &value.BudgetTokens,
		&value.BudgetCostMicros, &value.SpentTokens, &value.SpentCostMicros, &value.MaxAgents,
		&policy, &value.Version, &value.CreatedAt, &value.UpdatedAt)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(policy, &value.Policy); err != nil {
		return nil, fmt.Errorf("解析 swarm.policy: %w", err)
	}
	return value, nil
}

func scanTemplate(row rowScanner) (*agent.Template, error) {
	value := new(agent.Template)
	var skills, tools, permissions []byte
	err := row.Scan(&value.ID, &value.Name, &value.Role, &value.Prompt, &value.Model,
		&skills, &tools, &permissions, &value.TemplateVersion, &value.ContextWindow,
		&value.RiskZone, &value.CostPer1KTokensMicros, &value.Enabled,
		&value.CreatedAt, &value.UpdatedAt)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(skills, &value.Skills); err != nil {
		return nil, fmt.Errorf("解析 agent_template.skills: %w", err)
	}
	if err := json.Unmarshal(tools, &value.Tools); err != nil {
		return nil, fmt.Errorf("解析 agent_template.tools: %w", err)
	}
	if err := json.Unmarshal(permissions, &value.Permissions); err != nil {
		return nil, fmt.Errorf("解析 agent_template.permissions: %w", err)
	}
	return value, nil
}

func scanInstance(row rowScanner) (*agent.Instance, error) {
	value := new(agent.Instance)
	err := row.Scan(&value.ID, &value.TemplateID, &value.SwarmID, &value.Name, &value.Status,
		&value.CurrentTaskID, &value.Load, &value.HeartbeatAt, &value.Version,
		&value.CreatedAt, &value.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func scanTask(row rowScanner) (*task.Task, error) {
	return scanTaskExtra(row)
}

// scanTaskExtra 允许调度查询在标准 Task 列后追加 QueueScore 所需的聚合列。
func scanTaskExtra(row rowScanner, extra ...any) (*task.Task, error) {
	value := new(task.Task)
	var input, requirements, acceptance, policy []byte
	destinations := []any{&value.ID, &value.SwarmID, &value.ParentID, &value.Name, &value.Goal,
		&value.Status, &value.Priority, &input, &requirements, &acceptance, &policy,
		&value.AssignedAgentID, &value.AttemptCount, &value.AvailableAt, &value.Deadline,
		&value.Version, &value.CreatedAt, &value.UpdatedAt}
	destinations = append(destinations, extra...)
	err := row.Scan(destinations...)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(input, &value.Input); err != nil {
		return nil, fmt.Errorf("解析 task.input: %w", err)
	}
	if err := json.Unmarshal(requirements, &value.Requirements); err != nil {
		return nil, fmt.Errorf("解析 task.requirements: %w", err)
	}
	if err := json.Unmarshal(acceptance, &value.Acceptance); err != nil {
		return nil, fmt.Errorf("解析 task.acceptance: %w", err)
	}
	if err := json.Unmarshal(policy, &value.ExecutionPolicy); err != nil {
		return nil, fmt.Errorf("解析 task.execution_policy: %w", err)
	}
	return value, nil
}
