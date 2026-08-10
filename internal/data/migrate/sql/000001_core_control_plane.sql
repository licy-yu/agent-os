-- SwarmOS 核心控制面数据模型。
-- 所有时间使用 TIMESTAMPTZ，确保跨时区部署时语义一致。

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS swarms (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(128) NOT NULL,
    goal TEXT NOT NULL,
    status VARCHAR(32) NOT NULL CHECK (status IN ('PENDING','RUNNING','SUCCEEDED','FAILED','CANCELED')),
    budget_tokens BIGINT NOT NULL DEFAULT 0 CHECK (budget_tokens >= 0),
    budget_cost_micros BIGINT NOT NULL DEFAULT 0 CHECK (budget_cost_micros >= 0),
    spent_tokens BIGINT NOT NULL DEFAULT 0 CHECK (spent_tokens >= 0),
    spent_cost_micros BIGINT NOT NULL DEFAULT 0 CHECK (spent_cost_micros >= 0),
    max_agents INTEGER NOT NULL DEFAULT 8 CHECK (max_agents BETWEEN 1 AND 1000),
    policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE swarms IS '一次用户目标对应的蜂群执行实例';
COMMENT ON COLUMN swarms.version IS '乐观锁版本号；每次状态变更必须递增';

CREATE INDEX IF NOT EXISTS idx_swarms_created ON swarms (created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_swarms_status ON swarms (status, created_at DESC);

CREATE TABLE IF NOT EXISTS agent_templates (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(128) NOT NULL,
    role TEXT NOT NULL,
    prompt TEXT NOT NULL,
    model VARCHAR(128) NOT NULL,
    skills JSONB NOT NULL DEFAULT '{}'::jsonb,
    tools JSONB NOT NULL DEFAULT '[]'::jsonb,
    permissions JSONB NOT NULL DEFAULT '[]'::jsonb,
    template_version VARCHAR(64) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (name, template_version)
);

COMMENT ON TABLE agent_templates IS '可版本化 Agent 能力定义；不等于运行实例';
CREATE INDEX IF NOT EXISTS idx_agent_templates_created ON agent_templates (created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_agent_templates_enabled ON agent_templates (enabled) WHERE enabled = true;

CREATE TABLE IF NOT EXISTS agent_instances (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    template_id UUID NOT NULL REFERENCES agent_templates(id),
    swarm_id UUID REFERENCES swarms(id) ON DELETE SET NULL,
    name VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL CHECK (status IN (
        'REGISTERED','IDLE','RESERVED','RUNNING','WAITING_TOOL','WAITING_INPUT',
        'BACKOFF','OFFLINE','DRAINING','STOPPED'
    )),
    current_task_id UUID,
    load DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (load BETWEEN 0 AND 1),
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE agent_instances IS 'Scheduler 可调度的 Agent 运行实例';
CREATE INDEX IF NOT EXISTS idx_agents_swarm_status ON agent_instances (swarm_id, status);
CREATE INDEX IF NOT EXISTS idx_agents_heartbeat ON agent_instances (heartbeat_at) WHERE status NOT IN ('OFFLINE','STOPPED');
CREATE INDEX IF NOT EXISTS idx_agent_instances_created ON agent_instances (created_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    swarm_id UUID NOT NULL REFERENCES swarms(id) ON DELETE CASCADE,
    parent_id UUID REFERENCES tasks(id) ON DELETE SET NULL,
    name VARCHAR(256) NOT NULL,
    goal TEXT NOT NULL,
    status VARCHAR(32) NOT NULL CHECK (status IN (
        'CREATED','PLANNING','BLOCKED','READY','SCHEDULING','ASSIGNED','RUNNING',
        'WAITING_TOOL','WAITING_INPUT','REVIEW','RETRY_WAIT','SUCCEEDED','FAILED',
        'CANCELED','REJECTED'
    )),
    priority INTEGER NOT NULL DEFAULT 50 CHECK (priority BETWEEN 0 AND 1000),
    input JSONB NOT NULL DEFAULT '{}'::jsonb,
    requirements JSONB NOT NULL DEFAULT '{}'::jsonb,
    acceptance JSONB NOT NULL DEFAULT '{}'::jsonb,
    execution_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    assigned_agent_id UUID REFERENCES agent_instances(id) ON DELETE SET NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deadline TIMESTAMPTZ,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE agent_instances
    ADD CONSTRAINT fk_agent_current_task
    FOREIGN KEY (current_task_id) REFERENCES tasks(id) ON DELETE SET NULL;

COMMENT ON TABLE tasks IS '逻辑任务合同；每次实际执行写入独立 task_attempt';
CREATE INDEX IF NOT EXISTS idx_tasks_swarm_status ON tasks (swarm_id, status, priority DESC, created_at);
CREATE INDEX IF NOT EXISTS idx_tasks_ready_queue ON tasks (priority DESC, available_at, created_at)
    WHERE status = 'READY';
CREATE INDEX IF NOT EXISTS idx_tasks_created ON tasks (created_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS task_dependencies (
    task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    depends_on_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    dependency_type VARCHAR(32) NOT NULL DEFAULT 'HARD' CHECK (dependency_type IN ('HARD','SOFT')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (task_id, depends_on_id),
    CHECK (task_id <> depends_on_id)
);

COMMENT ON TABLE task_dependencies IS 'Task DAG 有向边：task_id 必须等待 depends_on_id';
CREATE INDEX IF NOT EXISTS idx_task_dependencies_parent ON task_dependencies (depends_on_id);

CREATE TABLE IF NOT EXISTS task_attempts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    agent_id UUID NOT NULL REFERENCES agent_instances(id),
    attempt_no INTEGER NOT NULL CHECK (attempt_no > 0),
    status VARCHAR(32) NOT NULL CHECK (status IN ('CREATED','RUNNING','REVIEW','SUCCEEDED','FAILED','ABORTED','CANCELED')),
    input_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    output_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    model VARCHAR(128),
    prompt_version VARCHAR(64),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    tokens_in BIGINT NOT NULL DEFAULT 0,
    tokens_out BIGINT NOT NULL DEFAULT 0,
    cost_micros BIGINT NOT NULL DEFAULT 0,
    error_code VARCHAR(128),
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (task_id, attempt_no)
);

COMMENT ON TABLE task_attempts IS '任务每次执行的不可覆盖历史记录';
CREATE INDEX IF NOT EXISTS idx_task_attempts_task ON task_attempts (task_id, attempt_no DESC);
CREATE INDEX IF NOT EXISTS idx_task_attempts_agent ON task_attempts (agent_id, created_at DESC);

CREATE TABLE IF NOT EXISTS event_outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(128) NOT NULL,
    aggregate_version BIGINT NOT NULL,
    payload JSONB NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (aggregate_type, aggregate_id, aggregate_version, event_type)
);

COMMENT ON TABLE event_outbox IS '与领域变更同事务写入的可靠事件发件箱';
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON event_outbox (available_at, created_at)
    WHERE published_at IS NULL;

