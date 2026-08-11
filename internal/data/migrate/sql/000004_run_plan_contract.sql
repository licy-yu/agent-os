-- SwarmOS V1.5 阶段 A：把“一次执行”提升为 Run，并引入不可变 PlanVersion。
--
-- 迁移策略说明：
-- 1. 不重命名 V1 的 swarms 物理表，避免破坏既有外键、REST API 和控制台；
-- 2. 在 V1.5 领域层中把 swarms 解释为 Run，旧 /swarms 接口保留为兼容别名；
-- 3. 所有新增列都给出兼容默认值，旧版 Worker 在滚动升级期间仍可以写入；
-- 4. 先建立单租户默认数据，再给核心表补 tenant_id。后续接入身份系统时无需重写业务主键。

CREATE TABLE IF NOT EXISTS tenants (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    slug VARCHAR(64) NOT NULL UNIQUE,
    name VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE'
        CHECK (status IN ('ACTIVE','SUSPENDED','DELETING')),
    quota JSONB NOT NULL DEFAULT '{}'::jsonb,
    policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE tenants IS '租户安全边界；所有运行时事实最终都归属于一个 tenant';

-- 固定 UUID 只用于升级时创建本地默认租户和项目，方便单机部署开箱即用。
-- 生产多租户环境可以继续创建独立 Tenant/Project，业务代码不会依赖这两个固定值。
INSERT INTO tenants(id,slug,name)
VALUES ('00000000-0000-0000-0000-000000000001','default','默认租户')
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS projects (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    slug VARCHAR(64) NOT NULL,
    name VARCHAR(128) NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, slug)
);

COMMENT ON TABLE projects IS 'Run、项目记忆与工作空间的稳定作用域';

INSERT INTO projects(id,tenant_id,slug,name)
VALUES (
    '00000000-0000-0000-0000-000000000002',
    '00000000-0000-0000-0000-000000000001',
    'default',
    '默认项目'
)
ON CONFLICT (id) DO NOTHING;

-- SwarmDefinition 是可复用定义；Run 是该定义的一次有审计记录的执行。
CREATE TABLE IF NOT EXISTS swarm_definitions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    project_id UUID NOT NULL REFERENCES projects(id),
    name VARCHAR(128) NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    default_goal TEXT NOT NULL DEFAULT '',
    default_budget JSONB NOT NULL DEFAULT '{}'::jsonb,
    default_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name, version)
);

-- 给 V1 核心表补租户边界。DEFAULT 只用于兼容旧写路径；V1.5 仓储仍会显式写 tenant_id。
ALTER TABLE swarms
    ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL
        DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id),
    ADD COLUMN IF NOT EXISTS project_id UUID NOT NULL
        DEFAULT '00000000-0000-0000-0000-000000000002' REFERENCES projects(id),
    ADD COLUMN IF NOT EXISTS definition_id UUID REFERENCES swarm_definitions(id),
    ADD COLUMN IF NOT EXISTS normalized_goal JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS priority INTEGER NOT NULL DEFAULT 50 CHECK (priority BETWEEN 0 AND 1000),
    ADD COLUMN IF NOT EXISTS desired_state VARCHAR(32) NOT NULL DEFAULT 'RUNNING',
    ADD COLUMN IF NOT EXISTS execution_engine VARCHAR(32) NOT NULL DEFAULT 'LEGACY',
    ADD COLUMN IF NOT EXISTS admission_result JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS deadline TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS admitted_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS paused_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS canceled_at TIMESTAMPTZ;

ALTER TABLE swarms DROP CONSTRAINT IF EXISTS swarms_status_check;
ALTER TABLE swarms ADD CONSTRAINT swarms_status_check CHECK (status IN (
    -- PENDING/SUCCEEDED 是旧 /swarms API 的兼容状态，其余状态由 V1.5 RunController 使用。
    'PENDING','SUCCEEDED','CREATED','ADMISSION','PENDING_CAPACITY','PENDING_QUOTA',
    'PENDING_RESOURCE','PLANNING','READY','RUNNING','WAITING_USER','WAITING_EXTERNAL',
    'PAUSED','RECOVERING','DEGRADED','VERIFYING','COMPLETED','FAILED','CANCELED','EXPIRED'
));

ALTER TABLE swarms DROP CONSTRAINT IF EXISTS swarms_desired_state_check;
ALTER TABLE swarms ADD CONSTRAINT swarms_desired_state_check
    CHECK (desired_state IN ('RUNNING','PAUSED','CANCELED'));

ALTER TABLE swarms DROP CONSTRAINT IF EXISTS swarms_execution_engine_check;
ALTER TABLE swarms ADD CONSTRAINT swarms_execution_engine_check
    CHECK (execution_engine IN ('LEGACY','TEMPORAL'));

COMMENT ON COLUMN swarms.desired_state IS '用户期望状态；Controller/Workflow 负责把实际 status 收敛过去';
COMMENT ON COLUMN swarms.execution_engine IS '滚动迁移隔离开关，防止 Legacy Controller 与 Temporal 双重推进同一 Run';

ALTER TABLE agent_templates
    ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL
        DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id);
ALTER TABLE agent_instances
    ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL
        DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id);
ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL
        DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id);
ALTER TABLE task_attempts
    ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL
        DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id);
ALTER TABLE artifacts
    ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL
        DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id);
ALTER TABLE tool_calls
    ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL
        DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id);
ALTER TABLE evaluations
    ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL
        DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id);
ALTER TABLE event_outbox
    ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL
        DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id),
    ADD COLUMN IF NOT EXISTS correlation_id UUID,
    ADD COLUMN IF NOT EXISTS causation_id UUID;

-- PlanVersion 只追加、不覆盖。candidate_spec 保存 Planner 输出，compiled_spec 保存确定性 Compiler 产物。
CREATE TABLE IF NOT EXISTS plan_versions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    run_id UUID NOT NULL REFERENCES swarms(id) ON DELETE CASCADE,
    version_no INTEGER NOT NULL CHECK (version_no > 0),
    source VARCHAR(32) NOT NULL CHECK (source IN ('USER','PLANNER','REPLAN','RECOVERY')),
    status VARCHAR(32) NOT NULL CHECK (status IN (
        'DRAFT','VALIDATING','VALID','REJECTED','ACTIVE','SUPERSEDED'
    )),
    candidate_spec JSONB NOT NULL DEFAULT '{}'::jsonb,
    compiled_spec JSONB NOT NULL DEFAULT '{}'::jsonb,
    validation_errors JSONB NOT NULL DEFAULT '[]'::jsonb,
    diff_from_previous JSONB NOT NULL DEFAULT '{}'::jsonb,
    compiler_version VARCHAR(64) NOT NULL,
    content_hash VARCHAR(128) NOT NULL,
    created_by VARCHAR(128) NOT NULL,
    activated_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, run_id, version_no),
    UNIQUE (tenant_id, run_id, content_hash)
);

COMMENT ON TABLE plan_versions IS '不可变执行计划；Replan 必须创建新版本并保留旧版本证据';
CREATE INDEX IF NOT EXISTS idx_plan_versions_run
    ON plan_versions (tenant_id, run_id, version_no DESC);

ALTER TABLE swarms
    ADD COLUMN IF NOT EXISTS current_plan_version_id UUID REFERENCES plan_versions(id);

-- Task Contract 扩展。V1 字段保留，新增结构为 Planner/Compiler 与 Runtime 的稳定合同。
ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS plan_version_id UUID REFERENCES plan_versions(id),
    ADD COLUMN IF NOT EXISTS logical_key VARCHAR(256),
    ADD COLUMN IF NOT EXISTS task_type VARCHAR(64) NOT NULL DEFAULT 'GENERAL',
    ADD COLUMN IF NOT EXISTS output_spec JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS artifact_contract JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS context_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS side_effect_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS retry_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS critical_path_weight DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS workspace_mode VARCHAR(32) NOT NULL DEFAULT 'SHARED';

ALTER TABLE tasks DROP CONSTRAINT IF EXISTS tasks_status_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_status_check CHECK (status IN (
    'CREATED','PLANNING','BLOCKED','READY','SCHEDULING','ASSIGNED','RUNNING',
    'WAITING_TOOL','WAITING_INPUT','WAITING_APPROVAL','WAITING_EXTERNAL','REVIEW',
    'VERIFYING','RETRY_WAIT','SUCCEEDED','FAILED','CANCELED','REJECTED','SKIPPED'
));

ALTER TABLE tasks DROP CONSTRAINT IF EXISTS tasks_workspace_mode_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_workspace_mode_check
    CHECK (workspace_mode IN ('NONE','READ_ONLY','ISOLATED','SHARED'));

-- 依赖边不再只有“硬/软”两类；Data/Approval/Temporal 会参与 readiness 判定。
ALTER TABLE task_dependencies
    ADD COLUMN IF NOT EXISTS dependency_condition JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS artifact_type VARCHAR(64),
    ADD COLUMN IF NOT EXISTS required BOOLEAN NOT NULL DEFAULT true;
ALTER TABLE task_dependencies DROP CONSTRAINT IF EXISTS task_dependencies_dependency_type_check;
ALTER TABLE task_dependencies ADD CONSTRAINT task_dependencies_dependency_type_check
    CHECK (dependency_type IN ('HARD','SOFT','DATA','APPROVAL','TEMPORAL'));

CREATE UNIQUE INDEX IF NOT EXISTS uq_tasks_plan_logical_key
    ON tasks (tenant_id, plan_version_id, logical_key)
    WHERE plan_version_id IS NOT NULL AND logical_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_runs_tenant_status
    ON swarms (tenant_id, status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_tasks_tenant_run_status
    ON tasks (tenant_id, swarm_id, status, priority DESC, created_at);
CREATE INDEX IF NOT EXISTS idx_agents_tenant_status
    ON agent_instances (tenant_id, status, heartbeat_at DESC);
