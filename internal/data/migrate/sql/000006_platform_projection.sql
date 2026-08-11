-- SwarmOS V1.5 阶段 C/D：Timeline、调度解释、身份凭据、Memory 与 V2 扩展钩子。
--
-- 本迁移只建立生产数据合同。A2A、自动扩缩和自演化仍属于 V2/P2-P3，
-- 在显式启用对应 Controller 以前，相关表不会自动执行任何外部动作。

-- API Key 只保存 SHA-256/HMAC 后的不可逆摘要；明文只在创建时显示一次。
CREATE TABLE IF NOT EXISTS tenant_api_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name VARCHAR(128) NOT NULL,
    key_prefix VARCHAR(16) NOT NULL,
    key_hash VARCHAR(128) NOT NULL UNIQUE,
    scopes JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_by VARCHAR(128) NOT NULL,
    expires_at TIMESTAMPTZ,
    last_used_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tenant_api_keys_active
    ON tenant_api_keys (key_prefix)
    WHERE revoked_at IS NULL;

-- CredentialRef 只描述“向哪个受控 Broker 取哪份凭据”，绝不保存真实 secret。
CREATE TABLE IF NOT EXISTS credential_refs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name VARCHAR(128) NOT NULL,
    broker VARCHAR(64) NOT NULL,
    secret_ref VARCHAR(512) NOT NULL,
    allowed_tools JSONB NOT NULL DEFAULT '[]'::jsonb,
    allowed_hosts JSONB NOT NULL DEFAULT '[]'::jsonb,
    expires_at TIMESTAMPTZ,
    version BIGINT NOT NULL DEFAULT 1,
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name, version)
);

ALTER TABLE tool_registry
    ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL
        DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES tenants(id),
    ADD COLUMN IF NOT EXISTS credential_ref_id UUID REFERENCES credential_refs(id),
    ADD COLUMN IF NOT EXISTS output_schema JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS max_input_bytes BIGINT NOT NULL DEFAULT 1048576 CHECK (max_input_bytes > 0),
    ADD COLUMN IF NOT EXISTS max_output_bytes BIGINT NOT NULL DEFAULT 4194304 CHECK (max_output_bytes > 0),
    ADD COLUMN IF NOT EXISTS effect_type VARCHAR(128),
    ADD COLUMN IF NOT EXISTS supports_reconcile BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS supports_compensate BOOLEAN NOT NULL DEFAULT false;

-- Timeline 是面向运维与审计的追加投影，和负责可靠投递的 Outbox 职责分离。
CREATE TABLE IF NOT EXISTS timeline_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    run_id UUID NOT NULL REFERENCES swarms(id) ON DELETE CASCADE,
    task_id UUID REFERENCES tasks(id) ON DELETE CASCADE,
    attempt_id UUID REFERENCES task_attempts(id) ON DELETE CASCADE,
    source_event_id UUID UNIQUE,
    event_type VARCHAR(128) NOT NULL,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id UUID NOT NULL,
    summary TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    correlation_id UUID,
    causation_id UUID,
    trace_id VARCHAR(64),
    actor_type VARCHAR(64) NOT NULL DEFAULT 'SYSTEM',
    actor_id VARCHAR(256),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_timeline_run
    ON timeline_events (tenant_id, run_id, occurred_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_timeline_task
    ON timeline_events (tenant_id, task_id, occurred_at DESC)
    WHERE task_id IS NOT NULL;

-- 把历史 Outbox 事件回填到 Timeline。RunID 通过聚合类型或关联表确定。
INSERT INTO timeline_events(
    tenant_id,run_id,task_id,attempt_id,source_event_id,event_type,
    aggregate_type,aggregate_id,summary,payload,correlation_id,causation_id,occurred_at
)
SELECT
    e.tenant_id,
    COALESCE(
        CASE WHEN e.aggregate_type='swarm' THEN e.aggregate_id END,
        t.swarm_id,
        ta.swarm_id,
        aa.swarm_id,
        ev.swarm_id
    ) AS run_id,
    COALESCE(t.id,ta.task_id,ev.task_id) AS task_id,
    COALESCE(ta.attempt_id,ev.attempt_id) AS attempt_id,
    e.id,e.event_type,e.aggregate_type,e.aggregate_id,e.event_type,e.payload,
    e.correlation_id,e.causation_id,e.created_at
FROM event_outbox e
LEFT JOIN LATERAL (
    SELECT id,swarm_id FROM tasks
    WHERE e.aggregate_type='task' AND id=e.aggregate_id
) t ON true
LEFT JOIN LATERAL (
    SELECT a.id AS attempt_id,a.task_id,x.swarm_id
    FROM task_attempts a JOIN tasks x ON x.id=a.task_id
    WHERE e.aggregate_type='attempt' AND a.id=e.aggregate_id
) ta ON true
LEFT JOIN LATERAL (
    SELECT swarm_id FROM agent_instances
    WHERE e.aggregate_type='agent_instance' AND id=e.aggregate_id
) aa ON true
LEFT JOIN LATERAL (
    SELECT v.attempt_id,v.task_id,x.swarm_id
    FROM evaluations v JOIN tasks x ON x.id=v.task_id
    WHERE e.aggregate_type='evaluation' AND v.id=e.aggregate_id
) ev ON true
WHERE COALESCE(
    CASE WHEN e.aggregate_type='swarm' THEN e.aggregate_id END,
    t.swarm_id,ta.swarm_id,aa.swarm_id,ev.swarm_id
) IS NOT NULL
ON CONFLICT (source_event_id) DO NOTHING;

-- 新 Outbox 写入后同步生成 Timeline 投影。该触发器只做本库 INSERT，不进行网络 I/O。
CREATE OR REPLACE FUNCTION project_outbox_to_timeline()
RETURNS TRIGGER AS $$
DECLARE
    resolved_run UUID;
    resolved_task UUID;
    resolved_attempt UUID;
BEGIN
    IF NEW.aggregate_type = 'swarm' THEN
        resolved_run := NEW.aggregate_id;
    ELSIF NEW.aggregate_type = 'task' THEN
        SELECT id,swarm_id INTO resolved_task,resolved_run FROM tasks WHERE id=NEW.aggregate_id;
    ELSIF NEW.aggregate_type = 'attempt' THEN
        SELECT a.id,a.task_id,t.swarm_id INTO resolved_attempt,resolved_task,resolved_run
        FROM task_attempts a JOIN tasks t ON t.id=a.task_id WHERE a.id=NEW.aggregate_id;
    ELSIF NEW.aggregate_type = 'agent_instance' THEN
        SELECT swarm_id INTO resolved_run FROM agent_instances WHERE id=NEW.aggregate_id;
    ELSIF NEW.aggregate_type = 'evaluation' THEN
        SELECT v.attempt_id,v.task_id,t.swarm_id INTO resolved_attempt,resolved_task,resolved_run
        FROM evaluations v JOIN tasks t ON t.id=v.task_id WHERE v.id=NEW.aggregate_id;
    ELSIF NEW.aggregate_type = 'effect' THEN
        SELECT attempt_id,task_id,run_id INTO resolved_attempt,resolved_task,resolved_run
        FROM effects WHERE id=NEW.aggregate_id;
    ELSIF NEW.aggregate_type = 'interaction' THEN
        SELECT attempt_id,task_id,run_id INTO resolved_attempt,resolved_task,resolved_run
        FROM interactions WHERE id=NEW.aggregate_id;
    END IF;

    IF resolved_run IS NOT NULL THEN
        INSERT INTO timeline_events(
            tenant_id,run_id,task_id,attempt_id,source_event_id,event_type,
            aggregate_type,aggregate_id,summary,payload,correlation_id,causation_id,occurred_at
        ) VALUES (
            NEW.tenant_id,resolved_run,resolved_task,resolved_attempt,NEW.id,NEW.event_type,
            NEW.aggregate_type,NEW.aggregate_id,NEW.event_type,NEW.payload,
            NEW.correlation_id,NEW.causation_id,NEW.created_at
        ) ON CONFLICT (source_event_id) DO NOTHING;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_project_outbox_to_timeline ON event_outbox;
CREATE TRIGGER trg_project_outbox_to_timeline
AFTER INSERT ON event_outbox
FOR EACH ROW EXECUTE FUNCTION project_outbox_to_timeline();

-- Scheduler Explain 保存过滤、评分、选中/未选中的原因，便于线上追责和调参。
CREATE TABLE IF NOT EXISTS scheduler_decisions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    run_id UUID NOT NULL REFERENCES swarms(id) ON DELETE CASCADE,
    task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    scheduler_id VARCHAR(128) NOT NULL,
    task_version BIGINT NOT NULL,
    queue_score DOUBLE PRECISION NOT NULL DEFAULT 0,
    selected_agent_id UUID REFERENCES agent_instances(id),
    selected_score DOUBLE PRECISION,
    candidates JSONB NOT NULL DEFAULT '[]'::jsonb,
    filters JSONB NOT NULL DEFAULT '[]'::jsonb,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_scheduler_decisions_task
    ON scheduler_decisions (tenant_id, task_id, created_at DESC);

-- 成本账本按模型/工具/存储分别记账，Run 表上的 spent_* 只是快速聚合缓存。
CREATE TABLE IF NOT EXISTS cost_ledger (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    run_id UUID NOT NULL REFERENCES swarms(id) ON DELETE CASCADE,
    task_id UUID REFERENCES tasks(id) ON DELETE CASCADE,
    attempt_id UUID REFERENCES task_attempts(id) ON DELETE CASCADE,
    category VARCHAR(32) NOT NULL CHECK (category IN ('MODEL','TOOL','STORAGE','NETWORK','HUMAN')),
    provider VARCHAR(128) NOT NULL,
    resource VARCHAR(256) NOT NULL,
    quantity DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (quantity >= 0),
    unit VARCHAR(32) NOT NULL,
    cost_micros BIGINT NOT NULL DEFAULT 0 CHECK (cost_micros >= 0),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cost_ledger_run
    ON cost_ledger (tenant_id, run_id, occurred_at);

-- Memory 先建立候选晋升数据链；模型不能直接写入 PROMOTED 可信记忆。
CREATE TABLE IF NOT EXISTS memory_items (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    project_id UUID REFERENCES projects(id) ON DELETE CASCADE,
    run_id UUID REFERENCES swarms(id) ON DELETE CASCADE,
    scope_type VARCHAR(32) NOT NULL CHECK (scope_type IN ('AGENT','TASK','RUN','PROJECT')),
    scope_id UUID NOT NULL,
    status VARCHAR(32) NOT NULL CHECK (status IN ('CANDIDATE','EVALUATING','PROMOTED','REJECTED','SUPERSEDED')),
    memory_key VARCHAR(256) NOT NULL,
    content TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_attempt_id UUID REFERENCES task_attempts(id) ON DELETE SET NULL,
    source_evaluation_id UUID REFERENCES evaluations(id) ON DELETE SET NULL,
    quality_score DOUBLE PRECISION CHECK (quality_score BETWEEN 0 AND 1),
    content_hash VARCHAR(128) NOT NULL,
    valid_from TIMESTAMPTZ NOT NULL DEFAULT now(),
    valid_to TIMESTAMPTZ,
    supersedes_id UUID REFERENCES memory_items(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, scope_type, scope_id, memory_key, content_hash)
);

CREATE INDEX IF NOT EXISTS idx_memory_promoted_scope
    ON memory_items (tenant_id, scope_type, scope_id, memory_key)
    WHERE status = 'PROMOTED';

-- ExecutionFingerprint 为后续重复任务复用和演化评估提供稳定指纹，但不自动跳过验证。
CREATE TABLE IF NOT EXISTS execution_fingerprints (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    attempt_id UUID NOT NULL REFERENCES task_attempts(id) ON DELETE CASCADE,
    contract_hash VARCHAR(128) NOT NULL,
    input_hash VARCHAR(128) NOT NULL,
    context_hash VARCHAR(128) NOT NULL,
    agent_version_hash VARCHAR(128) NOT NULL,
    toolset_hash VARCHAR(128) NOT NULL,
    output_hash VARCHAR(128) NOT NULL,
    verification_hash VARCHAR(128) NOT NULL,
    reusable BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, contract_hash, input_hash, context_hash, agent_version_hash, toolset_hash)
);

-- AgentVersion/AgentSet 是不可变能力版本和可伸缩副本组，V1 的 Template/Instance 继续兼容。
CREATE TABLE IF NOT EXISTS agent_versions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    template_id UUID NOT NULL REFERENCES agent_templates(id) ON DELETE CASCADE,
    version VARCHAR(64) NOT NULL,
    manifest JSONB NOT NULL,
    content_hash VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL CHECK (status IN ('DRAFT','ACTIVE','DEPRECATED','REVOKED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, template_id, version),
    UNIQUE (tenant_id, content_hash)
);

CREATE TABLE IF NOT EXISTS agent_sets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    name VARCHAR(128) NOT NULL,
    agent_version_id UUID NOT NULL REFERENCES agent_versions(id),
    min_replicas INTEGER NOT NULL DEFAULT 0 CHECK (min_replicas >= 0),
    max_replicas INTEGER NOT NULL DEFAULT 1 CHECK (max_replicas >= min_replicas),
    desired_replicas INTEGER NOT NULL DEFAULT 0 CHECK (desired_replicas >= 0),
    scaling_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE','DRAINING','STOPPED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name)
);

-- 以下为 V2 扩展钩子。没有 Controller 时它们只是审计数据，不产生自动外部行为。
CREATE TABLE IF NOT EXISTS a2a_endpoints (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    name VARCHAR(128) NOT NULL,
    endpoint TEXT NOT NULL,
    agent_card JSONB NOT NULL DEFAULT '{}'::jsonb,
    credential_ref_id UUID REFERENCES credential_refs(id),
    trust_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    enabled BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name)
);

CREATE TABLE IF NOT EXISTS remote_task_mappings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    run_id UUID NOT NULL REFERENCES swarms(id) ON DELETE CASCADE,
    task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    endpoint_id UUID NOT NULL REFERENCES a2a_endpoints(id),
    remote_task_id VARCHAR(512) NOT NULL,
    status VARCHAR(64) NOT NULL,
    last_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (endpoint_id, remote_task_id)
);

CREATE TABLE IF NOT EXISTS scaling_decisions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    agent_set_id UUID NOT NULL REFERENCES agent_sets(id) ON DELETE CASCADE,
    from_replicas INTEGER NOT NULL,
    to_replicas INTEGER NOT NULL,
    reason TEXT NOT NULL,
    metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    status VARCHAR(32) NOT NULL CHECK (status IN ('PROPOSED','APPLIED','REJECTED','FAILED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS evolution_candidates (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    candidate_type VARCHAR(32) NOT NULL CHECK (candidate_type IN ('PROMPT','PLAN','TOOL','POLICY','AGENT')),
    base_ref JSONB NOT NULL,
    proposal JSONB NOT NULL,
    hypothesis TEXT NOT NULL,
    evaluation_plan JSONB NOT NULL,
    status VARCHAR(32) NOT NULL CHECK (status IN ('DRAFT','EVALUATING','APPROVED','REJECTED','ROLLED_BACK')),
    created_by VARCHAR(128) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
