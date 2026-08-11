-- SwarmOS V1.5 阶段 B：执行步骤、fencing、Effect、HITL、Workspace、Artifact 与验证证据。
--
-- 这些对象共同形成一条安全闭环：
-- TaskAttempt 拿到独占 fencing token → Step/Checkpoint 可恢复 → Tool 先写 Effect →
-- 高风险 Effect 等待 Interaction → 产物进入 Artifact/Lineage → 独立 Gate 验证 → Manifest 完成。

-- 单调 fencing_token 是数据库中的最终所有权凭据。Redis Lease 只负责减少争抢，不能替代它。
ALTER TABLE task_attempts
    ADD COLUMN IF NOT EXISTS fencing_token BIGINT NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
    ADD COLUMN IF NOT EXISTS runtime_name VARCHAR(64) NOT NULL DEFAULT 'legacy',
    ADD COLUMN IF NOT EXISTS runtime_version VARCHAR(64) NOT NULL DEFAULT 'v1',
    ADD COLUMN IF NOT EXISTS agent_version_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS policy_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS context_snapshot_ref UUID,
    ADD COLUMN IF NOT EXISTS workspace_revision_ref UUID,
    ADD COLUMN IF NOT EXISTS resume_checkpoint_id UUID;

CREATE INDEX IF NOT EXISTS idx_attempts_owner_fence
    ON task_attempts (id, worker_id, fencing_token)
    WHERE status = 'RUNNING';

-- Step 是 Attempt 内部的可审计最小执行单元，不再把整个 Agent Loop 当作黑箱。
CREATE TABLE IF NOT EXISTS task_steps (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    run_id UUID NOT NULL REFERENCES swarms(id) ON DELETE CASCADE,
    task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    attempt_id UUID NOT NULL REFERENCES task_attempts(id) ON DELETE CASCADE,
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    step_type VARCHAR(32) NOT NULL CHECK (step_type IN (
        'PLAN','MODEL','TOOL','OBSERVE','WRITE','VERIFY','CHECKPOINT','HANDOFF','HUMAN'
    )),
    name VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL CHECK (status IN (
        'PENDING','RUNNING','WAITING','SUCCEEDED','FAILED','SKIPPED','CANCELED'
    )),
    input JSONB NOT NULL DEFAULT '{}'::jsonb,
    output JSONB NOT NULL DEFAULT '{}'::jsonb,
    usage JSONB NOT NULL DEFAULT '{}'::jsonb,
    model_call_id VARCHAR(256),
    tool_call_id UUID REFERENCES tool_calls(id),
    checkpoint_id UUID REFERENCES checkpoints(id),
    error_code VARCHAR(128),
    error_message TEXT,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (attempt_id, sequence)
);

COMMENT ON TABLE task_steps IS 'Attempt 内不可覆盖的 typed step 账本；sequence 单调递增';
CREATE INDEX IF NOT EXISTS idx_task_steps_attempt
    ON task_steps (tenant_id, attempt_id, sequence);

-- Effect 记录可能改变外部世界的调用。UNKNOWN 必须先 Reconcile，禁止直接重试。
CREATE TABLE IF NOT EXISTS effects (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    run_id UUID NOT NULL REFERENCES swarms(id) ON DELETE CASCADE,
    task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    attempt_id UUID NOT NULL REFERENCES task_attempts(id) ON DELETE CASCADE,
    tool_call_id UUID REFERENCES tool_calls(id),
    idempotency_key VARCHAR(256) NOT NULL,
    effect_type VARCHAR(128) NOT NULL,
    risk_level VARCHAR(32) NOT NULL CHECK (risk_level IN (
        'R0_READ_ONLY','R1_SANDBOX_WRITE','R2_EXTERNAL_REVERSIBLE','R3_PRODUCTION_DESTRUCTIVE'
    )),
    status VARCHAR(32) NOT NULL CHECK (status IN (
        'PREPARED','AUTHORIZED','EXECUTING','SUCCEEDED','FAILED','UNKNOWN',
        'RECONCILING','COMPENSATING','COMPENSATED'
    )),
    request_hash VARCHAR(128) NOT NULL,
    sanitized_request JSONB NOT NULL DEFAULT '{}'::jsonb,
    sanitized_result JSONB NOT NULL DEFAULT '{}'::jsonb,
    external_ref VARCHAR(512),
    approval_interaction_id UUID,
    reconcile_after TIMESTAMPTZ,
    retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    compensation_spec JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_code VARCHAR(128),
    error_message TEXT,
    fencing_token BIGINT NOT NULL CHECK (fencing_token >= 0),
    version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
    prepared_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    authorized_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, idempotency_key)
);

COMMENT ON TABLE effects IS '外部副作用的幂等与对账事实；ToolCall 只是审计日志，Effect 才是执行凭据';
CREATE INDEX IF NOT EXISTS idx_effects_reconcile
    ON effects (tenant_id, reconcile_after, prepared_at)
    WHERE status IN ('UNKNOWN','RECONCILING');
CREATE INDEX IF NOT EXISTS idx_effects_attempt
    ON effects (tenant_id, attempt_id, prepared_at);

-- Interaction 是所有人工输入、审批和接管的统一持久化信箱。
CREATE TABLE IF NOT EXISTS interactions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    run_id UUID NOT NULL REFERENCES swarms(id) ON DELETE CASCADE,
    task_id UUID REFERENCES tasks(id) ON DELETE CASCADE,
    attempt_id UUID REFERENCES task_attempts(id) ON DELETE CASCADE,
    effect_id UUID REFERENCES effects(id) ON DELETE SET NULL,
    interaction_type VARCHAR(32) NOT NULL CHECK (interaction_type IN (
        'INPUT','APPROVAL','CHOICE','EDIT','AUTH','TAKEOVER'
    )),
    status VARCHAR(32) NOT NULL CHECK (status IN (
        'CREATED','WAITING','RESOLVED','EXPIRED','CANCELED'
    )),
    title VARCHAR(256) NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    allowed_actions JSONB NOT NULL DEFAULT '[]'::jsonb,
    response JSONB NOT NULL DEFAULT '{}'::jsonb,
    idempotency_key VARCHAR(256) NOT NULL,
    requested_by VARCHAR(128) NOT NULL,
    resolved_by VARCHAR(128),
    expires_at TIMESTAMPTZ,
    version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, idempotency_key)
);

ALTER TABLE effects
    ADD CONSTRAINT fk_effect_approval_interaction
    FOREIGN KEY (approval_interaction_id) REFERENCES interactions(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_interactions_inbox
    ON interactions (tenant_id, status, created_at DESC)
    WHERE status = 'WAITING';

-- CapabilityGrant 是一次 Attempt 的短期能力令牌；模板声明不等同于运行时授权。
CREATE TABLE IF NOT EXISTS capability_grants (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    attempt_id UUID NOT NULL REFERENCES task_attempts(id) ON DELETE CASCADE,
    resource_type VARCHAR(64) NOT NULL,
    resource_id VARCHAR(256) NOT NULL,
    actions JSONB NOT NULL DEFAULT '[]'::jsonb,
    constraints JSONB NOT NULL DEFAULT '{}'::jsonb,
    policy_version VARCHAR(64) NOT NULL,
    issued_by VARCHAR(128) NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_capability_grants_attempt
    ON capability_grants (tenant_id, attempt_id, expires_at)
    WHERE revoked_at IS NULL;

-- Workspace/Revision 把文件系统状态变成可恢复、可审计的版本事实。
CREATE TABLE IF NOT EXISTS workspaces (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    run_id UUID NOT NULL UNIQUE REFERENCES swarms(id) ON DELETE CASCADE,
    root_uri TEXT NOT NULL,
    status VARCHAR(32) NOT NULL CHECK (status IN ('CREATING','READY','LOCKED','ARCHIVED','FAILED')),
    current_revision_id UUID,
    lock_owner VARCHAR(256),
    lock_expires_at TIMESTAMPTZ,
    quota_bytes BIGINT NOT NULL DEFAULT 0 CHECK (quota_bytes >= 0),
    used_bytes BIGINT NOT NULL DEFAULT 0 CHECK (used_bytes >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS workspace_revisions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    workspace_id UUID NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    parent_id UUID REFERENCES workspace_revisions(id),
    revision_no BIGINT NOT NULL CHECK (revision_no > 0),
    revision_type VARCHAR(32) NOT NULL CHECK (revision_type IN (
        'INITIAL','CHECKPOINT','COMMIT','TAKEOVER','RESTORE','FINAL'
    )),
    revision_ref TEXT NOT NULL,
    manifest_hash VARCHAR(128) NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    created_by VARCHAR(128) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (workspace_id, revision_no)
);

ALTER TABLE workspaces
    ADD CONSTRAINT fk_workspace_current_revision
    FOREIGN KEY (current_revision_id) REFERENCES workspace_revisions(id) ON DELETE SET NULL;

-- 扩展 V1 Artifact 表；Run 级产物允许 task_id/attempt_id 为空，但必须有 run_id。
ALTER TABLE artifacts
    ADD COLUMN IF NOT EXISTS run_id UUID REFERENCES swarms(id) ON DELETE CASCADE,
    ADD COLUMN IF NOT EXISTS name VARCHAR(256) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS status VARCHAR(32) NOT NULL DEFAULT 'DRAFT',
    ADD COLUMN IF NOT EXISTS media_type VARCHAR(128) NOT NULL DEFAULT 'application/octet-stream',
    ADD COLUMN IF NOT EXISTS schema_version VARCHAR(64) NOT NULL DEFAULT '1',
    ADD COLUMN IF NOT EXISTS version_no INTEGER NOT NULL DEFAULT 1 CHECK (version_no > 0),
    ADD COLUMN IF NOT EXISTS size_bytes BIGINT NOT NULL DEFAULT 0 CHECK (size_bytes >= 0),
    ADD COLUMN IF NOT EXISTS object_key TEXT,
    ADD COLUMN IF NOT EXISTS validation JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS validated_at TIMESTAMPTZ;

UPDATE artifacts a
SET run_id = t.swarm_id,
    name = CASE WHEN a.name = '' THEN a.artifact_type || '-' || a.id::text ELSE a.name END
FROM tasks t
WHERE a.task_id = t.id AND a.run_id IS NULL;

ALTER TABLE artifacts DROP CONSTRAINT IF EXISTS artifacts_status_check;
ALTER TABLE artifacts ADD CONSTRAINT artifacts_status_check
    CHECK (status IN ('DRAFT','VALIDATING','VALID','INVALID','SUPERSEDED','ARCHIVED'));

CREATE TABLE IF NOT EXISTS artifact_relations (
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    from_artifact_id UUID NOT NULL REFERENCES artifacts(id) ON DELETE CASCADE,
    to_artifact_id UUID NOT NULL REFERENCES artifacts(id) ON DELETE CASCADE,
    relation_type VARCHAR(32) NOT NULL CHECK (relation_type IN (
        'DERIVED_FROM','REFINES','USES','REPLACES','VALIDATES'
    )),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (from_artifact_id, to_artifact_id, relation_type),
    CHECK (from_artifact_id <> to_artifact_id)
);

CREATE INDEX IF NOT EXISTS idx_artifacts_run
    ON artifacts (tenant_id, run_id, status, created_at DESC);

-- ContextSnapshot 记录“模型当时看到了什么”，大内容只保留 Artifact 引用与摘要。
CREATE TABLE IF NOT EXISTS context_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    run_id UUID NOT NULL REFERENCES swarms(id) ON DELETE CASCADE,
    task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    attempt_id UUID REFERENCES task_attempts(id) ON DELETE CASCADE,
    policy_version VARCHAR(64) NOT NULL,
    token_budget BIGINT NOT NULL CHECK (token_budget >= 0),
    token_used BIGINT NOT NULL DEFAULT 0 CHECK (token_used >= 0),
    manifest JSONB NOT NULL DEFAULT '{}'::jsonb,
    manifest_hash VARCHAR(128) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS context_sources (
    snapshot_id UUID NOT NULL REFERENCES context_snapshots(id) ON DELETE CASCADE,
    position INTEGER NOT NULL CHECK (position >= 0),
    source_type VARCHAR(64) NOT NULL,
    source_id VARCHAR(256) NOT NULL,
    source_version VARCHAR(128) NOT NULL DEFAULT '',
    score DOUBLE PRECISION NOT NULL DEFAULT 0,
    reason TEXT NOT NULL DEFAULT '',
    token_count BIGINT NOT NULL DEFAULT 0 CHECK (token_count >= 0),
    trust_label VARCHAR(32) NOT NULL DEFAULT 'UNTRUSTED'
        CHECK (trust_label IN ('SYSTEM','TRUSTED','UNTRUSTED','QUARANTINED')),
    content_hash VARCHAR(128) NOT NULL,
    artifact_id UUID REFERENCES artifacts(id),
    summary TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (snapshot_id, position)
);

-- Checkpoint 由“任意状态 JSON”扩展为完整恢复边界。
ALTER TABLE checkpoints
    ADD COLUMN IF NOT EXISTS context_snapshot_id UUID REFERENCES context_snapshots(id),
    ADD COLUMN IF NOT EXISTS workspace_revision_id UUID REFERENCES workspace_revisions(id),
    ADD COLUMN IF NOT EXISTS runtime_checkpoint JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS budget_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS effect_snapshot JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS fencing_token BIGINT NOT NULL DEFAULT 0 CHECK (fencing_token >= 0);

ALTER TABLE task_attempts
    ADD CONSTRAINT fk_attempt_context_snapshot
    FOREIGN KEY (context_snapshot_ref) REFERENCES context_snapshots(id) ON DELETE SET NULL,
    ADD CONSTRAINT fk_attempt_workspace_revision
    FOREIGN KEY (workspace_revision_ref) REFERENCES workspace_revisions(id) ON DELETE SET NULL,
    ADD CONSTRAINT fk_attempt_resume_checkpoint
    FOREIGN KEY (resume_checkpoint_id) REFERENCES checkpoints(id) ON DELETE SET NULL;

-- 独立 Gate 执行器产生机器证据；不能信任模型在 Output 中自报“测试通过”。
CREATE TABLE IF NOT EXISTS acceptance_gates (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    gate_key VARCHAR(128) NOT NULL,
    gate_type VARCHAR(32) NOT NULL CHECK (gate_type IN (
        'JSON_SCHEMA','COMMAND','UNIT_TEST','COVERAGE','FILE_EXISTS','SECURITY',
        'POLICY','ARTIFACT','LLM_JUDGE','HUMAN_APPROVAL'
    )),
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    required BOOLEAN NOT NULL DEFAULT true,
    version INTEGER NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (task_id, gate_key, version)
);

CREATE TABLE IF NOT EXISTS verification_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    run_id UUID NOT NULL REFERENCES swarms(id) ON DELETE CASCADE,
    task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    attempt_id UUID NOT NULL REFERENCES task_attempts(id) ON DELETE CASCADE,
    status VARCHAR(32) NOT NULL CHECK (status IN ('PENDING','RUNNING','PASSED','FAILED','ERROR','CANCELED')),
    verifier_version VARCHAR(64) NOT NULL,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gate_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    verification_run_id UUID NOT NULL REFERENCES verification_runs(id) ON DELETE CASCADE,
    gate_id UUID NOT NULL REFERENCES acceptance_gates(id) ON DELETE CASCADE,
    status VARCHAR(32) NOT NULL CHECK (status IN ('PASSED','FAILED','ERROR','SKIPPED')),
    metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    evidence_artifact_id UUID REFERENCES artifacts(id),
    output_excerpt TEXT NOT NULL DEFAULT '',
    error_message TEXT NOT NULL DEFAULT '',
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (verification_run_id, gate_id)
);

CREATE TABLE IF NOT EXISTS completion_manifests (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    run_id UUID NOT NULL REFERENCES swarms(id) ON DELETE CASCADE,
    task_id UUID REFERENCES tasks(id) ON DELETE CASCADE,
    attempt_id UUID REFERENCES task_attempts(id) ON DELETE SET NULL,
    status VARCHAR(32) NOT NULL CHECK (status IN ('DRAFT','VALID','INVALID')),
    summary TEXT NOT NULL,
    known_issues JSONB NOT NULL DEFAULT '[]'::jsonb,
    assumptions JSONB NOT NULL DEFAULT '[]'::jsonb,
    content_hash VARCHAR(128) NOT NULL,
    created_by VARCHAR(128) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    validated_at TIMESTAMPTZ,
    UNIQUE (tenant_id, run_id, task_id, content_hash)
);

CREATE TABLE IF NOT EXISTS completion_manifest_artifacts (
    manifest_id UUID NOT NULL REFERENCES completion_manifests(id) ON DELETE CASCADE,
    artifact_id UUID NOT NULL REFERENCES artifacts(id) ON DELETE RESTRICT,
    role VARCHAR(64) NOT NULL,
    PRIMARY KEY (manifest_id, artifact_id, role)
);

CREATE TABLE IF NOT EXISTS completion_manifest_evidence (
    manifest_id UUID NOT NULL REFERENCES completion_manifests(id) ON DELETE CASCADE,
    gate_result_id UUID NOT NULL REFERENCES gate_results(id) ON DELETE RESTRICT,
    PRIMARY KEY (manifest_id, gate_result_id)
);

ALTER TABLE swarms
    ADD COLUMN IF NOT EXISTS completion_manifest_id UUID REFERENCES completion_manifests(id);

-- Inbox 把 JetStream 的“至少一次”投递转换为永久业务幂等。
CREATE TABLE IF NOT EXISTS processed_events (
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    consumer_name VARCHAR(128) NOT NULL,
    event_id UUID NOT NULL,
    result JSONB NOT NULL DEFAULT '{}'::jsonb,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, consumer_name, event_id)
);

-- FailureClass 决定是在当前 Step、Attempt、Task、Plan 还是 Provider 层恢复。
CREATE TABLE IF NOT EXISTS failure_records (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    run_id UUID NOT NULL REFERENCES swarms(id) ON DELETE CASCADE,
    task_id UUID REFERENCES tasks(id) ON DELETE CASCADE,
    attempt_id UUID REFERENCES task_attempts(id) ON DELETE CASCADE,
    step_id UUID REFERENCES task_steps(id) ON DELETE SET NULL,
    failure_class VARCHAR(64) NOT NULL,
    retry_level VARCHAR(32) NOT NULL CHECK (retry_level IN (
        'NONE','STEP','ATTEMPT','TASK','PLAN','PROVIDER','HUMAN'
    )),
    code VARCHAR(128) NOT NULL,
    message TEXT NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    recovery_action VARCHAR(64) NOT NULL,
    resolved_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_failures_run
    ON failure_records (tenant_id, run_id, created_at DESC);
