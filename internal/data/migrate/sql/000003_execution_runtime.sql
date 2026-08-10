-- 阶段 3：Worker 执行账本、检查点、工具审计与 Reviewer 证据。
-- 这些表全部以 Attempt 为审计边界；逻辑 Task 可以重试，但历史执行记录永不覆盖。

ALTER TABLE task_attempts
    ADD COLUMN IF NOT EXISTS worker_id VARCHAR(128),
    ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS step_count INTEGER NOT NULL DEFAULT 0 CHECK (step_count >= 0),
    ADD COLUMN IF NOT EXISTS tool_call_count INTEGER NOT NULL DEFAULT 0 CHECK (tool_call_count >= 0),
    ADD COLUMN IF NOT EXISTS no_progress_rounds INTEGER NOT NULL DEFAULT 0 CHECK (no_progress_rounds >= 0);

CREATE INDEX IF NOT EXISTS idx_attempts_running_heartbeat
    ON task_attempts (heartbeat_at)
    WHERE status = 'RUNNING';

CREATE TABLE IF NOT EXISTS checkpoints (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    attempt_id UUID NOT NULL REFERENCES task_attempts(id) ON DELETE CASCADE,
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    step_name VARCHAR(128) NOT NULL,
    state JSONB NOT NULL DEFAULT '{}'::jsonb,
    artifact_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (attempt_id, sequence)
);

COMMENT ON TABLE checkpoints IS 'Worker 可恢复执行检查点；同一 Attempt 内 sequence 单调递增';
CREATE INDEX IF NOT EXISTS idx_checkpoints_attempt ON checkpoints (attempt_id, sequence DESC);

CREATE TABLE IF NOT EXISTS artifacts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    attempt_id UUID NOT NULL REFERENCES task_attempts(id) ON DELETE CASCADE,
    artifact_type VARCHAR(64) NOT NULL,
    uri TEXT NOT NULL,
    version VARCHAR(64) NOT NULL,
    content_hash VARCHAR(128) NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (attempt_id, uri, version)
);

COMMENT ON TABLE artifacts IS '执行产物只保存引用、版本和哈希，大对象交给对象存储';
CREATE INDEX IF NOT EXISTS idx_artifacts_task ON artifacts (task_id, created_at DESC);

CREATE TABLE IF NOT EXISTS tool_registry (
    name VARCHAR(128) PRIMARY KEY,
    description TEXT NOT NULL,
    adapter VARCHAR(32) NOT NULL CHECK (adapter IN ('native','mcp')),
    input_schema JSONB NOT NULL DEFAULT '{}'::jsonb,
    required_permissions JSONB NOT NULL DEFAULT '[]'::jsonb,
    risk_level VARCHAR(32) NOT NULL CHECK (risk_level IN ('read-only','sandbox','trusted','production')),
    enabled BOOLEAN NOT NULL DEFAULT true,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE tool_registry IS 'Tool Gateway 的声明式注册表；密钥不得存入 config';

CREATE TABLE IF NOT EXISTS tool_calls (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    attempt_id UUID NOT NULL REFERENCES task_attempts(id) ON DELETE CASCADE,
    task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    agent_id UUID NOT NULL REFERENCES agent_instances(id),
    tool_name VARCHAR(128) NOT NULL,
    arguments JSONB NOT NULL DEFAULT '{}'::jsonb,
    result JSONB NOT NULL DEFAULT '{}'::jsonb,
    status VARCHAR(32) NOT NULL CHECK (status IN ('STARTED','SUCCEEDED','FAILED','DENIED')),
    risk_level VARCHAR(32) NOT NULL,
    error_message TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ
);

COMMENT ON TABLE tool_calls IS '所有工具调用（包括被拒绝的调用）的完整审计日志';
CREATE INDEX IF NOT EXISTS idx_tool_calls_attempt ON tool_calls (attempt_id, started_at);
CREATE INDEX IF NOT EXISTS idx_tool_calls_task ON tool_calls (task_id, started_at DESC);

CREATE TABLE IF NOT EXISTS evaluations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    attempt_id UUID NOT NULL REFERENCES task_attempts(id) ON DELETE CASCADE,
    reviewer VARCHAR(128) NOT NULL,
    machine_pass BOOLEAN NOT NULL,
    policy_pass BOOLEAN NOT NULL,
    quality_score DOUBLE PRECISION NOT NULL CHECK (quality_score BETWEEN 0 AND 1),
    decision VARCHAR(32) NOT NULL CHECK (decision IN ('ACCEPT','RETRY','REJECT')),
    findings JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (attempt_id)
);

COMMENT ON TABLE evaluations IS 'Reviewer 的机器检查、策略检查、质量分和最终决策证据';
CREATE INDEX IF NOT EXISTS idx_evaluations_task ON evaluations (task_id, created_at DESC);

-- 内置 echo 只回显结构化参数，用于冒烟测试和演示完整审计链路。
-- ON CONFLICT 不覆盖管理员修改，迁移可以安全重复启动。
INSERT INTO tool_registry(name,description,adapter,input_schema,required_permissions,risk_level)
VALUES (
    'echo',
    '安全回显结构化参数，用于验证 Tool Gateway 权限、额度和审计链路',
    'native',
    '{"type":"object","additionalProperties":true}'::jsonb,
    '[]'::jsonb,
    'sandbox'
)
ON CONFLICT (name) DO NOTHING;
