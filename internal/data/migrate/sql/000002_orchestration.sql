-- 阶段 2：为可靠 Outbox 认领与调度查询补充字段和索引。

ALTER TABLE event_outbox
    ADD COLUMN IF NOT EXISTS locked_by VARCHAR(128),
    ADD COLUMN IF NOT EXISTS locked_until TIMESTAMPTZ;

-- 调度硬过滤和成本评分需要模板明确声明，而不能依赖模型名猜测。
ALTER TABLE agent_templates
    ADD COLUMN IF NOT EXISTS context_window BIGINT NOT NULL DEFAULT 0 CHECK (context_window >= 0),
    ADD COLUMN IF NOT EXISTS risk_zone VARCHAR(64) NOT NULL DEFAULT 'sandbox',
    ADD COLUMN IF NOT EXISTS cost_per_1k_tokens_micros BIGINT NOT NULL DEFAULT 0 CHECK (cost_per_1k_tokens_micros >= 0);

DROP INDEX IF EXISTS idx_outbox_unpublished;
CREATE INDEX idx_outbox_unpublished
    ON event_outbox (available_at, created_at)
    WHERE published_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_outbox_expired_lock
    ON event_outbox (locked_until)
    WHERE published_at IS NULL AND locked_by IS NOT NULL;

-- Controller 只观察少量非终态，部分索引能降低周期性 reconcile 的扫描成本。
CREATE INDEX IF NOT EXISTS idx_tasks_reconcile
    ON tasks (status, available_at, updated_at)
    WHERE status IN ('CREATED','PLANNING','BLOCKED','RETRY_WAIT');

-- Scheduler 的事实查询始终从 READY + available_at 开始。
CREATE INDEX IF NOT EXISTS idx_tasks_schedule_available
    ON tasks (available_at, priority DESC, created_at)
    WHERE status = 'READY';
