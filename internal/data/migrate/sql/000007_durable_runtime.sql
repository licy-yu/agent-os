-- SwarmOS V1.5 Durable Runtime 增量迁移。
--
-- Temporal WorkflowID 是业务 Run 的稳定外部身份，Temporal RunID 标识当前执行历史。
-- 两列允许 NULL，保证现有 LEGACY Run 和滚动升级期间的旧控制面继续工作。
ALTER TABLE swarms
    ADD COLUMN IF NOT EXISTS temporal_workflow_id VARCHAR(256),
    ADD COLUMN IF NOT EXISTS temporal_run_id VARCHAR(256),
    ADD COLUMN IF NOT EXISTS runtime_attached_at TIMESTAMPTZ;

CREATE UNIQUE INDEX IF NOT EXISTS idx_swarms_temporal_workflow
    ON swarms (tenant_id, temporal_workflow_id)
    WHERE temporal_workflow_id IS NOT NULL;

COMMENT ON COLUMN swarms.temporal_workflow_id IS
    '确定性 Temporal WorkflowID；格式 swarmos/run/{run_uuid}，用于幂等启动和 Signal';
COMMENT ON COLUMN swarms.temporal_run_id IS
    'Temporal Server 分配的具体 Workflow RunID，供恢复、重放和运维定位';
