-- SwarmOS V1.5：人工审批/外部对账必须是可长期等待、可恢复的持久状态。
--
-- 旧实现让 Attempt 一直保持 RUNNING，并每 5 秒 NAK JetStream 消息。审批窗口可达
-- 24 小时，但 Consumer 的投递上限很快就会耗尽，而且 RecoveryController 会把它
-- 误判成心跳超时。WAITING 表示 Worker 已经主动释放执行权：不需要心跳、不占消息，
-- 之后由审批/对账事务重新发出 task.assigned，并在 ClaimWork 时签发新 fence。

ALTER TABLE task_attempts DROP CONSTRAINT IF EXISTS task_attempts_status_check;
ALTER TABLE task_attempts ADD CONSTRAINT task_attempts_status_check CHECK (status IN (
    'CREATED','RUNNING','WAITING','REVIEW','SUCCEEDED','FAILED','ABORTED','CANCELED'
));

CREATE INDEX IF NOT EXISTS idx_task_attempts_waiting
    ON task_attempts (task_id, attempt_no DESC)
    WHERE status = 'WAITING';
