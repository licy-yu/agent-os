-- Runtime FailureRecord 闭环：Worker 分类器输出的是“实际恢复动作所在层级”，
-- 不是早期 schema 中的抽象 TASK/PLAN/PROVIDER 名称。保留旧值兼容已有数据，
-- 同时允许分类器当前会写出的四个精确层级，避免 CompleteAttempt 因 CHECK 失败回滚。
ALTER TABLE failure_records
    DROP CONSTRAINT IF EXISTS failure_records_retry_level_check;

ALTER TABLE failure_records
    ADD CONSTRAINT failure_records_retry_level_check CHECK (retry_level IN (
        'NONE','STEP','ATTEMPT','TASK','PLAN','PROVIDER','HUMAN',
        'AGENT_SWITCH','MODEL_SWITCH','TASK_REPLAN','RUN_REPLAN'
    ));
