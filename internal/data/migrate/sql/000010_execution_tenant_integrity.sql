-- SwarmOS V1.5：为核心执行链补齐数据库级租户一致性约束。
--
-- 000004 为兼容 V1 写路径，给若干历史表保留了默认 tenant_id。默认值只能帮助
-- 老数据升级，不能证明 Run、Task、Attempt 与 Effect 真的属于同一租户。本迁移先按
-- 各对象的事实父级回填，再增加 (id, tenant_id) 复合外键；今后仓储一旦漏传或传错
-- tenant_id，事务会立即失败，避免生成只能写入、无法审批或无法恢复的孤儿记录。

-- Task 的租户以所属 Run 为准。
UPDATE tasks AS t
SET tenant_id = s.tenant_id
FROM swarms AS s
WHERE s.id = t.swarm_id
  AND t.tenant_id IS DISTINCT FROM s.tenant_id;

-- Agent 模板是 Agent 身份的来源。若 Agent 同时绑定了不同租户的 Run，后面的复合
-- 外键校验会故意失败，要求运维先修正歧义，而不是静默选择某一方。
UPDATE agent_instances AS a
SET tenant_id = template.tenant_id
FROM agent_templates AS template
WHERE template.id = a.template_id
  AND a.tenant_id IS DISTINCT FROM template.tenant_id;

-- Attempt 及其执行证据以 Task 为事实租户；这是 Worker 恢复、审批与对账的共同边界。
UPDATE task_attempts AS attempt
SET tenant_id = task.tenant_id
FROM tasks AS task
WHERE task.id = attempt.task_id
  AND attempt.tenant_id IS DISTINCT FROM task.tenant_id;

UPDATE tool_calls AS call
SET tenant_id = task.tenant_id
FROM tasks AS task
WHERE task.id = call.task_id
  AND call.tenant_id IS DISTINCT FROM task.tenant_id;

UPDATE evaluations AS evaluation
SET tenant_id = task.tenant_id
FROM tasks AS task
WHERE task.id = evaluation.task_id
  AND evaluation.tenant_id IS DISTINCT FROM task.tenant_id;

UPDATE effects AS effect
SET tenant_id = task.tenant_id
FROM tasks AS task
WHERE task.id = effect.task_id
  AND effect.tenant_id IS DISTINCT FROM task.tenant_id;

-- Artifact 允许 Run 级记录没有 task_id；优先使用 Task，缺省时使用 Run。
UPDATE artifacts AS artifact
SET tenant_id = owner.tenant_id
FROM (
    SELECT artifact_owner.id,
           COALESCE(task.tenant_id, run.tenant_id) AS tenant_id
    FROM artifacts AS artifact_owner
    LEFT JOIN tasks AS task ON task.id = artifact_owner.task_id
    LEFT JOIN swarms AS run ON run.id = artifact_owner.run_id
) AS owner
WHERE owner.id = artifact.id
  AND owner.tenant_id IS NOT NULL
  AND artifact.tenant_id IS DISTINCT FROM owner.tenant_id;

-- Interaction 可能是 Run 级输入，也可能绑定 Task/Effect；Task 最具体，其次 Effect，
-- 最后回退到 Run。后面的所有复合外键仍会检查三者没有相互矛盾。
UPDATE interactions AS interaction
SET tenant_id = owner.tenant_id
FROM (
    SELECT interaction_owner.id,
           COALESCE(task.tenant_id, effect.tenant_id, run.tenant_id) AS tenant_id
    FROM interactions AS interaction_owner
    LEFT JOIN tasks AS task ON task.id = interaction_owner.task_id
    LEFT JOIN effects AS effect ON effect.id = interaction_owner.effect_id
    LEFT JOIN swarms AS run ON run.id = interaction_owner.run_id
) AS owner
WHERE owner.id = interaction.id
  AND owner.tenant_id IS NOT NULL
  AND interaction.tenant_id IS DISTINCT FROM owner.tenant_id;

-- PostgreSQL 的复合外键要求被引用列拥有唯一索引。id 本身已是主键，以下索引额外
-- 表达“对象 ID 与租户不可拆分”的引用合同。
CREATE UNIQUE INDEX IF NOT EXISTS ux_swarms_id_tenant
    ON swarms (id, tenant_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_agent_templates_id_tenant
    ON agent_templates (id, tenant_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_agent_instances_id_tenant
    ON agent_instances (id, tenant_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_tasks_id_tenant
    ON tasks (id, tenant_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_task_attempts_id_tenant
    ON task_attempts (id, tenant_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_tool_calls_id_tenant
    ON tool_calls (id, tenant_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_effects_id_tenant
    ON effects (id, tenant_id);

ALTER TABLE tasks
    ADD CONSTRAINT fk_tasks_run_tenant
    FOREIGN KEY (swarm_id, tenant_id) REFERENCES swarms (id, tenant_id)
    ON DELETE CASCADE NOT VALID;

ALTER TABLE agent_instances
    ADD CONSTRAINT fk_agent_template_tenant
    FOREIGN KEY (template_id, tenant_id) REFERENCES agent_templates (id, tenant_id)
    NOT VALID,
    ADD CONSTRAINT fk_agent_run_tenant
    FOREIGN KEY (swarm_id, tenant_id) REFERENCES swarms (id, tenant_id)
    -- 只清空可空的父 ID；tenant_id 是对象身份的一部分，永远不能被级联置空。
    ON DELETE SET NULL (swarm_id) NOT VALID;

ALTER TABLE task_attempts
    ADD CONSTRAINT fk_attempt_task_tenant
    FOREIGN KEY (task_id, tenant_id) REFERENCES tasks (id, tenant_id)
    ON DELETE CASCADE NOT VALID,
    ADD CONSTRAINT fk_attempt_agent_tenant
    FOREIGN KEY (agent_id, tenant_id) REFERENCES agent_instances (id, tenant_id)
    NOT VALID;

ALTER TABLE tool_calls
    ADD CONSTRAINT fk_tool_call_attempt_tenant
    FOREIGN KEY (attempt_id, tenant_id) REFERENCES task_attempts (id, tenant_id)
    ON DELETE CASCADE NOT VALID,
    ADD CONSTRAINT fk_tool_call_task_tenant
    FOREIGN KEY (task_id, tenant_id) REFERENCES tasks (id, tenant_id)
    ON DELETE CASCADE NOT VALID,
    ADD CONSTRAINT fk_tool_call_agent_tenant
    FOREIGN KEY (agent_id, tenant_id) REFERENCES agent_instances (id, tenant_id)
    NOT VALID;

ALTER TABLE effects
    ADD CONSTRAINT fk_effect_run_tenant
    FOREIGN KEY (run_id, tenant_id) REFERENCES swarms (id, tenant_id)
    ON DELETE CASCADE NOT VALID,
    ADD CONSTRAINT fk_effect_task_tenant
    FOREIGN KEY (task_id, tenant_id) REFERENCES tasks (id, tenant_id)
    ON DELETE CASCADE NOT VALID,
    ADD CONSTRAINT fk_effect_attempt_tenant
    FOREIGN KEY (attempt_id, tenant_id) REFERENCES task_attempts (id, tenant_id)
    ON DELETE CASCADE NOT VALID,
    ADD CONSTRAINT fk_effect_tool_call_tenant
    FOREIGN KEY (tool_call_id, tenant_id) REFERENCES tool_calls (id, tenant_id)
    NOT VALID;

ALTER TABLE interactions
    ADD CONSTRAINT fk_interaction_run_tenant
    FOREIGN KEY (run_id, tenant_id) REFERENCES swarms (id, tenant_id)
    ON DELETE CASCADE NOT VALID,
    ADD CONSTRAINT fk_interaction_task_tenant
    FOREIGN KEY (task_id, tenant_id) REFERENCES tasks (id, tenant_id)
    ON DELETE CASCADE NOT VALID,
    ADD CONSTRAINT fk_interaction_attempt_tenant
    FOREIGN KEY (attempt_id, tenant_id) REFERENCES task_attempts (id, tenant_id)
    ON DELETE CASCADE NOT VALID,
    ADD CONSTRAINT fk_interaction_effect_tenant
    FOREIGN KEY (effect_id, tenant_id) REFERENCES effects (id, tenant_id)
    ON DELETE SET NULL (effect_id) NOT VALID;

ALTER TABLE evaluations
    ADD CONSTRAINT fk_evaluation_task_tenant
    FOREIGN KEY (task_id, tenant_id) REFERENCES tasks (id, tenant_id)
    ON DELETE CASCADE NOT VALID,
    ADD CONSTRAINT fk_evaluation_attempt_tenant
    FOREIGN KEY (attempt_id, tenant_id) REFERENCES task_attempts (id, tenant_id)
    ON DELETE CASCADE NOT VALID;

ALTER TABLE artifacts
    ADD CONSTRAINT fk_artifact_run_tenant
    FOREIGN KEY (run_id, tenant_id) REFERENCES swarms (id, tenant_id)
    ON DELETE CASCADE NOT VALID,
    ADD CONSTRAINT fk_artifact_task_tenant
    FOREIGN KEY (task_id, tenant_id) REFERENCES tasks (id, tenant_id)
    ON DELETE CASCADE NOT VALID,
    ADD CONSTRAINT fk_artifact_attempt_tenant
    FOREIGN KEY (attempt_id, tenant_id) REFERENCES task_attempts (id, tenant_id)
    ON DELETE CASCADE NOT VALID;

-- NOT VALID 先缩短持锁时间；紧接着显式 VALIDATE，确保迁移成功即代表历史数据也满足合同。
ALTER TABLE tasks VALIDATE CONSTRAINT fk_tasks_run_tenant;
ALTER TABLE agent_instances
    VALIDATE CONSTRAINT fk_agent_template_tenant,
    VALIDATE CONSTRAINT fk_agent_run_tenant;
ALTER TABLE task_attempts
    VALIDATE CONSTRAINT fk_attempt_task_tenant,
    VALIDATE CONSTRAINT fk_attempt_agent_tenant;
ALTER TABLE tool_calls
    VALIDATE CONSTRAINT fk_tool_call_attempt_tenant,
    VALIDATE CONSTRAINT fk_tool_call_task_tenant,
    VALIDATE CONSTRAINT fk_tool_call_agent_tenant;
ALTER TABLE effects
    VALIDATE CONSTRAINT fk_effect_run_tenant,
    VALIDATE CONSTRAINT fk_effect_task_tenant,
    VALIDATE CONSTRAINT fk_effect_attempt_tenant,
    VALIDATE CONSTRAINT fk_effect_tool_call_tenant;
ALTER TABLE interactions
    VALIDATE CONSTRAINT fk_interaction_run_tenant,
    VALIDATE CONSTRAINT fk_interaction_task_tenant,
    VALIDATE CONSTRAINT fk_interaction_attempt_tenant,
    VALIDATE CONSTRAINT fk_interaction_effect_tenant;
ALTER TABLE evaluations
    VALIDATE CONSTRAINT fk_evaluation_task_tenant,
    VALIDATE CONSTRAINT fk_evaluation_attempt_tenant;
ALTER TABLE artifacts
    VALIDATE CONSTRAINT fk_artifact_run_tenant,
    VALIDATE CONSTRAINT fk_artifact_task_tenant,
    VALIDATE CONSTRAINT fk_artifact_attempt_tenant;

-- 运行时证据已经全部改为显式传 tenant_id，移除兼容默认值可让未来回归 fail-closed。
ALTER TABLE task_attempts ALTER COLUMN tenant_id DROP DEFAULT;
ALTER TABLE tool_calls ALTER COLUMN tenant_id DROP DEFAULT;
ALTER TABLE evaluations ALTER COLUMN tenant_id DROP DEFAULT;
ALTER TABLE artifacts ALTER COLUMN tenant_id DROP DEFAULT;
