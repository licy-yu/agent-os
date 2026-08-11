-- 仅用于验证 V1(000001~000003) → V1.5 的带数据升级路径。
-- 所有值都是固定的虚构测试数据，不得从生产库导出或复制真实业务内容。

INSERT INTO swarms(
    id,name,goal,status,budget_tokens,budget_cost_micros,spent_tokens,
    spent_cost_micros,max_agents,policy,version
) VALUES (
    '10000000-0000-0000-0000-000000000001','V1 升级夹具','验证兼容迁移','PENDING',
    100000,1000000,192,0,2,'{}'::jsonb,1
);

INSERT INTO agent_templates(
    id,name,role,prompt,model,skills,tools,permissions,template_version,enabled
) VALUES (
    '10000000-0000-0000-0000-000000000002','fixture-agent','测试执行器','只处理测试数据',
    'mock/deterministic','{"go":1}'::jsonb,'["echo"]'::jsonb,'[]'::jsonb,'fixture-v1',true
);

INSERT INTO agent_instances(
    id,template_id,swarm_id,name,status,load,version
) VALUES (
    '10000000-0000-0000-0000-000000000003',
    '10000000-0000-0000-0000-000000000002',
    '10000000-0000-0000-0000-000000000001',
    'fixture-worker','IDLE',0,1
);

INSERT INTO tasks(
    id,swarm_id,name,goal,status,priority,input,requirements,acceptance,
    execution_policy,attempt_count,version
) VALUES (
    '10000000-0000-0000-0000-000000000004',
    '10000000-0000-0000-0000-000000000001',
    'V1 测试任务','产生测试 Artifact','SUCCEEDED',50,'{}'::jsonb,
    '{"skills":{"go":0.5}}'::jsonb,
    '{"required_checks":["output_nonempty"]}'::jsonb,
    '{"max_attempts":3,"max_handoffs":10,"max_tokens":10000,"max_tool_calls":10,"max_no_progress_rounds":3,"timeout_seconds":300}'::jsonb,
    1,2
);

INSERT INTO task_attempts(
    id,task_id,agent_id,attempt_no,status,input_snapshot,output_snapshot,model,
    prompt_version,started_at,finished_at,tokens_in,tokens_out,cost_micros,
    worker_id,heartbeat_at,step_count,tool_call_count,no_progress_rounds
) VALUES (
    '10000000-0000-0000-0000-000000000005',
    '10000000-0000-0000-0000-000000000004',
    '10000000-0000-0000-0000-000000000003',
    1,'SUCCEEDED','{}'::jsonb,'{"output":{"ok":true}}'::jsonb,
    'mock/deterministic','fixture-v1',now(),now(),128,64,0,
    'fixture-worker',now(),2,1,0
);

INSERT INTO checkpoints(id,attempt_id,sequence,step_name,state,artifact_refs)
VALUES (
    '10000000-0000-0000-0000-000000000006',
    '10000000-0000-0000-0000-000000000005',
    1,'completed','{"done":true}'::jsonb,'["fixture://result"]'::jsonb
);

INSERT INTO artifacts(id,task_id,attempt_id,artifact_type,uri,version,content_hash,metadata)
VALUES (
    '10000000-0000-0000-0000-000000000007',
    '10000000-0000-0000-0000-000000000004',
    '10000000-0000-0000-0000-000000000005',
    'result','fixture://result','1','sha256:fixture','{}'::jsonb
);

INSERT INTO evaluations(
    id,task_id,attempt_id,reviewer,machine_pass,policy_pass,quality_score,decision,findings
) VALUES (
    '10000000-0000-0000-0000-000000000008',
    '10000000-0000-0000-0000-000000000004',
    '10000000-0000-0000-0000-000000000005',
    'fixture-reviewer',true,true,1,'ACCEPT','[]'::jsonb
);

INSERT INTO event_outbox(
    id,aggregate_type,aggregate_id,event_type,aggregate_version,payload,published_at
) VALUES (
    '10000000-0000-0000-0000-000000000009',
    'task','10000000-0000-0000-0000-000000000004','task.succeeded',2,
    '{"id":"10000000-0000-0000-0000-000000000004"}'::jsonb,now()
);
