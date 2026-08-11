-- 对 v1_upgrade_fixture.sql 升级后的关键不变量做数据库级断言。
DO $$
DECLARE
    fixture_run UUID := '10000000-0000-0000-0000-000000000001';
    fixture_task UUID := '10000000-0000-0000-0000-000000000004';
    fixture_artifact UUID := '10000000-0000-0000-0000-000000000007';
    fixture_event UUID := '10000000-0000-0000-0000-000000000009';
BEGIN
    IF (SELECT count(*) FROM tenants WHERE slug='default') <> 1 THEN
        RAISE EXCEPTION '默认 tenant 回填失败';
    END IF;
    IF (SELECT tenant_id IS NULL FROM swarms WHERE id=fixture_run) THEN
        RAISE EXCEPTION 'V1 Run tenant_id 未回填';
    END IF;
    IF (SELECT tenant_id IS NULL FROM tasks WHERE id=fixture_task) THEN
        RAISE EXCEPTION 'V1 Task tenant_id 未回填';
    END IF;
    IF (SELECT run_id FROM artifacts WHERE id=fixture_artifact) IS DISTINCT FROM fixture_run THEN
        RAISE EXCEPTION 'Artifact run_id 未从 task.swarm_id 回填';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM timeline_events
        WHERE source_event_id=fixture_event AND run_id=fixture_run AND task_id=fixture_task
    ) THEN
        RAISE EXCEPTION '历史 Outbox 未投影到正确 Timeline';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema='public' AND table_name='effects'
    ) OR NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema='public' AND table_name='interactions'
    ) OR NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema='public' AND table_name='verification_runs'
    ) THEN
        RAISE EXCEPTION 'P0 安全表缺失';
    END IF;
END $$;

SELECT json_build_object(
    'fixture_runs', (SELECT count(*) FROM swarms WHERE id='10000000-0000-0000-0000-000000000001'),
    'backfilled_artifacts', (SELECT count(*) FROM artifacts WHERE run_id='10000000-0000-0000-0000-000000000001'),
    'projected_timeline_events', (SELECT count(*) FROM timeline_events WHERE run_id='10000000-0000-0000-0000-000000000001'),
    'core_v15_tables', (SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name IN (
        'plan_versions','task_steps','effects','interactions','workspaces','context_snapshots',
        'acceptance_gates','verification_runs','completion_manifests','timeline_events'
    ))
);
