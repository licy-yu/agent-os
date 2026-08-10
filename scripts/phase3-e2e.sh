#!/usr/bin/env bash
# 在真实 PostgreSQL/Redis/NATS 上验证 Scheduler → Worker → Tool Gateway → Reviewer → DAG。
# 调用前必须已经通过环境变量注入连接参数；脚本不会打印任何连接串或密码。
set -euo pipefail

project_dir="${SWARMOS_PROJECT_DIR:-/srv/projects/agent-os}"
base_url="${SWARMOS_E2E_BASE_URL:-http://127.0.0.1:8080}"
cd "$project_dir"

control_log=/tmp/swarmos-phase3-control.log
worker_log=/tmp/swarmos-phase3-worker.log
: >"$control_log"
: >"$worker_log"
./bin/control-plane -config ./configs/config.yaml >"$control_log" 2>&1 &
control_pid=$!
worker_pid=""

cleanup() {
  kill "$control_pid" 2>/dev/null || true
  if [[ -n "$worker_pid" ]]; then
    kill "$worker_pid" 2>/dev/null || true
  fi
  wait "$control_pid" 2>/dev/null || true
  if [[ -n "$worker_pid" ]]; then
    wait "$worker_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT

for _ in $(seq 1 30); do
  if curl -fsS "$base_url/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl -fsS "$base_url/healthz" >/dev/null

./bin/worker -config ./configs/config.yaml >"$worker_log" 2>&1 &
worker_pid=$!

post() {
  local path=$1
  local body=$2
  curl -fsS -X POST "$base_url$path" -H 'Content-Type: application/json' --data-binary "$body"
}

suffix=$(date +%s)
swarm=$(post /api/v1/swarms "$(jq -nc --arg suffix "$suffix" '{
  name:("phase3-e2e-"+$suffix),
  goal:"验证 Worker、工具网关、检查点、审查与 DAG 闭环",
  budgetTokens:"300000",budgetCostMicros:"1000000",maxAgents:2
}')")
swarm_id=$(jq -r .id <<<"$swarm")

template=$(post /api/v1/agent-templates "$(jq -nc --arg suffix "$suffix" '{
  name:("phase3-worker-"+$suffix),role:"executor",prompt:"严格执行任务并提交验证证据",
  model:"mock/deterministic",skills:{go:1},tools:["echo"],permissions:[],
  templateVersion:"1.0.0",contextWindow:"128000",riskZone:"sandbox",
  costPer1KTokensMicros:"1000"
}')")
template_id=$(jq -r .id <<<"$template")

agent=$(post /api/v1/agents "$(jq -nc --arg template "$template_id" --arg swarm "$swarm_id" --arg suffix "$suffix" '{
  templateId:$template,swarmId:$swarm,name:("phase3-agent-"+$suffix)
}')")
agent_id=$(jq -r .id <<<"$agent")

task_a=$(post /api/v1/tasks "$(jq -nc --arg swarm "$swarm_id" '{
  swarmId:$swarm,name:"execution-a",goal:"完成第一个确定性任务",priority:100,
  requirements:{tools:["echo"],models:["mock/deterministic"],risk_zone:"sandbox"},
  acceptance:{build:true,unit_test:true,required_checks:["contract"]}
}')")
task_a_id=$(jq -r .id <<<"$task_a")

task_b=$(post /api/v1/tasks "$(jq -nc --arg swarm "$swarm_id" --arg dependency "$task_a_id" '{
  swarmId:$swarm,name:"execution-b",goal:"等待 A 后完成第二个任务",priority:90,
  requirements:{tools:["echo"],models:["mock/deterministic"],risk_zone:"sandbox"},
  acceptance:{required_checks:["contract"]},dependencyIds:[$dependency]
}')")
task_b_id=$(jq -r .id <<<"$task_b")

status_a=""
status_b=""
for _ in $(seq 1 90); do
  status_a=$(curl -fsS "$base_url/api/v1/tasks/$task_a_id" | jq -r .status)
  status_b=$(curl -fsS "$base_url/api/v1/tasks/$task_b_id" | jq -r .status)
  if [[ "$status_a" == "SUCCEEDED" && "$status_b" == "SUCCEEDED" ]]; then
    break
  fi
  sleep 1
done

read -r attempts checkpoints tool_calls evaluations unpublished <<<"$(
  psql "$SWARMOS_DATABASE_DSN" -Atc "
    SELECT
      (SELECT count(*) FROM task_attempts WHERE task_id IN ('$task_a_id'::uuid,'$task_b_id'::uuid)),
      (SELECT count(*) FROM checkpoints c JOIN task_attempts a ON a.id=c.attempt_id
        WHERE a.task_id IN ('$task_a_id'::uuid,'$task_b_id'::uuid)),
      (SELECT count(*) FROM tool_calls WHERE task_id IN ('$task_a_id'::uuid,'$task_b_id'::uuid)
        AND status='SUCCEEDED'),
      (SELECT count(*) FROM evaluations WHERE task_id IN ('$task_a_id'::uuid,'$task_b_id'::uuid)
        AND decision='ACCEPT'),
      (SELECT count(*) FROM event_outbox WHERE published_at IS NULL);
  " | tr '|' ' '
)"

if [[ "$status_a" != "SUCCEEDED" || "$status_b" != "SUCCEEDED" ||
      "$attempts" -ne 2 || "$checkpoints" -ne 4 || "$tool_calls" -ne 2 || "$evaluations" -ne 2 ]]; then
  echo "phase3_e2e=failed task_a=$status_a task_b=$status_b attempts=$attempts checkpoints=$checkpoints tools=$tool_calls evaluations=$evaluations unpublished=$unpublished"
  echo "control_tail:"
  tail -n 30 "$control_log"
  echo "worker_tail:"
  tail -n 30 "$worker_log"
  exit 1
fi

echo "phase3_e2e=passed swarm=$swarm_id agent=$agent_id task_a=$status_a task_b=$status_b attempts=$attempts checkpoints=$checkpoints tools=$tool_calls evaluations=$evaluations unpublished=$unpublished"
