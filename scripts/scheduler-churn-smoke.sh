#!/usr/bin/env bash
set -Eeuo pipefail

# 这个脚本专门验证生产调度器的“空转零写入”合同：
# 1. 创建 TEMPORAL Run，但暂不注册 Agent；
# 2. 等 Task 进入 READY，并记录 version、Task Outbox、Scheduler Explain 数量；
# 3. 持续观察一段时间，确认三个值均不再增长；
# 4. 注册 Agent，确认下一轮调度能够继续执行并最终完成。
#
# 脚本不会打印 API Key，也不会删除任何 Run、Artifact 或审计证据。成功产生的验收
# Run 会保留在 Console 中，便于发布后追溯。

PROJECT_DIR="${SWARMOS_PROJECT_DIR:-/srv/projects/agent-os}"
BASE_URL="${SWARMOS_BASE_URL:-http://127.0.0.1:8080}"
PG_CONTAINER="${SWARMOS_PG_CONTAINER:-dev-postgres}"
PG_USER="${SWARMOS_PG_USER:-app}"
PG_DATABASE="${SWARMOS_PG_DATABASE:-swarmos}"
OBSERVE_SECONDS="${SWARMOS_SCHEDULER_OBSERVE_SECONDS:-10}"

if [[ ! "${OBSERVE_SECONDS}" =~ ^[0-9]+$ || "${OBSERVE_SECONDS}" -lt 5 ]]; then
  echo "SWARMOS_SCHEDULER_OBSERVE_SECONDS 必须是大于等于 5 的整数秒" >&2
  exit 1
fi

for command_name in curl jq docker sed cut mktemp; do
  command -v "${command_name}" >/dev/null 2>&1 || {
    echo "缺少命令: ${command_name}" >&2
    exit 1
  }
done

cd "${PROJECT_DIR}"

# 优先使用调用者已注入的 Key；否则只读取 production .env 的目标变量。这里不 source
# 整个文件，避免把运维目录中非预期的 Shell 内容当成代码执行。
if [[ -z "${SWARMOS_API_KEY:-}" ]]; then
  SWARMOS_API_KEY="$(sed -n 's/^SWARMOS_API_KEY=//p' .env | tail -n 1)"
fi
if [[ ${#SWARMOS_API_KEY} -lt 32 ]]; then
  echo "SWARMOS_API_KEY 缺失或长度不足" >&2
  exit 1
fi

# 不把 Bearer 值放进 curl 的 argv；同机其他用户可能通过 ps 或 /proc 看到进程参数。
# Header 只写入 mktemp 创建的 0600 文件，curl argv 中仅出现文件名，退出时精确删除。
auth_header_file="$(mktemp "${TMPDIR:-/tmp}/swarmos-scheduler-smoke.XXXXXX")"
chmod 600 "${auth_header_file}"
cleanup() {
  rm -f -- "${auth_header_file}"
}
trap cleanup EXIT
printf 'Authorization: Bearer %s\n' "${SWARMOS_API_KEY}" >"${auth_header_file}"
unset SWARMOS_API_KEY

api_get() {
  curl -fsS --connect-timeout 5 --max-time 30 \
    -H "@${auth_header_file}" \
    "${BASE_URL}$1"
}

api_post() {
  local path="$1"
  curl -fsS --connect-timeout 5 --max-time 30 -X POST \
    -H "@${auth_header_file}" \
    -H 'Content-Type: application/json' \
    --data-binary @- \
    "${BASE_URL}${path}"
}

query_db() {
  docker exec "${PG_CONTAINER}" psql \
    -X -v ON_ERROR_STOP=1 -U "${PG_USER}" -d "${PG_DATABASE}" -Atc "$1"
}

template_id="$(api_get '/api/v1/agent-templates?limit=100' \
  | jq -er 'first(.items[] | select(.enabled == true and .model == "mock/deterministic") | .id)')"
if [[ ! "${template_id}" =~ ^[0-9a-fA-F-]{36}$ ]]; then
  echo "没有可用于验收的已启用 mock/deterministic AgentTemplate" >&2
  exit 1
fi

run_json="$(jq -nc '{
  name:"v1.5.4 Scheduler 空转验收",
  goal:"验证无 Agent 时 READY Task 不增加版本和 Outbox，注册 Agent 后正常完成",
  budgetTokens:300000,
  budgetCostMicros:5000000,
  maxAgents:1,
  priority:50,
  executionEngine:"TEMPORAL"
}' | api_post '/api/v1/runs')"
run_id="$(jq -er '.id' <<<"${run_json}")"
if [[ ! "${run_id}" =~ ^[0-9a-fA-F-]{36}$ ]]; then
  echo "创建 Run 后未取得合法 ID" >&2
  exit 1
fi
echo "scheduler_churn_run_id=${run_id}"

# 等待控制器把初始 Task 推进到 READY。整个等待期间故意不创建 Agent，因此后续任何
# version/Outbox 增长都只能来自调度空转，而不能归因于真实分配或执行。
task_id=""
for _ in $(seq 1 30); do
  task_row="$(query_db "SELECT id::text || '|' || status FROM tasks WHERE swarm_id='${run_id}'::uuid ORDER BY created_at LIMIT 1")"
  if [[ "${task_row}" == *'|READY' ]]; then
    task_id="${task_row%%|*}"
    break
  fi
  sleep 1
done
if [[ ! "${task_id}" =~ ^[0-9a-fA-F-]{36}$ ]]; then
  echo "Task 未在 30 秒内进入 READY" >&2
  exit 1
fi

# 先等第一条未选中 Explain 出现，再取基线；这样不会把一次合法的首次审计插入误判为
# 写放大。热修后，相同 task_id + task_version 的未选中 Explain 会在数据库中幂等合并。
version_before="$(query_db "SELECT version FROM tasks WHERE id='${task_id}'::uuid")"
for _ in $(seq 1 10); do
  explain_count="$(query_db "
    SELECT count(*) FROM scheduler_decisions
    WHERE task_id='${task_id}'::uuid
      AND task_version=${version_before}
      AND selected_agent_id IS NULL")"
  [[ "${explain_count}" -ge 1 ]] && break
  sleep 1
done
if [[ "${explain_count:-0}" -lt 1 ]]; then
  echo "Task READY 后 10 秒内没有生成 Scheduler Explain" >&2
  exit 1
fi

outbox_before="$(query_db "SELECT count(*) FROM event_outbox WHERE aggregate_type='task' AND aggregate_id='${task_id}'::uuid")"
explain_before="${explain_count}"
# count 只能证明没有新增行，无法识别同一行是否每秒 UPDATE。把 PostgreSQL xmin、
# created_at 和证据内容摘要一起纳入快照，可以捕捉任何 MVCC 行版本变化与内容变化。
explain_snapshot_before="$(query_db "
  SELECT id::text || '|' || xmin::text || '|' || created_at::text || '|' ||
         md5(candidates::text || '|' || filters::text || '|' || reason)
  FROM scheduler_decisions
  WHERE task_id='${task_id}'::uuid
    AND task_version=${version_before}
    AND selected_agent_id IS NULL
  ORDER BY created_at DESC,id DESC LIMIT 1")"
if [[ -z "${explain_snapshot_before}" ]]; then
  echo "Scheduler Explain 基线为空" >&2
  exit 1
fi

sleep "${OBSERVE_SECONDS}"

status_after="$(query_db "SELECT status FROM tasks WHERE id='${task_id}'::uuid")"
version_after="$(query_db "SELECT version FROM tasks WHERE id='${task_id}'::uuid")"
outbox_after="$(query_db "SELECT count(*) FROM event_outbox WHERE aggregate_type='task' AND aggregate_id='${task_id}'::uuid")"
explain_after="$(query_db "
  SELECT count(*) FROM scheduler_decisions
  WHERE task_id='${task_id}'::uuid
    AND task_version=${version_before}
    AND selected_agent_id IS NULL")"
explain_snapshot_after="$(query_db "
  SELECT id::text || '|' || xmin::text || '|' || created_at::text || '|' ||
         md5(candidates::text || '|' || filters::text || '|' || reason)
  FROM scheduler_decisions
  WHERE task_id='${task_id}'::uuid
    AND task_version=${version_before}
    AND selected_agent_id IS NULL
  ORDER BY created_at DESC,id DESC LIMIT 1")"
if [[ -z "${explain_snapshot_after}" ]]; then
  echo "Scheduler Explain 观察后快照为空" >&2
  exit 1
fi

[[ "${status_after}" == 'READY' ]]
[[ "${version_after}" == "${version_before}" ]]
[[ "${outbox_after}" == "${outbox_before}" ]]
[[ "${explain_after}" == "${explain_before}" ]]
[[ "${explain_snapshot_after}" == "${explain_snapshot_before}" ]]
explain_xmin="$(cut -d '|' -f 2 <<<"${explain_snapshot_after}")"
printf 'idle_window_seconds=%s task_version=%s task_outbox=%s scheduler_explain=%s scheduler_explain_xmin=%s\n' \
  "${OBSERVE_SECONDS}" "${version_after}" "${outbox_after}" "${explain_after}" "${explain_xmin}"

agent_json="$(jq -nc \
  --arg templateId "${template_id}" \
  --arg swarmId "${run_id}" \
  '{templateId:$templateId,swarmId:$swarmId,name:"v1.5.4-scheduler-smoke-agent"}' \
  | api_post '/api/v1/agents')"
agent_id="$(jq -er '.id' <<<"${agent_json}")"
[[ "${agent_id}" =~ ^[0-9a-fA-F-]{36}$ ]]

run_status=""
for _ in $(seq 1 50); do
  run_status="$(api_get "/api/v1/runs/${run_id}" | jq -er '.status')"
  [[ "${run_status}" == 'COMPLETED' ]] && break
  [[ "${run_status}" == 'FAILED' || "${run_status}" == 'CANCELED' ]] && break
  sleep 1
done
if [[ "${run_status}" != 'COMPLETED' ]]; then
  echo "注册 Agent 后 Run 未完成，最终状态=${run_status}" >&2
  exit 1
fi

manifest_id="$(query_db "SELECT completion_manifest_id::text FROM swarms WHERE id='${run_id}'::uuid")"
if [[ ! "${manifest_id}" =~ ^[0-9a-fA-F-]{36}$ ]]; then
  echo "Run 已完成，但没有合法 Completion Manifest ID" >&2
  exit 1
fi
manifest_http="$(curl -sS --connect-timeout 5 --max-time 30 -o /dev/null -w '%{http_code}' \
  -H "@${auth_header_file}" \
  "${BASE_URL}/api/v1/completion-manifests/${manifest_id}")"
if [[ "${manifest_http}" != '200' ]]; then
  echo "Completion Manifest 读取失败，HTTP=${manifest_http}" >&2
  exit 1
fi

echo "scheduler_churn_smoke=passed run_status=${run_status} manifest_http=${manifest_http} manifest_id=${manifest_id}"
