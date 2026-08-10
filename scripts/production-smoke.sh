#!/usr/bin/env bash
set -Eeuo pipefail

# 生产冒烟只读现有数据，不创建 Swarm/Task，也不会改变预算或调度状态。
base_url="${SWARMOS_BASE_URL:-http://127.0.0.1:8080}"
worker_metrics_url="${SWARMOS_WORKER_METRICS_URL:-http://127.0.0.1:9465/metrics}"

health="$(curl --fail --silent --show-error "${base_url}/healthz")"
if [[ "${health}" != "ok"$'\n' && "${health}" != "ok" ]]; then
  echo "控制面健康检查返回异常: ${health}" >&2
  exit 1
fi
page="$(curl --fail --silent --show-error "${base_url}/")"
control_metrics="$(curl --fail --silent --show-error "${base_url}/metrics")"
worker_metrics="$(curl --fail --silent --show-error "${worker_metrics_url}")"
grep --quiet "SwarmOS Control Room" <<<"${page}"
grep --quiet "swarmos_server_requests_total" <<<"${control_metrics}"
grep --quiet "go_goroutines" <<<"${worker_metrics}"

# 使用 Python 标准库解析 JSON，避免目标机必须额外安装 jq。
swarm_id="$(curl --fail --silent --show-error "${base_url}/api/v1/swarms?page_size=50" \
  | python3 -c 'import json,sys; data=json.load(sys.stdin); print(data["items"][0]["id"])')"
overview="$(curl --fail --silent --show-error "${base_url}/api/v1/console/overview?swarm_id=${swarm_id}")"
printf '%s' "${overview}" | python3 -c '
import json
import sys

data = json.load(sys.stdin)
assert data["swarm_id"]
print(
    "production_smoke=passed tasks=%d attempts=%d"
    % (sum(data["task_statuses"].values()), data["total_attempts"])
)
'
