#!/usr/bin/env bash
set -Eeuo pipefail

# 生产冒烟只读现有数据，不创建 Swarm/Task，也不会改变预算或调度状态。
base_url="${SWARMOS_BASE_URL:-http://127.0.0.1:8080}"
worker_metrics_url="${SWARMOS_WORKER_METRICS_URL:-http://127.0.0.1:9465/metrics}"
: "${SWARMOS_API_KEY:?请先从部署 .env 加载 SWARMOS_API_KEY，冒烟脚本不会打印它}"

# 所有业务 API 都走与浏览器相同的 Bearer 认证。Key 只驻留当前进程环境，不写临时文件、
# 不进入脚本输出；healthz/readyz/metrics 则故意保持未认证，供容器探针读取。
api_get() {
  curl --fail --silent --show-error \
    --header "Authorization: Bearer ${SWARMOS_API_KEY}" "$1"
}

health="$(curl --fail --silent --show-error "${base_url}/healthz")"
if [[ "${health}" != "ok"$'\n' && "${health}" != "ok" ]]; then
  echo "控制面健康检查返回异常: ${health}" >&2
  exit 1
fi
readiness="$(curl --fail --silent --show-error "${base_url}/readyz")"
if [[ "${readiness}" != "ready"$'\n' && "${readiness}" != "ready" ]]; then
  echo "控制面就绪检查返回异常: ${readiness}" >&2
  exit 1
fi
# 使用 Python 标准库解析 JSON，避免目标机必须额外安装 jq。
runs="$(api_get "${base_url}/api/v1/runs?limit=50")"
run_id="$(printf '%s' "${runs}" | python3 -c '
import json, sys
items = json.load(sys.stdin).get("items", [])
print(items[0]["id"] if items else "")
')"
run_count="$(printf '%s' "${runs}" | python3 -c 'import json,sys; print(len(json.load(sys.stdin).get("items", [])))')"

overview=""
if [[ -n "${run_id}" ]]; then
  overview="$(api_get "${base_url}/api/v1/console/overview?swarm_id=${run_id}")"
fi

# 先访问受中间件保护的 API 再抓指标，保证自定义 Counter/Histogram 已产生数据点。
page="$(curl --fail --silent --show-error "${base_url}/")"
control_metrics="$(curl --fail --silent --show-error "${base_url}/metrics")"
worker_metrics="$(curl --fail --silent --show-error "${worker_metrics_url}")"
grep --quiet "SwarmOS Control Room" <<<"${page}"
grep --quiet "swarmos_server_requests_total" <<<"${control_metrics}"
grep --quiet "go_goroutines" <<<"${worker_metrics}"

if [[ -n "${overview}" ]]; then
  printf '%s' "${overview}" | python3 -c '
import json
import sys

data = json.load(sys.stdin)
assert data["swarm_id"]
print(
    "production_smoke=passed runs=%s tasks=%d attempts=%d"
    % (sys.argv[1], sum(data["task_statuses"].values()), data["total_attempts"])
)
' "${run_count}"
else
  echo "production_smoke=passed runs=${run_count}（空库，已验证鉴权/依赖/静态页/指标）"
fi

# Compose 部署存在时额外确认四个长期进程都在运行。Schema/Namespace 是一次性 Job，
# 成功退出属于正常状态，因此不在此列表中。
if command -v docker >/dev/null 2>&1; then
  for container in swarmos-control-plane swarmos-worker swarmos-workflow-worker swarmos-temporal; do
    # 容器不存在与容器已退出同样是发布失败。不能用 `|| true` 把 inspect 失败变成
    # 空字符串，否则 workflow-worker 完全缺失时 smoke 仍可能假绿。
    if ! status="$(docker inspect --format '{{.State.Status}}' "${container}" 2>/dev/null)"; then
      echo "缺少必需容器: ${container}" >&2
      exit 1
    fi
    if [[ "${status}" != "running" ]]; then
      echo "容器 ${container} 状态异常: ${status}" >&2
      exit 1
    fi
  done
fi
