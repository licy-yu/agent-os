#!/bin/sh
# 幂等创建 SwarmOS 使用的 Temporal Namespace。健康检查与 Namespace 创建都有限次重试，
# 网络或 Server 配置异常时 Job 明确失败，Compose 不会继续启动业务控制面。
set -eu

TEMPORAL_ADDRESS="${TEMPORAL_ADDRESS:-temporal:7233}"
NAMESPACE="${DEFAULT_NAMESPACE:-default}"
RETENTION="${TEMPORAL_NAMESPACE_RETENTION:-30d}"
MAX_ATTEMPTS="${TEMPORAL_HEALTH_CHECK_MAX_ATTEMPTS:-30}"
SLEEP_SECONDS="${TEMPORAL_HEALTH_CHECK_SLEEP_SECONDS:-2}"

attempt=1
while ! temporal operator cluster health --address "${TEMPORAL_ADDRESS}" >/dev/null 2>&1; do
  if [ "${attempt}" -ge "${MAX_ATTEMPTS}" ]; then
    echo "Temporal 在 ${MAX_ATTEMPTS} 次检查后仍不健康。" >&2
    exit 1
  fi
  attempt=$((attempt + 1))
  sleep "${SLEEP_SECONDS}"
done

if temporal operator namespace describe --namespace "${NAMESPACE}" \
  --address "${TEMPORAL_ADDRESS}" >/dev/null 2>&1; then
  echo "Temporal Namespace ${NAMESPACE} 已存在。"
  exit 0
fi

temporal operator namespace create --namespace "${NAMESPACE}" --retention "${RETENTION}" \
  --address "${TEMPORAL_ADDRESS}"
echo "Temporal Namespace ${NAMESPACE} 创建完成。"
