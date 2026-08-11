#!/usr/bin/env bash
set -Eeuo pipefail

# 本脚本只安装进程守护配置，不创建或覆盖 .env。生产密钥始终由运维人员单独维护。
project_dir="${SWARMOS_PROJECT_DIR:-/srv/projects/agent-os}"
control_unit="${project_dir}/deploy/swarmos-control-plane.service"
worker_unit="${project_dir}/deploy/swarmos-worker.service"
workflow_worker_unit="${project_dir}/deploy/swarmos-workflow-worker.service"

required=(
  "${project_dir}/.env"
  "${project_dir}/configs/config.yaml"
  "${project_dir}/bin/control-plane"
  "${project_dir}/bin/worker"
  "${project_dir}/web/dist/index.html"
  "${control_unit}"
  "${worker_unit}"
)

# 兼容两种运行模式：LEGACY 不依赖 Temporal；TEMPORAL 才要求并启动
# workflow-worker。只读取精确变量，不 source .env，避免配置值被当作代码执行。
temporal_enabled="$(awk -F= '
  $1 == "SWARMOS_TEMPORAL_ENABLED" {
    value = substr($0, index($0, "=") + 1)
    gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
    print tolower(value)
  }
' "${project_dir}/.env" | tail -n 1)"

if [[ "${temporal_enabled}" == "true" ]]; then
  required+=("${project_dir}/bin/workflow-worker" "${workflow_worker_unit}")
fi
for path in "${required[@]}"; do
  if [[ ! -e "${path}" ]]; then
    echo "缺少部署文件: ${path}" >&2
    exit 1
  fi
done

# unit 文件使用固定绝对路径，避免 systemd 的工作目录或 PATH 差异改变启动结果。
sudo install -o root -g root -m 0644 "${control_unit}" /etc/systemd/system/swarmos-control-plane.service
sudo install -o root -g root -m 0644 "${worker_unit}" /etc/systemd/system/swarmos-worker.service
if [[ "${temporal_enabled}" == "true" ]]; then
  sudo install -o root -g root -m 0644 "${workflow_worker_unit}" /etc/systemd/system/swarmos-workflow-worker.service
fi
sudo systemctl daemon-reload
sudo systemctl enable swarmos-control-plane.service swarmos-worker.service
sudo systemctl restart swarmos-control-plane.service swarmos-worker.service

if [[ "${temporal_enabled}" == "true" ]]; then
  sudo systemctl enable swarmos-workflow-worker.service
  sudo systemctl restart swarmos-workflow-worker.service
else
  # 从 TEMPORAL 回退到 LEGACY 时停止旧进程，避免它继续消费工作流任务。
  sudo systemctl disable --now swarmos-workflow-worker.service 2>/dev/null || true
fi

# is-active 失败会使脚本返回非零，发布流程可据此立即中止而不是留下半成功状态。
sudo systemctl is-active --quiet swarmos-control-plane.service
sudo systemctl is-active --quiet swarmos-worker.service
if [[ "${temporal_enabled}" == "true" ]]; then
  sudo systemctl is-active --quiet swarmos-workflow-worker.service
fi
echo "SwarmOS systemd 服务已安装并运行"
