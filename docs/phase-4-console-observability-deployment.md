# 阶段 4：控制台、可观测性与生产部署

## Web Console

`web/` 是 React 19 + TypeScript + Vite 的真实运维控制台，不包含演示数据。V1 兼容页面与 V1.5 Run 页面共存，页面会从控制面读取：

- Run/Swarm 目标、预算和总体进度；
- PlanVersion、按依赖层级计算的 Task DAG 与 Scheduler Explain；
- Agent 状态、负载与心跳；
- Attempt 模型、Token、Checkpoint、工具审计和 Reviewer 结论；
- Interaction、Effect、Artifact、Timeline，以及 Transactional Outbox 的已发布/待发布事件。

生产构建由 Kratos HTTP Server 同源托管。`/api/v1`、`/healthz`、`/readyz`、`/metrics` 优先匹配，其他 GET/HEAD 路径由 SPA Handler 返回静态文件或回退到 `index.html`。Vite 哈希资源使用一年强缓存，入口 HTML 使用 `no-cache`。生产不需要常驻 Node 服务；LEGACY 模式运行 control-plane 与普通 worker，启用 Temporal 的目标拓扑还必须运行独立的 workflow-worker，因此当前 production Compose 管理三个 Go 进程。

本地开发：

```bash
cd web
npm ci
npm run dev
```

生产构建：

```bash
cd web
npm ci
npm run build
```

## Trace、Metrics 与日志

控制面和 Worker 都使用同一套 `internal/observability` 初始化流程：

- W3C Trace Context + Baggage 负责跨 HTTP/gRPC 传播；
- Kratos 服务端中间件记录请求次数与耗时；
- Worker 的 `worker.execute` Span 关联 swarm/task/agent/attempt/model；
- Tool Gateway 的 `tool.call` Span 记录工具、风险级别、状态和错误；
- 结构化日志包含 `trace_id`、`span_id`、服务版本和 Git commit；
- 控制面 `:8080/metrics` 与 Worker `127.0.0.1:9465/metrics` 输出 OpenMetrics。

未设置 OTLP 端点时不会创建网络导出器，但 Trace ID 仍可用于日志关联。接入 Collector 只需在 `.env` 设置标准变量，无需重新编译：

```dotenv
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4317
OTEL_EXPORTER_OTLP_INSECURE=true
```

Metrics 标签只放服务、方法、状态码等低基数字段；Swarm/Task/Attempt 的高基数明细留在 Trace 和 PostgreSQL Console 视图，避免时序库基数爆炸。

## Ubuntu 生产部署

目标目录固定为 `/srv/projects/agent-os`，`.env` 必须由服务器单独维护且权限为 `0600`。2026-08-11 已在 Ubuntu VM `192.168.110.128` 使用 production Compose 完成一次基础部署；当前生产应用镜像为 `swarmos-app:v1.5.4`，发布提交为 `6fd6043`，已推送公开 GitHub 分支。正常空转 Run `927451fd-8590-4ae1-a8ae-a439d6bc9996` 在 10 秒内保持 Task `version=1`、Outbox `=1`、Explain `=1`、`xmin=649540`，随后 COMPLETED，Manifest `c3e3ffe0-51ba-5a6d-b041-ec09164d4083` HTTP 200；安全 Header 临时文件前后均为 0。崩溃注入 Run `b935dd37-2594-4148-b20c-9ccd5014c27d` 的 Task `b11fd38f-88c4-4fa2-9e26-fb97b68ebefd` 从人工 SCHEDULING/version 2 被 Controller 恢复为 READY/version 3，`task.ready` 恰好 1 条，随后 COMPLETED，Manifest `84adba6c-7127-5ee8-bf96-1bb11772bffe` HTTP 200。UFW 放行后的宿主机入口为 `http://192.168.110.128:8080/`，Windows 直连仍待用户执行 LAN 白名单并复测。完整产物哈希、备份、迁移、容器状态、探针结果和剩余边界见 [V1.5 目标 VM 部署报告](v1.5-deployment-report.md)。这份基础部署记录不等于 [15 项故障演练](v1.5-acceptance.md) 生产通过；Case 15 仍因没有 Run→Manifest 快捷 API、独立 cost ledger 和五类完整证据演练而保持“部分”。

通用发布流程：

1. 在可信构建机执行前端构建，并以 `CGO_ENABLED=0 GOOS=linux GOARCH=amd64` 交叉编译 control-plane、worker、workflow-worker；
2. 上传三个 `bin/` 产物、`web/dist`、配置和编排文件，逐一核对 commit 与 SHA256；
3. 在目标机执行 `python3 scripts/configure-production-env.py --path /srv/projects/agent-os/.env`，再执行 `docker compose -f compose.production.yaml config --quiet`；脚本不会打印密钥值；
4. 启动生产 Compose，验证容器、健康、就绪、指标、控制台/API、普通 worker 和 workflow-worker 的真实消费；
5. 执行 production smoke 与 `scripts/scheduler-churn-smoke.sh`，并核对 Manifest、Artifact、Timeline 读路径，把成功、失败和未执行项目原样归档到部署报告。V1.5.4 的 Scheduler 与 Manifest 证据不能外推为其他未执行的故障场景也已通过。

systemd 是 Compose 之外的替代路径。目标机账号可使用 sudo 时，`scripts/install-systemd.sh` 会安装 control-plane、普通 worker 和 workflow-worker 三个服务，包含自动重启、优雅终止、文件描述符上限和宿主加固。它不会修改 `.env`，也不会把密钥输出到日志。当前目标 VM 实际使用的是 Compose，不能因为 unit 文件存在就宣称 systemd 已验收。

使用生产 Compose：

```bash
cd /srv/projects/agent-os
python3 scripts/configure-production-env.py --path /srv/projects/agent-os/.env
docker compose -f compose.production.yaml config --quiet
docker compose -f compose.production.yaml up -d --build
docker compose -f compose.production.yaml ps
```

当前生产应用镜像标签为 `swarmos-app:v1.5.4`。镜像以 UID 10001 运行，容器根文件系统只读、丢弃全部 capabilities，并启用日志轮转。`.dockerignore` 使用“默认拒绝”白名单，`.env`、Git 历史和本地依赖不会发送到 Docker 构建上下文。容器使用 host network 是因为目标服务器的中间件只绑定 `127.0.0.1`；此决策仅适用于受控 Linux 主机，不能直接照搬到多租户节点。

常用命令：

```bash
sudo systemctl status swarmos-control-plane swarmos-worker swarmos-workflow-worker
sudo journalctl -u swarmos-control-plane -u swarmos-worker -u swarmos-workflow-worker -f
docker compose -f compose.production.yaml ps
docker compose -f compose.production.yaml logs -f --tail=200 control-plane worker workflow-worker temporal
curl --fail http://127.0.0.1:8080/healthz
curl --fail http://127.0.0.1:8080/readyz
curl --fail http://127.0.0.1:8080/metrics
curl --fail http://127.0.0.1:9465/metrics
```

发布后验证 Scheduler 无候选零写入合同：

```bash
cd /srv/projects/agent-os
bash scripts/scheduler-churn-smoke.sh
```

脚本默认从当前目录 `.env` 只读取 `SWARMOS_API_KEY`，使用本机 `http://127.0.0.1:8080`、业务 PostgreSQL 容器 `dev-postgres`，并观察 10 秒。Bearer Header 只写入 `mktemp` 创建并显式设为 `0600` 的临时文件，curl 参数只出现文件名；脚本退出时精确删除，生产验收前后该前缀临时文件数量均为 0。脚本会创建一个 TEMPORAL Run，验证 READY Task 的 version、Task Outbox、Scheduler Explain 数量及 Explain `xmin` 不变，再注册 Agent并等待 Run 完成，最后要求 Completion Manifest HTTP 200。目标环境路径、地址、数据库容器或观察时间不同时，可分别设置 `SWARMOS_PROJECT_DIR`、`SWARMOS_BASE_URL`、`SWARMOS_PG_CONTAINER`、`SWARMOS_SCHEDULER_OBSERVE_SECONDS`。每次运行都会保留新 Run 和审计证据，不能把它当作无副作用的只读探针。

回滚前必须核对部署报告中的数据库备份、发布包和镜像标识。应用层回滚要把 control-plane、worker、workflow-worker 三个 `bin/` 文件和 `web/dist/` 一起恢复到同一个旧 Git 提交的构建产物，再执行：

```bash
sudo systemctl restart swarmos-control-plane swarmos-worker swarmos-workflow-worker
# 或者
docker compose -f compose.production.yaml up -d --build
```

数据库迁移采用只前进策略；涉及破坏性 DDL 的未来版本必须先做兼容迁移和数据备份，不能把数据库直接回滚到旧 schema。

目标 VM 当前还需要有 sudo 权限的用户亲自配置 UFW；自动部署没有修改防火墙。只允许实际 VMware LAN 网段，不得使用 `Anywhere`：

```bash
sudo ufw status verbose
sudo ufw allow from 192.168.110.0/24 to any port 8080 proto tcp
```

执行前先用 `ip route` 核对网段，并确认启用/修改 UFW 不会切断 SSH。API Key 只应在服务器受控终端用 `sed -n 's/^SWARMOS_API_KEY=//p' /srv/projects/agent-os/.env` 读取，再录入控制台右上角；不要把值写入文档、命令历史截图、URL 或仓库。
