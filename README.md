# SwarmOS / Agent OS

SwarmOS 把一次用户目标建模为可审计的 `Run -> PlanVersion -> Task -> TaskAttempt -> Step`，再通过声明式控制器、Scheduler、Agent Runtime、Tool Gateway 和 Verification 完成调度、执行、人工介入与证据归档。模型说“完成”不是完成；必需验收门通过并生成完成证据，才允许进入完成状态。

## 当前交付边界

本仓库正在从 V1 的 `Swarm` 兼容模型迁移到 V1.5 的 `Run` 模型。下表描述的是当前代码边界，不等同于已经通过全部生产演练。

| 优先级 | 当前状态 | 已进入代码的范围 | 明确未闭环的范围 |
| --- | --- | --- | --- |
| P0 | 已实现主体，仍需生产演练 | Run、不可变 PlanVersion、Task Contract、Attempt/Step/Checkpoint、LEGACY 与 Temporal Run 协调、Recovery、Tool Gateway、Effect、Artifact 描述符与血缘、Verification、Completion Manifest、Interaction | 15 项故障场景尚未全部在真实生产拓扑演练；Temporal 尚未接管整个 Task DAG；大对象存储适配器未交付 |
| P1 | 部分实现 | Context Engine、Plan Compiler、Replan、Scheduler Explain、Timeline、预算记账、Failure Classifier 证据 | 自动执行 Agent/Model Switch 与 Run Replan 的恢复闭环、Workspace Revision 用例、完整成本账本仍未完成 |
| P2 | 未实现 | 数据库中只有少量前瞻表结构 | AgentSet、AutoScaling、External A2A、Nested Swarm、Dynamic Team |
| P3 | 未实现 | 数据库中只有少量前瞻表结构 | Evaluation Dataset、Failure Mining、Shadow、Canary、Promotion、Rollback |

旧 `/api/v1/swarms`、AgentTemplate、Agent、Task API 保留；新能力以 `/api/v1/runs` 为入口。详细边界见 [V1.5 架构](docs/architecture.md) 与 [15 项验收矩阵](docs/v1.5-acceptance.md)。

2026-08-11 已在 Ubuntu 虚拟机上用 production Compose 完成 V1.5 基础部署，并发布 V1.5.4：生产应用镜像为 `swarmos-app:v1.5.4`，源码提交为 `6fd6043`，已推送到[公开 GitHub 分支](https://github.com/licy-yu/agent-os/tree/codex/swarmos-v15-production)。正常空转 Run `927451fd-8590-4ae1-a8ae-a439d6bc9996` 的 Task 在 10 秒内保持 `version=1`、Outbox `=1`、Explain `=1`、`xmin=649540`，随后 COMPLETED，Manifest `c3e3ffe0-51ba-5a6d-b041-ec09164d4083` HTTP 200；API Key 使用的 `0600` Header 临时文件执行前后数量均为 0。崩溃注入 Run `b935dd37-2594-4148-b20c-9ccd5014c27d` 的 Task `b11fd38f-88c4-4fa2-9e26-fb97b68ebefd` 被人工置为 SCHEDULING/version 2，Controller 恢复为 READY/version 3 且只产生 1 条 `task.ready`，随后 COMPLETED，Manifest `84adba6c-7127-5ee8-bf96-1bb11772bffe` HTTP 200。V1.5.4 采用 `max(2 × Lease TTL, 1 分钟)` 的过期阈值，并以数据库 CAS 保证多副本只有一个恢复者。这些证据不代表设计文档中的 15 项故障注入用例已经全部通过。UFW 放行后使用的局域网入口为 `http://192.168.110.128:8080/`，Windows 宿主机直连仍待用户执行 LAN 白名单后复测；完整证据、剩余边界和回滚信息见 [V1.5 目标 VM 部署报告](docs/v1.5-deployment-report.md)。

## 技术基线

- Go 1.24、Kratos v2.9.2；
- PostgreSQL 16+、Redis 7+、NATS JetStream 2.11+；
- 可选 Temporal Server 1.31.x 与独立 `workflow-worker`；
- React + TypeScript 运维控制台；
- OpenTelemetry / Prometheus 可观测性。

## 最短本地启动路径

默认配置关闭 Temporal，因此以下路径创建 `LEGACY` Run。首次启动前构建控制台：

```bash
cd web
npm ci
npm run build
cd ..

docker compose up -d --wait postgres redis nats
```

分别打开两个终端：

```bash
go run ./cmd/control-plane -config ./configs/config.yaml
```

```bash
go run ./cmd/worker -config ./configs/config.yaml
```

检查服务：

```bash
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/readyz
# 浏览器打开 http://127.0.0.1:8080/
```

控制台“新建 Run”当前默认选择 Temporal；使用上述本地配置时，请主动改选“数据库兼容模式（LEGACY）”。创建 Run 前至少注册一个已启用且带 capability 的 AgentTemplate，创建后再为该 Run 注册 Agent，否则 Planner 或 Scheduler 没有可用能力。

完整的模板注册、Run 操作、审批、证据查询和 Temporal 启动命令见 [V1.5 使用手册](docs/v1.5-usage.md)。

## API Key 与虚拟机访问

开发环境允许 `SWARMOS_API_KEY` 为空；只应在可信本机或隔离网络这样使用。生产必须同时设置：

```bash
export SWARMOS_ENVIRONMENT=production
export SWARMOS_API_KEY='<至少 32 字符的随机值>'
```

调用 API 时使用 `Authorization: Bearer <key>` 或 `X-API-Key: <key>`。控制台右上角可录入同一个 Key，它只保存在当前标签页的 `sessionStorage`，关闭标签页后清除。

HTTP 默认监听 `0.0.0.0:8080`，因此在防火墙允许且虚拟机网络可达时，可从宿主机访问：

```text
http://192.168.110.128:8080/
```

当前 VM 内部服务已就绪，但 Windows 宿主机直连尚未验收；需要先按 [使用手册的 UFW LAN 规则](docs/v1.5-usage.md#9-使用虚拟机-ip-访问) 放行，再从 Windows 重新验证页面、`/healthz` 与 `/readyz`。

页面打开后，点击右上角 `API KEY`，录入服务器 `.env` 中的同名配置。仓库和文档不会保存该值；需要时应登录服务器，在受控终端读取：

```bash
sed -n 's/^SWARMOS_API_KEY=//p' /srv/projects/agent-os/.env
```

当前仓库未内置 TLS 或反向代理。不要把 8080、无 API 鉴权的 9090 gRPC、9465 指标端口直接暴露到公网；优先使用仅主机网络、VPN、SSH 隧道或带 TLS/身份认证的反向代理。

## 两种执行模式

- `LEGACY`：PostgreSQL 是业务事实源，控制器在 control-plane 内运行，NATS 把 Task 分配给普通 worker。默认本地模式，无需 Temporal。
- `TEMPORAL`：在上述链路之外，由 Temporal 保存 Run 的可重放协调历史并接收 Pause/Resume/Cancel/Replan Signal。它仍不替代 NATS Agent Worker，也不拥有业务数据。

启用 Temporal 必须同时具备 Temporal Server、control-plane、普通 worker 和 `workflow-worker`。只启动 Temporal Server 并不代表 Task Queue 有消费者。

## 工程目录

```text
api/                    Protobuf 兼容 API 合同及生成代码
cmd/                    control-plane、worker、workflow-worker 入口
configs/                可提交的非敏感默认配置
docs/                   架构、使用、验收与阶段说明
internal/agentloop/     Eino Agent Runtime 与运行守卫
internal/contextengine/ Context Pack 构建、预算与大结果卸载
internal/data/          PostgreSQL 仓储与内嵌迁移
internal/durabletemporal/ Temporal Run 协调层
internal/execution/     Attempt、Checkpoint、Verification 执行合同
internal/planning/      Task Contract、Plan Compiler、不可变 PlanVersion
internal/runcontrol/    V1.5 Run 用例
internal/safetycontrol/ Interaction、Effect、Artifact 与证据读写面
internal/toolgateway/   工具权限、风险、幂等 Effect 与 MCP 适配
web/                    React/TypeScript 运维控制台
deploy/                 镜像与 systemd 文件
compose.yaml            本地 PostgreSQL/Redis/NATS
compose.production.yaml 生产候选编排与 Temporal 组件
```

## 安全约束

不要提交 `.env`、数据库密码、API Token、服务器凭据或 `CREDENTIALS.local.md`。Agent 无权绕过 Tool Gateway 直接制造外部副作用；R3 操作必须有 Approval，Effect 进入 `UNKNOWN` 后只能先 Reconcile。认证类 Interaction 只能提交 `credentialRef`，不能把明文密钥放入 `resolution`。

仓库已提供包含 Temporal 的生产候选 compose、三个 systemd unit、带 API Key/空库兼容的 smoke、Scheduler 空转验收脚本，以及检查 PostgreSQL、Redis、NATS、可选 Temporal 的 `/readyz`。目标 VM 已完成基础部署并归档 [部署报告](docs/v1.5-deployment-report.md)；V1.5.4（`6fd6043`）包含 Manifest 读模型、Scheduler 零写入、Bind/Reserve 冲突补偿与崩溃遗留 SCHEDULING 回收。15 项真实故障场景仍没有全部通过，Case 15 也仍因缺少 Run→Manifest 快捷 API、独立 cost ledger 和五类完整证据演练而保持“部分”。发布判断必须继续以 [V1.5 验收矩阵](docs/v1.5-acceptance.md) 为准，并确认普通 worker 与 workflow-worker 实际在消费，不能只以页面、`/healthz` 或 `/readyz` 成功作为生产通过依据。
