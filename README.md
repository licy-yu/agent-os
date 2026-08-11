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
http://<虚拟机 IP>:8080/
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

仓库已提供包含 Temporal 的生产候选 compose、三个 systemd unit、带 API Key/空库兼容的 smoke，以及检查 PostgreSQL、Redis、NATS、可选 Temporal 的 `/readyz`。这些仍只是部署代码，不是目标 VM 的通过记录；发布前必须逐项执行 [V1.5 验收矩阵](docs/v1.5-acceptance.md)，并确认普通 worker 与 workflow-worker 实际在消费，不能只以 `/healthz` 或 `/readyz` 成功作为上线依据。
