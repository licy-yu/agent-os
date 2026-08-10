# SwarmOS / Agent OS

SwarmOS 是一个面向真实工程交付的智能体蜂群操作系统。它不让多个 Agent 自由聊天，而是把用户目标转换为可审计的 Task DAG，再通过声明式 Controller、可插拔 Scheduler、受控 Agent Runtime 和 Tool Gateway 完成调度、执行、审核与恢复。

## V1 核心能力

- Kratos HTTP/gRPC 控制面；
- Swarm、AgentTemplate、AgentInstance、Task、TaskAttempt 等核心资源；
- Task DAG 与严格状态机；
- Filter / Score / Reserve / Bind 调度周期；
- PostgreSQL 最终事实源、Redis Lease、NATS JetStream 事件；
- Checkpoint、Heartbeat、Retry、Recovery 与 Reviewer；
- Tool Gateway 权限、风险、预算、限流和审计；
- Web Console 与 OpenTelemetry 可观测性。

A2A、AgentSet 自动扩缩和自动进化属于 V2/V3，不会混进 V1 的核心链路。

## 技术基线

- Go 1.24；
- Kratos v2.9.2；
- PostgreSQL 16+；
- Redis 7+；
- NATS JetStream 2.11+；
- React + TypeScript（阶段 4 控制台）。

选择 Kratos v2 而不是 v3 的原因是：v3.0.0 要求 Go 1.25，而目标运行服务器固定为 Go 1.24.13。项目优先保证本地、CI 和部署环境可复现。

## 本地启动

首次启动前构建 Web Console：

```bash
cd web && npm ci && npm run build && cd ..
docker compose up -d postgres redis nats
go run ./cmd/control-plane -config ./configs/config.yaml
go run ./cmd/worker -config ./configs/config.yaml
```

健康检查与控制台：

```bash
curl http://127.0.0.1:8080/healthz
# 浏览器打开 http://127.0.0.1:8080/
```

## 工程目录

```text
api/                    Protobuf API 契约及生成代码
cmd/                    各微服务进程入口
configs/                可提交的非敏感默认配置
docs/                   架构决策和阶段说明
internal/conf/          配置加载与校验
internal/domain/        领域实体、状态机和仓储接口
internal/data/          PostgreSQL 等基础设施实现
internal/service/       Kratos API 用例编排
internal/server/        HTTP/gRPC 传输层
internal/worker/        Agent 执行循环与模型适配
internal/toolgateway/   工具权限、额度、审计和 MCP 适配
internal/execution/     Attempt/Checkpoint/Evaluation 执行合同
internal/console/       只读运维聚合视图
internal/observability/ OpenTelemetry 与 Prometheus 初始化
web/                    React/TypeScript 运维控制台
deploy/                 systemd 生产服务定义
```

## 安全约束

不要提交 `.env`、数据库密码、Token、服务器凭据或 `CREDENTIALS.local.md`。Agent 无权直接执行工具；所有外部副作用必须通过 Tool Gateway，并落审计记录。

详细设计见 [docs/architecture.md](docs/architecture.md)，阶段验收见 [docs/roadmap.md](docs/roadmap.md)。

已完成的实现说明：

- [阶段 2：Controller、Scheduler 与可靠事件](docs/phase-2-orchestration.md)
- [阶段 3：Worker、工具治理、审查与恢复](docs/phase-3-execution.md)
- [阶段 4：控制台、可观测性与部署](docs/phase-4-console-observability-deployment.md)
