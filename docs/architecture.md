# SwarmOS V1 架构设计

## 1. 系统边界

SwarmOS 的事实对象是 `Swarm -> Task -> TaskAttempt`，Agent 只是被调度的执行资源。Agent 的自然语言回复不能直接改变最终任务状态，只有拥有状态转换权限的 Controller、Scheduler、Worker 或 Reviewer 可以提交状态变更。

```text
用户 / Web Console
        |
        v
Kratos Control Plane (HTTP + gRPC)
        |
        +---- Planner / TaskController / SwarmController
        +---- Scheduler / ReviewController / RecoveryController
        |
PostgreSQL + Transactional Outbox
        |
NATS JetStream ---- Agent Worker ---- Tool Gateway ---- MCP / Shell / Git / DB
        |
Redis Lease / Heartbeat Cache
```

Web Console 的生产构建由控制面同源托管，运维读请求经过 `internal/console` 聚合层访问 PostgreSQL，浏览器不接触数据库。控制面和 Worker 都输出 OpenTelemetry Trace、OpenMetrics 与带 Trace ID 的结构化日志。

## 2. 一致性策略

1. PostgreSQL 是任务、Agent 和预算的最终事实源。
2. 领域变更与 Outbox 事件在同一个事务中写入，避免“数据库成功但消息丢失”。
3. Redis Lease 只负责短期互斥；最终 Bind 仍使用 PostgreSQL `version` 乐观锁。
4. JetStream 使用 durable pull consumer 和显式 ACK，Worker 必须做到幂等。
5. 每次执行产生独立 TaskAttempt；重试不会覆盖历史数据。

## 3. 状态修改权限

| 组件 | 允许的关键转换 |
| --- | --- |
| Planner | CREATED -> BLOCKED / READY |
| TaskController | BLOCKED -> READY，RETRY_WAIT -> READY |
| Scheduler | READY -> SCHEDULING -> ASSIGNED |
| Worker | ASSIGNED -> RUNNING -> REVIEW |
| Reviewer | REVIEW -> SUCCEEDED / RETRY_WAIT / FAILED |
| RecoveryController | RUNNING -> RETRY_WAIT / FAILED |

任何不在状态机白名单内的转换都会被领域层拒绝，而不是依赖调用方自觉。

## 4. 调度周期

```text
QueueSort -> PreFilter -> Filter -> Score -> Reserve -> Bind -> Execute
```

- Filter 只处理技能、工具、权限、模型、上下文、预算、安全域等硬条件。
- Score 第一版包含技能匹配、历史成功率、上下文亲和度、质量、负载、成本和时延。
- Reserve 用 Redis NX Lease 防止并发调度器同时占用同一 Agent。
- Bind 用数据库版本号做 CAS；失败后释放 Lease 并重新入队。

## 5. 版本兼容决策

项目运行环境为 Go 1.24.13。Kratos v3.0.0 和当前最新 pgx/NATS Go 客户端要求 Go 1.25，因此 V1 使用仍受支持且兼容 Go 1.24 的依赖版本。升级 Go 与 Kratos 主版本必须单独做兼容性阶段，不能在业务提交中隐式升级。
