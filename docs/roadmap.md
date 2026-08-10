# 实施阶段与验收标准

## 阶段 1：基础控制面

状态：已完成（提交 `a4604ba`）。

- 工程可以在 Go 1.24 编译；
- 核心资源具有领域模型、状态机、PostgreSQL DDL 和仓储；
- Kratos 同时提供 HTTP 与 gRPC API；
- migration、单元测试和静态检查通过。

## 阶段 2：编排和调度

状态：已完成（提交 `537b121`），并通过目标服务器验收。

- 依赖完成后 Task 能从 BLOCKED 进入 READY；
- Filter / Score 插件可独立测试；
- Redis Lease + PostgreSQL CAS 完成 Reserve / Bind；
- Transactional Outbox 能可靠投递 JetStream。

## 阶段 3：执行和可靠性

状态：已完成（提交 `1fe2888`）并通过目标服务器验收。

- Worker 创建 Attempt 并按步骤保存 Checkpoint；
- Tool Gateway 执行权限、风险、预算、限流和审计；
- Reviewer 同时支持确定性检查与策略检查；
- 心跳超时可触发恢复和有上限重试。

## 阶段 4：控制台和部署

状态：已完成实现，生产发布按 `docs/phase-4-console-observability-deployment.md` 执行。

- Web Console 可查看 Swarm、DAG、Agent、Attempt 和事件；
- Trace、Metrics、结构化日志可关联 swarm/task/attempt；
- systemd 管理两个 Go 进程，PostgreSQL、认证 Redis 和 NATS JetStream 使用目标服务器现有基础设施；
- 端到端示例任务通过，回滚与运维步骤有文档。
