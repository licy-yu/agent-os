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

状态：已完成实现，并于 2026-08-11 在目标 Ubuntu VM 完成 production Compose 基础部署；当前生产应用镜像为 `swarmos-app:v1.5.4`，发布提交为 `6fd6043`，已推送公开 GitHub 分支。完整 V1.5 生产验收仍在进行。当前操作步骤见 `docs/v1.5-usage.md`，目标机证据见 `docs/v1.5-deployment-report.md`。

- Web Console 可查看 Run/Swarm、Plan/DAG、Agent、Attempt、Interaction、Effect、Artifact 和 Timeline；
- Trace、Metrics、结构化日志可关联 swarm/task/attempt；
- 加固的生产 Compose 管理 control-plane、普通 worker 与 workflow-worker 三个 Go 进程，并编排 Temporal Server 和独立 Temporal PostgreSQL；业务 PostgreSQL、认证 Redis 与 NATS JetStream 使用目标服务器现有基础设施；
- UFW 放行后的局域网入口为 `http://192.168.110.128:8080/`；Windows 宿主机直连仍待用户执行 LAN 白名单并复测，API Key 只从服务器 `.env` 读取，不进入仓库；
- V1.5.4 使用 `max(2 × Lease TTL, 1 分钟)` 回收崩溃遗留 SCHEDULING，多副本通过数据库 CAS 保证只恢复一次；
- 正常空转 Run `927451fd-8590-4ae1-a8ae-a439d6bc9996` 的 Task 在 10 秒内保持 `version=1`、Outbox `=1`、Explain `=1`、`xmin=649540`，随后完成；Manifest `c3e3ffe0-51ba-5a6d-b041-ec09164d4083` HTTP 200，安全 Header 临时文件前后均为 0；
- 崩溃注入 Run `b935dd37-2594-4148-b20c-9ccd5014c27d` 的 Task `b11fd38f-88c4-4fa2-9e26-fb97b68ebefd` 从 SCHEDULING/version 2 恢复到 READY/version 3，`task.ready` 恰好 1 条，随后完成；Manifest `84adba6c-7127-5ee8-bf96-1bb11772bffe` HTTP 200；
- Case 15 仍因缺少 Run→Manifest 快捷 API、独立 cost ledger 和五类完整证据演练而保持“部分”，15 项故障注入也未全部通过，因此本阶段不能表述成“生产验收完成”；
- 回滚与运维步骤已有文档；UFW 的 LAN 白名单仍需用户以 sudo 权限在目标 VM 上执行。
