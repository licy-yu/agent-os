# 阶段 2：Controller、Scheduler 与可靠事件

## 控制循环

`TaskController` 每次只推进一条合法状态边：

```text
CREATED -> PLANNING -> READY
                    -> BLOCKED -> READY
RETRY_WAIT -------------------> READY
```

每次转换同时检查 `status + version`，并在同一 PostgreSQL 事务写入 Outbox。另一个控制面实例抢先更新时，本实例把冲突视为正常竞争，下一轮重新观察，而不是覆盖新状态。

`AgentController` 把完成注册的实例从 `REGISTERED` 推进到 `IDLE`，之后 Scheduler 才能看见该实例。

## 调度周期

1. `QueueSort`：业务优先级 + 等待时间 + 阻塞下游数量 + deadline 紧迫度；
2. `Filter`：状态、技能、工具、权限、模型、上下文窗口、预算和风险域都是硬条件；
3. `Score`：技能 30%、成功率 20%、上下文亲和度 15%、质量 15%、负载 10%、成本 5%、时延 5%；
4. `Reserve`：Redis `SET NX EX` 为 Agent 建立短租约；
5. `Bind`：PostgreSQL 在一个事务里将 Task 改为 `ASSIGNED`、Agent 改为 `RESERVED`，双方都使用版本 CAS；
6. Bind 完成后用比较 token 的 Lua 脚本释放 Redis 短租约，PostgreSQL 状态继续作为最终事实。

## Outbox 与 JetStream

- 事件领取使用 `FOR UPDATE SKIP LOCKED`，允许多控制面实例水平扩展；
- 数据库锁带 `locked_until`，进程崩溃后其他实例可自动回收；
- JetStream 同步 ACK 后才写 `published_at`；
- Outbox UUID 用作 NATS message ID，ACK 后崩溃引起的重复投递可在去重窗口内消除；
- 发布失败按指数退避重排，单条失败不会阻塞同一批其他事件。

## 目标服务器验收结果

在 Ubuntu 22.04 / Go 1.24.13 上使用 PostgreSQL、带认证 Redis 和 NATS JetStream 验证：

- 无依赖任务自动到达 `ASSIGNED`；
- 匹配 Agent 自动到达 `RESERVED`；
- 有未完成 HARD 依赖的任务保持 `BLOCKED`；
- 调度 Redis Lease 在 Bind 后归零；
- 本次资源相关 Outbox 未发布数为 0；
- JetStream 已持久化领域事件。

