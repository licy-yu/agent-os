# 阶段 3：Worker、工具治理、审查与恢复

## 执行边界

控制面只负责把 Task 绑定到 Agent；独立的 `swarmos-worker` Kratos 进程通过 JetStream Durable Pull Consumer 消费 `task.assigned`。消息采用显式 ACK：只有结果和 Outbox 在 PostgreSQL 提交成功后才 `DoubleAck`，暂时性错误使用延迟 NAK。

领取任务是一个原子事务：

```text
Task:  ASSIGNED -> RUNNING
Agent: RESERVED -> RUNNING
Attempt:          -> RUNNING
Outbox: task.running + attempt.running
```

事务同时锁定 Task 与 Agent，并校验 `assigned_agent_id/current_task_id`，因此陈旧分配消息不能覆盖新绑定。Task 已进入 REVIEW 或终态时，重复消息可安全确认。

## Checkpoint 与执行器

每个模型步骤通过 `(attempt_id, sequence)` 唯一键写入 Checkpoint；重复投递不会产生重复步骤。写入同时更新 Attempt 心跳和最大步骤号。当前有两类执行器：

- `mock/deterministic`：不伪装为大模型，专门用于无 API Key 的可靠集成测试；它仍经过真实 NATS、数据库、Tool Gateway 和 Reviewer；
- OpenAI：使用官方 Go SDK v3.44.0 和 Responses API，SDK 自动读取 `OPENAI_API_KEY`，密钥不进入 YAML、数据库或 Git。v3.44.0 是支持 Go 1.24 的兼容固定版本；更新的 SDK 版本要求 Go 1.25。

模型输出不能直接把 Task 设为成功。Worker 只能提交输出、机器检查、策略违规、质量分和 token/cost 证据，并把 Task 推到 REVIEW。

## Tool Gateway 与 MCP

每次工具调用依次执行：

```text
注册表 -> 模板白名单 -> 权限 -> 风险区 -> Attempt 调用上限 -> 审计预留 -> Adapter -> 审计完成
```

- 被拒绝的调用也写入 `tool_calls`，并占用调用额度，防止模型无限试探；
- 调用额度在 Attempt 行锁下原子递增，并发调用不能突破 `MaxToolCalls`；
- `native:echo` 用于验证无副作用的完整审计链路；
- MCP Adapter 遵循 2026-07-28 无状态 Streamable HTTP：每次 `tools/call` 都携带 `MCP-Protocol-Version`、`Mcp-Method`、`Mcp-Name` 和客户端元数据；支持 JSON 与 SSE 响应；
- MCP endpoint 和远端工具名来自管理员注册表，可选 Bearer Token 只通过注册表指定的环境变量名读取；远端 MCP 仍在本地 Gateway 策略之后，不能绕过权限和审计。

## Reviewer 与预算

内置 Reviewer 只读取持久化证据：

1. `build/unit_test/security_review/required_checks` 必须有明确机器检查结果；
2. 任一策略违规都会硬拒绝；
3. 质量分必须不低于 0.8；
4. 未通过且尚有 Attempt 额度时进入指数退避的 `RETRY_WAIT`，否则 FAILED；
5. 通过后 Attempt 和 Task 同事务进入 SUCCEEDED，并写 Evaluation 与 Outbox。

Worker 完成时把实际输入/输出 token 与微成本记入 Swarm 总账。Scheduler 仍按 Task 的最大 token 上限预留预算，而不是按平均消耗乐观调度；阶段验收中，这一规则成功阻止了剩余预算不足的第二个任务，增加测试预算后才允许继续绑定。

## 故障恢复

Worker 定期同时刷新数据库心跳和 JetStream `InProgress`。`RecoveryController` 使用 `FOR UPDATE SKIP LOCKED` 批量回收超时 Attempt：

- Attempt 进入 ABORTED，并记录 `HEARTBEAT_TIMEOUT`；
- Agent 进入 OFFLINE，防止立即再次调度到失联实例；
- Task 在 Attempt 上限内进入 RETRY_WAIT，否则 FAILED；
- 全部状态与恢复事件在同一事务提交。

## 目标服务器验收

可复用脚本为 `scripts/phase3-e2e.sh`。在 Ubuntu 22.04 / Go 1.24.13、PostgreSQL、认证 Redis 和 NATS JetStream 上的结果：

```text
task_a=SUCCEEDED task_b=SUCCEEDED
attempts=2 checkpoints=4 tools=2 evaluations=2
```

这次验证覆盖了 HARD 依赖解锁、二次调度、Worker 消费、工具审计、Checkpoint、预算记账、Reviewer 接受和最终 DAG 完成。
