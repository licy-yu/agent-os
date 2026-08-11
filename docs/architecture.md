# SwarmOS V1.5 架构与实现边界

本文描述当前仓库实际进入代码的 V1.5 架构，以及仍需实现或生产演练的边界。它不是把《生产级完整设计》逐项标成“完成”的宣言；最终上线结论以 [15 项验收矩阵](v1.5-acceptance.md) 的生产证据为准。

## 1. 核心事实模型

V1.5 的中心从 Agent 转为 Run：

```text
SwarmDefinition（可复用定义，当前只完成数据模型）
  └── Run / 兼容表名 swarms（一次执行）
      ├── PlanVersion 1..n（不可变；Replan 追加新版本）
      │   └── Task Contract / Dependency / AcceptanceGate
      ├── TaskAttempt 1..n
      │   ├── typed Step / Checkpoint
      │   ├── Model Call / Tool Call
      │   └── VerificationRun / GateResult
      ├── Artifact / ArtifactRelation
      ├── Effect / Interaction
      ├── SchedulerDecision / TimelineEvent
      └── CompletionManifest
```

数据库继续复用 V1 的 `swarms` 作为 Run 物理表，API 同时保留 `/swarms` 兼容入口。这是迁移兼容，不表示新的 Run 仍以“多个 Agent 聊天”为中心。

三个不可破坏的规则：

1. Agent 输出只是候选结果，不能直接把 Task 或 Run 标成完成。
2. 外部副作用必须经过 Tool Gateway 和 Effect 状态机，不能因 HTTP/Worker 超时直接盲重试。
3. Replan 只能追加 PlanVersion；旧计划、已完成 Task、Artifact、Effect 和验收证据不得原地覆盖。

## 2. 当前运行架构

```text
浏览器 / API Client
        |
        | HTTP :8080 + API Key
        v
Kratos Control Plane
  ├── Run Service / Plan Compiler / Replanner
  ├── Task、Agent、Scheduler、Reviewer、Recovery Controllers
  ├── Safety Control / Console Projection
  ├── Transactional Outbox Dispatcher
  └── Temporal Client（仅 TEMPORAL 模式）
        |
        +----------------------- PostgreSQL（业务事实源）
        +----------------------- Redis Lease
        +----------------------- NATS JetStream
                                      |
                                      v
                               Agent Worker Pool
                               ├── Context Engine
                               ├── Eino Agent Loop
                               ├── Checkpoint Writer
                               ├── Tool Gateway / Effect
                               └── Candidate Result

TEMPORAL 模式另有：
Control Plane --Signal--> Temporal Server <--poll-- Workflow Worker
                         |
                         └── 可重放 Run 协调历史
```

### 2.1 Control Plane

control-plane 同时承载 REST/gRPC、Run 用例、控制器循环、Scheduler、Reviewer、Recovery、Outbox Dispatcher 和静态控制台。启动时执行内嵌、仅向前的 PostgreSQL 迁移。`/healthz` 保留数据库兼容健康检查；`/readyz` 在 5 秒总超时内检查 PostgreSQL、Redis、NATS，以及启用时的 Temporal 连接。readiness 不检查普通 worker 或 workflow-worker 是否真的在消费 Task Queue，仍需进程/积压监控。

### 2.2 Agent Worker

普通 worker 使用 JetStream durable pull consumer 获取 `task.assigned`。领取时锁定 Task 与 Agent，创建 Attempt 并带上 `worker_id + fencing_token`；所有 Checkpoint、工具调用与完成写回都校验 owner。消息只在结果和 Outbox 同事务提交后 ACK，暂时错误 NAK。

Worker 的真实执行链是：

```text
ClaimWork -> Restore Checkpoint -> Build ContextPack -> Eino Loop
          -> Tool Gateway / Effect -> Candidate Result -> Verification / Reviewer
```

`mock/deterministic` 用于不依赖模型密钥的集成路径；其他模型通过 OpenAI executor。当前并非任意 Agent 框架或任意工具都已接入。

### 2.3 Temporal Workflow Worker

`workflow-worker` 只在 `SWARMOS_TEMPORAL_ENABLED=true` 时启动。当前 Workflow 负责：

- 为业务 Run 保存稳定 Workflow ID 与可重放协调状态；
- 处理 Pause、Resume、Cancel、Replan Signal；
- 每 15 秒通过 Activity 读取 PostgreSQL 投影，修复“数据库事务已提交但 Signal 暂时失败”的窗口；
- 在 Workflow History 中保留短期 Run 协调历史。

当前 Temporal 不负责：

- 替代 PostgreSQL 成为业务事实源；
- 替代 NATS 投递 Agent Task；
- 托管 Scheduler、Reviewer、Recovery 的全部循环；
- 自动证明普通 worker 或 Task Queue 消费者在线。

因此，`TEMPORAL` 的准确含义是“耐久 Run 协调”，不是“整条 DAG 已完全 Temporal 化”。

## 3. 一致性与恢复策略

| 边界 | 当前策略 | 仍需生产验证 |
| --- | --- | --- |
| 业务写入与事件 | PostgreSQL 事务同时写 Aggregate 与 Outbox | 数据库主从、磁盘满、发布重启时的完整性 |
| Scheduler 并发 | Redis NX Lease 做短期互斥，PostgreSQL `version` 做最终 CAS | 双 Scheduler 进程与网络分区演练 |
| Worker 重投 | JetStream durable consumer、显式 ACK/NAK；Attempt/Checkpoint 幂等 | Step 20 kill、消息重复、ACK 超时演练 |
| Zombie Worker | Attempt owner 和 fencing token 参与写回校验 | 旧进程恢复后继续写的真实竞争演练 |
| Effect | 请求哈希 + 幂等键；`UNKNOWN` 只能 Reconcile；R3 必须 Approval | Git/外部 HTTP/DB 真实适配器的故障注入 |
| Replan | Run 版本 CAS；旧 Plan 置为 SUPERSEDED；只取消未终态工作 | 已完成 Artifact 跨 Plan 复用的长链路演练 |
| Temporal 双写 | PostgreSQL 先提交，Signal 失败后 Workflow 轮询投影 | Temporal 断连、积压、恢复和版本升级 |

Pause 的语义是“在安全边界停止新调度”，不是强杀正在外部系统执行的 Effect。Cancel 会隔离未完成 Attempt、释放 Agent 并取消未终态 Task；已经发生的外部副作用仍必须通过 Effect 对账或补偿。

## 4. 状态与修改权限

Run 领域包含 `CREATED / ADMISSION / PENDING_CAPACITY / PENDING_QUOTA / PENDING_RESOURCE / PLANNING / READY / RUNNING / WAITING_USER / WAITING_EXTERNAL / PAUSED / RECOVERING / DEGRADED / VERIFYING / COMPLETED / FAILED / CANCELED / EXPIRED`。各角色只能执行白名单转换；HTTP 动作最终仍要经过状态机与数据库 CAS。

Task 继续采用 Controller / Scheduler / Worker / Reviewer 分权：

| 角色 | 关键职责 |
| --- | --- |
| Planner / TaskController | 编译合同、建立依赖、将满足条件的 Task 置为 READY |
| Scheduler | Filter、Score、Reserve、Bind，并持久化候选、拒绝原因和选中理由 |
| Worker | ASSIGNED -> RUNNING，写 Step/Checkpoint，提交候选结果 |
| Reviewer / Verification | 只根据持久化证据接受、重试或失败 |
| RecoveryController | 回收心跳超时 Attempt，并让 Task 进入重试或失败 |

Run 动作的客户端合同当前不接收 `expectedVersion`；服务端先读当前版本再 CAS。因此它具备并发冲突保护，但不能夸大为完整的客户端幂等命令协议。Interaction 与 Effect 写命令则明确要求 `version` 和 `Idempotency-Key`。

## 5. 计划、上下文、产物与验证

### 5.1 Plan Compiler 与 Replan

Plan Compiler 检查循环依赖、缺失依赖、孤儿任务、Artifact 数据依赖、Catalog 中不存在的 capability/tool/model/permission 和预算违规。Run 创建会原子写入 Run、PlanVersion、Task、Dependency、AcceptanceGate 和 Outbox。

若请求没有显式 `plan`，当前 Planner 不是通用 LLM 规划器，而是从第一个已启用 AgentTemplate capability 生成一个确定性的单 Task 计划。没有可用 capability 时，Run 创建直接失败。Replan 前必须先 Pause，或已处于 WAITING_USER、VERIFYING、DEGRADED；存在 `EXECUTING / UNKNOWN / RECONCILING / COMPENSATING` Effect 时拒绝激活新计划。

### 5.2 Context Engine

Context Engine 已进入 worker 执行链，能够按 Task Contract 构建 ContextPack、追踪来源、脱敏，并对超长 Checkpoint/Tool Result 生成摘要与外部引用，避免把 50,000 行结果原样塞入 Prompt。它已有组件自动测试，但对象存储和向量记忆的生产适配、跨 Run 长期 Memory 仍未交付。

### 5.3 Artifact

当前 Artifact API 提供元数据、版本、内容哈希、URI/ObjectKey、验证信息和血缘查询。系统会为候选/最终结果生成数据库内的 Artifact 描述符和完成证据。仓库没有交付通用 MinIO/S3 大对象上传下载 API；`contentUri` 存在不等于该对象一定可以由当前控制台下载。

### 5.4 Verification 与 Completion Manifest

系统硬门独立检查候选 Artifact、策略违规和必需 Gate；Reviewer 声称成功不能覆盖失败证据。最后一个成功 Task 会触发 Run 级完成检查并生成 Final Artifact 与 Completion Manifest。当前已有确定性 Gate 和数据库闭环代码，但 CommandGate 对真实外部构建环境、LLM Judge、人工 Gate 的生产插件覆盖并不完整。

## 6. 安全、多租户与人机交互

HTTP `/api/*` 使用 `SWARMOS_API_KEY`，接受 Bearer 或 `X-API-Key`。tenantId 与审计 subject 只从服务端配置注入，请求体和请求头不能覆盖。读写仓储均以 tenantId 过滤；跨租户和不存在统一返回 404，避免资源枚举。

当前认证模型仍是“单部署、单配置租户、单管理 API Key”，不是面向多个终端用户的 OIDC/RBAC。gRPC 9090 当前没有与 HTTP 等价的 API Key/TLS 边界，必须只绑定可信网络或由外部代理保护。

Interaction 支持 `INPUT / APPROVAL / CHOICE / EDIT / AUTH / TAKEOVER` 合同和统一 Inbox：

- `status=PENDING` 仅作为旧前端别名，服务端归一化为 `WAITING`；
- approve/reject 只用于 APPROVAL；其他类型走 resolve；
- 写入需要客户端最后看到的 `version` 和稳定 `Idempotency-Key`；
- AUTH resolution 只能是 `credentialRef`，禁止明文 Secret；
- R3 Effect 的 Interaction、Effect 授权/拒绝、审计和幂等结果在同一事务内提交。

R3 Effect 能自动创建审批 Interaction，并在批准后由重投的普通 worker 继续。通用“任务缺信息 -> Run 自动切 WAITING_USER -> Input 后 Temporal Signal -> 原 Checkpoint 继续”的跨组件编排尚未完全接线，属于验收待闭环项。

## 7. P0 / P1 实现盘点

### P0

| 能力 | 当前实现程度 | 说明 |
| --- | --- | --- |
| Run | 已实现主体 | 列表/详情、创建、Pause/Resume/Cancel/Replan、状态机、CAS、预算聚合 |
| Task Contract / PlanVersion | 已实现主体 | 编译与原子落库；控制台 Task 读模型尚未返回全部新合同字段 |
| TaskAttempt / Step / Checkpoint | 已实现主体 | owner/fence、typed step、最新 Checkpoint 恢复；真实 kill 演练待做 |
| Temporal Durable Runtime | 部分实现 | Run Signal 与投影协调已实现；完整 DAG 所有权未迁移 |
| Recovery | 已实现组件 | 心跳回收、重试证据、失败分类；自动 Agent/Model Switch/Replan 未闭环 |
| Tool Gateway / Effect | 已实现主体 | R0-R3、幂等、UNKNOWN/Reconcile、Approval；真实高风险适配器待演练 |
| Artifact | 部分实现 | 描述符、版本、血缘与完成证据；通用对象存储读写未交付 |
| Verification | 已实现主体 | 硬门、GateResult、Manifest；生产 Gate 插件覆盖待扩展 |
| Interaction | 已实现主体 | API、CAS、幂等 Inbox、R3 原子审批；通用缺输入编排未全接线 |

### P1

| 能力 | 当前实现程度 | 说明 |
| --- | --- | --- |
| Context Engine | 已接入 worker | 预算、脱敏、来源、大结果卸载；长期 Memory/对象存储待补 |
| Plan Compiler | 已实现主体 | DAG/Catalog/预算/Artifact 依赖校验；默认规划器当前只是单 Task |
| Replanner | 已实现主体 | 安全点、不可变版本、未决 Effect 阻断；Artifact 复用生产演练待做 |
| Scheduler Explain | 已实现主体 | 决策持久化与读 API；控制台部分字段显示仍需联调 |
| Failure Classifier | 部分接线 | worker 会分类并持久化恢复建议；建议尚未自动驱动完整分层恢复 |
| Timeline | 已实现主体 | tenant scoped 聚合 API；生产保留期与大规模查询待压测 |
| Cost Budget | 部分实现 | Scheduler 预算过滤、Run 实耗累计；独立 `cost_ledger` 闭环未完成 |
| Workspace Revision | 仅数据结构 | 没有可用 API 与 Take Over 工作流 |

## 8. P2 / P3 非目标

迁移中创建的 `agent_sets`、`a2a_endpoints`、`scaling_decisions`、`evolution_candidates` 等表只是前瞻 schema，不代表对应服务已经实现。当前不得对外宣称支持：

- P2：AgentSet 自动控制器、自动扩缩、跨平台 A2A、Nested Swarm、动态团队；
- P3：数据集驱动评估、失败挖掘、候选生成、Benchmark、Shadow、Canary、Promotion、Rollback。

## 9. 部署与兼容风险

### 9.1 兼容模式

- 旧客户端可继续使用 `/swarms`、`/tasks` 等 protobuf JSON API。
- 新控制台仅在 Run 列表端点返回 404/405/501 时降级到旧 Swarm 只读模式；401、503 和网络错误不会伪装成兼容模式。
- 默认本地配置关闭 Temporal，但控制台新建表单默认选择 Temporal；本地用户必须改选 LEGACY，或完整启动 Temporal。
- PostgreSQL 迁移向前兼容 V1 数据；生产升级前仍必须做备份并在 V1 fixture/数据副本上验证。

### 9.2 生产拓扑

生产候选至少包含 PostgreSQL、Redis、NATS、control-plane、普通 worker；TEMPORAL 模式还必须有独立 Temporal PostgreSQL、Temporal Server、namespace 初始化和 workflow-worker。MinIO/S3、向量库、OTel Collector、TLS 反向代理当前不由主 compose 完整交付。

当前工作树已经提供三个二进制的镜像白名单、workflow-worker systemd unit、强制 production 的 compose、带鉴权且兼容空库的 smoke，以及 `/readyz` 聚合探针。发布时仍必须复核：

1. 三个 Linux 二进制与 `web/dist` 是当前 commit 的产物，镜像构建真实成功；
2. systemd unit 中固定的 `User=hale` 与 `/srv/projects/agent-os` 符合目标机；安装脚本依据 `SWARMOS_TEMPORAL_ENABLED` 启用或停用 workflow-worker；
3. `SWARMOS_ENVIRONMENT=production` 生效，并使用至少 32 字符随机 API Key；
4. smoke 与 readiness 在空库、有 Run、依赖故障三种情况下行为符合预期；
5. 普通 worker 与 workflow-worker 有存活、消费积压和失败告警；
6. 8080 经 TLS/身份边界暴露，9090、9465、7233 与数据库端口留在可信网络。

这些门槛只有在最终工作树和目标 VM 上验证后才能标记为完成。

## 10. 完成定义

当前代码已经形成 V1.5 P0 主链路和 P1 的重要组件，但尚不能仅凭单元测试宣布满足设计文档的“生产可用 V1.5”。正式完成至少需要：

- 所有 15 项用例有可重复证据；
- Worker/全服务重启、Effect UNKNOWN、Zombie fencing 和租户隔离在真实拓扑演练；
- Temporal 与 LEGACY 分别执行长时 Run；
- Final Artifact、Completion Manifest、Verification、Timeline 与 Cost Summary 可从一次真实完成 Run 交叉核对；
- 部署、备份、回滚、告警、TLS 与密钥轮换均有运维记录。

具体执行方式和当前证据级别见 [V1.5 验收矩阵](v1.5-acceptance.md)。
