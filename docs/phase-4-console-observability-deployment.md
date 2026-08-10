# 阶段 4：控制台、可观测性与生产部署

## Web Console

`web/` 是 React 19 + TypeScript + Vite 的真实运维控制台，不包含演示数据。页面每 5 秒从控制面读取：

- Swarm 目标、预算和总体进度；
- 按依赖层级计算的 Task DAG；
- Agent 状态、负载与心跳；
- Attempt 模型、Token、Checkpoint、工具审计和 Reviewer 结论；
- Transactional Outbox 的已发布/待发布事件。

生产构建由 Kratos HTTP Server 同源托管。`/api/v1`、`/healthz`、`/metrics` 优先匹配，其他 GET/HEAD 路径由 SPA Handler 返回静态文件或回退到 `index.html`。Vite 哈希资源使用一年强缓存，入口 HTML 使用 `no-cache`。这样生产只需要两个 Go 进程，不需要常驻 Node 服务，也不存在跨域配置。

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

目标目录固定为 `/srv/projects/agent-os`，`.env` 必须由服务器单独维护且权限为 `0600`。发布流程：

1. 在可信构建机执行前端构建和 `CGO_ENABLED=0 GOOS=linux GOARCH=amd64` 交叉编译；
2. 上传 `bin/control-plane`、`bin/worker`、`web/dist`、配置和 systemd unit；
3. 执行 `bash scripts/install-systemd.sh`；
4. 验证健康、指标、控制台 API 和静态页面。

systemd 使用独立的控制面/Worker 服务，包含自动重启、优雅终止、文件描述符上限和宿主加固。它不会修改 `.env`，也不会把密钥输出到日志。

常用命令：

```bash
sudo systemctl status swarmos-control-plane swarmos-worker
sudo journalctl -u swarmos-control-plane -u swarmos-worker -f
curl --fail http://127.0.0.1:8080/healthz
curl --fail http://127.0.0.1:8080/metrics
curl --fail http://127.0.0.1:9465/metrics
```

回滚时把两个 `bin/` 文件和 `web/dist/` 恢复到上一个 Git 提交的构建产物，再执行：

```bash
sudo systemctl restart swarmos-control-plane swarmos-worker
```

数据库迁移采用只前进策略；涉及破坏性 DDL 的未来版本必须先做兼容迁移和数据备份，不能把数据库直接回滚到旧 schema。
