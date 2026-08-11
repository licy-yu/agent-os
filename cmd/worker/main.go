// worker 是 SwarmOS 独立执行面进程：消费分配事件、运行模型/工具并提交审查证据。
package main

import (
	"context"
	"flag"
	"os"
	"time"

	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware/tracing"
	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/conf"
	"github.com/licy-yu/agent-os/internal/data/postgres"
	"github.com/licy-yu/agent-os/internal/infrastructure/natsevent"
	"github.com/licy-yu/agent-os/internal/observability"
	"github.com/licy-yu/agent-os/internal/server"
	"github.com/licy-yu/agent-os/internal/toolgateway"
	"github.com/licy-yu/agent-os/internal/worker"
)

var (
	configPath   = flag.String("config", "./configs/config.yaml", "配置文件路径")
	buildVersion = "dev"
	buildCommit  = "unknown"
)

func main() {
	flag.Parse()
	workerID := hostname() + "-" + uuid.NewString()
	logger := log.With(log.NewStdLogger(os.Stdout),
		"ts", log.DefaultTimestamp, "caller", log.DefaultCaller,
		"service", "swarmos-worker", "worker_id", workerID,
		"version", buildVersion, "commit", buildCommit,
		"trace_id", tracing.TraceID(), "span_id", tracing.SpanID(),
	)
	cfg, err := conf.Load(*configPath)
	if err != nil {
		log.NewHelper(logger).Fatalf("加载配置失败: %v", err)
	}
	initCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	telemetry, err := observability.New(initCtx, cfg.Observability, "swarmos-worker", buildVersion, cfg.Server.Environment)
	if err != nil {
		log.NewHelper(logger).Fatalf("初始化可观测性失败: %v", err)
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = telemetry.Shutdown(shutdownCtx)
	}()
	repository, err := postgres.New(initCtx, cfg.Data.DatabaseDSN)
	if err != nil {
		log.NewHelper(logger).Fatalf("初始化数据层失败: %v", err)
	}
	defer repository.Close()
	if err := repository.Migrate(initCtx); err != nil {
		log.NewHelper(logger).Fatalf("执行数据库迁移失败: %v", err)
	}
	eventBus, err := natsevent.New(initCtx, cfg.Data.NATSURL)
	if err != nil {
		log.NewHelper(logger).Fatalf("初始化 NATS JetStream 失败: %v", err)
	}
	defer func() { _ = eventBus.Close() }()
	source, err := eventBus.NewTaskAssignmentSource(initCtx, cfg.Worker.Durable, cfg.Worker.AckWait)
	if err != nil {
		log.NewHelper(logger).Fatalf("初始化任务消费者失败: %v", err)
	}
	router, err := worker.NewRouter(worker.NewOpenAIExecutor())
	if err != nil {
		log.NewHelper(logger).Fatalf("初始化 Eino Agent Runtime 失败: %v", err)
	}
	adapters := map[string]toolgateway.Adapter{
		"native:echo": toolgateway.EchoAdapter{},
		"mcp:*":       toolgateway.NewMCPAdapter(nil),
	}
	runtime := worker.NewRuntime(
		workerID, source, repository, repository, router, adapters,
		cfg.Worker.Concurrency, cfg.Worker.HeartbeatInterval, logger,
	)
	metricsServer := server.NewMetricsServer(cfg.Worker.MetricsAddr, telemetry.MetricsHandler())
	app := kratos.New(
		kratos.ID(workerID), kratos.Name("swarmos-worker"), kratos.Version(buildVersion),
		kratos.Metadata(map[string]string{"environment": cfg.Server.Environment, "commit": buildCommit}),
		kratos.Logger(logger), kratos.Server(runtime, metricsServer), kratos.StopTimeout(cfg.Server.ShutdownTimeout),
	)
	if err := app.Run(); err != nil {
		log.NewHelper(logger).Fatalf("Worker 退出: %v", err)
	}
}

func hostname() string {
	value, err := os.Hostname()
	if err != nil || value == "" {
		return "unknown-host"
	}
	return value
}
