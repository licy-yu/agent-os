// control-plane 是 SwarmOS 声明式控制面的进程入口。
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
	consoleview "github.com/licy-yu/agent-os/internal/console"
	"github.com/licy-yu/agent-os/internal/data/postgres"
	"github.com/licy-yu/agent-os/internal/durabletemporal"
	"github.com/licy-yu/agent-os/internal/event"
	"github.com/licy-yu/agent-os/internal/infrastructure/natsevent"
	"github.com/licy-yu/agent-os/internal/infrastructure/redislease"
	"github.com/licy-yu/agent-os/internal/observability"
	"github.com/licy-yu/agent-os/internal/orchestrator"
	"github.com/licy-yu/agent-os/internal/runcontrol"
	"github.com/licy-yu/agent-os/internal/safetycontrol"
	"github.com/licy-yu/agent-os/internal/server"
	"github.com/licy-yu/agent-os/internal/service"
)

var (
	configPath   = flag.String("config", "./configs/config.yaml", "配置文件路径")
	buildVersion = "dev"
	buildCommit  = "unknown"
)

func main() {
	flag.Parse()
	logger := log.With(log.NewStdLogger(os.Stdout),
		"ts", log.DefaultTimestamp,
		"caller", log.DefaultCaller,
		"service", "swarmos-control-plane",
		"version", buildVersion,
		"commit", buildCommit,
		"trace_id", tracing.TraceID(),
		"span_id", tracing.SpanID(),
	)

	cfg, err := conf.Load(*configPath)
	if err != nil {
		log.NewHelper(logger).Fatalf("加载配置失败: %v", err)
	}
	initCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	telemetry, err := observability.New(initCtx, cfg.Observability, "swarmos-control-plane", buildVersion, cfg.Server.Environment)
	if err != nil {
		log.NewHelper(logger).Fatalf("初始化可观测性失败: %v", err)
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = telemetry.Shutdown(shutdownCtx)
	}()

	// 启动阶段设置超时，防止数据库网络故障让进程永久卡在初始化。
	repository, err := postgres.New(initCtx, cfg.Data.DatabaseDSN)
	if err != nil {
		log.NewHelper(logger).Fatalf("初始化数据层失败: %v", err)
	}
	defer repository.Close()
	if err := repository.Migrate(initCtx); err != nil {
		log.NewHelper(logger).Fatalf("执行数据库迁移失败: %v", err)
	}
	leaseManager, err := redislease.New(initCtx, cfg.Data.RedisAddr, cfg.Data.RedisPassword)
	if err != nil {
		log.NewHelper(logger).Fatalf("初始化 Redis Lease 失败: %v", err)
	}
	defer func() { _ = leaseManager.Close() }()
	eventBus, err := natsevent.New(initCtx, cfg.Data.NATSURL)
	if err != nil {
		log.NewHelper(logger).Fatalf("初始化 NATS JetStream 失败: %v", err)
	}
	defer func() { _ = eventBus.Close() }()

	svc := service.NewControlPlaneService(repository, repository, repository, repository)
	runSvc := runcontrol.NewService(repository, false)
	var temporalGateway *durabletemporal.Gateway
	if cfg.Temporal.Enabled {
		// 只有连接和 gRPC 健康检查都成功后才允许创建 TEMPORAL Run。Workflow Worker
		// 使用同一个 TaskQueue；其不可用会由 Temporal backlog/监控直接暴露。
		temporalGateway, err = durabletemporal.Dial(initCtx, cfg.Temporal)
		if err != nil {
			log.NewHelper(logger).Fatalf("初始化 Temporal Durable Runtime 失败: %v", err)
		}
		defer temporalGateway.Close()
		runSvc.WithDurableRuntime(temporalGateway)
	}
	consoleSvc := consoleview.NewService(repository)
	safetySvc := safetycontrol.NewService(repository)
	dependencyProbes := []server.DependencyProbe{
		{Name: "postgres", Check: repository.Ping},
		{Name: "redis", Check: leaseManager.Ping},
		{Name: "nats", Check: eventBus.Ping},
	}
	if temporalGateway != nil {
		dependencyProbes = append(dependencyProbes,
			server.DependencyProbe{Name: "temporal", Check: temporalGateway.Ping})
	}
	httpServer := server.NewHTTPServer(cfg.Server, cfg.Security, svc, runSvc, safetySvc, consoleSvc,
		server.NewReadinessProbe(dependencyProbes...), telemetry, logger)
	grpcServer := server.NewGRPCServer(cfg.Server, svc, telemetry, logger)
	taskController := orchestrator.NewTaskController(repository, logger)
	agentController := orchestrator.NewAgentController(repository)
	scheduler := orchestrator.NewScheduler(repository, leaseManager, cfg.Runtime.LeaseTTL, logger)
	reviewer := orchestrator.NewReviewerController(repository)
	recovery := orchestrator.NewRecoveryController(repository, cfg.Runtime.HeartbeatTimeout)
	dispatcher := event.NewDispatcher(repository, eventBus, hostname()+"-"+uuid.NewString(), cfg.Runtime.OutboxInterval, logger)
	orchestratorRuntime := orchestrator.NewRuntime(
		taskController, agentController, scheduler, reviewer, recovery, dispatcher,
		cfg.Runtime.ReconcileInterval, logger,
	)

	app := kratos.New(
		kratos.ID(hostname()),
		kratos.Name(cfg.Server.Name),
		kratos.Version(buildVersion),
		kratos.Metadata(map[string]string{"environment": cfg.Server.Environment, "commit": buildCommit}),
		kratos.Logger(logger),
		kratos.Server(httpServer, grpcServer, orchestratorRuntime),
		kratos.StopTimeout(cfg.Server.ShutdownTimeout),
	)
	if err := app.Run(); err != nil {
		log.NewHelper(logger).Fatalf("控制面退出: %v", err)
	}
}

func hostname() string {
	value, err := os.Hostname()
	if err != nil || value == "" {
		return "unknown-host"
	}
	return value
}
