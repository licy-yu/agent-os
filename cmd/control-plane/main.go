// control-plane 是 SwarmOS 声明式控制面的进程入口。
package main

import (
	"context"
	"flag"
	"os"
	"time"

	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/licy-yu/agent-os/internal/conf"
	"github.com/licy-yu/agent-os/internal/data/postgres"
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
	)

	cfg, err := conf.Load(*configPath)
	if err != nil {
		log.NewHelper(logger).Fatalf("加载配置失败: %v", err)
	}

	// 启动阶段设置超时，防止数据库网络故障让进程永久卡在初始化。
	initCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	repository, err := postgres.New(initCtx, cfg.Data.DatabaseDSN)
	if err != nil {
		log.NewHelper(logger).Fatalf("初始化数据层失败: %v", err)
	}
	defer repository.Close()
	if err := repository.Migrate(initCtx); err != nil {
		log.NewHelper(logger).Fatalf("执行数据库迁移失败: %v", err)
	}

	svc := service.NewControlPlaneService(repository, repository, repository, repository)
	httpServer := server.NewHTTPServer(cfg.Server, svc, logger)
	grpcServer := server.NewGRPCServer(cfg.Server, svc, logger)

	app := kratos.New(
		kratos.ID(hostname()),
		kratos.Name(cfg.Server.Name),
		kratos.Version(buildVersion),
		kratos.Metadata(map[string]string{"environment": cfg.Server.Environment, "commit": buildCommit}),
		kratos.Logger(logger),
		kratos.Server(httpServer, grpcServer),
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
