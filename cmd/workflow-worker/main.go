// workflow-worker 承载 Temporal Workflow/Activity，和执行模型/工具的 worker 分开部署。
// 这样 Agent 执行槽耗尽或崩溃时，Run 的 Pause/Resume/Recovery 协调仍能继续处理。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/licy-yu/agent-os/internal/conf"
	"github.com/licy-yu/agent-os/internal/data/postgres"
	"github.com/licy-yu/agent-os/internal/durabletemporal"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

var configPath = flag.String("config", "./configs/config.yaml", "配置文件路径")

func main() {
	flag.Parse()
	cfg, err := conf.Load(*configPath)
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}
	if !cfg.Temporal.Enabled {
		log.Fatal("Temporal 未启用：请设置 SWARMOS_TEMPORAL_ENABLED=true")
	}

	initCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	repository, err := postgres.New(initCtx, cfg.Data.DatabaseDSN)
	if err != nil {
		log.Fatalf("初始化数据层失败: %v", err)
	}
	defer repository.Close()
	if err := repository.Migrate(initCtx); err != nil {
		log.Fatalf("执行数据库迁移失败: %v", err)
	}

	temporalClient, err := client.DialContext(initCtx, client.Options{
		HostPort: cfg.Temporal.Address, Namespace: cfg.Temporal.Namespace,
		Identity: hostname() + "-swarmos-workflow-worker",
	})
	if err != nil {
		log.Fatalf("连接 Temporal 失败: %v", err)
	}
	defer temporalClient.Close()
	if _, err := temporalClient.CheckHealth(initCtx, nil); err != nil {
		log.Fatalf("Temporal 健康检查失败: %v", err)
	}

	runtime, err := durabletemporal.NewWorker(temporalClient, cfg.Temporal,
		&durabletemporal.Activities{Reader: repository})
	if err != nil {
		log.Fatalf("注册 Workflow/Activity 失败: %v", err)
	}
	log.Printf("SwarmOS Workflow Worker 已启动：namespace=%s task_queue=%s",
		cfg.Temporal.Namespace, cfg.Temporal.TaskQueue)
	// SDK 的 InterruptCh 监听 SIGINT/SIGTERM，并在退出前等待正在处理的 Workflow Task
	// 完成，避免容器滚动升级留下半个 Activity 响应。
	if err := runtime.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("Workflow Worker 退出: %v", err)
	}
}

func hostname() string {
	value, err := os.Hostname()
	if err != nil || value == "" {
		return fmt.Sprintf("unknown-%d", os.Getpid())
	}
	return value
}
