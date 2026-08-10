// Package conf 负责加载并校验 SwarmOS 进程配置。
//
// 配置遵循“文件提供非敏感默认值、环境变量覆盖敏感项”的原则。这样既能保证
// 本地开发开箱即用，也不会要求把生产数据库密码提交到 Git。
package conf

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

// Config 是控制面进程的完整配置。
type Config struct {
	Server  ServerConfig  `yaml:"server"`
	Data    DataConfig    `yaml:"data"`
	Runtime RuntimeConfig `yaml:"runtime"`
}

// ServerConfig 描述 Kratos HTTP/gRPC 监听参数。
type ServerConfig struct {
	Name            string        `yaml:"name"`
	Environment     string        `yaml:"environment"`
	HTTPAddr        string        `yaml:"http_addr"`
	GRPCAddr        string        `yaml:"grpc_addr"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
}

// DataConfig 保存外部基础设施地址。密码应通过环境变量注入。
type DataConfig struct {
	DatabaseDSN   string `yaml:"database_dsn"`
	RedisAddr     string `yaml:"redis_addr"`
	RedisPassword string `yaml:"redis_password"`
	NATSURL       string `yaml:"nats_url"`
}

// RuntimeConfig 控制调度与控制循环的时间参数。
type RuntimeConfig struct {
	HeartbeatTimeout  time.Duration `yaml:"heartbeat_timeout"`
	LeaseTTL          time.Duration `yaml:"lease_ttl"`
	ReconcileInterval time.Duration `yaml:"reconcile_interval"`
	OutboxInterval    time.Duration `yaml:"outbox_interval"`
}

// Load 读取 YAML 后应用环境变量覆盖，最后统一做配置校验。
// 所有错误都包含具体字段，避免部署时只能看到模糊的“启动失败”。
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件 %q: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件 %q: %w", path, err)
	}

	applyEnvironment(&cfg)
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("配置校验失败: %w", err)
	}
	return &cfg, nil
}

// applyEnvironment 只声明项目允许覆盖的变量，避免把整个进程环境隐式映射到配置。
func applyEnvironment(cfg *Config) {
	overrideString("SWARMOS_SERVICE_NAME", &cfg.Server.Name)
	overrideString("SWARMOS_ENVIRONMENT", &cfg.Server.Environment)
	overrideString("SWARMOS_HTTP_ADDR", &cfg.Server.HTTPAddr)
	overrideString("SWARMOS_GRPC_ADDR", &cfg.Server.GRPCAddr)
	overrideDuration("SWARMOS_SHUTDOWN_TIMEOUT", &cfg.Server.ShutdownTimeout)

	overrideString("SWARMOS_DATABASE_DSN", &cfg.Data.DatabaseDSN)
	overrideString("SWARMOS_REDIS_ADDR", &cfg.Data.RedisAddr)
	overrideString("SWARMOS_REDIS_PASSWORD", &cfg.Data.RedisPassword)
	overrideString("SWARMOS_NATS_URL", &cfg.Data.NATSURL)

	overrideDuration("SWARMOS_HEARTBEAT_TIMEOUT", &cfg.Runtime.HeartbeatTimeout)
	overrideDuration("SWARMOS_LEASE_TTL", &cfg.Runtime.LeaseTTL)
	overrideDuration("SWARMOS_RECONCILE_INTERVAL", &cfg.Runtime.ReconcileInterval)
	overrideDuration("SWARMOS_OUTBOX_INTERVAL", &cfg.Runtime.OutboxInterval)
}

func overrideString(name string, target *string) {
	if value, ok := os.LookupEnv(name); ok {
		*target = value
	}
}

func overrideDuration(name string, target *time.Duration) {
	value, ok := os.LookupEnv(name)
	if !ok {
		return
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		// 用负值标记无效输入，统一交给 Validate 返回可读错误。
		*target = -1
		return
	}
	*target = duration
}

// Validate 拒绝会导致进程处于“看似启动、实际不可用”状态的配置。
func (c Config) Validate() error {
	switch {
	case c.Server.Name == "":
		return errors.New("server.name 不能为空")
	case c.Server.HTTPAddr == "":
		return errors.New("server.http_addr 不能为空")
	case c.Server.GRPCAddr == "":
		return errors.New("server.grpc_addr 不能为空")
	case c.Server.ShutdownTimeout <= 0:
		return errors.New("server.shutdown_timeout 必须是正时长")
	case c.Data.DatabaseDSN == "":
		return errors.New("data.database_dsn 不能为空")
	case c.Runtime.HeartbeatTimeout <= 0:
		return errors.New("runtime.heartbeat_timeout 必须是正时长")
	case c.Runtime.LeaseTTL <= 0:
		return errors.New("runtime.lease_ttl 必须是正时长")
	case c.Runtime.ReconcileInterval <= 0:
		return errors.New("runtime.reconcile_interval 必须是正时长")
	case c.Runtime.OutboxInterval <= 0:
		return errors.New("runtime.outbox_interval 必须是正时长")
	}
	return nil
}

// BoolFromEnv 为后续 Worker 的功能开关提供严格布尔值解析。
// 未设置时返回 defaultValue，设置了非法值时明确报错。
func BoolFromEnv(name string, defaultValue bool) (bool, error) {
	value, ok := os.LookupEnv(name)
	if !ok {
		return defaultValue, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("环境变量 %s 不是合法布尔值: %w", name, err)
	}
	return parsed, nil
}
