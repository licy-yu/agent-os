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
	"strings"
	"time"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

// Config 是控制面进程的完整配置。
type Config struct {
	Server        ServerConfig        `yaml:"server"`
	Data          DataConfig          `yaml:"data"`
	Runtime       RuntimeConfig       `yaml:"runtime"`
	Temporal      TemporalConfig      `yaml:"temporal"`
	Security      SecurityConfig      `yaml:"security"`
	Worker        WorkerConfig        `yaml:"worker"`
	Observability ObservabilityConfig `yaml:"observability"`
}

// SecurityConfig 定义 HTTP API 的可信租户边界。APIKey 只允许环境变量注入，yaml 标签
// 明确忽略它，防止生产密钥误提交到仓库。
type SecurityConfig struct {
	APIKey   string `yaml:"-"`
	TenantID string `yaml:"tenant_id"`
	Subject  string `yaml:"subject"`
}

// TemporalConfig 控制 Durable Run Runtime。Enabled=false 时仅允许 LEGACY Run，避免在
// Temporal Server/Workflow Worker 未就绪时产生“看似运行、实际无人消费”的记录。
type TemporalConfig struct {
	Enabled   bool   `yaml:"enabled"`
	Address   string `yaml:"address"`
	Namespace string `yaml:"namespace"`
	TaskQueue string `yaml:"task_queue"`
}

// WorkerConfig 控制独立执行进程的 Durable Consumer、并发和双层心跳。
type WorkerConfig struct {
	Durable           string        `yaml:"durable"`
	Concurrency       int           `yaml:"concurrency"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	AckWait           time.Duration `yaml:"ack_wait"`
	MetricsAddr       string        `yaml:"metrics_addr"`
}

// ObservabilityConfig 只保存非敏感采样策略；OTLP 端点和鉴权使用 OpenTelemetry 标准环境变量。
type ObservabilityConfig struct {
	TraceSampleRatio float64 `yaml:"trace_sample_ratio"`
}

// ServerConfig 描述 Kratos HTTP/gRPC 监听参数。
type ServerConfig struct {
	Name            string        `yaml:"name"`
	Environment     string        `yaml:"environment"`
	HTTPAddr        string        `yaml:"http_addr"`
	GRPCAddr        string        `yaml:"grpc_addr"`
	WebDir          string        `yaml:"web_dir"`
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

	// 新增配置项提供保守默认值，旧版部署文件升级二进制时无需一次性补齐非敏感字段。
	cfg := Config{Security: SecurityConfig{
		TenantID: "00000000-0000-0000-0000-000000000001", Subject: "api-operator",
	}, Temporal: TemporalConfig{
		Address: "127.0.0.1:7233", Namespace: "default", TaskQueue: "swarmos-runs-v1-5",
	}, Worker: WorkerConfig{
		Durable: "swarmos-workers", Concurrency: 4,
		HeartbeatInterval: 10 * time.Second, AckWait: 30 * time.Second,
		MetricsAddr: "127.0.0.1:9465",
	}, Observability: ObservabilityConfig{TraceSampleRatio: .1}}
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件 %q: %w", path, err)
	}

	applyEnvironment(&cfg)
	temporalEnabled, err := BoolFromEnv("SWARMOS_TEMPORAL_ENABLED", cfg.Temporal.Enabled)
	if err != nil {
		return nil, fmt.Errorf("配置校验失败: %w", err)
	}
	cfg.Temporal.Enabled = temporalEnabled
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
	overrideString("SWARMOS_WEB_DIR", &cfg.Server.WebDir)
	overrideDuration("SWARMOS_SHUTDOWN_TIMEOUT", &cfg.Server.ShutdownTimeout)

	overrideString("SWARMOS_DATABASE_DSN", &cfg.Data.DatabaseDSN)
	overrideString("SWARMOS_REDIS_ADDR", &cfg.Data.RedisAddr)
	overrideString("SWARMOS_REDIS_PASSWORD", &cfg.Data.RedisPassword)
	overrideString("SWARMOS_NATS_URL", &cfg.Data.NATSURL)

	overrideDuration("SWARMOS_HEARTBEAT_TIMEOUT", &cfg.Runtime.HeartbeatTimeout)
	overrideDuration("SWARMOS_LEASE_TTL", &cfg.Runtime.LeaseTTL)
	overrideDuration("SWARMOS_RECONCILE_INTERVAL", &cfg.Runtime.ReconcileInterval)
	overrideDuration("SWARMOS_OUTBOX_INTERVAL", &cfg.Runtime.OutboxInterval)
	overrideString("SWARMOS_TEMPORAL_ADDRESS", &cfg.Temporal.Address)
	overrideString("SWARMOS_TEMPORAL_NAMESPACE", &cfg.Temporal.Namespace)
	overrideString("SWARMOS_TEMPORAL_TASK_QUEUE", &cfg.Temporal.TaskQueue)
	overrideString("SWARMOS_API_KEY", &cfg.Security.APIKey)
	overrideString("SWARMOS_TENANT_ID", &cfg.Security.TenantID)
	overrideString("SWARMOS_API_SUBJECT", &cfg.Security.Subject)

	overrideString("SWARMOS_WORKER_DURABLE", &cfg.Worker.Durable)
	overrideInt("SWARMOS_WORKER_CONCURRENCY", &cfg.Worker.Concurrency)
	overrideDuration("SWARMOS_WORKER_HEARTBEAT_INTERVAL", &cfg.Worker.HeartbeatInterval)
	overrideDuration("SWARMOS_WORKER_ACK_WAIT", &cfg.Worker.AckWait)
	overrideString("SWARMOS_WORKER_METRICS_ADDR", &cfg.Worker.MetricsAddr)
	overrideFloat("SWARMOS_TRACE_SAMPLE_RATIO", &cfg.Observability.TraceSampleRatio)
}

func overrideInt(name string, target *int) {
	value, ok := os.LookupEnv(name)
	if !ok {
		return
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		*target = -1
		return
	}
	*target = parsed
}

func overrideFloat(name string, target *float64) {
	value, ok := os.LookupEnv(name)
	if !ok {
		return
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		*target = -1
		return
	}
	*target = parsed
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
	case c.Temporal.Enabled && c.Temporal.Address == "":
		return errors.New("temporal.address 在启用时不能为空")
	case c.Temporal.Enabled && c.Temporal.Namespace == "":
		return errors.New("temporal.namespace 在启用时不能为空")
	case c.Temporal.Enabled && c.Temporal.TaskQueue == "":
		return errors.New("temporal.task_queue 在启用时不能为空")
	case strings.EqualFold(strings.TrimSpace(c.Server.Environment), "production") && len(c.Security.APIKey) < 32:
		return errors.New("生产环境 SWARMOS_API_KEY 至少需要 32 个字符")
	case c.Security.TenantID == "":
		return errors.New("security.tenant_id 不能为空")
	case c.Security.Subject == "":
		return errors.New("security.subject 不能为空")
	case c.Worker.Durable == "":
		return errors.New("worker.durable 不能为空")
	case c.Worker.Concurrency <= 0:
		return errors.New("worker.concurrency 必须为正整数")
	case c.Worker.HeartbeatInterval <= 0:
		return errors.New("worker.heartbeat_interval 必须是正时长")
	case c.Worker.AckWait <= c.Worker.HeartbeatInterval:
		return errors.New("worker.ack_wait 必须大于 heartbeat_interval")
	case c.Worker.MetricsAddr == "":
		return errors.New("worker.metrics_addr 不能为空")
	case c.Observability.TraceSampleRatio < 0 || c.Observability.TraceSampleRatio > 1:
		return errors.New("observability.trace_sample_ratio 必须在 0~1 之间")
	}
	if _, err := uuid.Parse(c.Security.TenantID); err != nil {
		return fmt.Errorf("security.tenant_id 不是合法 UUID: %w", err)
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
