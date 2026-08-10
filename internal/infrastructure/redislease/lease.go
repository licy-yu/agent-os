// Package redislease 实现 Scheduler 的短期 Agent Reserve 租约。
package redislease

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/lease"
	"github.com/redis/go-redis/v9"
)

const keyPrefix = "swarmos:scheduler:agent:"

// Manager 封装 Redis 客户端；PostgreSQL 仍然是最终绑定事实源。
type Manager struct {
	client *redis.Client
}

// New 建立 Redis 客户端并 Ping，启动时尽早暴露地址或密码错误。
func New(ctx context.Context, addr, password string) (*Manager, error) {
	client := redis.NewClient(&redis.Options{
		Addr: addr, Password: password, DB: 0,
		DialTimeout: 5 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second,
	})
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("连接 Redis: %w", err)
	}
	return &Manager{client: client}, nil
}

// Reserve 使用 SET NX EX 原子抢占 Agent。
func (m *Manager) Reserve(ctx context.Context, agentID, taskID uuid.UUID, ttl time.Duration) (*lease.Lease, bool, error) {
	token := uuid.NewString()
	value := taskID.String() + ":" + token
	ok, err := m.client.SetNX(ctx, key(agentID), value, ttl).Result()
	if err != nil {
		return nil, false, fmt.Errorf("创建 agent lease: %w", err)
	}
	if !ok {
		return nil, false, nil
	}
	return &lease.Lease{AgentID: agentID, TaskID: taskID, Token: value, TTL: ttl}, true, nil
}

// Release 用 Lua 比较 token 后删除，绝不删除已经过期并被其他 Scheduler 重建的租约。
func (m *Manager) Release(ctx context.Context, value *lease.Lease) error {
	if value == nil {
		return nil
	}
	const script = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0`
	if _, err := m.client.Eval(ctx, script, []string{key(value.AgentID)}, value.Token).Result(); err != nil {
		return fmt.Errorf("释放 agent lease: %w", err)
	}
	return nil
}

func (m *Manager) Close() error { return m.client.Close() }

func key(agentID uuid.UUID) string { return keyPrefix + agentID.String() }
