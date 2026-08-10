// Package event 实现 Transactional Outbox 的领取、发布和重试循环。
package event

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/uuid"
)

// OutboxEvent 是已提交数据库、等待发送到 JetStream 的领域事件。
type OutboxEvent struct {
	ID               uuid.UUID
	AggregateType    string
	AggregateID      uuid.UUID
	Type             string
	AggregateVersion int64
	Payload          []byte
	Attempts         int32
	CreatedAt        time.Time
}

// Store 定义 Dispatcher 所需的最小数据库能力。
type Store interface {
	ClaimOutbox(context.Context, string, int, time.Duration) ([]OutboxEvent, error)
	MarkOutboxPublished(context.Context, uuid.UUID, string) error
	RescheduleOutbox(context.Context, uuid.UUID, string, time.Time, string) error
}

// Bus 隔离 NATS 实现，单元测试可使用内存总线验证重试语义。
type Bus interface {
	Publish(context.Context, OutboxEvent) error
}

// Dispatcher 配置 Outbox 的批量、锁和重试策略。
type Dispatcher struct {
	store    Store
	bus      Bus
	owner    string
	batch    int
	lockTTL  time.Duration
	interval time.Duration
	logger   *log.Helper
}

// NewDispatcher 创建可靠事件发布器。
func NewDispatcher(store Store, bus Bus, owner string, interval time.Duration, logger log.Logger) *Dispatcher {
	return &Dispatcher{
		store: store, bus: bus, owner: owner, batch: 100,
		lockTTL: 30 * time.Second, interval: interval,
		logger: log.NewHelper(log.With(logger, "component", "outbox-dispatcher")),
	}
}

// Run 持续认领批次。空批次使用配置的 interval 等待，避免空转打满数据库。
func (d *Dispatcher) Run(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		count, err := d.DispatchOnce(ctx)
		if err != nil {
			d.logger.Errorf("发布 Outbox 批次失败: %v", err)
		}
		delay := d.interval
		if count == d.batch {
			// 批次已满说明可能还有积压，立即领取下一批。
			delay = 0
		}
		timer.Reset(delay)
	}
}

// DispatchOnce 认领并逐条同步等待 JetStream ACK。
// 每条消息携带 Outbox UUID 作为去重 ID，进程在 ACK 后崩溃导致的重发也可去重。
func (d *Dispatcher) DispatchOnce(ctx context.Context) (int, error) {
	events, err := d.store.ClaimOutbox(ctx, d.owner, d.batch, d.lockTTL)
	if err != nil {
		return 0, fmt.Errorf("认领 Outbox: %w", err)
	}
	for _, value := range events {
		if err := d.bus.Publish(ctx, value); err != nil {
			next := time.Now().UTC().Add(retryDelay(value.Attempts + 1))
			if storeErr := d.store.RescheduleOutbox(ctx, value.ID, d.owner, next, err.Error()); storeErr != nil {
				return len(events), fmt.Errorf("发布事件 %s 失败且重排失败: publish=%v reschedule=%w", value.ID, err, storeErr)
			}
			continue
		}
		if err := d.store.MarkOutboxPublished(ctx, value.ID, d.owner); err != nil {
			return len(events), fmt.Errorf("标记事件 %s 已发布: %w", value.ID, err)
		}
	}
	return len(events), nil
}

func retryDelay(attempt int32) time.Duration {
	// 指数退避上限 5 分钟，避免 NATS 故障时持续压测数据库和网络。
	exponent := math.Min(float64(attempt), 8)
	delay := time.Duration(math.Pow(2, exponent)) * time.Second
	if delay > 5*time.Minute {
		return 5 * time.Minute
	}
	return delay
}
