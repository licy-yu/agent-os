// Package natsevent 使用 NATS JetStream 实现可靠领域事件总线。
package natsevent

import (
	"context"
	"fmt"
	"time"

	"github.com/licy-yu/agent-os/internal/event"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const streamName = "SWARM_EVENTS"

// Bus 持有 NATS 连接与 JetStream 上下文。
type Bus struct {
	connection *nats.Conn
	jetStream  jetstream.JetStream
}

// New 建立连接并声明事件流。CreateOrUpdateStream 使启动过程可重复执行。
func New(ctx context.Context, url string) (*Bus, error) {
	connection, err := nats.Connect(url,
		nats.Name("swarmos-control-plane"),
		nats.Timeout(5*time.Second),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("连接 NATS: %w", err)
	}
	js, err := jetstream.New(connection)
	if err != nil {
		connection.Close()
		return nil, fmt.Errorf("创建 JetStream 上下文: %w", err)
	}
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        streamName,
		Description: "SwarmOS 领域事件流",
		Subjects: []string{
			"swarm.>", "task.>", "agent.>", "agent_template.>",
			"attempt.>", "tool.>", "evaluation.>",
		},
		Storage:    jetstream.FileStorage,
		Retention:  jetstream.LimitsPolicy,
		MaxAge:     30 * 24 * time.Hour,
		Duplicates: 10 * time.Minute,
	}); err != nil {
		connection.Close()
		return nil, fmt.Errorf("声明 JetStream %s: %w", streamName, err)
	}
	return &Bus{connection: connection, jetStream: js}, nil
}

// Publish 同步等待服务端 ACK；Outbox ID 同时作为 JetStream 去重键。
func (b *Bus) Publish(ctx context.Context, value event.OutboxEvent) error {
	_, err := b.jetStream.Publish(ctx, value.Type, value.Payload,
		jetstream.WithMsgID(value.ID.String()),
		jetstream.WithExpectStream(streamName),
	)
	if err != nil {
		return fmt.Errorf("发布 %s 到 JetStream: %w", value.Type, err)
	}
	return nil
}

// Close 先 Drain 再关闭连接，让缓冲区中已发送的数据得到处理机会。
func (b *Bus) Close() error {
	if err := b.connection.Drain(); err != nil {
		b.connection.Close()
		return fmt.Errorf("drain NATS 连接: %w", err)
	}
	b.connection.Close()
	return nil
}
