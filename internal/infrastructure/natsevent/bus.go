// Package natsevent 使用 NATS JetStream 实现可靠领域事件总线。
package natsevent

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/licy-yu/agent-os/internal/assignment"
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

// TaskAssignmentSource 是 Worker 对 task.assigned 的持久消费者。
// Durable 名称固定后，Worker 重启会从上次 ACK 的位置继续，而不是跳过停机期间的任务。
type TaskAssignmentSource struct {
	consumer jetstream.Consumer
}

// AssignmentMessage 包装 JetStream 消息，Worker 只依赖 ACK/NAK 合同而不依赖 NATS 类型。
type AssignmentMessage struct{ message jetstream.Msg }

func (m *AssignmentMessage) Data() []byte { return m.message.Data() }
func (m *AssignmentMessage) Ack(ctx context.Context) error {
	return m.message.DoubleAck(ctx)
}
func (m *AssignmentMessage) Retry(delay time.Duration) error { return m.message.NakWithDelay(delay) }
func (m *AssignmentMessage) InProgress() error               { return m.message.InProgress() }

// NewTaskAssignmentSource 声明显式 ACK 的 Pull Consumer。
// AckWait 大于单次心跳周期；长任务会通过 InProgress 延长服务端 ACK 计时。
func (b *Bus) NewTaskAssignmentSource(ctx context.Context, durable string, ackWait time.Duration) (*TaskAssignmentSource, error) {
	consumer, err := b.jetStream.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Name: durable, Durable: durable, Description: "SwarmOS Worker 任务分配消费者",
		DeliverPolicy: jetstream.DeliverAllPolicy, AckPolicy: jetstream.AckExplicitPolicy,
		AckWait: ackWait, MaxDeliver: 20, FilterSubject: "task.assigned",
		BackOff: []time.Duration{time.Second, 5 * time.Second, 15 * time.Second, time.Minute},
	})
	if err != nil {
		return nil, fmt.Errorf("声明 task.assigned consumer %s: %w", durable, err)
	}
	return &TaskAssignmentSource{consumer: consumer}, nil
}

// Next 最多等待 1 秒，使 Kratos Stop 不必等待长时间阻塞的拉取请求。
// 超时不是错误，以 (nil,nil) 通知 Worker 再检查一次退出上下文。
func (s *TaskAssignmentSource) Next(_ context.Context) (assignment.Message, error) {
	message, err := s.consumer.Next(jetstream.FetchMaxWait(time.Second))
	if err != nil {
		if errors.Is(err, jetstream.ErrNoMessages) || errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
			return nil, nil
		}
		return nil, fmt.Errorf("拉取 task.assigned: %w", err)
	}
	return &AssignmentMessage{message: message}, nil
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
