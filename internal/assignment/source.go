// Package assignment 定义 Scheduler 到 Worker 的可靠任务分配消息端口。
package assignment

import (
	"context"
	"time"
)

// Message 暴露显式确认、延迟重投和长任务续期能力。
type Message interface {
	Data() []byte
	Ack(context.Context) error
	Retry(time.Duration) error
	InProgress() error
}

// Source 隔离具体消息中间件，生产由 JetStream 实现，测试可使用内存队列。
type Source interface {
	Next(context.Context) (Message, error)
}
