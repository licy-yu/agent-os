// Package worker 实现独立于控制面的 Agent 执行进程。
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/assignment"
	"github.com/licy-yu/agent-os/internal/execution"
	"github.com/licy-yu/agent-os/internal/toolgateway"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type assignedEvent struct {
	TaskID  uuid.UUID `json:"id"`
	AgentID uuid.UUID `json:"agent_id"`
}

// ExecutorRouter 根据 AgentTemplate.model 选择确定性或 OpenAI Responses 执行器。
type ExecutorRouter interface {
	ForModel(string) execution.Executor
}

// Runtime 同时满足 Kratos Server 接口，负责并发、心跳、ACK 和优雅退出。
type Runtime struct {
	workerID          string
	source            assignment.Source
	store             execution.Store
	toolStore         toolgateway.Store
	router            ExecutorRouter
	adapters          map[string]toolgateway.Adapter
	concurrency       int
	heartbeatInterval time.Duration
	logger            *log.Helper

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

func NewRuntime(workerID string, source assignment.Source, store execution.Store, toolStore toolgateway.Store,
	router ExecutorRouter, adapters map[string]toolgateway.Adapter, concurrency int,
	heartbeatInterval time.Duration, logger log.Logger,
) *Runtime {
	if concurrency < 1 {
		concurrency = 1
	}
	return &Runtime{
		workerID: workerID, source: source, store: store, toolStore: toolStore,
		router: router, adapters: adapters, concurrency: concurrency,
		heartbeatInterval: heartbeatInterval,
		logger:            log.NewHelper(log.With(logger, "component", "worker-runtime", "worker_id", workerID)),
	}
}

// Start 启动固定数量的拉取协程。JetStream Durable Consumer 在多个进程间自然做负载均衡。
func (r *Runtime) Start(parent context.Context) error {
	ctx, cancel := context.WithCancel(parent)
	r.mu.Lock()
	r.cancel, r.done = cancel, make(chan struct{})
	done := r.done
	r.mu.Unlock()

	var workers sync.WaitGroup
	workers.Add(r.concurrency)
	for index := 0; index < r.concurrency; index++ {
		go func(slot int) {
			defer workers.Done()
			r.consume(ctx, slot)
		}(index)
	}
	<-ctx.Done()
	workers.Wait()
	close(done)
	return nil
}

func (r *Runtime) Stop(ctx context.Context) error {
	r.mu.Lock()
	cancel, done := r.cancel, r.done
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Runtime) consume(ctx context.Context, slot int) {
	for ctx.Err() == nil {
		message, err := r.source.Next(ctx)
		if err != nil {
			r.logger.Errorf("执行槽 %d 拉取任务失败: %v", slot, err)
			r.wait(ctx, time.Second)
			continue
		}
		if message == nil {
			continue
		}
		if err := r.handle(ctx, message); err != nil {
			r.logger.Errorf("执行槽 %d 处理任务失败: %v", slot, err)
			if retryErr := message.Retry(5 * time.Second); retryErr != nil {
				r.logger.Errorf("任务 NAK 失败: %v", retryErr)
			}
		}
	}
}

func (r *Runtime) handle(parent context.Context, message assignment.Message) error {
	var event assignedEvent
	if err := json.Unmarshal(message.Data(), &event); err != nil || event.TaskID == uuid.Nil || event.AgentID == uuid.Nil {
		// 永久格式错误不应无限重投；确认后依靠日志和 JetStream 原始消息审计。
		r.logger.Errorf("丢弃非法 task.assigned 消息: %v", err)
		return message.Ack(parent)
	}
	work, err := r.store.ClaimWork(parent, event.TaskID, event.AgentID, r.workerID)
	if err != nil {
		return fmt.Errorf("领取 task %s: %w", event.TaskID, err)
	}
	if work == nil {
		// 已处理或已重新绑定的陈旧消息可以安全 ACK。
		return message.Ack(parent)
	}
	parent, span := otel.Tracer("swarmos/worker").Start(parent, "worker.execute",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("swarmos.task.id", work.Task.ID.String()),
			attribute.String("swarmos.agent.id", work.Agent.ID.String()),
			attribute.String("swarmos.attempt.id", work.Attempt.ID.String()),
			attribute.String("gen_ai.request.model", work.Template.Model),
		),
	)
	defer span.End()

	timeout := work.Task.ExecutionPolicy.Timeout()
	if timeout <= 0 {
		timeout = 30 * time.Minute
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	heartbeatDone := make(chan struct{})
	go r.heartbeat(ctx, work.Attempt.ID, message, heartbeatDone)

	writer := &checkpointWriter{store: r.store, attemptID: work.Attempt.ID, sequence: work.Attempt.StepCount}
	gateway := toolgateway.New(r.toolStore, work, r.adapters)
	result, executeErr := r.router.ForModel(work.Template.Model).Execute(ctx, work, writer, gateway)
	cancel()
	<-heartbeatDone
	if executeErr != nil {
		span.RecordError(executeErr)
		span.SetStatus(codes.Error, executeErr.Error())
		// 模型/工具错误也形成 Reviewer 可见证据，避免只能等待心跳超时才能重试。
		result.Output = map[string]any{"execution_error": executeErr.Error()}
		result.Checks = map[string]bool{"execution": false}
		result.QualityScore = 0
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			result.Output["error_code"] = "EXECUTION_TIMEOUT"
		}
	}
	if err := r.store.CompleteAttempt(parent, work.Attempt.ID, result); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("提交 attempt %s 结果: %w", work.Attempt.ID, err)
	}
	if err := message.Ack(parent); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("确认 task %s 消息: %w", event.TaskID, err)
	}
	span.SetStatus(codes.Ok, "execution submitted for review")
	return nil
}

func (r *Runtime) heartbeat(ctx context.Context, attemptID uuid.UUID, message assignment.Message, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(r.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.store.HeartbeatAttempt(ctx, attemptID, r.workerID); err != nil {
				r.logger.Warnf("刷新 attempt %s 心跳失败: %v", attemptID, err)
			}
			if err := message.InProgress(); err != nil {
				r.logger.Warnf("延长 attempt %s JetStream ACK 等待失败: %v", attemptID, err)
			}
		}
	}
}

func (r *Runtime) wait(ctx context.Context, delay time.Duration) {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

type checkpointWriter struct {
	store     execution.Store
	attemptID uuid.UUID
	sequence  int32
}

func (w *checkpointWriter) Save(ctx context.Context, step string, state map[string]any, artifacts []string) error {
	w.sequence++
	return w.store.SaveCheckpoint(ctx, execution.Checkpoint{
		ID: uuid.New(), AttemptID: w.attemptID, Sequence: w.sequence,
		StepName: step, State: state, ArtifactRefs: artifacts, CreatedAt: time.Now().UTC(),
	})
}
