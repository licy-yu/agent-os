// Package worker 实现独立于控制面的 Agent 执行进程。
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/agentloop"
	"github.com/licy-yu/agent-os/internal/assignment"
	"github.com/licy-yu/agent-os/internal/contextengine"
	"github.com/licy-yu/agent-os/internal/execution"
	"github.com/licy-yu/agent-os/internal/failure"
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
	// activeTasks 是进程内 Task singleflight。workerID 按进程唯一，但一个进程有多个
	// 并发槽；若同一 task.assigned 被重复投递，不能让两个槽共享同一 fence 执行。
	activeTasks map[uuid.UUID]chan struct{}
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
		activeTasks:       make(map[uuid.UUID]chan struct{}),
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
		return ackMessage(parent, message)
	}
	releaseTask, err := r.acquireTask(parent, event.TaskID, message)
	if err != nil {
		return err
	}
	defer releaseTask()
	work, err := r.store.ClaimWork(parent, event.TaskID, event.AgentID, r.workerID)
	if err != nil {
		return fmt.Errorf("领取 task %s: %w", event.TaskID, err)
	}
	if work == nil {
		// 已处理或已重新绑定的陈旧消息可以安全 ACK。
		return ackMessage(parent, message)
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
	owner := work.Attempt.Owner()
	go r.heartbeat(ctx, owner, message, heartbeatDone)

	sequence := checkpointResumeSequence(work)
	writer := &checkpointWriter{store: r.store, owner: owner, sequence: sequence}
	gateway := toolgateway.New(r.toolStore, work, r.adapters)
	result, executeErr := r.router.ForModel(work.Template.Model).Execute(ctx, work, writer, gateway)
	// cancel 之后 ctx.Err() 必然至少是 context.Canceled；先保存真实执行结果，才能
	// 区分 DeadlineExceeded 与 Worker 主动结束心跳，避免把超时误分类为普通模型错误。
	executionContextErr := ctx.Err()
	cancel()
	<-heartbeatDone
	if keepAttemptOpen(executeErr) {
		// 审批窗口可能长达 24 小时，UNKNOWN 对账也没有固定完成时间。这里把执行权
		// 持久化为 WAITING 并 ACK 当前消息；审批/对账完成事务会重新发布 task.assigned。
		// 这样既不会耗尽 JetStream MaxDeliver，也不会被心跳恢复器误判成 Worker 崩溃。
		reason := execution.WaitForApproval
		if errors.Is(executeErr, toolgateway.ErrEffectReconcileRequired) {
			reason = execution.WaitForExternal
		}
		wait := execution.AttemptWait{Reason: reason, EffectID: toolgateway.WaitingEffectID(executeErr)}
		if err := r.store.SuspendAttempt(parent, owner, wait); err != nil {
			return fmt.Errorf("挂起 attempt %s: %w", work.Attempt.ID, err)
		}
		if err := ackMessage(parent, message); err != nil {
			return fmt.Errorf("确认已挂起 task %s 的消息: %w", event.TaskID, err)
		}
		span.SetStatus(codes.Ok, "execution suspended at durable wait boundary")
		return nil
	}
	if executeErr != nil {
		span.RecordError(executeErr)
		span.SetStatus(codes.Error, executeErr.Error())
		// 模型/工具错误也形成 Reviewer 可见证据，避免只能等待心跳超时才能重试。
		decision := classifyRuntimeFailure(executeErr, executionContextErr)
		result.Output = map[string]any{
			"execution_error": executeErr.Error(),
			"failure_class":   decision.Class,
			"recovery_action": decision.Action,
			"retry_level":     decision.RetryLevel,
			"retryable":       decision.Retryable,
		}
		result.Checks = map[string]bool{"execution": false}
		result.QualityScore = 0
		if decision.Class == failure.PolicyViolation {
			// Runtime 的结构化结论只能增加策略违规；数据库 system.policy Gate 还会
			// 独立检查 DENIED ToolCall，模型无法通过伪造空数组把它抹掉。
			result.PolicyViolations = append(result.PolicyViolations, executeErr.Error())
		}
	}
	if err := r.store.CompleteAttempt(parent, owner, result); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("提交 attempt %s 结果: %w", work.Attempt.ID, err)
	}
	if err := ackMessage(parent, message); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("确认 task %s 消息: %w", event.TaskID, err)
	}
	span.SetStatus(codes.Ok, "execution submitted for review")
	return nil
}

// acquireTask 串行化同一进程内相同 Task 的重复投递。等待中的副本定期 InProgress，
// 因此不会消耗 MaxDeliver；前一个执行流离开后，副本再从数据库重新判定是否需要工作。
func (r *Runtime) acquireTask(ctx context.Context, taskID uuid.UUID,
	message assignment.Message,
) (func(), error) {
	for {
		r.mu.Lock()
		active, exists := r.activeTasks[taskID]
		if !exists {
			active = make(chan struct{})
			r.activeTasks[taskID] = active
			r.mu.Unlock()
			return func() {
				r.mu.Lock()
				if current, ok := r.activeTasks[taskID]; ok && current == active {
					delete(r.activeTasks, taskID)
					close(active)
				}
				r.mu.Unlock()
			}, nil
		}
		r.mu.Unlock()

		interval := r.heartbeatInterval
		if interval <= 0 {
			interval = 10 * time.Second
		}
		ticker := time.NewTicker(interval)
		select {
		case <-ctx.Done():
			ticker.Stop()
			return nil, ctx.Err()
		case <-active:
			ticker.Stop()
			// 前一个执行流已提交状态；重新竞争 singleflight 后再读数据库。
			continue
		case <-ticker.C:
			ticker.Stop()
			if err := message.InProgress(); err != nil {
				return nil, fmt.Errorf("续期重复 task %s 消息: %w", taskID, err)
			}
		}
	}
}

func ackMessage(parent context.Context, message assignment.Message) error {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	return message.Ack(ctx)
}

func keepAttemptOpen(err error) bool {
	return errors.Is(err, toolgateway.ErrApprovalPending) ||
		errors.Is(err, toolgateway.ErrEffectReconcileRequired)
}

// classifyRuntimeFailure 把已知类型优先映射成结构化 Hint，再由 failure 包兼容 Provider/MCP
// 文本错误。这里不直接执行恢复动作：Checkpoint、UNKNOWN Reconcile 已在当前边界处理，
// 其余 Decision 会随 Attempt 证据持久化，交由 Reviewer/Recovery/Replanner 做分层恢复。
func classifyRuntimeFailure(err, executionContextErr error) failure.Decision {
	hint := failure.Hint{}
	switch {
	case errors.Is(err, toolgateway.ErrDenied):
		hint.Class = failure.PolicyViolation
	case errors.Is(err, contextengine.ErrBudgetTooSmall):
		hint.Class = failure.ContextOverflow
	case errors.Is(err, agentloop.ErrGuardExceeded):
		if strings.Contains(strings.ToLower(err.Error()), "没有进展") ||
			strings.Contains(strings.ToLower(err.Error()), "no progress") {
			hint.Class = failure.NoProgress
		} else {
			hint.Class = failure.BudgetExceeded
		}
	case errors.Is(executionContextErr, context.DeadlineExceeded):
		hint.Class = failure.NetworkTemporary
	}
	return failure.Classify(err, hint)
}

func (r *Runtime) heartbeat(ctx context.Context, owner execution.AttemptOwner, message assignment.Message, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(r.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.store.HeartbeatAttempt(ctx, owner); err != nil {
				r.logger.Warnf("刷新 attempt %s 心跳失败: %v", owner.AttemptID, err)
			}
			if err := message.InProgress(); err != nil {
				r.logger.Warnf("延长 attempt %s JetStream ACK 等待失败: %v", owner.AttemptID, err)
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

// checkpointResumeSequence 同时兼容同一 Attempt 消息重放和新 Attempt 从上一恢复点继续。
// sequence 只增不减，避免恢复后覆盖旧的 typed Step 编号。
func checkpointResumeSequence(work *execution.Work) int32 {
	sequence := work.Attempt.StepCount
	if work.LatestCheckpoint != nil && work.LatestCheckpoint.Sequence > sequence {
		sequence = work.LatestCheckpoint.Sequence
	}
	return sequence
}

type checkpointWriter struct {
	store    execution.Store
	owner    execution.AttemptOwner
	sequence int32
}

func (w *checkpointWriter) Save(ctx context.Context, step string, state map[string]any, artifacts []string) error {
	w.sequence++
	return w.store.SaveCheckpoint(ctx, w.owner, execution.Checkpoint{
		ID: uuid.New(), AttemptID: w.owner.AttemptID, Sequence: w.sequence,
		StepName: step, State: state, ArtifactRefs: artifacts,
		FencingToken: w.owner.FencingToken, CreatedAt: time.Now().UTC(),
	})
}
