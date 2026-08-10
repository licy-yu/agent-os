package orchestrator

import (
	"context"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/licy-yu/agent-os/internal/event"
)

// Runtime 把 Controller、Scheduler 和 Outbox Dispatcher 作为一个 Kratos 后台 Server 运行。
// Start 会阻塞到上下文取消，Stop 会等待所有循环退出，满足 Kratos 的优雅关闭约定。
type Runtime struct {
	tasks      *TaskController
	agents     *AgentController
	scheduler  *Scheduler
	reviewer   *ReviewerController
	recovery   *RecoveryController
	dispatcher *event.Dispatcher
	interval   time.Duration
	logger     *log.Helper

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

func NewRuntime(tasks *TaskController, agents *AgentController, scheduler *Scheduler,
	reviewer *ReviewerController, recovery *RecoveryController, dispatcher *event.Dispatcher,
	interval time.Duration, logger log.Logger,
) *Runtime {
	return &Runtime{
		tasks: tasks, agents: agents, scheduler: scheduler, reviewer: reviewer,
		recovery: recovery, dispatcher: dispatcher,
		interval: interval, logger: log.NewHelper(log.With(logger, "component", "orchestrator-runtime")),
	}
}

// Start 启动四条相互独立的控制循环。单次数据库/网络错误只记录并在下一轮重试，
// 不会让整个 API 进程因为暂时性故障退出。
func (r *Runtime) Start(parent context.Context) error {
	ctx, cancel := context.WithCancel(parent)
	r.mu.Lock()
	r.cancel = cancel
	r.done = make(chan struct{})
	done := r.done
	r.mu.Unlock()

	var workers sync.WaitGroup
	workers.Add(6)
	go func() {
		defer workers.Done()
		r.runTaskController(ctx)
	}()
	go func() {
		defer workers.Done()
		r.runAgentController(ctx)
	}()
	go func() {
		defer workers.Done()
		r.runScheduler(ctx)
	}()
	go func() {
		defer workers.Done()
		r.runReviewer(ctx)
	}()
	go func() {
		defer workers.Done()
		r.runRecovery(ctx)
	}()
	go func() {
		defer workers.Done()
		r.dispatcher.Run(ctx)
	}()

	<-ctx.Done()
	workers.Wait()
	close(done)
	return nil
}

func (r *Runtime) runReviewer(ctx context.Context) {
	r.runPeriodic(ctx, r.interval, func() (bool, error) {
		changed, err := r.reviewer.ReconcileOnce(ctx)
		return changed > 0, err
	}, "ReviewerController")
}

func (r *Runtime) runRecovery(ctx context.Context) {
	r.runPeriodic(ctx, r.interval, func() (bool, error) {
		changed, err := r.recovery.ReconcileOnce(ctx)
		return changed > 0, err
	}, "RecoveryController")
}

// Stop 请求后台循环退出并尊重 Kratos 的停止超时。
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

func (r *Runtime) runTaskController(ctx context.Context) {
	r.runPeriodic(ctx, r.interval, func() (bool, error) {
		changed, err := r.tasks.ReconcileOnce(ctx)
		return changed > 0, err
	}, "TaskController")
}

func (r *Runtime) runAgentController(ctx context.Context) {
	r.runPeriodic(ctx, r.interval, func() (bool, error) {
		changed, err := r.agents.ReconcileOnce(ctx)
		return changed > 0, err
	}, "AgentController")
}

func (r *Runtime) runScheduler(ctx context.Context) {
	interval := r.interval
	if interval > time.Second {
		interval = time.Second
	}
	r.runPeriodic(ctx, interval, func() (bool, error) {
		return r.scheduler.ScheduleOnce(ctx)
	}, "Scheduler")
}

// runPeriodic 在发生实际进展时立即运行下一轮，空闲时才等待 interval。
func (r *Runtime) runPeriodic(ctx context.Context, interval time.Duration, action func() (bool, error), name string) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		progress, err := action()
		if err != nil {
			r.logger.Errorf("%s 单轮执行失败: %v", name, err)
		}
		delay := interval
		if progress && err == nil {
			delay = 0
		}
		timer.Reset(delay)
	}
}
