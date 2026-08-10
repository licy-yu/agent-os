package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/licy-yu/agent-os/internal/domain"
	"github.com/licy-yu/agent-os/internal/domain/task"
)

// TaskController 观察数据库中的非终态 Task，并把实际状态拉向期望状态。
type TaskController struct {
	store  Store
	batch  int
	logger *log.Helper
}

func NewTaskController(store Store, logger log.Logger) *TaskController {
	return &TaskController{
		store: store, batch: 100,
		logger: log.NewHelper(log.With(logger, "component", "task-controller")),
	}
}

// ReconcileOnce 每次只推进一个合法状态边；这样每次变更都有独立版本和事件，便于审计与恢复。
func (c *TaskController) ReconcileOnce(ctx context.Context) (int, error) {
	items, err := c.store.ListReconcileTasks(ctx, c.batch)
	if err != nil {
		return 0, fmt.Errorf("列出待协调任务: %w", err)
	}
	changed := 0
	for _, value := range items {
		var actor task.Actor
		var next task.Status
		switch value.Status {
		case task.StatusCreated:
			actor, next = task.ActorPlanner, task.StatusPlanning
		case task.StatusPlanning:
			ready, checkErr := c.store.DependenciesSatisfied(ctx, value.ID)
			if checkErr != nil {
				return changed, fmt.Errorf("检查任务 %s 依赖: %w", value.ID, checkErr)
			}
			actor = task.ActorPlanner
			if ready {
				next = task.StatusReady
			} else {
				next = task.StatusBlocked
			}
		case task.StatusBlocked:
			ready, checkErr := c.store.DependenciesSatisfied(ctx, value.ID)
			if checkErr != nil {
				return changed, fmt.Errorf("检查任务 %s 依赖: %w", value.ID, checkErr)
			}
			if !ready {
				continue
			}
			actor, next = task.ActorController, task.StatusReady
		case task.StatusRetryWait:
			// available_at 是 Reviewer/Recovery 计算好的退避边界。
			if value.AvailableAt.After(c.now()) {
				continue
			}
			actor, next = task.ActorController, task.StatusReady
		default:
			continue
		}

		if _, err := transition(ctx, c.store, value, actor, next); err != nil {
			if errors.Is(err, domain.ErrConflict) {
				// 另一个控制面已推进该版本，下一轮读取新状态即可。
				continue
			}
			return changed, err
		}
		changed++
	}
	return changed, nil
}

// now 单独保留方法，后续可直接替换为注入时钟；当前统一使用 UTC。
func (c *TaskController) now() time.Time { return time.Now().UTC() }

// AgentController 将完成注册的实例推进到 IDLE，交给 Scheduler 使用。
type AgentController struct {
	store Store
	batch int
}

func NewAgentController(store Store) *AgentController {
	return &AgentController{store: store, batch: 100}
}

func (c *AgentController) ReconcileOnce(ctx context.Context) (int, error) {
	return c.store.ActivateRegisteredAgents(ctx, c.batch)
}

func transition(ctx context.Context, store Store, value *task.Task, actor task.Actor, next task.Status) (int64, error) {
	copyValue := *value
	if err := copyValue.Transition(actor, next); err != nil {
		return 0, err
	}
	version, err := store.TransitionTask(ctx, value.ID, value.Version, value.Status, next, "task."+statusEvent(next))
	if err != nil {
		return 0, err
	}
	value.Status = next
	value.Version = version
	return version, nil
}

func statusEvent(status task.Status) string {
	switch status {
	case task.StatusPlanning:
		return "planning"
	case task.StatusBlocked:
		return "blocked"
	case task.StatusReady:
		return "ready"
	case task.StatusScheduling:
		return "scheduling"
	case task.StatusAssigned:
		return "assigned"
	default:
		return string(status)
	}
}
