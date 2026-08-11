// Package agentloop 把 Eino 的图编排能力限制在单个 TaskAttempt 内。
//
// SwarmOS 的 Run/Task/Attempt/Effect 状态仍由平台控制面持有；Eino 只负责一次
// Attempt 中的“恢复上下文 -> 执行 Agent -> 校验运行时保险丝”流程。这个边界可避免
// Agent 框架绕过平台的预算、Checkpoint、Tool Gateway 和验收状态机。
package agentloop

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/cloudwego/eino/compose"
	"github.com/licy-yu/agent-os/internal/execution"
)

var (
	// ErrGuardExceeded 表示 Attempt 命中了确定性的运行时上限。调用方应把它分类为
	// NO_PROGRESS 或 BUDGET_EXCEEDED，而不是无限重试同一个 Agent Loop。
	ErrGuardExceeded = errors.New("Agent Runtime Guard 已触发")
	// ErrInvalidWork 表示调度/持久化没有提供完整的 Attempt 快照。
	ErrInvalidWork = errors.New("非法 Agent Work")
)

// Executor 是生产 Worker 使用的 Eino 图执行器。compiled 在进程启动时构建一次；每次
// Invoke 的可变数据全部放在 frame 内，因此多个执行槽可以安全并发复用同一张图。
type Executor struct {
	compiled compose.Runnable[*frame, *frame]
	delegate execution.Executor
}

// frame 是图节点之间传递的单次执行上下文。它不是长期状态；真正需要恢复的数据必须
// 通过 CheckpointWriter 落库，Worker 重启后再由 Work.LatestCheckpoint 注入。
type frame struct {
	work        *execution.Work
	delegate    execution.Executor
	checkpoints execution.CheckpointWriter
	tools       execution.ToolCaller
	result      execution.ExecutionResult
}

// New 构建固定的 Eino DAG。delegate 可以是 OpenAI、私有模型或测试执行器，但无论是哪种
// 后端，都必须经过相同的恢复节点和 Runtime Guard 节点。
func New(delegate execution.Executor) (*Executor, error) {
	if delegate == nil {
		return nil, fmt.Errorf("%w: delegate 不能为空", ErrInvalidWork)
	}
	graph := compose.NewGraph[*frame, *frame]()
	if err := graph.AddLambdaNode("restore_context", compose.InvokableLambda(restoreContext)); err != nil {
		return nil, fmt.Errorf("注册 Eino restore_context 节点: %w", err)
	}
	if err := graph.AddLambdaNode("run_agent", compose.InvokableLambda(runAgent)); err != nil {
		return nil, fmt.Errorf("注册 Eino run_agent 节点: %w", err)
	}
	if err := graph.AddLambdaNode("enforce_guards", compose.InvokableLambda(enforceGuards)); err != nil {
		return nil, fmt.Errorf("注册 Eino enforce_guards 节点: %w", err)
	}
	for _, edge := range [][2]string{
		{compose.START, "restore_context"},
		{"restore_context", "run_agent"},
		{"run_agent", "enforce_guards"},
		{"enforce_guards", compose.END},
	} {
		if err := graph.AddEdge(edge[0], edge[1]); err != nil {
			return nil, fmt.Errorf("连接 Eino 节点 %s -> %s: %w", edge[0], edge[1], err)
		}
	}
	compiled, err := graph.Compile(context.Background(), compose.WithGraphName("swarmos-attempt-v1.5"))
	if err != nil {
		return nil, fmt.Errorf("编译 Eino Attempt Graph: %w", err)
	}
	return &Executor{compiled: compiled, delegate: delegate}, nil
}

// Execute 满足 execution.Executor。Graph 负责结构，guardedCheckpointWriter 和
// guardedToolCaller 则在真正的 I/O 边界计数，不能依赖模型自报“调用了几次工具”。
func (e *Executor) Execute(ctx context.Context, work *execution.Work,
	checkpoints execution.CheckpointWriter, tools execution.ToolCaller,
) (execution.ExecutionResult, error) {
	if e == nil || e.compiled == nil {
		return execution.ExecutionResult{}, errors.New("Eino Executor 未初始化")
	}
	if checkpoints == nil || tools == nil {
		return execution.ExecutionResult{}, fmt.Errorf("%w: CheckpointWriter/ToolCaller 不能为空", ErrInvalidWork)
	}
	guard := newGuard(work)
	guard.delegate = e.delegate
	out, err := e.compiled.Invoke(ctx, &frame{
		work: work, delegate: guard.wrapExecutor(),
		checkpoints: &guardedCheckpointWriter{next: checkpoints, guard: guard},
		tools:       &guardedToolCaller{next: tools, guard: guard},
	})
	if err != nil {
		return execution.ExecutionResult{}, err
	}
	return out.result, nil
}

// restoreContext 显式记录恢复来源。这里只保存元数据，不把整个历史 Prompt 重新塞给模型；
// LatestCheckpoint.State 已由持久化层提供，是唯一可信的恢复边界。
func restoreContext(ctx context.Context, value *frame) (*frame, error) {
	if value == nil || value.work == nil || value.work.Task == nil || value.work.Attempt == nil ||
		value.work.Agent == nil || value.work.Template == nil || value.delegate == nil {
		return nil, fmt.Errorf("%w: Task/Attempt/Agent/Template 快照不完整", ErrInvalidWork)
	}
	resume := map[string]any{
		"attemptId": value.work.Attempt.ID.String(),
		"resumed":   value.work.LatestCheckpoint != nil,
	}
	if value.work.LatestCheckpoint != nil {
		resume["checkpointId"] = value.work.LatestCheckpoint.ID.String()
		resume["checkpointSequence"] = value.work.LatestCheckpoint.Sequence
		resume["checkpointStep"] = value.work.LatestCheckpoint.StepName
	}
	if err := value.checkpoints.Save(ctx, "eino_restore_context", resume, nil); err != nil {
		return nil, fmt.Errorf("保存 Eino 恢复点: %w", err)
	}
	return value, nil
}

func runAgent(ctx context.Context, value *frame) (*frame, error) {
	result, err := value.delegate.Execute(ctx, value.work, value.checkpoints, value.tools)
	if err != nil {
		return nil, err
	}
	value.result = result
	return value, nil
}

// enforceGuards 对模型返回的真实用量做最终兜底。模型不能通过在 output 中声称“预算内”
// 来越过这里；超预算的候选结果不会进入 Reviewer 的可接受路径。
func enforceGuards(_ context.Context, value *frame) (*frame, error) {
	policy := value.work.Task.ExecutionPolicy
	usedTokens := value.result.TokensIn + value.result.TokensOut
	if policy.MaxTokens > 0 && usedTokens > policy.MaxTokens {
		return nil, fmt.Errorf("%w: token %d > %d", ErrGuardExceeded, usedTokens, policy.MaxTokens)
	}
	if len(value.result.Output) == 0 {
		return nil, fmt.Errorf("%w: Agent 没有产生候选输出", ErrGuardExceeded)
	}
	return value, nil
}

// guard 由真实 Checkpoint/Tool 边界驱动。mutex 使未来并行 Tool Node 仍共享同一预算。
type guard struct {
	mu                  sync.Mutex
	maxTurns            int32
	maxToolCalls        int32
	maxNoProgressRounds int32
	turns               int32
	toolCalls           int32
	noProgressRounds    int32
	lastStateHash       string
	delegate            execution.Executor
}

func newGuard(work *execution.Work) *guard {
	value := &guard{maxTurns: 64, maxToolCalls: 100, maxNoProgressRounds: 3}
	if work != nil && work.Task != nil {
		policy := work.Task.ExecutionPolicy
		// 旧 Task Contract 还没有独立 MaxTurns，使用工具上限和无进展上限组合出保守值。
		if policy.MaxToolCalls >= 0 {
			value.maxToolCalls = policy.MaxToolCalls
		}
		if policy.MaxNoProgressRounds > 0 {
			value.maxNoProgressRounds = policy.MaxNoProgressRounds
		}
		if policy.MaxToolCalls > 0 && policy.MaxToolCalls < value.maxTurns {
			value.maxTurns = policy.MaxToolCalls + value.maxNoProgressRounds + 4
		}
	}
	return value
}

func (g *guard) wrapExecutor() execution.Executor {
	return executorWithGuard{next: g.delegate, guard: g}
}

type executorWithGuard struct {
	next  execution.Executor
	guard *guard
}

func (e executorWithGuard) Execute(ctx context.Context, work *execution.Work,
	checkpoints execution.CheckpointWriter, tools execution.ToolCaller,
) (execution.ExecutionResult, error) {
	return e.next.Execute(ctx, work, checkpoints, tools)
}

type guardedCheckpointWriter struct {
	next  execution.CheckpointWriter
	guard *guard
}

func (w *guardedCheckpointWriter) Save(ctx context.Context, step string, state map[string]any, artifacts []string) error {
	digest, err := stableHash(state)
	if err != nil {
		return fmt.Errorf("Checkpoint 状态不能序列化: %w", err)
	}
	w.guard.mu.Lock()
	w.guard.turns++
	if digest != "" && digest == w.guard.lastStateHash {
		w.guard.noProgressRounds++
	} else {
		w.guard.noProgressRounds = 0
		w.guard.lastStateHash = digest
	}
	turns, noProgress := w.guard.turns, w.guard.noProgressRounds
	maxTurns, maxNoProgress := w.guard.maxTurns, w.guard.maxNoProgressRounds
	w.guard.mu.Unlock()
	if turns > maxTurns {
		return fmt.Errorf("%w: turn %d > %d", ErrGuardExceeded, turns, maxTurns)
	}
	if maxNoProgress > 0 && noProgress >= maxNoProgress {
		return fmt.Errorf("%w: 连续 %d 轮状态没有进展", ErrGuardExceeded, noProgress)
	}
	return w.next.Save(ctx, strings.TrimSpace(step), state, artifacts)
}

type guardedToolCaller struct {
	next  execution.ToolCaller
	guard *guard
}

func (t *guardedToolCaller) Call(ctx context.Context, name string, input map[string]any) (map[string]any, error) {
	t.guard.mu.Lock()
	t.guard.toolCalls++
	calls, limit := t.guard.toolCalls, t.guard.maxToolCalls
	t.guard.mu.Unlock()
	if limit >= 0 && calls > limit {
		return nil, fmt.Errorf("%w: tool call %d > %d", ErrGuardExceeded, calls, limit)
	}
	return t.next.Call(ctx, name, input)
}

func stableHash(value map[string]any) (string, error) {
	if value == nil {
		value = map[string]any{}
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}
