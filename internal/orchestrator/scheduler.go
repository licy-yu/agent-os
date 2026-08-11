package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/domain"
	"github.com/licy-yu/agent-os/internal/domain/agent"
	"github.com/licy-yu/agent-os/internal/domain/task"
	"github.com/licy-yu/agent-os/internal/lease"
)

// ScoreWeights 与规划中的第一版权重一致，且总和必须为 1。
type ScoreWeights struct {
	SkillMatch      float64
	HistorySuccess  float64
	ContextAffinity float64
	Quality         float64
	Load            float64
	Cost            float64
	Latency         float64
}

var defaultWeights = ScoreWeights{
	SkillMatch: 0.30, HistorySuccess: 0.20, ContextAffinity: 0.15,
	Quality: 0.15, Load: 0.10, Cost: 0.05, Latency: 0.05,
}

// schedulerCompensationTimeout 独立限制调度补偿写的最长时间。补偿使用 WithoutCancel
// 脱离原请求取消信号：例如 Redis Reserve 因上游 context 取消而返回错误时，仍要给
// PostgreSQL 一小段确定的时间把已经声明的 SCHEDULING 恢复成 READY。
const schedulerCompensationTimeout = 5 * time.Second

// Scheduler 执行 QueueSort -> Filter -> Score -> Reserve -> Bind。
type Scheduler struct {
	store       Store
	leases      lease.Manager
	leaseTTL    time.Duration
	batch       int
	clock       Clock
	logger      *log.Helper
	schedulerID string
}

func NewScheduler(store Store, leases lease.Manager, leaseTTL time.Duration, logger log.Logger) *Scheduler {
	return &Scheduler{
		store: store, leases: leases, leaseTTL: leaseTTL, batch: 50,
		clock:       func() time.Time { return time.Now().UTC() },
		logger:      log.NewHelper(log.With(logger, "component", "scheduler")),
		schedulerID: schedulerIdentity(),
	}
}

// ScheduleOnce 尝试绑定一个任务。返回 true 表示完成了一次 Bind。
func (s *Scheduler) ScheduleOnce(ctx context.Context) (bool, error) {
	queued, err := s.store.ListQueuedTasks(ctx, s.batch)
	if err != nil {
		return false, fmt.Errorf("读取 READY 队列: %w", err)
	}
	SortQueue(queued, s.clock())

	// taskLoop 标签用于处理多副本 Scheduler 的正常 CAS 竞争：当前快照一旦过期，必须
	// 立即释放已经取得的 Redis 短租约并处理下一项，不能继续拿旧快照尝试其他 Agent。
taskLoop:
	for _, item := range queued {
		value := item.Task

		// 候选查询、硬过滤、评分和 Redis Reserve 都是“观察/短租约”操作。此时 Task
		// 必须继续保持 READY：没有 Agent 或所有租约都被占用并不是领域状态变化，如果
		// 先写成 SCHEDULING 再恢复 READY，1 秒一次的调度循环会无限增加 version、
		// Timeline 和 Outbox。只有真正拿到某个 Agent 的短租约后，才进入持久化状态机。
		candidates, err := s.store.ListSchedulerCandidates(ctx, value.SwarmID, value.ExecutionPolicy.MaxTokens)
		if err != nil {
			return false, fmt.Errorf("查询任务 %s 候选 Agent: %w", value.ID, err)
		}
		candidates, explanation := ExplainCandidates(value, candidates)
		ScoreCandidates(value, candidates, defaultWeights)
		applyCandidateScores(explanation, candidates)
		schedulingClaimed := false
		for index := range candidates {
			candidate := &candidates[index]
			reservation, ok, reserveErr := s.leases.Reserve(ctx, candidate.Instance.ID, value.ID, s.leaseTTL)
			if reserveErr != nil {
				cause := fmt.Errorf("为任务 %s Reserve Agent %s: %w", value.ID, candidate.Instance.ID, reserveErr)
				if schedulingClaimed {
					return false, s.compensateSchedulingFailure(ctx, value, cause)
				}
				return false, cause
			}
			if !ok {
				continue
			}

			if !schedulingClaimed {
				// Redis Lease 只减少竞争，数据库 CAS 才是 Task 所有权的最终事实。两个
				// Scheduler 可能分别租到不同 Agent，但只有一个能把同一 READY 版本推进
				// 到 SCHEDULING；失败者释放自己的 Lease 即可，不产生补偿状态写入。
				if _, err := transition(ctx, s.store, value, task.ActorScheduler, task.StatusScheduling); err != nil {
					releaseErr := s.leases.Release(ctx, reservation)
					if releaseErr != nil {
						s.logger.Warnf("任务 %s 的调度 CAS 失败，且释放 Agent %s 短租约失败: %v",
							value.ID, candidate.Instance.ID, releaseErr)
					}
					if errors.Is(err, domain.ErrConflict) {
						continue taskLoop
					}
					return false, err
				}
				schedulingClaimed = true
			}

			selectedID, selectedScore := candidate.Instance.ID, candidate.FinalScore
			if err := s.store.RecordSchedulerDecision(ctx, SchedulerDecision{
				SchedulerID: s.schedulerID, TaskID: value.ID, TaskVersion: value.Version,
				QueueScore: item.QueueScore, SelectedAgentID: &selectedID, SelectedScore: &selectedScore,
				Candidates: explanation, Reason: "候选通过硬过滤与评分，并成功取得 Reserve 租约",
			}); err != nil {
				if releaseErr := s.leases.Release(ctx, reservation); releaseErr != nil {
					s.logger.Warnf("任务 %s 写入 Explain 失败，且释放 Agent %s 短租约失败: %v",
						value.ID, candidate.Instance.ID, releaseErr)
				}
				cause := fmt.Errorf("记录 Scheduler Explain: %w", err)
				return false, s.compensateSchedulingFailure(ctx, value, cause)
			}
			bindErr := s.store.BindTask(ctx, value.ID, value.Version, candidate.Instance.ID, candidate.Instance.Version)
			releaseErr := s.leases.Release(ctx, reservation)
			if bindErr == nil {
				if releaseErr != nil {
					s.logger.Warnf("任务 %s 已绑定，但释放 Agent %s 短租约失败: %v", value.ID, candidate.Instance.ID, releaseErr)
				}
				return true, nil
			}
			if !errors.Is(bindErr, domain.ErrConflict) {
				if releaseErr != nil {
					s.logger.Warnf("任务 %s Bind 失败，且释放 Agent %s 短租约失败: %v",
						value.ID, candidate.Instance.ID, releaseErr)
				}
				cause := fmt.Errorf("绑定 task/agent: %w", bindErr)
				return false, s.compensateSchedulingFailure(ctx, value, cause)
			}
			if releaseErr != nil {
				s.logger.Warnf("任务 %s Bind CAS 竞争失败，且释放 Agent %s 短租约失败: %v",
					value.ID, candidate.Instance.ID, releaseErr)
			}
		}
		reason := "候选通过过滤，但 Reserve 租约均被其他调度器占用"
		if len(explanation) == 0 {
			reason = "当前没有已注册的 Agent 候选"
		} else if len(candidates) == 0 {
			reason = "所有候选均未通过 Task Contract 硬条件"
		} else if schedulingClaimed {
			reason = "候选取得租约，但 Bind CAS 竞争失败"
		}
		if err := s.store.RecordSchedulerDecision(ctx, SchedulerDecision{
			SchedulerID: s.schedulerID, TaskID: value.ID, TaskVersion: value.Version,
			QueueScore: item.QueueScore, Candidates: explanation, Reason: reason,
		}); err != nil {
			if schedulingClaimed {
				return false, s.compensateSchedulingFailure(ctx, value,
					fmt.Errorf("记录未调度原因: %w", err))
			}
			return false, fmt.Errorf("记录未调度原因: %w", err)
		}

		// 只有已经成功声明 SCHEDULING、随后却在 Bind CAS 中失利的任务才需要补偿回
		// READY。单纯无候选/租约占用时从未离开 READY，因此这里绝不能做“恢复”写入。
		if schedulingClaimed {
			if err := s.restoreClaimedTask(ctx, value); err != nil {
				return false, err
			}
		}
	}
	return false, nil
}

func (s *Scheduler) restoreReady(ctx context.Context, value *task.Task) error {
	_, err := transition(ctx, s.store, value, task.ActorScheduler, task.StatusReady)
	return err
}

// compensateSchedulingFailure 是所有“已声明 SCHEDULING 后异常退出”路径的统一出口。
// 原始错误决定本轮为何失败，恢复错误决定 Task 是否可能滞留；二者都对运维有意义，
// 因此补偿失败时用 errors.Join 同时返回，绝不再用 `_ = restoreReady(...)` 静默吞错。
func (s *Scheduler) compensateSchedulingFailure(ctx context.Context, value *task.Task, cause error) error {
	if err := s.restoreClaimedTask(ctx, value); err != nil {
		s.logger.Errorf("调度失败后的状态补偿也失败: cause=%v compensation=%v", cause, err)
		return errors.Join(cause, err)
	}
	return cause
}

// restoreClaimedTask 给所有 SCHEDULING 补偿统一提供脱离上游取消信号的短超时上下文。
// 这样异常分支与“所有 Bind CAS 均失败”的正常收尾使用完全相同的恢复可靠性约束。
func (s *Scheduler) restoreClaimedTask(ctx context.Context, value *task.Task) error {
	restoreCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), schedulerCompensationTimeout)
	defer cancel()
	if err := s.restoreReady(restoreCtx, value); err != nil {
		return fmt.Errorf("任务 %s 从 SCHEDULING 补偿恢复 READY: %w", value.ID, err)
	}
	return nil
}

// SortQueue 根据业务优先级、等待时间、阻塞下游数量和 deadline 紧迫度排序。
func SortQueue(items []QueuedTask, now time.Time) {
	for index := range items {
		value := &items[index]
		waitMinutes := math.Max(0, now.Sub(value.Task.CreatedAt).Minutes())
		waitScore := math.Min(waitMinutes*0.1, 30)
		downstreamScore := float64(value.BlockedChildren) * 5
		deadlineScore := 0.0
		if value.Task.Deadline != nil {
			remaining := value.Task.Deadline.Sub(now)
			if remaining <= 0 {
				deadlineScore = 100
			} else if remaining < time.Hour {
				deadlineScore = 100 * (1 - remaining.Hours())
			}
		}
		value.QueueScore = float64(value.Task.Priority) + waitScore + downstreamScore + deadlineScore
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].QueueScore == items[j].QueueScore {
			return items[i].Task.CreatedAt.Before(items[j].Task.CreatedAt)
		}
		return items[i].QueueScore > items[j].QueueScore
	})
}

// FilterCandidates 只做硬条件判断，不满足任一条件的 Agent 不进入评分阶段。
func FilterCandidates(value *task.Task, candidates []Candidate) []Candidate {
	result, _ := ExplainCandidates(value, candidates)
	return result
}

// ExplainCandidates 与 FilterCandidates 使用同一套谓词，同时保留每个拒绝原因，避免
// 控制台根据最终列表反向猜测调度决策。
func ExplainCandidates(value *task.Task, candidates []Candidate) ([]Candidate, []CandidateDecision) {
	result := make([]Candidate, 0, len(candidates))
	explanation := make([]CandidateDecision, 0, len(candidates))
	for _, candidate := range candidates {
		decision := CandidateDecision{Reasons: []string{}}
		if candidate.Instance == nil || candidate.Template == nil {
			decision.Reasons = append(decision.Reasons, "候选快照不完整")
			explanation = append(explanation, decision)
			continue
		}
		decision.AgentID, decision.TemplateID = candidate.Instance.ID, candidate.Template.ID
		if candidate.Instance.Status != agent.StatusIdle {
			decision.Reasons = append(decision.Reasons, "Agent 不是 IDLE")
		}
		if !candidate.Template.Enabled {
			decision.Reasons = append(decision.Reasons, "AgentTemplate 已禁用")
		}
		if !candidate.BudgetAllowed {
			decision.Reasons = append(decision.Reasons, "Run 剩余预算不足")
		}
		if !skillsMatch(value.Requirements.Skills, candidate.Template.Skills) {
			decision.Reasons = append(decision.Reasons, "能力等级不满足 requirements.skills")
		}
		if !containsAll(candidate.Template.Tools, value.Requirements.Tools) {
			decision.Reasons = append(decision.Reasons, "缺少必需工具")
		}
		if !containsAll(candidate.Template.Permissions, value.Requirements.Permissions) {
			decision.Reasons = append(decision.Reasons, "权限交集不满足 Task Contract")
		}
		if len(value.Requirements.Models) > 0 && !contains(value.Requirements.Models, candidate.Template.Model) {
			decision.Reasons = append(decision.Reasons, "模型不在允许集合")
		}
		if value.Requirements.MaxContextTokens > 0 && candidate.Template.ContextWindow < value.Requirements.MaxContextTokens {
			decision.Reasons = append(decision.Reasons, "上下文窗口不足")
		}
		if !riskZoneAllows(candidate.Template.RiskZone, value.Requirements.RiskZone) {
			decision.Reasons = append(decision.Reasons, "风险区等级不足")
		}
		decision.Accepted = len(decision.Reasons) == 0
		explanation = append(explanation, decision)
		if decision.Accepted {
			result = append(result, candidate)
		}
	}
	return result, explanation
}

func applyCandidateScores(decisions []CandidateDecision, candidates []Candidate) {
	scores := make(map[uuid.UUID]float64, len(candidates))
	for _, candidate := range candidates {
		if candidate.Instance != nil {
			scores[candidate.Instance.ID] = candidate.FinalScore
		}
	}
	for index := range decisions {
		if decisions[index].Accepted {
			decisions[index].Score = scores[decisions[index].AgentID]
		}
	}
}

func schedulerIdentity() string {
	value, err := os.Hostname()
	if err != nil || strings.TrimSpace(value) == "" {
		return "swarmos-scheduler"
	}
	return value + "/swarmos-scheduler"
}

// ScoreCandidates 计算 0~100 分并按分数、UUID 稳定排序。
func ScoreCandidates(value *task.Task, candidates []Candidate, weights ScoreWeights) {
	for index := range candidates {
		candidate := &candidates[index]
		skill := skillScore(value.Requirements.Skills, candidate.Template.Skills)
		loadScore := clamp01(1 - candidate.Instance.Load)
		costScore := 1.0
		if candidate.Template.CostPer1KTokensMicros > 0 {
			costScore = 1 / (1 + float64(candidate.Template.CostPer1KTokensMicros)/10_000)
		}
		candidate.FinalScore = 100 * (skill*weights.SkillMatch +
			clamp01(candidate.HistorySuccess)*weights.HistorySuccess +
			clamp01(candidate.ContextAffinity)*weights.ContextAffinity +
			clamp01(candidate.QualityScore)*weights.Quality +
			loadScore*weights.Load +
			costScore*weights.Cost +
			clamp01(candidate.LatencyScore)*weights.Latency)
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].FinalScore == candidates[j].FinalScore {
			return candidates[i].Instance.ID.String() < candidates[j].Instance.ID.String()
		}
		return candidates[i].FinalScore > candidates[j].FinalScore
	})
}

func skillsMatch(required, actual map[string]float64) bool {
	for name, minimum := range required {
		if actual[name] < minimum {
			return false
		}
	}
	return true
}

func skillScore(required, actual map[string]float64) float64 {
	if len(required) == 0 {
		return 1
	}
	total := 0.0
	for name, minimum := range required {
		if minimum <= 0 {
			total += 1
			continue
		}
		total += math.Min(actual[name]/minimum, 1)
	}
	return total / float64(len(required))
}

func containsAll(actual, required []string) bool {
	set := make(map[string]struct{}, len(actual))
	for _, value := range actual {
		set[strings.ToLower(value)] = struct{}{}
	}
	for _, value := range required {
		if _, ok := set[strings.ToLower(value)]; !ok {
			return false
		}
	}
	return true
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if strings.EqualFold(value, target) {
			return true
		}
	}
	return false
}

func riskZoneAllows(actual, required string) bool {
	if required == "" {
		return true
	}
	ranks := map[string]int{"read-only": 0, "sandbox": 1, "trusted": 2, "production": 3}
	actualRank, actualKnown := ranks[strings.ToLower(actual)]
	requiredRank, requiredKnown := ranks[strings.ToLower(required)]
	if !actualKnown || !requiredKnown {
		return strings.EqualFold(actual, required)
	}
	return actualRank >= requiredRank
}

func clamp01(value float64) float64 {
	return math.Max(0, math.Min(1, value))
}
