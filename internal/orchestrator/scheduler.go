package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v2/log"
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

// Scheduler 执行 QueueSort -> Filter -> Score -> Reserve -> Bind。
type Scheduler struct {
	store    Store
	leases   lease.Manager
	leaseTTL time.Duration
	batch    int
	clock    Clock
	logger   *log.Helper
}

func NewScheduler(store Store, leases lease.Manager, leaseTTL time.Duration, logger log.Logger) *Scheduler {
	return &Scheduler{
		store: store, leases: leases, leaseTTL: leaseTTL, batch: 50,
		clock:  func() time.Time { return time.Now().UTC() },
		logger: log.NewHelper(log.With(logger, "component", "scheduler")),
	}
}

// ScheduleOnce 尝试绑定一个任务。返回 true 表示完成了一次 Bind。
func (s *Scheduler) ScheduleOnce(ctx context.Context) (bool, error) {
	queued, err := s.store.ListQueuedTasks(ctx, s.batch)
	if err != nil {
		return false, fmt.Errorf("读取 READY 队列: %w", err)
	}
	SortQueue(queued, s.clock())
	for _, item := range queued {
		value := item.Task
		if _, err := transition(ctx, s.store, value, task.ActorScheduler, task.StatusScheduling); err != nil {
			if errors.Is(err, domain.ErrConflict) {
				continue
			}
			return false, err
		}

		candidates, err := s.store.ListSchedulerCandidates(ctx, value.SwarmID, value.ExecutionPolicy.MaxTokens)
		if err != nil {
			_ = s.restoreReady(ctx, value)
			return false, fmt.Errorf("查询任务 %s 候选 Agent: %w", value.ID, err)
		}
		candidates = FilterCandidates(value, candidates)
		ScoreCandidates(value, candidates, defaultWeights)
		for index := range candidates {
			candidate := &candidates[index]
			reservation, ok, reserveErr := s.leases.Reserve(ctx, candidate.Instance.ID, value.ID, s.leaseTTL)
			if reserveErr != nil {
				_ = s.restoreReady(ctx, value)
				return false, reserveErr
			}
			if !ok {
				continue
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
				_ = s.restoreReady(ctx, value)
				return false, fmt.Errorf("绑定 task/agent: %w", bindErr)
			}
		}

		// 没有可用 Agent 时恢复 READY；它会在下一轮等待新心跳或实例释放。
		if err := s.restoreReady(ctx, value); err != nil && !errors.Is(err, domain.ErrConflict) {
			return false, err
		}
	}
	return false, nil
}

func (s *Scheduler) restoreReady(ctx context.Context, value *task.Task) error {
	_, err := transition(ctx, s.store, value, task.ActorScheduler, task.StatusReady)
	return err
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
	result := make([]Candidate, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.Instance.Status != agent.StatusIdle || !candidate.Template.Enabled || !candidate.BudgetAllowed {
			continue
		}
		if !skillsMatch(value.Requirements.Skills, candidate.Template.Skills) ||
			!containsAll(candidate.Template.Tools, value.Requirements.Tools) ||
			!containsAll(candidate.Template.Permissions, value.Requirements.Permissions) {
			continue
		}
		if len(value.Requirements.Models) > 0 && !contains(value.Requirements.Models, candidate.Template.Model) {
			continue
		}
		if value.Requirements.MaxContextTokens > 0 && candidate.Template.ContextWindow < value.Requirements.MaxContextTokens {
			continue
		}
		if !riskZoneAllows(candidate.Template.RiskZone, value.Requirements.RiskZone) {
			continue
		}
		result = append(result, candidate)
	}
	return result
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
