package orchestrator

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/domain"
	"github.com/licy-yu/agent-os/internal/execution"
)

// ReviewerController 只根据持久化证据裁决，不信任模型在自然语言中声称“已完成”。
type ReviewerController struct {
	store   execution.Store
	batch   int
	quality float64
	name    string
}

func NewReviewerController(store execution.Store) *ReviewerController {
	return &ReviewerController{store: store, batch: 100, quality: .8, name: "builtin-policy-reviewer/v1"}
}

func (c *ReviewerController) ReconcileOnce(ctx context.Context) (int, error) {
	works, err := c.store.ListReviewWork(ctx, c.batch)
	if err != nil {
		return 0, err
	}
	changed := 0
	for _, work := range works {
		evaluation := c.evaluate(work)
		retryAt := time.Now().UTC()
		if evaluation.Decision == execution.DecisionRetry {
			retryAt = retryAt.Add(reviewRetryDelay(work.Attempt.Number))
		}
		if err := c.store.ApplyReview(ctx, work, evaluation, retryAt); err != nil {
			if errors.Is(err, domain.ErrConflict) {
				continue
			}
			return changed, err
		}
		changed++
	}
	return changed, nil
}

func (c *ReviewerController) evaluate(work *execution.Work) execution.Evaluation {
	checks := boolMap(work.Attempt.OutputSnapshot["checks"])
	violations := stringSlice(work.Attempt.OutputSnapshot["policy_violations"])
	quality, _ := work.Attempt.OutputSnapshot["quality_score"].(float64)
	findings := make([]string, 0)
	machinePass := true
	require := func(required bool, name string) {
		if required && !checks[name] {
			machinePass = false
			findings = append(findings, "缺少或未通过机器检查: "+name)
		}
	}
	require(work.Task.Acceptance.Build, "build")
	require(work.Task.Acceptance.UnitTest, "unit_test")
	require(work.Task.Acceptance.SecurityReview, "security_review")
	for _, name := range work.Task.Acceptance.RequiredChecks {
		require(true, name)
	}
	if !checks["execution"] {
		machinePass = false
		findings = append(findings, "执行器未正常完成")
	}
	policyPass := len(violations) == 0
	for _, violation := range violations {
		findings = append(findings, "策略违规: "+violation)
	}
	qualityPass := quality >= c.quality
	if !qualityPass {
		findings = append(findings, "质量分低于阈值 0.8")
	}
	decision := execution.DecisionAccept
	if !machinePass || !policyPass || !qualityPass {
		decision = execution.DecisionRetry
		if work.Attempt.Number >= work.Task.ExecutionPolicy.MaxAttempts || !policyPass {
			decision = execution.DecisionReject
		}
	}
	return execution.Evaluation{
		ID: uuid.New(), TaskID: work.Task.ID, AttemptID: work.Attempt.ID, Reviewer: c.name,
		MachinePass: machinePass, PolicyPass: policyPass, QualityScore: quality,
		Decision: decision, Findings: findings, CreatedAt: time.Now().UTC(),
	}
}

func reviewRetryDelay(attempt int32) time.Duration {
	delay := time.Duration(math.Pow(2, float64(attempt-1))) * 5 * time.Second
	if delay > 5*time.Minute {
		return 5 * time.Minute
	}
	return delay
}

func boolMap(value any) map[string]bool {
	result := map[string]bool{}
	if values, ok := value.(map[string]any); ok {
		for key, raw := range values {
			result[key], _ = raw.(bool)
		}
	}
	return result
}

func stringSlice(value any) []string {
	result := []string{}
	if values, ok := value.([]any); ok {
		for _, raw := range values {
			if text, ok := raw.(string); ok {
				result = append(result, text)
			}
		}
	}
	return result
}

// RecoveryController 将超时阈值统一应用到所有 Worker；真正的 CAS/回收在 Store 事务中完成。
type RecoveryController struct {
	store   execution.Store
	timeout time.Duration
	batch   int
}

func NewRecoveryController(store execution.Store, timeout time.Duration) *RecoveryController {
	return &RecoveryController{store: store, timeout: timeout, batch: 100}
}

func (c *RecoveryController) ReconcileOnce(ctx context.Context) (int, error) {
	return c.store.RecoverTimedOut(ctx, time.Now().UTC().Add(-c.timeout), c.batch)
}
