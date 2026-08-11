package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/effect"
	"github.com/licy-yu/agent-os/internal/execution"
	"github.com/licy-yu/agent-os/internal/interaction"
	"github.com/licy-yu/agent-os/internal/safetycontrol"
	"github.com/stretchr/testify/require"
)

// TestInteractionExpiryPostgresClosure 是显式启用的真实 PostgreSQL 合同测试。它验证
// 自动过期不是单独改一列，而是将审批、Effect、ToolCall、Attempt、Task、Agent、Run
// 和 Outbox 一次提交。只能把 SWARMOS_INTEGRATION_DATABASE_URL 指向可丢弃测试库。
func TestInteractionExpiryPostgresClosure(t *testing.T) {
	dsn := os.Getenv("SWARMOS_INTEGRATION_DATABASE_URL")
	if dsn == "" {
		t.Skip("未设置 SWARMOS_INTEGRATION_DATABASE_URL，跳过 Interaction 过期合同测试")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	repository, err := New(ctx, dsn)
	require.NoError(t, err)
	defer repository.Close()
	if os.Getenv("SWARMOS_INTEGRATION_MIGRATE") == "1" {
		require.NoError(t, repository.Migrate(ctx))
	}

	fixture := seedHITLFixture(t, ctx, repository, "expire")
	defer cleanupHITLFixture(t, repository, fixture)
	owner := execution.AttemptOwner{
		AttemptID: fixture.attemptID, WorkerID: "worker-before-approval", FencingToken: 1,
	}
	require.NoError(t, repository.SuspendAttempt(ctx, owner, execution.AttemptWait{
		Reason: execution.WaitForApproval, EffectID: fixture.effectID,
	}))

	toolCallID := uuid.New()
	deadline := time.Now().UTC().Add(-time.Minute)
	_, err = repository.pool.Exec(ctx, `
		INSERT INTO tool_calls(
			id,tenant_id,attempt_id,task_id,agent_id,tool_name,arguments,status,risk_level,started_at
		) VALUES($1,$2,$3,$4,$5,'integration.deploy','{}'::jsonb,'STARTED','production',$6)`,
		toolCallID, fixture.tenantID, fixture.attemptID, fixture.taskID, fixture.agentID,
		deadline.Add(-time.Minute))
	require.NoError(t, err)
	_, err = repository.pool.Exec(ctx, `
		UPDATE effects SET tool_call_id=$3 WHERE tenant_id=$1 AND id=$2`,
		fixture.tenantID, fixture.effectID, toolCallID)
	require.NoError(t, err)
	_, err = repository.pool.Exec(ctx, `
		UPDATE interactions SET expires_at=$3 WHERE tenant_id=$1 AND id=$2`,
		fixture.tenantID, fixture.interactionID, deadline)
	require.NoError(t, err)

	expiredAt := deadline.Add(time.Second)
	changed, err := repository.ExpireWaitingInteractions(ctx, expiredAt, 100)
	require.NoError(t, err)
	require.Equal(t, 1, changed)

	var interactionStatus, effectStatus, toolStatus string
	var attemptStatus, taskStatus, runStatus, agentStatus string
	var resolvedBy, effectErrorCode string
	var sanitized []byte
	err = repository.pool.QueryRow(ctx, `
		SELECT x.status,e.status,tc.status,a.status,t.status,r.status,ai.status,
		       COALESCE(x.resolved_by,''),COALESCE(e.error_code,''),e.sanitized_result
		FROM interactions x
		JOIN effects e ON e.tenant_id=x.tenant_id AND e.id=x.effect_id
		JOIN tool_calls tc ON tc.tenant_id=x.tenant_id AND tc.id=e.tool_call_id
		JOIN task_attempts a ON a.tenant_id=x.tenant_id AND a.id=x.attempt_id
		JOIN tasks t ON t.tenant_id=x.tenant_id AND t.id=x.task_id
		JOIN swarms r ON r.tenant_id=x.tenant_id AND r.id=x.run_id
		JOIN agent_instances ai ON ai.tenant_id=x.tenant_id AND ai.id=a.agent_id
		WHERE x.tenant_id=$1 AND x.id=$2`, fixture.tenantID, fixture.interactionID).Scan(
		&interactionStatus, &effectStatus, &toolStatus, &attemptStatus, &taskStatus,
		&runStatus, &agentStatus, &resolvedBy, &effectErrorCode, &sanitized,
	)
	require.NoError(t, err)
	require.Equal(t, "EXPIRED", interactionStatus)
	require.Equal(t, string(effect.StatusPrepared), effectStatus)
	require.Equal(t, "DENIED", toolStatus)
	require.Equal(t, "ABORTED", attemptStatus)
	require.Equal(t, "FAILED", taskStatus)
	require.Equal(t, "FAILED", runStatus)
	require.Equal(t, "IDLE", agentStatus)
	require.Equal(t, interactionExpiryActor, resolvedBy)
	require.Equal(t, "APPROVAL_EXPIRED", effectErrorCode)
	var result map[string]any
	require.NoError(t, json.Unmarshal(sanitized, &result))
	authorization := result["_authorization"].(map[string]any)
	require.Equal(t, true, authorization["blocked"])
	require.Contains(t, authorization["reason"], "24 小时")

	// 七类租户事件覆盖整个安全闭环；任何一类写入失败都会回滚上述状态。
	var eventCount int
	require.NoError(t, repository.pool.QueryRow(ctx, `
		SELECT count(*) FROM event_outbox
		WHERE tenant_id=$1 AND event_type=ANY($2)`, fixture.tenantID, []string{
		"interaction.expired", "effect.authorization_expired", "tool_call.denied",
		"attempt.aborted", "task.failed", "agent.idle", "run.failed",
	}).Scan(&eventCount))
	require.Equal(t, 7, eventCount)

	// 第二轮扫描及迟到审批都不能复活已经封存的 Effect。
	changed, err = repository.ExpireWaitingInteractions(ctx, expiredAt.Add(time.Hour), 100)
	require.NoError(t, err)
	require.Zero(t, changed)
	_, err = safetycontrol.NewService(repository).Approve(
		hitlPrincipal(ctx, fixture.tenantID), fixture.interactionID, fixture.mutationKey+"-late",
		safetycontrol.InteractionCommandRequest{Version: 1},
	)
	require.Error(t, err)
	require.True(t, errors.Is(err, safetycontrol.ErrVersionConflict) ||
		errors.Is(err, interaction.ErrInvalidTransition))
}

// TestInteractionExpiryAndApprovalRacePostgres 验证两条真实事务并发时只能出现完整的
// APPROVED 或完整的 EXPIRED 结果，不能产生 RESOLVED+blocked 等混合状态。
func TestInteractionExpiryAndApprovalRacePostgres(t *testing.T) {
	dsn := os.Getenv("SWARMOS_INTEGRATION_DATABASE_URL")
	if dsn == "" {
		t.Skip("未设置 SWARMOS_INTEGRATION_DATABASE_URL，跳过审批/过期竞争测试")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	repository, err := New(ctx, dsn)
	require.NoError(t, err)
	defer repository.Close()
	if os.Getenv("SWARMOS_INTEGRATION_MIGRATE") == "1" {
		require.NoError(t, repository.Migrate(ctx))
	}

	fixture := seedHITLFixture(t, ctx, repository, "expire-race")
	defer cleanupHITLFixture(t, repository, fixture)
	require.NoError(t, repository.SuspendAttempt(ctx, execution.AttemptOwner{
		AttemptID: fixture.attemptID, WorkerID: "worker-before-approval", FencingToken: 1,
	}, execution.AttemptWait{Reason: execution.WaitForApproval, EffectID: fixture.effectID}))
	realDeadline := time.Now().UTC().Add(time.Hour)
	_, err = repository.pool.Exec(ctx, `
		UPDATE interactions SET expires_at=$3 WHERE tenant_id=$1 AND id=$2`,
		fixture.tenantID, fixture.interactionID, realDeadline)
	require.NoError(t, err)

	start := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(2)
	var expired int
	var expireErr, approveErr error
	go func() {
		defer wait.Done()
		<-start
		expired, expireErr = repository.ExpireWaitingInteractions(ctx, realDeadline.Add(time.Second), 100)
	}()
	go func() {
		defer wait.Done()
		<-start
		_, approveErr = safetycontrol.NewService(repository).Approve(
			hitlPrincipal(ctx, fixture.tenantID), fixture.interactionID, fixture.mutationKey+"-race",
			safetycontrol.InteractionCommandRequest{Version: 1},
		)
	}()
	close(start)
	wait.Wait()
	require.NoError(t, expireErr)

	var interactionStatus, effectStatus, attemptStatus, taskStatus, runStatus, agentStatus string
	var blocked bool
	require.NoError(t, repository.pool.QueryRow(ctx, `
		SELECT x.status,e.status,a.status,t.status,r.status,ai.status,
		       (COALESCE(e.sanitized_result #>> '{_authorization,blocked}','false')='true')
		FROM interactions x
		JOIN effects e ON e.tenant_id=x.tenant_id AND e.id=x.effect_id
		JOIN task_attempts a ON a.tenant_id=x.tenant_id AND a.id=x.attempt_id
		JOIN tasks t ON t.tenant_id=x.tenant_id AND t.id=x.task_id
		JOIN swarms r ON r.tenant_id=x.tenant_id AND r.id=x.run_id
		JOIN agent_instances ai ON ai.tenant_id=x.tenant_id AND ai.id=a.agent_id
		WHERE x.tenant_id=$1 AND x.id=$2`, fixture.tenantID, fixture.interactionID).Scan(
		&interactionStatus, &effectStatus, &attemptStatus, &taskStatus, &runStatus,
		&agentStatus, &blocked,
	))
	if interactionStatus == "RESOLVED" {
		require.NoError(t, approveErr)
		require.Zero(t, expired)
		require.Equal(t, "AUTHORIZED", effectStatus)
		require.Equal(t, "WAITING", attemptStatus)
		require.Equal(t, "ASSIGNED", taskStatus)
		require.Equal(t, "RUNNING", runStatus)
		require.Equal(t, "RESERVED", agentStatus)
		require.False(t, blocked)
		return
	}
	require.Equal(t, "EXPIRED", interactionStatus)
	require.Error(t, approveErr)
	require.Equal(t, 1, expired)
	require.Equal(t, "PREPARED", effectStatus)
	require.Equal(t, "ABORTED", attemptStatus)
	require.Equal(t, "FAILED", taskStatus)
	require.Equal(t, "FAILED", runStatus)
	require.Equal(t, "IDLE", agentStatus)
	require.True(t, blocked)
}
