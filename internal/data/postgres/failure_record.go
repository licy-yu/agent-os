package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/licy-yu/agent-os/internal/domain"
	"github.com/licy-yu/agent-os/internal/failure"
)

const runtimeFailureCode = "WORKER_EXECUTION_FAILED"

// failureRecordTx 收窄到本操作真正依赖的事务能力，既明确原子写边界，也让 SQL
// 幂等合同可以用无数据库副作用的单元测试覆盖。生产调用传入的 pgx.Tx 满足该接口。
type failureRecordTx interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// runtimeFailureRecord 是 Worker 已完成分类后的不可变失败事实。
//
// retryable 放进 details 而不是另加数据库列，是因为它描述当前策略是否可自动恢复，
// 不参与 FailureRecord 的身份；后续策略升级可以新增 details 字段而不破坏审计结构。
type runtimeFailureRecord struct {
	Class      failure.Class
	RetryLevel failure.RetryLevel
	Action     failure.Action
	Retryable  bool
	Message    string
}

// parseRuntimeFailure 只识别 Runtime 的结构化失败信封。普通业务输出即使包含名为
// execution_error 的字段，只要没有 failure_class，也不会被误记为平台执行失败。
// 一旦出现 failure_class，其余字段必须完整且属于封闭枚举，宁可让当前事务失败，
// 也不能静默丢失或写入无法驱动恢复控制器的半条 FailureRecord。
func parseRuntimeFailure(output map[string]any) (*runtimeFailureRecord, error) {
	classText, present, err := requiredFailureString(output, "failure_class", false)
	if err != nil || !present {
		return nil, err
	}
	message, _, err := requiredFailureString(output, "execution_error", true)
	if err != nil {
		return nil, err
	}
	actionText, _, err := requiredFailureString(output, "recovery_action", true)
	if err != nil {
		return nil, err
	}
	retryLevelText, _, err := requiredFailureString(output, "retry_level", true)
	if err != nil {
		return nil, err
	}
	retryable, ok := output["retryable"].(bool)
	if !ok {
		return nil, fmt.Errorf("runtime failure.retryable 必须是布尔值")
	}

	record := &runtimeFailureRecord{
		Class: failure.Class(classText), RetryLevel: failure.RetryLevel(retryLevelText),
		Action: failure.Action(actionText), Retryable: retryable, Message: message,
	}
	if !validFailureClass(record.Class) {
		return nil, fmt.Errorf("runtime failure.failure_class 非法: %q", classText)
	}
	if !validRetryLevel(record.RetryLevel) {
		return nil, fmt.Errorf("runtime failure.retry_level 非法: %q", retryLevelText)
	}
	if !validRecoveryAction(record.Action) {
		return nil, fmt.Errorf("runtime failure.recovery_action 非法: %q", actionText)
	}
	return record, nil
}

func requiredFailureString(output map[string]any, key string, required bool) (string, bool, error) {
	raw, present := output[key]
	if !present {
		if required {
			return "", false, fmt.Errorf("runtime failure.%s 缺失", key)
		}
		return "", false, nil
	}
	value, ok := raw.(string)
	if !ok || strings.TrimSpace(value) == "" {
		return "", true, fmt.Errorf("runtime failure.%s 必须是非空字符串", key)
	}
	return strings.TrimSpace(value), true, nil
}

// persistRuntimeFailureRecord 与 Candidate Artifact、VerificationRun 和 Attempt REVIEW
// 状态共用 CompleteAttempt 的同一事务。ID 只由 Attempt 派生，因此数据库事务重试、
// 消息重放都只会得到同一条记录；若同一 Attempt 重放了不同分类，则明确报冲突。
func persistRuntimeFailureRecord(ctx context.Context, tx failureRecordTx, tenantID, runID, taskID,
	attemptID uuid.UUID, output map[string]any,
) error {
	record, err := parseRuntimeFailure(output)
	if err != nil || record == nil {
		return err
	}
	details, err := marshalJSON(map[string]any{
		"retryable": record.Retryable,
		"source":    "worker.runtime",
	}, "failure_record.details")
	if err != nil {
		return err
	}
	failureID := stableExecutionID("runtime-failure-record", attemptID.String())
	command, err := tx.Exec(ctx, `
		INSERT INTO failure_records(
			id,tenant_id,run_id,task_id,attempt_id,failure_class,retry_level,
			code,message,details,recovery_action
		) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT(id) DO NOTHING`, failureID, tenantID, runID, taskID, attemptID,
		record.Class, record.RetryLevel, runtimeFailureCode, record.Message, details, record.Action)
	if err != nil {
		return mapWriteError("保存 Runtime FailureRecord", err)
	}
	if command.RowsAffected() == 1 {
		return nil
	}

	// ON CONFLICT 只解决同载荷重放，不允许不同结果碰巧覆盖相同的稳定 ID。
	var persistedClass, persistedLevel, persistedCode, persistedMessage, persistedAction string
	var persistedDetails []byte
	if err := tx.QueryRow(ctx, `
		SELECT failure_class,retry_level,code,message,details,recovery_action
		FROM failure_records
		WHERE id=$1 AND tenant_id=$2 AND run_id=$3 AND task_id=$4 AND attempt_id=$5`,
		failureID, tenantID, runID, taskID, attemptID).Scan(
		&persistedClass, &persistedLevel, &persistedCode, &persistedMessage,
		&persistedDetails, &persistedAction,
	); err != nil {
		return mapReadError("读取幂等 Runtime FailureRecord", err)
	}
	var expectedDetails, actualDetails map[string]any
	if err := json.Unmarshal(details, &expectedDetails); err != nil {
		return err
	}
	if err := json.Unmarshal(persistedDetails, &actualDetails); err != nil {
		return fmt.Errorf("解析已保存 FailureRecord details: %w", err)
	}
	if persistedClass == string(record.Class) && persistedLevel == string(record.RetryLevel) &&
		persistedCode == runtimeFailureCode && persistedMessage == record.Message &&
		persistedAction == string(record.Action) && reflect.DeepEqual(actualDetails, expectedDetails) {
		return nil
	}
	return fmt.Errorf("%w: attempt %s 已记录不同的 Runtime FailureRecord", domain.ErrConflict, attemptID)
}

func validFailureClass(value failure.Class) bool {
	switch value {
	case failure.NetworkTemporary, failure.RateLimit, failure.ModelProviderError,
		failure.ModelOutputInvalid, failure.ToolTemporary, failure.ToolAuthRequired,
		failure.ToolUnknownEffect, failure.WorkerLost, failure.ContextOverflow,
		failure.NoProgress, failure.VerificationFailed, failure.PlanInvalid,
		failure.CapabilityMismatch, failure.BudgetExceeded, failure.PolicyViolation,
		failure.UserRejected:
		return true
	default:
		return false
	}
}

func validRetryLevel(value failure.RetryLevel) bool {
	switch value {
	case failure.RetryNone, failure.RetryStep, failure.RetryAttempt, failure.RetryAgent,
		failure.RetryModel, failure.RetryTask, failure.RetryRun:
		return true
	default:
		return false
	}
}

func validRecoveryAction(value failure.Action) bool {
	switch value {
	case failure.ActionBackoffRetry, failure.ActionProviderFallback, failure.ActionRepairPrompt,
		failure.ActionToolRetry, failure.ActionWaitUser, failure.ActionReconcile,
		failure.ActionResume, failure.ActionOffloadContext, failure.ActionSelfCheck,
		failure.ActionCorrect, failure.ActionReplan, failure.ActionReschedule,
		failure.ActionPause, failure.ActionStop:
		return true
	default:
		return false
	}
}
