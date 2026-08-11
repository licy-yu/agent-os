package postgres

import (
	"testing"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/effect"
	"github.com/stretchr/testify/require"
)

// TestClassifyTimedOutEffectsCoversCommitSuspendCrashWindow 覆盖 Effect 事务已经提交，
// 但 Worker 在调用 SuspendAttempt 之前崩溃的全部持久状态。这里刻意不根据内存 error
// 猜测结果：RecoveryController 只能相信数据库里的 Effect/Interaction 事实。
func TestClassifyTimedOutEffectsCoversCommitSuspendCrashWindow(t *testing.T) {
	t.Parallel()
	effectID := uuid.New()
	tests := []struct {
		name        string
		effects     []timedOutRecoveryEffect
		want        timedOutEffectDisposition
		failureCode string
	}{
		{name: "no effect keeps ordinary retry", want: timedOutWithoutEffect},
		{
			name: "prepared waiting approval parks same attempt",
			effects: []timedOutRecoveryEffect{{
				ID: effectID, Status: effect.StatusPrepared, InteractionStatus: "WAITING",
			}},
			want: timedOutWaitApproval,
		},
		{
			name:    "authorized requeues same attempt",
			effects: []timedOutRecoveryEffect{{ID: effectID, Status: effect.StatusAuthorized}},
			want:    timedOutReassignSameAttempt,
		},
		{
			name:    "executing must become unknown then wait",
			effects: []timedOutRecoveryEffect{{ID: effectID, Status: effect.StatusExecuting}},
			want:    timedOutWaitExternal,
		},
		{
			name:    "unknown waits for reconciliation",
			effects: []timedOutRecoveryEffect{{ID: effectID, Status: effect.StatusUnknown}},
			want:    timedOutWaitExternal,
		},
		{
			name:    "reconciling remains externally waiting",
			effects: []timedOutRecoveryEffect{{ID: effectID, Status: effect.StatusReconciling}},
			want:    timedOutWaitExternal,
		},
		{
			name:    "succeeded requeues same attempt",
			effects: []timedOutRecoveryEffect{{ID: effectID, Status: effect.StatusSucceeded}},
			want:    timedOutReassignSameAttempt,
		},
		{
			name: "failed closes same attempt and run",
			effects: []timedOutRecoveryEffect{{
				ID: effectID, Status: effect.StatusFailed, ErrorMessage: "remote rejected change",
			}},
			want: timedOutFailSameAttempt, failureCode: "EFFECT_TERMINAL_FAILURE",
		},
		{
			name: "authorization block closes same attempt and run",
			effects: []timedOutRecoveryEffect{{
				ID: effectID, Status: effect.StatusPrepared,
				AuthorizationBlocked: true, BlockReason: "reviewer rejected",
			}},
			want: timedOutFailSameAttempt, failureCode: "EFFECT_AUTHORIZATION_BLOCKED",
		},
		{
			name: "orphan prepared fails closed",
			effects: []timedOutRecoveryEffect{{
				ID: effectID, Status: effect.StatusPrepared,
			}},
			want: timedOutFailSameAttempt, failureCode: "EFFECT_AUTHORIZATION_INCOMPLETE",
		},
		{
			name: "pending external effect outranks older success",
			effects: []timedOutRecoveryEffect{
				{ID: effectID, Status: effect.StatusSucceeded},
				{ID: uuid.New(), Status: effect.StatusUnknown},
			},
			want: timedOutWaitExternal,
		},
		{
			name: "blocked effect outranks unknown",
			effects: []timedOutRecoveryEffect{
				{ID: effectID, Status: effect.StatusUnknown},
				{ID: uuid.New(), Status: effect.StatusPrepared, AuthorizationBlocked: true},
			},
			want: timedOutFailSameAttempt, failureCode: "EFFECT_AUTHORIZATION_BLOCKED",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := classifyTimedOutEffects(test.effects)
			require.Equal(t, test.want, got.Disposition)
			require.Equal(t, test.failureCode, got.FailureCode)
			if len(test.effects) > 0 {
				require.NotEqual(t, uuid.Nil, got.EffectID)
			}
		})
	}
}
