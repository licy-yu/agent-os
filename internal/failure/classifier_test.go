package failure

import (
	"context"
	"errors"
	"testing"
)

func TestClassifyRecoveryPolicy(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		hint      Hint
		class     Class
		action    Action
		level     RetryLevel
		retryable bool
	}{
		{name: "provider rate limit switches model", err: errors.New("HTTP 429 too many requests"), class: RateLimit, action: ActionProviderFallback, level: RetryModel, retryable: true},
		{name: "unknown effect never retries", err: errors.New("effect outcome unknown"), class: ToolUnknownEffect, action: ActionReconcile, level: RetryNone},
		{name: "auth waits for user", hint: Hint{Class: ToolAuthRequired}, class: ToolAuthRequired, action: ActionWaitUser, level: RetryNone},
		{name: "deadline retries attempt", err: context.DeadlineExceeded, class: NetworkTemporary, action: ActionBackoffRetry, level: RetryAttempt, retryable: true},
		{name: "no progress switches agent", err: errors.New("repeated state: no progress"), class: NoProgress, action: ActionSelfCheck, level: RetryAgent, retryable: true},
		{name: "policy fails closed", err: errors.New("policy denied shell write"), class: PolicyViolation, action: ActionStop, level: RetryNone},
		{name: "verification gets correction", hint: Hint{Class: VerificationFailed}, class: VerificationFailed, action: ActionCorrect, level: RetryAttempt, retryable: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decision := Classify(test.err, test.hint)
			if decision.Class != test.class || decision.Action != test.action ||
				decision.RetryLevel != test.level || decision.Retryable != test.retryable {
				t.Fatalf("unexpected decision: %+v", decision)
			}
		})
	}
}

func TestStructuredErrorWinsOverTextHeuristic(t *testing.T) {
	err := &Error{FailureClass: BudgetExceeded, Cause: errors.New("HTTP 429")}
	decision := Classify(err, Hint{})
	if decision.Class != BudgetExceeded || decision.Action != ActionPause || decision.Retryable {
		t.Fatalf("structured class must win: %+v", decision)
	}
}

func TestExplicitHintWinsOverStructuredError(t *testing.T) {
	err := &Error{FailureClass: RateLimit, Cause: errors.New("provider unavailable")}
	decision := Classify(err, Hint{Class: UserRejected})
	if decision.Class != UserRejected || decision.Action != ActionStop {
		t.Fatalf("explicit hint must win: %+v", decision)
	}
}
