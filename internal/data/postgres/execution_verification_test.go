package postgres

import (
	"testing"

	"github.com/licy-yu/agent-os/internal/execution"
	"github.com/stretchr/testify/require"
)

func TestSystemHardGatesIgnoreClaimedSuccess(t *testing.T) {
	t.Parallel()
	claimed := execution.ExecutionResult{
		Output: map[string]any{},
		Checks: map[string]bool{
			"system.output_nonempty":    true,
			"system.output_json_schema": true,
			"system.policy":             true,
		},
		PolicyViolations: []string{"禁止访问生产密钥"},
	}

	status, _, _ := evaluateAcceptanceGate(persistedGate{
		Key: "system.output_nonempty", Type: "ARTIFACT", Required: true,
	}, claimed)
	require.Equal(t, gateFailed, status)
	status, excerpt, _ := evaluateAcceptanceGate(persistedGate{
		Key: "system.policy", Type: "POLICY", Required: true,
	}, claimed)
	require.Equal(t, gateFailed, status)
	require.Contains(t, excerpt, "生产密钥")
}

func TestJSONSchemaGateValidatesCandidateRatherThanClaimedCheck(t *testing.T) {
	t.Parallel()
	gate := persistedGate{
		Key: "system.output_json_schema", Type: "JSON_SCHEMA", Required: true,
		Config: map[string]any{"schema": map[string]any{
			"type": "object", "required": []any{"name", "score"},
			"properties": map[string]any{
				"name":  map[string]any{"type": "string", "minLength": float64(1)},
				"score": map[string]any{"type": "number", "minimum": float64(0.8)},
			},
		}},
	}
	failed := execution.ExecutionResult{
		Output: map[string]any{"name": "", "score": 0.2},
		Checks: map[string]bool{gate.Key: true},
	}
	status, excerpt, _ := evaluateAcceptanceGate(gate, failed)
	require.Equal(t, gateFailed, status)
	require.Contains(t, excerpt, "$.name")
	require.Contains(t, excerpt, "$.score")

	passed := failed
	passed.Output = map[string]any{"name": "artifact", "score": 0.95}
	status, _, _ = evaluateAcceptanceGate(gate, passed)
	require.Equal(t, gatePassed, status)
}

func TestRequiredGateFailureOverridesReviewerAccept(t *testing.T) {
	t.Parallel()
	evaluation := execution.Evaluation{
		MachinePass: true, PolicyPass: true, Decision: execution.DecisionAccept,
	}
	closure := verificationClosure{
		RequiredPassed: false, PolicyPassed: true,
		FailedGateKeys: []string{"system.output_json_schema"},
	}
	result := enforceRequiredGates(evaluation, closure, 1, 3)
	require.Equal(t, execution.DecisionRetry, result.Decision)
	require.False(t, result.MachinePass)
	require.Contains(t, result.Findings[0], "system.output_json_schema")

	result = enforceRequiredGates(evaluation, closure, 3, 3)
	require.Equal(t, execution.DecisionReject, result.Decision)
}

func TestPolicyGateFailureRejectsWithoutRetry(t *testing.T) {
	t.Parallel()
	evaluation := execution.Evaluation{
		MachinePass: true, PolicyPass: true, Decision: execution.DecisionAccept,
	}
	result := enforceRequiredGates(evaluation, verificationClosure{
		RequiredPassed: false, PolicyPassed: false, FailedGateKeys: []string{"system.policy"},
	}, 1, 3)
	require.Equal(t, execution.DecisionReject, result.Decision)
	require.False(t, result.PolicyPass)
}

func TestStableExecutionIDIsReplayDeterministic(t *testing.T) {
	t.Parallel()
	first := stableExecutionID("verification-run", "attempt-31")
	require.Equal(t, first, stableExecutionID("verification-run", "attempt-31"))
	require.NotEqual(t, first, stableExecutionID("verification-run", "attempt-32"))
}

func TestPolicyRiskRankOrdersRuntimeRiskLevels(t *testing.T) {
	t.Parallel()
	readOnly, ok := policyRiskRank("R0_READ_ONLY")
	require.True(t, ok)
	production, ok := policyRiskRank("production")
	require.True(t, ok)
	require.Less(t, readOnly, production)
}
