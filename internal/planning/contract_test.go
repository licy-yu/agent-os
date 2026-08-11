package planning

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestValidateContractAcceptsExplicitEmptyRootInputs(t *testing.T) {
	t.Parallel()
	value := validTaskContract(uuid.New(), uuid.New(), "root")
	value.Inputs = []InputSpec{}

	require.Empty(t, ValidateContract(value))
}

func TestValidateContractDistinguishesMissingInputsAndCollectsErrors(t *testing.T) {
	t.Parallel()
	value := TaskContract{
		ID: uuid.New(), RunID: uuid.New(), Name: "invalid",
		Inputs: nil, Output: OutputSpec{}, Requirements: Requirements{},
		RetryPolicy: RetryPolicy{}, Budget: ResourceBudget{}, RuntimeGuard: RuntimeGuard{},
	}

	violations := ValidateContract(value)
	fields := make([]string, 0, len(violations))
	for _, violation := range violations {
		fields = append(fields, violation.Field)
	}
	require.Contains(t, fields, "goal")
	require.Contains(t, fields, "inputs")
	require.Contains(t, fields, "output.type")
	require.Contains(t, fields, "requirements.capabilities")
	require.Contains(t, fields, "acceptance")
	require.Contains(t, fields, "retry_policy.max_attempts")
	require.Contains(t, fields, "budget.max_tokens")
	require.Contains(t, fields, "runtime_guard")
}

func TestDefaultPoliciesContainLoopFuses(t *testing.T) {
	t.Parallel()
	retry := DefaultRetryPolicy()
	guard := DefaultRuntimeGuard()

	require.EqualValues(t, 3, retry.MaxAttempts)
	require.Greater(t, guard.MaxTurns, int32(0))
	require.Greater(t, guard.MaxModelCalls, int32(0))
	require.EqualValues(t, 3, guard.MaxNoProgressRounds)
}

func TestValidateContractAcceptsCanonicalEffectRiskLevel(t *testing.T) {
	t.Parallel()
	value := validTaskContract(uuid.New(), uuid.New(), "risk")
	value.SideEffectPolicy.MaxRisk = "R1_SANDBOX_WRITE"
	require.Empty(t, ValidateContract(value))
}

func validTaskContract(runID, taskID uuid.UUID, name string) TaskContract {
	return TaskContract{
		ID: taskID, RunID: runID, Name: name, Goal: "完成 " + name,
		Inputs: []InputSpec{},
		Output: OutputSpec{Type: "artifact", Artifacts: []ArtifactSpec{{Name: name, Type: "report"}}},
		Requirements: Requirements{
			Capabilities: map[string]float64{"golang": 0.8},
			Tools:        []string{"repo.read"}, Permissions: []string{"repo.read"}, Models: []string{"gpt-test"},
			RiskZone: "sandbox",
		},
		ContextPolicy:    ContextPolicy{MaxTokens: 8_000, IncludeParentArtifacts: true},
		SideEffectPolicy: SideEffectPolicy{MaxRisk: "sandbox"},
		Acceptance:       []AcceptanceCriterion{{Name: "schema", Type: "schema_gate"}},
		RetryPolicy:      DefaultRetryPolicy(), Budget: ResourceBudget{
			MaxTokens: 1_000, MaxCostMicros: 100, MaxDurationSeconds: 60,
		},
		RuntimeGuard: DefaultRuntimeGuard(), Priority: 50, ExpectedDurationSeconds: 10,
	}
}
