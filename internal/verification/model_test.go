package verification

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompletionManifestAcceptsOnlyWhenAllRequiredGatesPass(t *testing.T) {
	manifest := CompletionManifest{
		Artifacts: []string{"artifact://patch/31", "artifact://test/50"},
		Evidence: []GateResult{
			{GateName: "go-build", Status: GatePass},
			{GateName: "go-test", Status: GatePass, EvidenceRefs: []string{"artifact://test/50"}},
			{GateName: "llm-review", Status: GateFail}, // 可选质量 Gate 不属于硬验收条件。
		},
	}
	decision := manifest.Decide([]GateSpec{
		{Name: "go-build", Required: true},
		{Name: "go-test", Required: true},
		{Name: "llm-review", Required: false},
	})

	require.True(t, decision.Accepted())
	require.Equal(t, ManifestAccepted, decision.Status)
}

func TestCompletionManifestReportsMissingGateAsIncomplete(t *testing.T) {
	manifest := CompletionManifest{Evidence: []GateResult{{GateName: "go-build", Status: GatePass}}}
	decision := manifest.Decide([]GateSpec{
		{Name: "go-build", Required: true},
		{Name: "go-test", Required: true},
	})

	require.Equal(t, ManifestIncomplete, decision.Status)
	require.Equal(t, []string{"go-test"}, decision.MissingGates)
}

func TestCompletionManifestRejectsFailedAndErroredGates(t *testing.T) {
	manifest := CompletionManifest{Evidence: []GateResult{
		{GateName: "unit-test", Status: GateFail},
		{GateName: "security", Status: GateError},
	}}
	decision := manifest.Decide([]GateSpec{
		{Name: "unit-test", Required: true},
		{Name: "security", Required: true},
	})

	require.Equal(t, ManifestRejected, decision.Status)
	require.Equal(t, []string{"unit-test"}, decision.FailedGates)
	require.Equal(t, []string{"security"}, decision.ErrorGates)
}

func TestCompletionManifestRejectsAmbiguousDuplicateEvidence(t *testing.T) {
	manifest := CompletionManifest{Evidence: []GateResult{
		{GateName: "go-test", Status: GatePass},
		{GateName: "GO-TEST", Status: GateFail},
	}}
	decision := manifest.Decide([]GateSpec{{Name: "go-test", Required: true}})

	require.Equal(t, ManifestInvalid, decision.Status)
	require.Contains(t, decision.InvalidReasons, `GateResult "GO-TEST" 重复`)
}

func TestRequiredSkippedGateDoesNotCountAsPass(t *testing.T) {
	manifest := CompletionManifest{Evidence: []GateResult{{GateName: "human-approval", Status: GateSkipped}}}
	decision := manifest.Decide([]GateSpec{{Name: "human-approval", Required: true}})

	require.Equal(t, ManifestIncomplete, decision.Status)
	require.Equal(t, []string{"human-approval"}, decision.MissingGates)
}
