package postgres

import (
	"testing"

	"github.com/licy-yu/agent-os/internal/effect"
	"github.com/stretchr/testify/require"
)

func TestEffectPolicyDeniesRiskAndUnknownEffectType(t *testing.T) {
	t.Parallel()
	policy := gatewayEffectPolicy{
		MaxRisk: "R2_EXTERNAL_REVERSIBLE", AllowedEffects: []string{"create-pr"},
	}
	require.Contains(t, effectPolicyDenial(policy, effect.RiskR3ProductionDestructive, "create-pr"), "超过")
	require.Contains(t, effectPolicyDenial(policy, effect.RiskR2ExternalReversible, "deploy"), "允许列表")
	require.Empty(t, effectPolicyDenial(policy, effect.RiskR2ExternalReversible, "create-pr"))
}

func TestSanitizeEffectValueRedactsNestedCredentials(t *testing.T) {
	t.Parallel()
	value := sanitizeEffectValue(map[string]any{
		"target": "production",
		"auth":   map[string]any{"api_token": "secret-value", "user": "operator"},
	}).(map[string]any)
	auth := value["auth"].(map[string]any)
	require.Equal(t, "[REDACTED]", auth["api_token"])
	require.Equal(t, "operator", auth["user"])
	require.Equal(t, "production", value["target"])
}
