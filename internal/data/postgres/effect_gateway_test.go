package postgres

import (
	"strings"
	"testing"

	"github.com/licy-yu/agent-os/internal/effect"
	"github.com/licy-yu/agent-os/internal/toolgateway"
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

func TestFinishToolEffectMergesResultWithoutReplacingAuthorization(t *testing.T) {
	t.Parallel()
	// 这是生产 UPDATE 使用的 SQL 合同：右侧文档只带 result，jsonb || 仅覆盖同名顶层键，
	// 因此原文档里的 _authorization 会原样保留。
	normalized := strings.Join(strings.Fields(finishToolEffectUpdateSQL), " ")
	require.Contains(t, normalized,
		"sanitized_result=COALESCE(sanitized_result,'{}'::jsonb) || $6::jsonb")
	require.NotContains(t, normalized, "SET status=$5, sanitized_result=$6")
}

func TestSameToolEffectCompletionIgnoresAuthorizationMetadataButChecksOwnedFields(t *testing.T) {
	t.Parallel()
	completion := toolgateway.EffectCompletion{
		Status: effect.StatusSucceeded,
		Result: map[string]any{
			"deployment_id": "deploy-17",
			"api_token":     "should-never-persist",
			"replicas":      3,
		},
		ExternalRef: "deploy-17",
	}
	current := &toolEffectRow{
		Status:      effect.StatusSucceeded,
		ExternalRef: "deploy-17",
		Result: map[string]any{
			"_authorization": map[string]any{
				"approved_by": "reviewer-1", "approved_at": "2026-08-11T00:00:00Z",
			},
			"result": map[string]any{
				"deployment_id": "deploy-17", "api_token": "[REDACTED]", "replicas": float64(3),
			},
		},
	}
	normalized, err := marshalJSON(map[string]any{
		"result": sanitizeEffectValue(completion.Result),
	}, "test.effect.result")
	require.NoError(t, err)
	require.True(t, sameToolEffectCompletion(current, completion, "", normalized),
		"审批元数据不属于 Adapter 完成命令，不能破坏同结果幂等重放")

	current.ExternalRef = "deploy-18"
	require.False(t, sameToolEffectCompletion(current, completion, "", normalized),
		"同结果但不同外部引用不是同一个完成命令")
}
