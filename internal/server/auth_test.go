package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/licy-yu/agent-os/internal/conf"
	"github.com/licy-yu/agent-os/internal/runcontrol"
	"github.com/stretchr/testify/require"
)

func TestAPIAuthFilterProtectsOnlyAPIAndInjectsPrincipal(t *testing.T) {
	cfg := conf.SecurityConfig{
		APIKey:   "0123456789abcdef0123456789abcdef",
		TenantID: "00000000-0000-0000-0000-000000000099", Subject: "operator-1",
	}
	var subject string
	next := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		subject = runcontrol.PrincipalFromContext(request.Context()).Subject
		w.WriteHeader(http.StatusNoContent)
	})
	handler := APIAuthFilter(cfg)(next)

	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil))
	require.Equal(t, http.StatusUnauthorized, unauthorized.Code)
	require.Empty(t, subject)

	authorizedRequest := httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil)
	authorizedRequest.Header.Set("Authorization", "Bearer "+cfg.APIKey)
	authorized := httptest.NewRecorder()
	handler.ServeHTTP(authorized, authorizedRequest)
	require.Equal(t, http.StatusNoContent, authorized.Code)
	require.Equal(t, "operator-1", subject)

	// 健康检查不要求密钥，便于容器编排和负载均衡器探活。
	subject = ""
	health := httptest.NewRecorder()
	handler.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	require.Equal(t, http.StatusNoContent, health.Code)
}

func TestBearerTokenRejectsAmbiguousSchemes(t *testing.T) {
	require.Equal(t, "key", bearerToken("Bearer key"))
	require.Equal(t, "key", bearerToken("bearer key"))
	require.Empty(t, bearerToken("Basic key"))
	require.Empty(t, bearerToken("Bearer key extra"))
}
