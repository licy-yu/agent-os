package server

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"

	khttp "github.com/go-kratos/kratos/v2/transport/http"
	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/conf"
	"github.com/licy-yu/agent-os/internal/runcontrol"
)

// APIAuthFilter 在 HTTP 路由进入业务层前建立可信 Principal。tenantId 不接受请求头或
// JSON 覆盖，因而知道 API Key 的调用方也只能访问该部署显式配置的租户。
func APIAuthFilter(cfg conf.SecurityConfig) khttp.FilterFunc {
	tenantID, _ := uuid.Parse(cfg.TenantID)
	expected := sha256.Sum256([]byte(cfg.APIKey))
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			if !strings.HasPrefix(request.URL.Path, "/api/") {
				next.ServeHTTP(w, request)
				return
			}
			if cfg.APIKey != "" {
				provided := bearerToken(request.Header.Get("Authorization"))
				if provided == "" {
					provided = strings.TrimSpace(request.Header.Get("X-API-Key"))
				}
				actual := sha256.Sum256([]byte(provided))
				if provided == "" || subtle.ConstantTimeCompare(actual[:], expected[:]) != 1 {
					w.Header().Set("Content-Type", "application/json; charset=utf-8")
					w.Header().Set("WWW-Authenticate", `Bearer realm="swarmos"`)
					w.WriteHeader(http.StatusUnauthorized)
					_ = json.NewEncoder(w).Encode(map[string]any{
						"code": 401, "reason": "UNAUTHORIZED", "message": "缺少或无效的 SwarmOS API Key",
					})
					return
				}
			}
			principal := runcontrol.Principal{
				TenantID: tenantID, Subject: cfg.Subject, Scopes: map[string]bool{"*": true},
			}
			next.ServeHTTP(w, request.WithContext(runcontrol.WithPrincipal(request.Context(), principal)))
		})
	}
}

func bearerToken(value string) string {
	parts := strings.Fields(value)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return ""
	}
	return parts[1]
}
