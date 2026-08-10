package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSPAHandlerServesAssetsAndFallsBackToIndex(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "assets"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(root, "index.html"), []byte("<main>console</main>"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(root, "assets", "app.js"), []byte("console.log('ok')"), 0o600))
	handler := newSPAHandler(root)

	// 哈希静态资源走强缓存，前端虚拟路由则返回不可缓存的入口页。
	assetRequest := httptest.NewRequest(http.MethodGet, "/assets/app.js", nil)
	assetReply := httptest.NewRecorder()
	handler.ServeHTTP(assetReply, assetRequest)
	require.Equal(t, http.StatusOK, assetReply.Code)
	require.Contains(t, assetReply.Header().Get("Cache-Control"), "immutable")

	routeRequest := httptest.NewRequest(http.MethodGet, "/operations/swarm-1", nil)
	routeReply := httptest.NewRecorder()
	handler.ServeHTTP(routeReply, routeRequest)
	require.Equal(t, http.StatusOK, routeReply.Code)
	require.Contains(t, routeReply.Body.String(), "console")
	require.Equal(t, "no-cache", routeReply.Header().Get("Cache-Control"))
}

func TestSPAHandlerRejectsMutation(t *testing.T) {
	handler := newSPAHandler(t.TempDir())
	reply := httptest.NewRecorder()
	handler.ServeHTTP(reply, httptest.NewRequest(http.MethodPost, "/", nil))
	require.Equal(t, http.StatusMethodNotAllowed, reply.Code)
}
