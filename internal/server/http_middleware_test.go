package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-kratos/kratos/v2/middleware"
	khttp "github.com/go-kratos/kratos/v2/transport/http"
	"github.com/stretchr/testify/require"
)

type middlewareMarker struct{}

func TestWithOperationPassesDerivedContextWithoutCycle(t *testing.T) {
	// 这个测试专门防止把派生 Context 重新装回以原 HTTP Context 为父级的请求，后者会形成
	// context.Done 递归环并最终导致生产进程栈溢出。
	mark := func(next middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req any) (any, error) {
			return next(context.WithValue(ctx, middlewareMarker{}, true), req)
		}
	}
	srv := khttp.NewServer(khttp.Middleware(mark))
	srv.Route("/").GET("/probe", withOperation("/swarmos.test/Probe", func(ctx khttp.Context, callCtx context.Context) error {
		require.Equal(t, true, callCtx.Value(middlewareMarker{}))
		select {
		case <-callCtx.Done():
			t.Fatal("调用 Context 不应在正常请求中提前取消")
		default:
		}
		return ctx.String(http.StatusOK, "ok")
	}))

	reply := httptest.NewRecorder()
	srv.ServeHTTP(reply, httptest.NewRequest(http.MethodGet, "/probe", nil))
	require.Equal(t, http.StatusOK, reply.Code)
	require.Equal(t, "ok", reply.Body.String())
}
