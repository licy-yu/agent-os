package server

import (
	"net/http"

	khttp "github.com/go-kratos/kratos/v2/transport/http"
)

// NewMetricsServer 为不暴露业务 HTTP 的 Worker 提供独立健康检查和 Prometheus 抓取端口。
func NewMetricsServer(address string, metrics http.Handler) *khttp.Server {
	server := khttp.NewServer(khttp.Address(address))
	server.Handle("/metrics", metrics)
	server.HandleFunc("/healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = writer.Write([]byte("ok\n"))
	})
	return server
}
