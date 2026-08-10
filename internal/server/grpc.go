package server

import (
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/middleware/logging"
	"github.com/go-kratos/kratos/v2/middleware/recovery"
	"github.com/go-kratos/kratos/v2/transport/grpc"
	v1 "github.com/licy-yu/agent-os/api/controlplane/v1"
	"github.com/licy-yu/agent-os/internal/conf"
	"github.com/licy-yu/agent-os/internal/observability"
	"github.com/licy-yu/agent-os/internal/service"
)

// NewGRPCServer 暴露与 HTTP 相同的 ControlPlane API。
func NewGRPCServer(cfg conf.ServerConfig, svc *service.ControlPlaneService, telemetry *observability.Telemetry, logger log.Logger) *grpc.Server {
	middlewares := []middleware.Middleware{recovery.Recovery()}
	middlewares = append(middlewares, telemetry.Middlewares()...)
	middlewares = append(middlewares, logging.Server(logger))
	srv := grpc.NewServer(
		grpc.Address(cfg.GRPCAddr),
		grpc.Middleware(middlewares...),
	)
	v1.RegisterControlPlaneServer(srv, svc)
	return srv
}
