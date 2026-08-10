// Package server 构造 Kratos 传输层。
package server

import (
	"context"
	"net/http"
	"strconv"

	kratosErrors "github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware/logging"
	"github.com/go-kratos/kratos/v2/middleware/recovery"
	khttp "github.com/go-kratos/kratos/v2/transport/http"
	v1 "github.com/licy-yu/agent-os/api/controlplane/v1"
	"github.com/licy-yu/agent-os/internal/conf"
	"github.com/licy-yu/agent-os/internal/service"
	"google.golang.org/protobuf/types/known/emptypb"
)

// NewHTTPServer 注册显式 REST 路由。
// API 内部仍复用 protobuf 消息与 service 方法，因此它不是另一套业务实现。
func NewHTTPServer(cfg conf.ServerConfig, svc *service.ControlPlaneService, logger log.Logger) *khttp.Server {
	srv := khttp.NewServer(
		khttp.Address(cfg.HTTPAddr),
		khttp.Middleware(recovery.Recovery(), logging.Server(logger)),
	)

	srv.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		reply, err := svc.Health(r.Context(), &emptypb.Empty{})
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("unhealthy\n"))
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte(reply.Status + "\n"))
	})

	route := srv.Route("/api/v1")
	route.POST("/swarms", unaryBody(http.StatusCreated, svc.CreateSwarm))
	route.GET("/swarms", func(ctx khttp.Context) error {
		page, err := pageFromQuery(ctx)
		if err != nil {
			return err
		}
		return ctx.Returns(svc.ListSwarms(ctx, &v1.ListSwarmsRequest{Page: page}))
	})
	route.GET("/swarms/{id}", func(ctx khttp.Context) error {
		return ctx.Returns(svc.GetSwarm(ctx, &v1.GetSwarmRequest{Id: ctx.Vars().Get("id")}))
	})

	route.POST("/agent-templates", unaryBody(http.StatusCreated, svc.CreateAgentTemplate))
	route.GET("/agent-templates", func(ctx khttp.Context) error {
		page, err := pageFromQuery(ctx)
		if err != nil {
			return err
		}
		return ctx.Returns(svc.ListAgentTemplates(ctx, &v1.ListAgentTemplatesRequest{Page: page}))
	})
	route.POST("/agents", unaryBody(http.StatusCreated, svc.RegisterAgent))
	route.GET("/agents", func(ctx khttp.Context) error {
		page, err := pageFromQuery(ctx)
		if err != nil {
			return err
		}
		return ctx.Returns(svc.ListAgents(ctx, &v1.ListAgentsRequest{SwarmId: ctx.Query().Get("swarm_id"), Page: page}))
	})

	route.POST("/tasks", unaryBody(http.StatusCreated, svc.CreateTask))
	route.GET("/tasks", func(ctx khttp.Context) error {
		page, err := pageFromQuery(ctx)
		if err != nil {
			return err
		}
		return ctx.Returns(svc.ListTasks(ctx, &v1.ListTasksRequest{
			SwarmId: ctx.Query().Get("swarm_id"), Status: ctx.Query().Get("status"), Page: page,
		}))
	})
	route.GET("/tasks/{id}", func(ctx khttp.Context) error {
		return ctx.Returns(svc.GetTask(ctx, &v1.GetTaskRequest{Id: ctx.Vars().Get("id")}))
	})
	return srv
}

// unaryBody 是带 protobuf JSON 请求体的通用处理器。
func unaryBody[Req any, Reply any](status int, call func(context.Context, *Req) (*Reply, error)) khttp.HandlerFunc {
	return func(ctx khttp.Context) error {
		request := new(Req)
		if err := ctx.Bind(request); err != nil {
			return err
		}
		reply, err := call(ctx, request)
		if err != nil {
			return err
		}
		return ctx.Result(status, reply)
	}
}

func pageFromQuery(ctx khttp.Context) (*v1.PageRequest, error) {
	pageSize := int32(0)
	if raw := ctx.Query().Get("page_size"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			return nil, kratosErrors.BadRequest("INVALID_PAGE_SIZE", "page_size 必须是整数")
		}
		pageSize = int32(parsed)
	}
	return &v1.PageRequest{PageSize: pageSize, PageToken: ctx.Query().Get("page_token")}, nil
}
