// Package server 构造 Kratos 传输层。
package server

import (
	"context"
	"net/http"
	"strconv"

	kratosErrors "github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/middleware/logging"
	"github.com/go-kratos/kratos/v2/middleware/recovery"
	khttp "github.com/go-kratos/kratos/v2/transport/http"
	"github.com/google/uuid"
	v1 "github.com/licy-yu/agent-os/api/controlplane/v1"
	"github.com/licy-yu/agent-os/internal/conf"
	consoleview "github.com/licy-yu/agent-os/internal/console"
	"github.com/licy-yu/agent-os/internal/observability"
	"github.com/licy-yu/agent-os/internal/service"
	"google.golang.org/protobuf/types/known/emptypb"
)

// NewHTTPServer 注册显式 REST 路由。
// API 内部仍复用 protobuf 消息与 service 方法，因此它不是另一套业务实现。

func NewHTTPServer(cfg conf.ServerConfig, svc *service.ControlPlaneService, consoleSvc *consoleview.Service,
	telemetry *observability.Telemetry, logger log.Logger,
) *khttp.Server {
	middlewares := []middleware.Middleware{recovery.Recovery()}
	middlewares = append(middlewares, telemetry.Middlewares()...)
	middlewares = append(middlewares, logging.Server(logger))
	srv := khttp.NewServer(
		khttp.Address(cfg.HTTPAddr),
		khttp.Middleware(middlewares...),
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
	srv.Handle("/metrics", telemetry.MetricsHandler())

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

	// Console API 是只读的聚合视图，使用普通 JSON，避免把运维查询混入领域 Protobuf。
	route.GET("/console/overview", func(ctx khttp.Context) error {
		id, err := consoleID(ctx.Query().Get("swarm_id"), "swarm_id")
		if err != nil {
			return err
		}
		value, err := consoleSvc.Overview(ctx, id)
		if err != nil {
			return err
		}
		return ctx.JSON(http.StatusOK, value)
	})
	route.GET("/console/attempts", func(ctx khttp.Context) error {
		id, err := consoleID(ctx.Query().Get("task_id"), "task_id")
		if err != nil {
			return err
		}
		items, err := consoleSvc.Attempts(ctx, id)
		if err != nil {
			return err
		}
		return ctx.JSON(http.StatusOK, map[string]any{"items": items})
	})
	route.GET("/console/events", func(ctx khttp.Context) error {
		id, err := consoleID(ctx.Query().Get("swarm_id"), "swarm_id")
		if err != nil {
			return err
		}
		items, err := consoleSvc.Events(ctx, id)
		if err != nil {
			return err
		}
		return ctx.JSON(http.StatusOK, map[string]any{"items": items})
	})

	// SPA 必须最后注册。Kratos 路由器会优先匹配前面已经声明的 API、健康检查和指标端点，
	// 其余浏览器路径再回退到 index.html，从而支持前端刷新深层路由。
	if cfg.WebDir != "" {
		srv.HandlePrefix("/", newSPAHandler(cfg.WebDir))
	}
	return srv
}

func consoleID(raw, field string) (uuid.UUID, error) {
	value, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, kratosErrors.BadRequest("INVALID_ID", field+" 必须是 UUID")
	}
	return value, nil
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
