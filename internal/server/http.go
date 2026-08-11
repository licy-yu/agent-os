// Package server 构造 Kratos 传输层。
package server

import (
	"context"
	stdErrors "errors"
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
	"github.com/licy-yu/agent-os/internal/domain"
	"github.com/licy-yu/agent-os/internal/observability"
	"github.com/licy-yu/agent-os/internal/runcontrol"
	"github.com/licy-yu/agent-os/internal/service"
	"google.golang.org/protobuf/types/known/emptypb"
)

// NewHTTPServer 注册显式 REST 路由。
// API 内部仍复用 protobuf 消息与 service 方法，因此它不是另一套业务实现。

func NewHTTPServer(cfg conf.ServerConfig, svc *service.ControlPlaneService, runSvc *runcontrol.Service,
	consoleSvc *consoleview.Service,
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
	route.POST("/swarms", withOperation(
		"/swarmos.controlplane.v1.ControlPlane/CreateSwarm",
		unaryBody(http.StatusCreated, svc.CreateSwarm),
	))
	route.GET("/swarms", withOperation("/swarmos.controlplane.v1.ControlPlane/ListSwarms", func(ctx khttp.Context, callCtx context.Context) error {
		page, err := pageFromQuery(ctx)
		if err != nil {
			return err
		}
		return ctx.Returns(svc.ListSwarms(callCtx, &v1.ListSwarmsRequest{Page: page}))
	}))
	route.GET("/swarms/{id}", withOperation("/swarmos.controlplane.v1.ControlPlane/GetSwarm", func(ctx khttp.Context, callCtx context.Context) error {
		return ctx.Returns(svc.GetSwarm(callCtx, &v1.GetSwarmRequest{Id: ctx.Vars().Get("id")}))
	}))

	route.POST("/agent-templates", withOperation(
		"/swarmos.controlplane.v1.ControlPlane/CreateAgentTemplate",
		unaryBody(http.StatusCreated, svc.CreateAgentTemplate),
	))
	route.GET("/agent-templates", withOperation("/swarmos.controlplane.v1.ControlPlane/ListAgentTemplates", func(ctx khttp.Context, callCtx context.Context) error {
		page, err := pageFromQuery(ctx)
		if err != nil {
			return err
		}
		return ctx.Returns(svc.ListAgentTemplates(callCtx, &v1.ListAgentTemplatesRequest{Page: page}))
	}))
	route.POST("/agents", withOperation(
		"/swarmos.controlplane.v1.ControlPlane/RegisterAgent",
		unaryBody(http.StatusCreated, svc.RegisterAgent),
	))
	route.GET("/agents", withOperation("/swarmos.controlplane.v1.ControlPlane/ListAgents", func(ctx khttp.Context, callCtx context.Context) error {
		page, err := pageFromQuery(ctx)
		if err != nil {
			return err
		}
		return ctx.Returns(svc.ListAgents(callCtx, &v1.ListAgentsRequest{SwarmId: ctx.Query().Get("swarm_id"), Page: page}))
	}))

	route.POST("/tasks", withOperation(
		"/swarmos.controlplane.v1.ControlPlane/CreateTask",
		unaryBody(http.StatusCreated, svc.CreateTask),
	))
	route.GET("/tasks", withOperation("/swarmos.controlplane.v1.ControlPlane/ListTasks", func(ctx khttp.Context, callCtx context.Context) error {
		page, err := pageFromQuery(ctx)
		if err != nil {
			return err
		}
		return ctx.Returns(svc.ListTasks(callCtx, &v1.ListTasksRequest{
			SwarmId: ctx.Query().Get("swarm_id"), Status: ctx.Query().Get("status"), Page: page,
		}))
	}))
	route.GET("/tasks/{id}", withOperation("/swarmos.controlplane.v1.ControlPlane/GetTask", func(ctx khttp.Context, callCtx context.Context) error {
		return ctx.Returns(svc.GetTask(callCtx, &v1.GetTaskRequest{Id: ctx.Vars().Get("id")}))
	}))

	// V1.5 Run API 使用普通 JSON DTO。旧 /swarms API 继续可用，但所有新的暂停、恢复、
	// Replan 和不可变 PlanVersion 能力都以 Run 为第一等对象。
	route.POST("/runs", withOperation("/swarmos.run.v1.RunService/CreateRun", func(ctx khttp.Context, callCtx context.Context) error {
		request := runcontrol.CreateRunRequest{}
		if err := ctx.Bind(&request); err != nil {
			return kratosErrors.BadRequest("INVALID_JSON", "请求体不是合法 Run JSON")
		}
		value, err := runSvc.Create(callCtx, request)
		if err != nil {
			return translateRunError(err)
		}
		return ctx.JSON(http.StatusCreated, value)
	}))
	route.GET("/runs", withOperation("/swarmos.run.v1.RunService/ListRuns", func(ctx khttp.Context, callCtx context.Context) error {
		limit := 100
		if raw := ctx.Query().Get("limit"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil {
				return kratosErrors.BadRequest("INVALID_LIMIT", "limit 必须是整数")
			}
			limit = parsed
		}
		items, err := runSvc.List(callCtx, limit)
		if err != nil {
			return translateRunError(err)
		}
		return ctx.JSON(http.StatusOK, map[string]any{"items": items})
	}))
	route.GET("/runs/{id}", withOperation("/swarmos.run.v1.RunService/GetRun", func(ctx khttp.Context, callCtx context.Context) error {
		id, err := consoleID(ctx.Vars().Get("id"), "run.id")
		if err != nil {
			return err
		}
		value, err := runSvc.Get(callCtx, id)
		if err != nil {
			return translateRunError(err)
		}
		return ctx.JSON(http.StatusOK, value)
	}))
	route.GET("/runs/{id}/plans/{plan_id}", withOperation("/swarmos.run.v1.RunService/GetPlan", func(ctx khttp.Context, callCtx context.Context) error {
		runID, err := consoleID(ctx.Vars().Get("id"), "run.id")
		if err != nil {
			return err
		}
		planID, err := consoleID(ctx.Vars().Get("plan_id"), "plan.id")
		if err != nil {
			return err
		}
		value, err := runSvc.GetPlan(callCtx, runID, planID)
		if err != nil {
			return translateRunError(err)
		}
		return ctx.JSON(http.StatusOK, value)
	}))
	registerRunAction := func(path, operation string,
		action func(context.Context, uuid.UUID, string) (*runcontrol.RunView, error),
	) {
		route.POST(path, withOperation(operation, func(ctx khttp.Context, callCtx context.Context) error {
			id, err := consoleID(ctx.Vars().Get("id"), "run.id")
			if err != nil {
				return err
			}
			request := struct {
				Reason string `json:"reason"`
			}{}
			// 空请求体等价于空 reason，Pause/Resume/Cancel 仍会留下稳定的系统动作原因。
			_ = ctx.Bind(&request)
			value, err := action(callCtx, id, request.Reason)
			if err != nil {
				return translateRunError(err)
			}
			return ctx.JSON(http.StatusOK, value)
		}))
	}
	registerRunAction("/runs/{id}:pause", "/swarmos.run.v1.RunService/PauseRun", runSvc.Pause)
	registerRunAction("/runs/{id}:resume", "/swarmos.run.v1.RunService/ResumeRun", runSvc.Resume)
	registerRunAction("/runs/{id}:cancel", "/swarmos.run.v1.RunService/CancelRun", runSvc.Cancel)
	route.POST("/runs/{id}:replan", withOperation("/swarmos.run.v1.RunService/ReplanRun", func(ctx khttp.Context, callCtx context.Context) error {
		id, err := consoleID(ctx.Vars().Get("id"), "run.id")
		if err != nil {
			return err
		}
		request := runcontrol.ReplanRequest{}
		if err := ctx.Bind(&request); err != nil {
			return kratosErrors.BadRequest("INVALID_JSON", "请求体不是合法 Replan JSON")
		}
		value, err := runSvc.Replan(callCtx, id, request)
		if err != nil {
			return translateRunError(err)
		}
		return ctx.JSON(http.StatusOK, value)
	}))

	// Console API 是只读的聚合视图，使用普通 JSON，避免把运维查询混入领域 Protobuf。
	route.GET("/console/overview", withOperation("/swarmos.console.v1.Console/Overview", func(ctx khttp.Context, callCtx context.Context) error {
		id, err := consoleID(ctx.Query().Get("swarm_id"), "swarm_id")
		if err != nil {
			return err
		}
		value, err := consoleSvc.Overview(callCtx, id)
		if err != nil {
			return err
		}
		return ctx.JSON(http.StatusOK, value)
	}))
	route.GET("/console/attempts", withOperation("/swarmos.console.v1.Console/ListAttempts", func(ctx khttp.Context, callCtx context.Context) error {
		id, err := consoleID(ctx.Query().Get("task_id"), "task_id")
		if err != nil {
			return err
		}
		items, err := consoleSvc.Attempts(callCtx, id)
		if err != nil {
			return err
		}
		return ctx.JSON(http.StatusOK, map[string]any{"items": items})
	}))
	route.GET("/console/events", withOperation("/swarmos.console.v1.Console/ListEvents", func(ctx khttp.Context, callCtx context.Context) error {
		id, err := consoleID(ctx.Query().Get("swarm_id"), "swarm_id")
		if err != nil {
			return err
		}
		items, err := consoleSvc.Events(callCtx, id)
		if err != nil {
			return err
		}
		return ctx.JSON(http.StatusOK, map[string]any{"items": items})
	}))

	// SPA 必须最后注册。Kratos 路由器会优先匹配前面已经声明的 API、健康检查和指标端点，
	// 其余浏览器路径再回退到 index.html，从而支持前端刷新深层路由。
	if cfg.WebDir != "" {
		srv.HandlePrefix("/", newSPAHandler(cfg.WebDir))
	}
	return srv
}

func translateRunError(err error) error {
	switch {
	case stdErrors.Is(err, runcontrol.ErrInvalidRequest):
		return kratosErrors.BadRequest("INVALID_RUN", err.Error())
	case stdErrors.Is(err, runcontrol.ErrTemporalDisabled):
		return kratosErrors.ServiceUnavailable("TEMPORAL_DISABLED", err.Error())
	case stdErrors.Is(err, domain.ErrNotFound):
		return kratosErrors.NotFound("NOT_FOUND", err.Error())
	case stdErrors.Is(err, domain.ErrConflict), stdErrors.Is(err, domain.ErrInvalidTransition):
		return kratosErrors.Conflict("RUN_CONFLICT", err.Error())
	default:
		return kratosErrors.InternalServer("INTERNAL_ERROR", "Run 控制面内部错误")
	}
}

// observedHandler 同时接收 Kratos HTTP Context 与中间件产生的调用 Context：前者负责绑定和
// 编码 HTTP 数据，后者必须传入 service/仓储，才能传播 Trace、取消信号与截止时间。
type observedHandler func(khttp.Context, context.Context) error

// withOperation 为手写 REST 路由补齐 Kratos 生成代码同等的执行语义：先设置固定操作名，
// 再进入 recovery、trace、metrics、logging 中间件链。固定操作名不会把资源 ID 写入指标标签。
func withOperation(operation string, next observedHandler) khttp.HandlerFunc {
	return func(ctx khttp.Context) error {
		khttp.SetOperation(ctx, operation)
		handler := ctx.Middleware(func(callCtx context.Context, _ any) (any, error) {
			return nil, next(ctx, callCtx)
		})
		_, err := handler(ctx, nil)
		return err
	}
}

func consoleID(raw, field string) (uuid.UUID, error) {
	value, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, kratosErrors.BadRequest("INVALID_ID", field+" 必须是 UUID")
	}
	return value, nil
}

// unaryBody 是带 protobuf JSON 请求体的通用处理器。
func unaryBody[Req any, Reply any](status int, call func(context.Context, *Req) (*Reply, error)) observedHandler {
	return func(ctx khttp.Context, callCtx context.Context) error {
		request := new(Req)
		if err := ctx.Bind(request); err != nil {
			return err
		}
		reply, err := call(callCtx, request)
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
