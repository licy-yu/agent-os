// Package server 构造 Kratos 传输层。
package server

import (
	"context"
	stdErrors "errors"
	"net/http"
	"strconv"
	"time"

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
	"github.com/licy-yu/agent-os/internal/effect"
	"github.com/licy-yu/agent-os/internal/interaction"
	"github.com/licy-yu/agent-os/internal/observability"
	"github.com/licy-yu/agent-os/internal/runcontrol"
	"github.com/licy-yu/agent-os/internal/safetycontrol"
	"github.com/licy-yu/agent-os/internal/service"
	"google.golang.org/protobuf/types/known/emptypb"
)

// NewHTTPServer 注册显式 REST 路由。
// API 内部仍复用 protobuf 消息与 service 方法，因此它不是另一套业务实现。

func NewHTTPServer(cfg conf.ServerConfig, security conf.SecurityConfig,
	svc *service.ControlPlaneService, runSvc *runcontrol.Service,
	safetySvc *safetycontrol.Service, consoleSvc *consoleview.Service,
	readiness ReadinessProbe, telemetry *observability.Telemetry, logger log.Logger,
) *khttp.Server {
	middlewares := []middleware.Middleware{recovery.Recovery()}
	middlewares = append(middlewares, telemetry.Middlewares()...)
	middlewares = append(middlewares, logging.Server(logger))
	srv := khttp.NewServer(
		khttp.Address(cfg.HTTPAddr),
		khttp.Middleware(middlewares...),
		// Filter 位于 Kratos 路由分发之前，所有 /api/*（包括手写 REST 路由和后续
		// 新增路由）都会先完成 API Key 校验和可信 Principal 注入；静态页面、
		// healthz 与 metrics 不携带业务数据，仍可供浏览器和探针直接访问。
		khttp.Filter(APIAuthFilter(security)),
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
	// healthz 保留轻量进程/数据库兼容检查；readyz 额外验证调度和 Durable Runtime
	// 的全部关键依赖。Compose 只在 readyz 通过后启动普通 Worker，避免半就绪接单。
	srv.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		probeCtx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if readiness == nil || readiness(probeCtx) != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("unavailable\n"))
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ready\n"))
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
		runID := ctx.Query().Get("swarm_id")
		if runID != "" {
			parsed, parseErr := consoleID(runID, "swarm_id")
			if parseErr != nil {
				return parseErr
			}
			if _, getErr := runSvc.Get(callCtx, parsed); getErr != nil {
				return translateRunError(getErr)
			}
		}
		return ctx.Returns(svc.ListAgents(callCtx, &v1.ListAgentsRequest{SwarmId: runID, Page: page}))
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
		runID := ctx.Query().Get("swarm_id")
		if runID != "" {
			parsed, parseErr := consoleID(runID, "swarm_id")
			if parseErr != nil {
				return parseErr
			}
			if _, getErr := runSvc.Get(callCtx, parsed); getErr != nil {
				return translateRunError(getErr)
			}
		}
		return ctx.Returns(svc.ListTasks(callCtx, &v1.ListTasksRequest{
			SwarmId: runID, Status: ctx.Query().Get("status"), Page: page,
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

	// V1.5 安全控制面统一暴露 Run 任务、Timeline、Artifact、Effect、Verification 与
	// Interaction。写端点强制使用 Idempotency-Key；tenantId 和审计 actor 永远来自
	// 认证中间件注入的 Principal，而不是请求 JSON。
	route.GET("/runs/{id}/tasks", withOperation("/swarmos.safety.v1.SafetyControl/ListRunTasks", func(ctx khttp.Context, callCtx context.Context) error {
		runID, err := consoleID(ctx.Vars().Get("id"), "run.id")
		if err != nil {
			return err
		}
		// 旧 Task protobuf Store 尚未显式携带 tenant_id；先通过 Run Service 验证当前
		// Principal 确实拥有该 Run，再复用兼容列表，从而不形成跨租户枚举入口。
		if _, err := runSvc.Get(callCtx, runID); err != nil {
			return translateRunError(err)
		}
		return ctx.Returns(svc.ListTasks(callCtx, &v1.ListTasksRequest{
			SwarmId: runID.String(), Page: &v1.PageRequest{PageSize: 200},
		}))
	}))

	route.GET("/runs/{id}/timeline", withOperation("/swarmos.safety.v1.SafetyControl/ListTimeline", func(ctx khttp.Context, callCtx context.Context) error {
		runID, err := consoleID(ctx.Vars().Get("id"), "run.id")
		if err != nil {
			return err
		}
		limit, err := safetyLimit(ctx)
		if err != nil {
			return err
		}
		items, err := safetySvc.ListTimeline(callCtx, safetycontrol.TimelineQuery{RunID: runID, Limit: limit})
		if err != nil {
			return translateSafetyError(err)
		}
		return ctx.JSON(http.StatusOK, map[string]any{"items": items})
	}))

	route.GET("/runs/{id}/artifacts", withOperation("/swarmos.safety.v1.SafetyControl/ListArtifacts", func(ctx khttp.Context, callCtx context.Context) error {
		runID, err := consoleID(ctx.Vars().Get("id"), "run.id")
		if err != nil {
			return err
		}
		limit, err := safetyLimit(ctx)
		if err != nil {
			return err
		}
		items, err := safetySvc.ListArtifacts(callCtx, safetycontrol.ArtifactQuery{
			RunID: runID, Status: ctx.Query().Get("status"), Limit: limit,
		})
		if err != nil {
			return translateSafetyError(err)
		}
		return ctx.JSON(http.StatusOK, map[string]any{"items": items})
	}))

	route.GET("/artifacts/{id}", withOperation("/swarmos.safety.v1.SafetyControl/GetArtifact", func(ctx khttp.Context, callCtx context.Context) error {
		id, err := consoleID(ctx.Vars().Get("id"), "artifact.id")
		if err != nil {
			return err
		}
		value, err := safetySvc.GetArtifact(callCtx, id)
		if err != nil {
			return translateSafetyError(err)
		}
		return ctx.JSON(http.StatusOK, value)
	}))

	route.GET("/artifacts/{id}/lineage", withOperation("/swarmos.safety.v1.SafetyControl/GetArtifactLineage", func(ctx khttp.Context, callCtx context.Context) error {
		id, err := consoleID(ctx.Vars().Get("id"), "artifact.id")
		if err != nil {
			return err
		}
		depth := 0
		if raw := ctx.Query().Get("depth"); raw != "" {
			parsed, parseErr := strconv.Atoi(raw)
			if parseErr != nil {
				return kratosErrors.BadRequest("INVALID_DEPTH", "depth 必须是整数")
			}
			depth = parsed
		}
		value, err := safetySvc.GetArtifactLineage(callCtx, id, safetycontrol.ArtifactLineageQuery{
			Direction: ctx.Query().Get("direction"), Depth: depth,
		})
		if err != nil {
			return translateSafetyError(err)
		}
		return ctx.JSON(http.StatusOK, value)
	}))

	route.GET("/runs/{id}/scheduler-decisions", withOperation("/swarmos.safety.v1.SafetyControl/ListSchedulerDecisions", func(ctx khttp.Context, callCtx context.Context) error {
		runID, err := consoleID(ctx.Vars().Get("id"), "run.id")
		if err != nil {
			return err
		}
		limit, err := safetyLimit(ctx)
		if err != nil {
			return err
		}
		items, err := safetySvc.ListSchedulerDecisions(callCtx, safetycontrol.SchedulerDecisionQuery{
			RunID: runID, Limit: limit,
		})
		if err != nil {
			return translateSafetyError(err)
		}
		return ctx.JSON(http.StatusOK, map[string]any{"items": items})
	}))

	// Interaction Inbox 支持 runId 可选过滤；status=PENDING 由应用层兼容映射为 WAITING。
	route.GET("/interactions", withOperation("/swarmos.safety.v1.SafetyControl/ListInteractions", func(ctx khttp.Context, callCtx context.Context) error {
		runID, err := optionalQueryID(ctx, "runId", "run_id")
		if err != nil {
			return err
		}
		limit, err := safetyLimit(ctx)
		if err != nil {
			return err
		}
		items, err := safetySvc.ListInteractions(callCtx, safetycontrol.InteractionQuery{
			RunID: runID, Status: interaction.Status(ctx.Query().Get("status")), Limit: limit,
		})
		if err != nil {
			return translateSafetyError(err)
		}
		return ctx.JSON(http.StatusOK, map[string]any{"items": items})
	}))

	route.GET("/interactions/{id}", withOperation("/swarmos.safety.v1.SafetyControl/GetInteraction", func(ctx khttp.Context, callCtx context.Context) error {
		id, err := consoleID(ctx.Vars().Get("id"), "interaction.id")
		if err != nil {
			return err
		}
		value, err := safetySvc.GetInteraction(callCtx, id)
		if err != nil {
			return translateSafetyError(err)
		}
		return ctx.JSON(http.StatusOK, value)
	}))

	route.POST("/interactions", withOperation("/swarmos.safety.v1.SafetyControl/CreateInteraction", func(ctx khttp.Context, callCtx context.Context) error {
		request := safetycontrol.CreateInteractionRequest{}
		if err := ctx.Bind(&request); err != nil {
			return kratosErrors.BadRequest("INVALID_JSON", "请求体不是合法 Interaction JSON")
		}
		value, err := safetySvc.CreateInteraction(callCtx, idempotencyKey(ctx), request)
		if err != nil {
			return translateSafetyError(err)
		}
		return ctx.JSON(http.StatusCreated, value)
	}))

	registerInteractionCommand := func(path, operation string,
		command func(context.Context, uuid.UUID, string, safetycontrol.InteractionCommandRequest) (*safetycontrol.InteractionView, error),
	) {
		route.POST(path, withOperation(operation, func(ctx khttp.Context, callCtx context.Context) error {
			id, err := consoleID(ctx.Vars().Get("id"), "interaction.id")
			if err != nil {
				return err
			}
			request := safetycontrol.InteractionCommandRequest{}
			if err := ctx.Bind(&request); err != nil {
				return kratosErrors.BadRequest("INVALID_JSON", "请求体不是合法 Interaction 命令 JSON")
			}
			// 当前控制台的“提交并继续”只发送 resolution。resolve 端点的动作名没有
			// 歧义，因此在传输边界补成稳定的 resolve；Service 仍会校验它属于创建时
			// 固化的 allowedActions，不能借此扩大权限。
			if path == "/interactions/{id}:resolve" && request.Action == "" {
				request.Action = "resolve"
			}
			value, err := command(callCtx, id, idempotencyKey(ctx), request)
			if err != nil {
				return translateSafetyError(err)
			}
			return ctx.JSON(http.StatusOK, value)
		}))
	}
	registerInteractionCommand("/interactions/{id}:approve", "/swarmos.safety.v1.SafetyControl/ApproveInteraction", safetySvc.Approve)
	registerInteractionCommand("/interactions/{id}:reject", "/swarmos.safety.v1.SafetyControl/RejectInteraction", safetySvc.Reject)
	registerInteractionCommand("/interactions/{id}:resolve", "/swarmos.safety.v1.SafetyControl/ResolveInteraction", safetySvc.Resolve)

	route.GET("/runs/{id}/effects", withOperation("/swarmos.safety.v1.SafetyControl/ListEffects", func(ctx khttp.Context, callCtx context.Context) error {
		runID, err := consoleID(ctx.Vars().Get("id"), "run.id")
		if err != nil {
			return err
		}
		limit, err := safetyLimit(ctx)
		if err != nil {
			return err
		}
		risk := ctx.Query().Get("riskLevel")
		if risk == "" {
			risk = ctx.Query().Get("risk_level")
		}
		items, err := safetySvc.ListEffects(callCtx, safetycontrol.EffectQuery{
			RunID: runID, Status: effect.Status(ctx.Query().Get("status")),
			RiskLevel: effect.RiskLevel(risk), Limit: limit,
		})
		if err != nil {
			return translateSafetyError(err)
		}
		return ctx.JSON(http.StatusOK, map[string]any{"items": items})
	}))

	route.GET("/effects/{id}", withOperation("/swarmos.safety.v1.SafetyControl/GetEffect", func(ctx khttp.Context, callCtx context.Context) error {
		id, err := consoleID(ctx.Vars().Get("id"), "effect.id")
		if err != nil {
			return err
		}
		value, err := safetySvc.GetEffect(callCtx, id)
		if err != nil {
			return translateSafetyError(err)
		}
		return ctx.JSON(http.StatusOK, value)
	}))

	registerEffectCommand := func(path, operation string,
		command func(context.Context, uuid.UUID, string, safetycontrol.EffectCommandRequest) (*safetycontrol.EffectView, error),
	) {
		route.POST(path, withOperation(operation, func(ctx khttp.Context, callCtx context.Context) error {
			id, err := consoleID(ctx.Vars().Get("id"), "effect.id")
			if err != nil {
				return err
			}
			request := safetycontrol.EffectCommandRequest{}
			if err := ctx.Bind(&request); err != nil {
				return kratosErrors.BadRequest("INVALID_JSON", "请求体不是合法 Effect 命令 JSON")
			}
			value, err := command(callCtx, id, idempotencyKey(ctx), request)
			if err != nil {
				return translateSafetyError(err)
			}
			return ctx.JSON(http.StatusOK, value)
		}))
	}
	registerEffectCommand("/effects/{id}:reconcile", "/swarmos.safety.v1.SafetyControl/ReconcileEffect", safetySvc.Reconcile)
	registerEffectCommand("/effects/{id}:compensate", "/swarmos.safety.v1.SafetyControl/CompensateEffect", safetySvc.Compensate)

	route.GET("/runs/{id}/verifications", withOperation("/swarmos.safety.v1.SafetyControl/ListVerificationRuns", func(ctx khttp.Context, callCtx context.Context) error {
		runID, err := consoleID(ctx.Vars().Get("id"), "run.id")
		if err != nil {
			return err
		}
		limit, err := safetyLimit(ctx)
		if err != nil {
			return err
		}
		items, err := safetySvc.ListVerificationRuns(callCtx, safetycontrol.VerificationQuery{
			RunID: runID, Limit: limit,
		})
		if err != nil {
			return translateSafetyError(err)
		}
		return ctx.JSON(http.StatusOK, map[string]any{"items": items})
	}))

	route.GET("/verifications/{id}", withOperation("/swarmos.safety.v1.SafetyControl/GetVerificationRun", func(ctx khttp.Context, callCtx context.Context) error {
		id, err := consoleID(ctx.Vars().Get("id"), "verification.id")
		if err != nil {
			return err
		}
		value, err := safetySvc.GetVerificationRun(callCtx, id)
		if err != nil {
			return translateSafetyError(err)
		}
		return ctx.JSON(http.StatusOK, value)
	}))

	getManifest := func(ctx khttp.Context, callCtx context.Context) error {
		id, err := consoleID(ctx.Vars().Get("id"), "manifest.id")
		if err != nil {
			return err
		}
		value, err := safetySvc.GetCompletionManifest(callCtx, id)
		if err != nil {
			return translateSafetyError(err)
		}
		return ctx.JSON(http.StatusOK, value)
	}
	route.GET("/completion-manifests/{id}", withOperation("/swarmos.safety.v1.SafetyControl/GetCompletionManifest", getManifest))
	// /manifests 是早期 V1.5 设计稿中的短别名，保留它可避免控制台滚动升级期间 404。
	route.GET("/manifests/{id}", withOperation("/swarmos.safety.v1.SafetyControl/GetCompletionManifestAlias", getManifest))

	// Console API 是只读的聚合视图，使用普通 JSON，避免把运维查询混入领域 Protobuf。
	route.GET("/console/overview", withOperation("/swarmos.console.v1.Console/Overview", func(ctx khttp.Context, callCtx context.Context) error {
		id, err := consoleID(ctx.Query().Get("swarm_id"), "swarm_id")
		if err != nil {
			return err
		}
		if _, err := runSvc.Get(callCtx, id); err != nil {
			return translateRunError(err)
		}
		value, err := consoleSvc.Overview(callCtx, id)
		if err != nil {
			return err
		}
		return ctx.JSON(http.StatusOK, value)
	}))
	route.GET("/console/attempts", withOperation("/swarmos.console.v1.Console/ListAttempts", func(ctx khttp.Context, callCtx context.Context) error {
		runID, err := consoleID(ctx.Query().Get("run_id"), "run_id")
		if err != nil {
			return err
		}
		if _, err := runSvc.Get(callCtx, runID); err != nil {
			return translateRunError(err)
		}
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
		if _, err := runSvc.Get(callCtx, id); err != nil {
			return translateRunError(err)
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

// translateSafetyError 把领域错误稳定映射为 HTTP 语义。跨租户与不存在都返回 404；
// CAS、幂等异参与状态机冲突返回 409，便于前端刷新读模型后再由用户决定是否重试。
func translateSafetyError(err error) error {
	switch {
	case stdErrors.Is(err, safetycontrol.ErrInvalidRequest),
		stdErrors.Is(err, safetycontrol.ErrIdempotencyRequired),
		stdErrors.Is(err, effect.ErrInvalidRequest),
		stdErrors.Is(err, interaction.ErrInvalidInteraction),
		stdErrors.Is(err, interaction.ErrInvalidResponse):
		return kratosErrors.BadRequest("INVALID_SAFETY_REQUEST", err.Error())
	case stdErrors.Is(err, safetycontrol.ErrForbidden):
		return kratosErrors.Forbidden("FORBIDDEN", err.Error())
	case stdErrors.Is(err, safetycontrol.ErrTenantBoundary), stdErrors.Is(err, domain.ErrNotFound):
		return kratosErrors.NotFound("NOT_FOUND", "资源不存在")
	case stdErrors.Is(err, safetycontrol.ErrVersionConflict),
		stdErrors.Is(err, safetycontrol.ErrIdempotencyConflict),
		stdErrors.Is(err, safetycontrol.ErrApprovalRequired),
		stdErrors.Is(err, effect.ErrInvalidTransition),
		stdErrors.Is(err, interaction.ErrInvalidTransition),
		stdErrors.Is(err, domain.ErrConflict),
		stdErrors.Is(err, domain.ErrInvalidTransition):
		return kratosErrors.Conflict("SAFETY_CONFLICT", err.Error())
	default:
		return kratosErrors.InternalServer("INTERNAL_ERROR", "安全控制面内部错误")
	}
}

// safetyLimit 允许 limit 与旧分页参数 page_size 共存；应用层会把 0 归一化为默认 100，
// 并把过大的值钳制到安全默认值。
func safetyLimit(ctx khttp.Context) (int, error) {
	raw := ctx.Query().Get("limit")
	if raw == "" {
		raw = ctx.Query().Get("page_size")
	}
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, kratosErrors.BadRequest("INVALID_LIMIT", "limit 必须是整数")
	}
	return value, nil
}

func optionalQueryID(ctx khttp.Context, names ...string) (uuid.UUID, error) {
	for _, name := range names {
		if raw := ctx.Query().Get(name); raw != "" {
			return consoleID(raw, name)
		}
	}
	return uuid.Nil, nil
}

func idempotencyKey(ctx khttp.Context) string {
	return ctx.Header().Get("Idempotency-Key")
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
