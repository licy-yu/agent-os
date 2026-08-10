// Package observability 统一初始化 OpenTelemetry Trace 与 Prometheus Metrics。
package observability

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/go-kratos/kratos/v2/middleware"
	kratosmetrics "github.com/go-kratos/kratos/v2/middleware/metrics"
	"github.com/go-kratos/kratos/v2/middleware/tracing"
	"github.com/licy-yu/agent-os/internal/conf"
	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
)

// Telemetry 持有每个进程独立的 Provider、HTTP 指标处理器和 Kratos 中间件仪表。
type Telemetry struct {
	tracerProvider *sdktrace.TracerProvider
	meterProvider  *sdkmetric.MeterProvider
	metricsHandler http.Handler
	requests       otelmetric.Int64Counter
	seconds        otelmetric.Float64Histogram
}

// New 遵循标准 OTEL_EXPORTER_OTLP_* 环境变量。未配置端点时仍生成 Trace ID 供日志关联，
// 但不创建网络导出器；生产接入 Collector 时不需要改变二进制或业务代码。
func New(ctx context.Context, cfg conf.ObservabilityConfig, serviceName, version, environment string) (*Telemetry, error) {
	res, err := resource.New(ctx, resource.WithAttributes(
		semconv.ServiceName(serviceName),
		semconv.ServiceVersion(version),
		attribute.String("deployment.environment.name", environment),
	))
	if err != nil {
		return nil, fmt.Errorf("创建 OpenTelemetry resource: %w", err)
	}
	traceOptions := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.TraceSampleRatio))),
	}
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" || os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != "" {
		exporter, err := otlptracegrpc.New(ctx)
		if err != nil {
			return nil, fmt.Errorf("创建 OTLP trace exporter: %w", err)
		}
		traceOptions = append(traceOptions, sdktrace.WithBatcher(exporter))
	}
	tracerProvider := sdktrace.NewTracerProvider(traceOptions...)
	otel.SetTracerProvider(tracerProvider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	registry := promclient.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	promExporter, err := otelprom.New(otelprom.WithRegisterer(registry))
	if err != nil {
		return nil, fmt.Errorf("创建 Prometheus exporter: %w", err)
	}
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(promExporter), sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(meterProvider)
	meter := meterProvider.Meter("github.com/licy-yu/agent-os")
	requests, err := kratosmetrics.DefaultRequestsCounter(meter, "swarmos_server_requests_total")
	if err != nil {
		return nil, fmt.Errorf("创建请求计数器: %w", err)
	}
	seconds, err := kratosmetrics.DefaultSecondsHistogram(meter, "swarmos_server_request_duration_seconds")
	if err != nil {
		return nil, fmt.Errorf("创建请求延迟直方图: %w", err)
	}
	return &Telemetry{
		tracerProvider: tracerProvider, meterProvider: meterProvider,
		metricsHandler: promhttp.HandlerFor(registry, promhttp.HandlerOpts{EnableOpenMetrics: true}),
		requests:       requests, seconds: seconds,
	}, nil
}

func (t *Telemetry) TracerProvider() trace.TracerProvider { return t.tracerProvider }
func (t *Telemetry) MetricsHandler() http.Handler         { return t.metricsHandler }

// Middlewares 保证 tracing 位于 logging 之前，日志 Valuer 才能读取当前 trace/span ID。
func (t *Telemetry) Middlewares() []middleware.Middleware {
	return []middleware.Middleware{
		tracing.Server(tracing.WithTracerProvider(t.tracerProvider)),
		kratosmetrics.Server(
			kratosmetrics.WithRequests(t.requests),
			kratosmetrics.WithSeconds(t.seconds),
		),
	}
}

func (t *Telemetry) Shutdown(ctx context.Context) error {
	return errors.Join(t.meterProvider.Shutdown(ctx), t.tracerProvider.Shutdown(ctx))
}
