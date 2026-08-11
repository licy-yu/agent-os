package server

import (
	"context"
	"errors"
	"fmt"
)

// DependencyProbe 是一个具名外部依赖检查。Name 只进入服务端日志/错误链，HTTP 响应
// 仍返回统一 unavailable，避免把数据库或中间件地址泄露给未认证的探针调用方。
type DependencyProbe struct {
	Name  string
	Check func(context.Context) error
}

// ReadinessProbe 是 /readyz 使用的聚合函数。
type ReadinessProbe func(context.Context) error

// NewReadinessProbe 按确定顺序执行全部检查并聚合错误。每个具体 Client 都接受同一个
// Context；HTTP handler 的总超时到达后，后续检查会立即失败，不会长期占用连接池。
func NewReadinessProbe(probes ...DependencyProbe) ReadinessProbe {
	return func(ctx context.Context) error {
		failures := make([]error, 0)
		for _, probe := range probes {
			if probe.Check == nil {
				failures = append(failures, fmt.Errorf("%s: 未配置检查函数", probe.Name))
				continue
			}
			if err := probe.Check(ctx); err != nil {
				failures = append(failures, fmt.Errorf("%s: %w", probe.Name, err))
			}
		}
		return errors.Join(failures...)
	}
}
