// Package swarm 定义蜂群资源及其状态机。
package swarm

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/domain"
)

// Status 是蜂群聚合的生命周期状态。
type Status string

const (
	StatusPending   Status = "PENDING"
	StatusRunning   Status = "RUNNING"
	StatusSucceeded Status = "SUCCEEDED"
	StatusFailed    Status = "FAILED"
	StatusCanceled  Status = "CANCELED"
)

// Swarm 表示一次用户目标的声明式执行实例。
type Swarm struct {
	ID               uuid.UUID
	Name             string
	Goal             string
	Status           Status
	BudgetTokens     int64
	BudgetCostMicros int64
	SpentTokens      int64
	SpentCostMicros  int64
	MaxAgents        int32
	Policy           map[string]any
	Version          int64
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// Transition 只允许控制器沿显式白名单推进状态。
func (s *Swarm) Transition(next Status) error {
	allowed := map[Status]map[Status]bool{
		StatusPending: {StatusRunning: true, StatusCanceled: true},
		StatusRunning: {StatusSucceeded: true, StatusFailed: true, StatusCanceled: true},
	}
	if !allowed[s.Status][next] {
		return fmt.Errorf("%w: swarm %s 不能从 %s 变为 %s", domain.ErrInvalidTransition, s.ID, s.Status, next)
	}
	s.Status = next
	return nil
}

// IsTerminal 表示 Controller 不应再为该蜂群创建新工作。
func (s Swarm) IsTerminal() bool {
	return s.Status == StatusSucceeded || s.Status == StatusFailed || s.Status == StatusCanceled
}
