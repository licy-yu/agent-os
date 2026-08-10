// Package agent 定义 Agent 模板和运行实例。
package agent

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/domain"
)

// Status 对应 Agent Runtime 的完整生命周期，而不是简单的 idle/running。
type Status string

const (
	StatusRegistered   Status = "REGISTERED"
	StatusIdle         Status = "IDLE"
	StatusReserved     Status = "RESERVED"
	StatusRunning      Status = "RUNNING"
	StatusWaitingTool  Status = "WAITING_TOOL"
	StatusWaitingInput Status = "WAITING_INPUT"
	StatusBackoff      Status = "BACKOFF"
	StatusOffline      Status = "OFFLINE"
	StatusDraining     Status = "DRAINING"
	StatusStopped      Status = "STOPPED"
)

// Template 是可版本化、可复用的 Agent 能力声明。
// Skills 的值归一化为 0~1，表示熟练度或匹配权重。
type Template struct {
	ID                    uuid.UUID
	Name                  string
	Role                  string
	Prompt                string
	Model                 string
	Skills                map[string]float64
	Tools                 []string
	Permissions           []string
	TemplateVersion       string
	ContextWindow         int64
	RiskZone              string
	CostPer1KTokensMicros int64
	Enabled               bool
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

// Instance 是 Scheduler 实际可以 Reserve/Bind 的运行资源。
type Instance struct {
	ID            uuid.UUID
	TemplateID    uuid.UUID
	SwarmID       *uuid.UUID
	Name          string
	Status        Status
	CurrentTaskID *uuid.UUID
	Load          float64
	HeartbeatAt   time.Time
	Version       int64
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// Transition 保护 Agent 状态，避免跳过 Reserve 或 Drain 步骤。
func (a *Instance) Transition(next Status) error {
	allowed := map[Status]map[Status]bool{
		StatusRegistered:   {StatusIdle: true, StatusOffline: true, StatusStopped: true},
		StatusIdle:         {StatusReserved: true, StatusDraining: true, StatusOffline: true},
		StatusReserved:     {StatusRunning: true, StatusIdle: true, StatusOffline: true},
		StatusRunning:      {StatusIdle: true, StatusWaitingTool: true, StatusWaitingInput: true, StatusBackoff: true, StatusOffline: true},
		StatusWaitingTool:  {StatusRunning: true, StatusBackoff: true, StatusOffline: true},
		StatusWaitingInput: {StatusRunning: true, StatusBackoff: true, StatusOffline: true},
		StatusBackoff:      {StatusIdle: true, StatusOffline: true},
		StatusOffline:      {StatusIdle: true, StatusStopped: true},
		StatusDraining:     {StatusStopped: true, StatusOffline: true},
	}
	if !allowed[a.Status][next] {
		return fmt.Errorf("%w: agent %s 不能从 %s 变为 %s", domain.ErrInvalidTransition, a.ID, a.Status, next)
	}
	a.Status = next
	return nil
}
