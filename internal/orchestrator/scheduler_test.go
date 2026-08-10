package orchestrator

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/domain/agent"
	"github.com/licy-yu/agent-os/internal/domain/task"
	"github.com/stretchr/testify/require"
)

func TestFilterCandidatesUsesHardRequirements(t *testing.T) {
	t.Parallel()
	value := &task.Task{Requirements: task.Requirements{
		Skills: map[string]float64{"golang": 0.8, "kratos": 0.7},
		Tools:  []string{"shell"}, Permissions: []string{"repo:write"},
		Models: []string{"gpt-coding"}, MaxContextTokens: 32_000, RiskZone: "trusted",
	}}
	good := Candidate{
		Instance: &agent.Instance{ID: uuid.New(), Status: agent.StatusIdle},
		Template: &agent.Template{Enabled: true, Model: "gpt-coding",
			Skills: map[string]float64{"golang": .9, "kratos": .8},
			Tools:  []string{"shell", "filesystem"}, Permissions: []string{"repo:write"},
			ContextWindow: 128_000, RiskZone: "trusted"},
		BudgetAllowed: true,
	}
	badSkill := good
	badSkill.Instance = &agent.Instance{ID: uuid.New(), Status: agent.StatusIdle}
	badTemplate := *good.Template
	badTemplate.Skills = map[string]float64{"golang": .9, "kratos": .2}
	badSkill.Template = &badTemplate

	filtered := FilterCandidates(value, []Candidate{badSkill, good})
	require.Len(t, filtered, 1)
	require.Equal(t, good.Instance.ID, filtered[0].Instance.ID)
}

func TestScorePrefersProjectContext(t *testing.T) {
	t.Parallel()
	value := &task.Task{Requirements: task.Requirements{Skills: map[string]float64{"golang": .8}}}
	baseTemplate := &agent.Template{Skills: map[string]float64{"golang": .9}, Enabled: true}
	contextAgent := Candidate{
		Instance: &agent.Instance{ID: uuid.New(), Status: agent.StatusIdle}, Template: baseTemplate,
		BudgetAllowed: true, HistorySuccess: .8, ContextAffinity: 1, QualityScore: .8, LatencyScore: .8,
	}
	newAgent := contextAgent
	newAgent.Instance = &agent.Instance{ID: uuid.New(), Status: agent.StatusIdle}
	newAgent.ContextAffinity = .1

	candidates := []Candidate{newAgent, contextAgent}
	ScoreCandidates(value, candidates, defaultWeights)
	require.Equal(t, contextAgent.Instance.ID, candidates[0].Instance.ID)
}

func TestQueueSortRaisesCriticalPath(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 11, 1, 0, 0, 0, time.UTC)
	normal := QueuedTask{Task: &task.Task{ID: uuid.New(), Priority: 50, CreatedAt: now.Add(-time.Minute)}}
	blocking := QueuedTask{Task: &task.Task{ID: uuid.New(), Priority: 50, CreatedAt: now.Add(-time.Minute)}, BlockedChildren: 3}
	items := []QueuedTask{normal, blocking}
	SortQueue(items, now)
	require.Equal(t, blocking.Task.ID, items[0].Task.ID)
}
