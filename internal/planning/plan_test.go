package planning

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestPlanVersionDefensivelyCopiesCandidateAndStatus(t *testing.T) {
	t.Parallel()
	runID, taskID, planID := uuid.New(), uuid.New(), uuid.New()
	candidate := PlanCandidate{
		Tasks:        []TaskContract{validTaskContract(runID, taskID, "task")},
		Deliverables: []Deliverable{{Name: "report", Type: "report", TaskID: taskID}},
	}
	candidate.Tasks[0].Output.Schema = map[string]any{
		"properties": map[string]any{"summary": map[string]any{"type": "string"}},
	}
	value, err := NewPlanVersion(PlanVersionParams{
		ID: planID, RunID: runID, Version: 1, Status: PlanDraft,
		PlannerVersion: "planner/v1", Candidate: candidate, CreatedAt: time.Now().UTC(),
	})
	require.NoError(t, err)
	originalHash := value.GraphHash()

	// 修改构造参数和读取副本都不能污染版本内部快照。
	candidate.Tasks[0].Goal = "被外部修改"
	copyCandidate := value.Candidate()
	copyCandidate.Tasks[0].Goal = "再次修改"
	require.Equal(t, "完成 task", value.Candidate().Tasks[0].Goal)
	require.Equal(t, originalHash, value.GraphHash())

	validating, err := value.WithStatus(PlanValidating, "开始确定性编译")
	require.NoError(t, err)
	require.Equal(t, PlanDraft, value.Status(), "状态发布必须返回新值，不能修改原版本")
	require.Equal(t, PlanValidating, validating.Status())
	valid, err := validating.WithStatus(PlanValid, "编译通过")
	require.NoError(t, err)
	active, err := valid.WithStatus(PlanActive, "激活")
	require.NoError(t, err)
	require.Equal(t, PlanActive, active.Status())
}

func TestPlanHashIgnoresTaskAndDependencyArrayOrder(t *testing.T) {
	t.Parallel()
	runID := uuid.New()
	taskA := validTaskContract(runID, uuid.New(), "a")
	taskB := validTaskContract(runID, uuid.New(), "b")
	edge := Dependency{TaskID: taskB.ID, DependsOnID: taskA.ID, Type: DependencyHard}
	first := PlanCandidate{
		Tasks: []TaskContract{taskA, taskB}, Dependencies: []Dependency{edge},
		Deliverables: []Deliverable{{Name: "result", Type: "report", TaskID: taskB.ID}},
	}
	second := PlanCandidate{
		Tasks: []TaskContract{taskB, taskA}, Dependencies: []Dependency{edge},
		Deliverables: []Deliverable{{Name: "result", Type: "report", TaskID: taskB.ID}},
	}

	firstHash, err := hashCandidate(first)
	require.NoError(t, err)
	secondHash, err := hashCandidate(second)
	require.NoError(t, err)
	require.Equal(t, firstHash, secondHash)
}

func TestPlanVersionRequiresAppendOnlyParentChain(t *testing.T) {
	t.Parallel()
	_, err := NewPlanVersion(PlanVersionParams{
		ID: uuid.New(), RunID: uuid.New(), Version: 2, Status: PlanDraft,
		PlannerVersion: "planner/v1", CreatedAt: time.Now().UTC(),
	})
	require.Error(t, err)
}
