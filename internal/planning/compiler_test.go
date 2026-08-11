package planning

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestPlanCompilerBuildsStableExecutableDAG(t *testing.T) {
	t.Parallel()
	request, first, second := validCompileRequest()
	compiler := NewPlanCompiler(CompilerOptions{})

	plan, compileErrors := compiler.Compile(request)
	require.Empty(t, compileErrors)
	require.NotNil(t, plan)
	require.Equal(t, []uuid.UUID{first.ID, second.ID}, plan.TopologicalOrder)
	require.Equal(t, []uuid.UUID{first.ID, second.ID}, plan.CriticalPath)
	require.NotEmpty(t, plan.GraphHash)
	require.EqualValues(t, 2_000, plan.EstimatedBudget.MaxTokens)

	// Planner 数组顺序不是语义的一部分，反转 Task 顺序后结果必须相同。
	request.Candidate.Tasks[0], request.Candidate.Tasks[1] = request.Candidate.Tasks[1], request.Candidate.Tasks[0]
	reordered, compileErrors := compiler.Compile(request)
	require.Empty(t, compileErrors)
	require.Equal(t, plan.TopologicalOrder, reordered.TopologicalOrder)
	require.Equal(t, plan.GraphHash, reordered.GraphHash)
}

func TestPlanCompilerRejectsCycleAndMissingDependency(t *testing.T) {
	t.Parallel()
	request, first, second := validCompileRequest()
	request.Candidate.Dependencies = append(request.Candidate.Dependencies,
		Dependency{TaskID: first.ID, DependsOnID: second.ID, Type: DependencyHard},
		Dependency{TaskID: first.ID, DependsOnID: uuid.New(), Type: DependencyHard},
	)

	plan, compileErrors := NewPlanCompiler(CompilerOptions{}).Compile(request)
	require.Nil(t, plan)
	require.True(t, compileErrors.HasCode(CodeCycleDetected))
	require.True(t, compileErrors.HasCode(CodeDependencyNotFound))
}

func TestPlanCompilerRejectsOrphanTask(t *testing.T) {
	t.Parallel()
	request, _, _ := validCompileRequest()
	orphan := validTaskContract(request.RunID, uuid.New(), "orphan")
	request.Candidate.Tasks = append(request.Candidate.Tasks, orphan)

	plan, compileErrors := NewPlanCompiler(CompilerOptions{}).Compile(request)
	require.Nil(t, plan)
	require.True(t, compileErrors.HasCode(CodeOrphanTask))
}

func TestPlanCompilerRejectsCatalogAndBudgetViolations(t *testing.T) {
	t.Parallel()
	request, _, second := validCompileRequest()
	second.Requirements.Capabilities["golang"] = 0.95
	second.Requirements.Tools = append(second.Requirements.Tools, "shell.test")
	second.Requirements.Models = append(second.Requirements.Models, "missing-model")
	second.Requirements.Permissions = append(second.Requirements.Permissions, "production.deploy")
	request.Candidate.Tasks[1] = second
	request.RunBudget.MaxTokens = 1_000

	plan, compileErrors := NewPlanCompiler(CompilerOptions{}).Compile(request)
	require.Nil(t, plan)
	require.True(t, compileErrors.HasCode(CodeCapabilityUnsatisfied))
	require.True(t, compileErrors.HasCode(CodeToolUnavailable))
	require.True(t, compileErrors.HasCode(CodeModelUnavailable))
	require.True(t, compileErrors.HasCode(CodePlanPermissionDenied))
	require.True(t, compileErrors.HasCode(CodePlanOverBudget))
}

func TestPlanCompilerReturnsContractErrorsForRepairLoop(t *testing.T) {
	t.Parallel()
	request, _, second := validCompileRequest()
	second.Goal = ""
	second.Inputs = nil
	request.Candidate.Tasks[1] = second

	plan, compileErrors := NewPlanCompiler(CompilerOptions{}).Compile(request)
	require.Nil(t, plan)
	require.True(t, compileErrors.HasCode(CodeInvalidContract))
	// 多个字段错误应在一次编译中完整返回，便于 Planner 一轮修复。
	var goal, inputs bool
	for _, item := range compileErrors {
		goal = goal || item.Field == "goal"
		inputs = inputs || item.Field == "inputs"
	}
	require.True(t, goal)
	require.True(t, inputs)
}

func TestPlanCompilerRejectsDataDependencyWithoutArtifact(t *testing.T) {
	t.Parallel()
	request, _, _ := validCompileRequest()
	request.Candidate.Dependencies[0].Type = DependencyData

	plan, compileErrors := NewPlanCompiler(CompilerOptions{}).Compile(request)
	require.Nil(t, plan)
	require.True(t, compileErrors.HasCode(CodeInvalidDependency))
}

func TestPlanCompilerRejectsUndeclaredDataArtifact(t *testing.T) {
	t.Parallel()
	request, _, _ := validCompileRequest()
	request.Candidate.Dependencies[0].Type = DependencyData
	request.Candidate.Dependencies[0].Artifact = "missing-artifact"

	plan, compileErrors := NewPlanCompiler(CompilerOptions{}).Compile(request)
	require.Nil(t, plan)
	require.True(t, compileErrors.HasCode(CodeInvalidDependency))
}

func validCompileRequest() (CompileRequest, TaskContract, TaskContract) {
	runID := uuid.New()
	// 固定 UUID 字典序，测试同时验证稳定拓扑顺序。
	firstID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	secondID := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	first := validTaskContract(runID, firstID, "analyze")
	second := validTaskContract(runID, secondID, "implement")
	second.Inputs = []InputSpec{{Name: "analysis", Type: "artifact", Source: "artifact://analysis/1", Required: true}}
	second.ExpectedDurationSeconds = 20
	candidate := PlanCandidate{
		Tasks:        []TaskContract{first, second},
		Dependencies: []Dependency{{TaskID: second.ID, DependsOnID: first.ID, Type: DependencyHard}},
		Deliverables: []Deliverable{{Name: "result", Type: "code_patch", TaskID: second.ID}},
	}
	return CompileRequest{
		RunID:                 runID,
		RunBudget:             ResourceBudget{MaxTokens: 5_000, MaxCostMicros: 1_000, MaxDurationSeconds: 300},
		AvailableCapabilities: map[string]float64{"golang": 0.9},
		AvailableTools:        map[string]bool{"repo.read": true},
		AvailableModels:       map[string]bool{"gpt-test": true},
		AllowedPermissions:    map[string]bool{"repo.read": true},
		Candidate:             candidate,
	}, first, second
}
