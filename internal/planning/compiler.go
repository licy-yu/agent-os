package planning

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/google/uuid"
)

// CompileErrorCode 是 Plan Repair Loop 可稳定消费的机器错误码。
type CompileErrorCode string

const (
	CodeInvalidPlan           CompileErrorCode = "INVALID_PLAN"
	CodeInvalidContract       CompileErrorCode = "INVALID_CONTRACT"
	CodeDuplicateTask         CompileErrorCode = "DUPLICATE_TASK"
	CodeInvalidDependency     CompileErrorCode = "INVALID_DEPENDENCY"
	CodeDependencyNotFound    CompileErrorCode = "DEPENDENCY_NOT_FOUND"
	CodeCycleDetected         CompileErrorCode = "CYCLE_DETECTED"
	CodeOrphanTask            CompileErrorCode = "ORPHAN_TASK"
	CodeNoDeliverable         CompileErrorCode = "NO_DELIVERABLE"
	CodeCapabilityUnsatisfied CompileErrorCode = "CAPABILITY_UNSATISFIED"
	CodeToolUnavailable       CompileErrorCode = "TOOL_UNAVAILABLE"
	CodeModelUnavailable      CompileErrorCode = "MODEL_UNAVAILABLE"
	CodePlanPermissionDenied  CompileErrorCode = "PLAN_PERMISSION_DENIED"
	CodePlanOverBudget        CompileErrorCode = "PLAN_OVER_BUDGET"
	CodePlanTooLarge          CompileErrorCode = "PLAN_TOO_LARGE"
	CodePlanTooDeep           CompileErrorCode = "PLAN_TOO_DEEP"
)

// CompileError 精确定位到 Task、依赖或字段，Planner 不需要解析自然语言错误。
type CompileError struct {
	Code        CompileErrorCode
	TaskID      uuid.UUID
	DependsOnID uuid.UUID
	Field       string
	Message     string
}

func (e CompileError) Error() string {
	location := e.Field
	if e.TaskID != uuid.Nil {
		location = e.TaskID.String() + ":" + location
	}
	return fmt.Sprintf("%s [%s] %s", e.Code, strings.TrimSuffix(location, ":"), e.Message)
}

// CompileErrors 保留全部确定性编译错误，供 Planner 一次性修复多个问题。
type CompileErrors []CompileError

func (e CompileErrors) Error() string {
	if len(e) == 0 {
		return ""
	}
	parts := make([]string, len(e))
	for index := range e {
		parts[index] = e[index].Error()
	}
	return strings.Join(parts, "; ")
}

// HasCode 便于 API 和测试按机器码处理编译失败。
func (e CompileErrors) HasCode(code CompileErrorCode) bool {
	for _, item := range e {
		if item.Code == code {
			return true
		}
	}
	return false
}

// CompilerOptions 限制异常大的图，防止 Planner 输出消耗控制面过多 CPU/内存。
type CompilerOptions struct {
	MaxTasks int
	MaxDepth int
}

// DefaultCompilerOptions 是生产默认值；调用方可以为特定租户设置更严格上限。
func DefaultCompilerOptions() CompilerOptions {
	return CompilerOptions{MaxTasks: 200, MaxDepth: 32}
}

// CompileRequest 包含候选计划以及当前控制面的事实快照。所有 Catalog 均由平台提供，
// 不能信任 Planner 在 Candidate 中自行声明“工具可用”或“拥有权限”。
type CompileRequest struct {
	RunID                 uuid.UUID
	RunBudget             ResourceBudget
	AvailableCapabilities map[string]float64
	AvailableTools        map[string]bool
	AvailableModels       map[string]bool
	AllowedPermissions    map[string]bool
	Candidate             PlanCandidate
}

// ExecutablePlan 是 Compiler 的成功输出。TopologicalOrder 和 CriticalPath 都使用稳定的
// UUID 顺序解决同分歧义，因此相同输入在不同进程上得到完全一致的结果。
type ExecutablePlan struct {
	RunID            uuid.UUID
	Tasks            []TaskContract
	Dependencies     []Dependency
	Deliverables     []Deliverable
	TopologicalOrder []uuid.UUID
	CriticalPath     []uuid.UUID
	EstimatedBudget  ResourceBudget
	GraphHash        string
}

// PlanCompiler 只持有确定性限制参数，不包含时钟、随机数、数据库或模型客户端。
type PlanCompiler struct {
	options CompilerOptions
}

func NewPlanCompiler(options CompilerOptions) *PlanCompiler {
	defaults := DefaultCompilerOptions()
	if options.MaxTasks <= 0 {
		options.MaxTasks = defaults.MaxTasks
	}
	if options.MaxDepth <= 0 {
		options.MaxDepth = defaults.MaxDepth
	}
	return &PlanCompiler{options: options}
}

// Compile 依次验证合同、Catalog、依赖图、最终交付路径和预算。任何失败都会返回 nil Plan；
// 调用方不得执行部分成功的 Candidate。
func (c *PlanCompiler) Compile(request CompileRequest) (*ExecutablePlan, CompileErrors) {
	if c == nil {
		c = NewPlanCompiler(CompilerOptions{})
	}
	errorsFound := make(CompileErrors, 0)
	add := func(value CompileError) { errorsFound = append(errorsFound, value) }
	if request.RunID == uuid.Nil {
		add(CompileError{Code: CodeInvalidPlan, Field: "run_id", Message: "不能为空"})
	}
	if len(request.Candidate.Tasks) == 0 {
		add(CompileError{Code: CodeInvalidPlan, Field: "tasks", Message: "至少需要一个 Task"})
	}
	if len(request.Candidate.Tasks) > c.options.MaxTasks {
		add(CompileError{Code: CodePlanTooLarge, Field: "tasks", Message: fmt.Sprintf("Task 数 %d 超过上限 %d", len(request.Candidate.Tasks), c.options.MaxTasks)})
	}
	validateBudgetShape(request.RunBudget, "run_budget", add)
	validateBudgetShape(request.Candidate.EstimatedBudget, "estimated_budget", add)

	// 先按 UUID 排序，确保 map 输入顺序或 Planner 数组顺序不会改变错误输出。
	tasks := append([]TaskContract(nil), request.Candidate.Tasks...)
	sort.SliceStable(tasks, func(i, j int) bool { return tasks[i].ID.String() < tasks[j].ID.String() })
	taskByID := make(map[uuid.UUID]TaskContract, len(tasks))
	for _, value := range tasks {
		if value.ID != uuid.Nil {
			if _, exists := taskByID[value.ID]; exists {
				add(CompileError{Code: CodeDuplicateTask, TaskID: value.ID, Field: "id", Message: "Task ID 重复"})
				continue
			}
			taskByID[value.ID] = value
		}
		for _, violation := range ValidateContract(value) {
			add(CompileError{Code: CodeInvalidContract, TaskID: value.ID, Field: violation.Field, Message: violation.Message})
		}
		if request.RunID != uuid.Nil && value.RunID != request.RunID {
			add(CompileError{Code: CodeInvalidContract, TaskID: value.ID, Field: "run_id", Message: "必须与待编译 Run 一致"})
		}
	}

	capabilities := normalizeLevels(request.AvailableCapabilities)
	tools := normalizeAvailability(request.AvailableTools)
	models := normalizeAvailability(request.AvailableModels)
	permissions := normalizeAvailability(request.AllowedPermissions)
	for _, value := range tasks {
		for _, name := range sortedFloatKeys(value.Requirements.Capabilities) {
			required := value.Requirements.Capabilities[name]
			available, ok := capabilities[strings.ToLower(strings.TrimSpace(name))]
			if !ok || available < required {
				add(CompileError{Code: CodeCapabilityUnsatisfied, TaskID: value.ID,
					Field:   "requirements.capabilities." + name,
					Message: fmt.Sprintf("要求 %.2f，当前最高可用 %.2f", required, available)})
			}
		}
		validateAvailableNames(value.ID, "requirements.tools", value.Requirements.Tools, tools, CodeToolUnavailable, add)
		validateAvailableNames(value.ID, "requirements.models", value.Requirements.Models, models, CodeModelUnavailable, add)
		validateAvailableNames(value.ID, "requirements.permissions", value.Requirements.Permissions, permissions, CodePlanPermissionDenied, add)
	}

	dependencies := append([]Dependency(nil), request.Candidate.Dependencies...)
	sort.SliceStable(dependencies, func(i, j int) bool { return dependencyKey(dependencies[i]) < dependencyKey(dependencies[j]) })
	adjacency := make(map[uuid.UUID][]uuid.UUID, len(taskByID))
	prerequisites := make(map[uuid.UUID][]uuid.UUID, len(taskByID))
	indegree := make(map[uuid.UUID]int, len(taskByID))
	for id := range taskByID {
		indegree[id] = 0
	}
	seenEdges := make(map[string]struct{}, len(dependencies))
	for _, edge := range dependencies {
		valid := true
		if edge.TaskID == uuid.Nil || edge.DependsOnID == uuid.Nil {
			add(CompileError{Code: CodeInvalidDependency, TaskID: edge.TaskID, DependsOnID: edge.DependsOnID, Field: "dependency", Message: "两端 Task ID 均不能为空"})
			valid = false
		}
		if edge.TaskID == edge.DependsOnID && edge.TaskID != uuid.Nil {
			add(CompileError{Code: CodeInvalidDependency, TaskID: edge.TaskID, DependsOnID: edge.DependsOnID, Field: "dependency", Message: "Task 不能依赖自身"})
			valid = false
		}
		if !edge.Type.valid() {
			add(CompileError{Code: CodeInvalidDependency, TaskID: edge.TaskID, DependsOnID: edge.DependsOnID, Field: "dependency.type", Message: fmt.Sprintf("未知类型 %q", edge.Type)})
			valid = false
		}
		if edge.Type == DependencyData && strings.TrimSpace(edge.Artifact) == "" {
			add(CompileError{Code: CodeInvalidDependency, TaskID: edge.TaskID, DependsOnID: edge.DependsOnID, Field: "dependency.artifact", Message: "DATA 依赖必须声明 Artifact"})
			valid = false
		}
		if _, exists := taskByID[edge.TaskID]; !exists {
			add(CompileError{Code: CodeDependencyNotFound, TaskID: edge.TaskID, DependsOnID: edge.DependsOnID, Field: "dependency.task_id", Message: "引用的 Task 不存在"})
			valid = false
		}
		if _, exists := taskByID[edge.DependsOnID]; !exists {
			add(CompileError{Code: CodeDependencyNotFound, TaskID: edge.TaskID, DependsOnID: edge.DependsOnID, Field: "dependency.depends_on_id", Message: "引用的前置 Task 不存在"})
			valid = false
		}
		if valid && edge.Type == DependencyData && !taskProducesArtifact(taskByID[edge.DependsOnID], edge.Artifact) {
			add(CompileError{Code: CodeInvalidDependency, TaskID: edge.TaskID, DependsOnID: edge.DependsOnID,
				Field: "dependency.artifact", Message: "前置 Task 的输出合同未声明该 Artifact"})
			valid = false
		}
		pair := edge.TaskID.String() + "\x00" + edge.DependsOnID.String()
		if _, exists := seenEdges[pair]; exists {
			add(CompileError{Code: CodeInvalidDependency, TaskID: edge.TaskID, DependsOnID: edge.DependsOnID, Field: "dependency", Message: "同一 Task 对之间不能重复声明依赖"})
			valid = false
		} else {
			seenEdges[pair] = struct{}{}
		}
		if !valid {
			continue
		}
		// DependsOnID -> TaskID 是执行方向：前置任务完成后才能推进下游。
		adjacency[edge.DependsOnID] = append(adjacency[edge.DependsOnID], edge.TaskID)
		prerequisites[edge.TaskID] = append(prerequisites[edge.TaskID], edge.DependsOnID)
		indegree[edge.TaskID]++
	}
	for id := range adjacency {
		sortUUIDs(adjacency[id])
	}
	for id := range prerequisites {
		sortUUIDs(prerequisites[id])
	}

	producers := c.validateDeliverables(request.Candidate.Deliverables, taskByID, add)
	topologicalOrder := stableTopologicalOrder(indegree, adjacency)
	acyclic := len(topologicalOrder) == len(taskByID)
	if !acyclic && len(taskByID) > 0 {
		remaining := make([]uuid.UUID, 0)
		seen := make(map[uuid.UUID]struct{}, len(topologicalOrder))
		for _, id := range topologicalOrder {
			seen[id] = struct{}{}
		}
		for id := range taskByID {
			if _, ok := seen[id]; !ok {
				remaining = append(remaining, id)
			}
		}
		sortUUIDs(remaining)
		add(CompileError{Code: CodeCycleDetected, Field: "dependencies", Message: "存在环或被环阻塞的 Task: " + joinUUIDs(remaining)})
	}
	if acyclic {
		depth := graphDepth(topologicalOrder, prerequisites)
		if depth > c.options.MaxDepth {
			add(CompileError{Code: CodePlanTooDeep, Field: "dependencies", Message: fmt.Sprintf("DAG 深度 %d 超过上限 %d", depth, c.options.MaxDepth)})
		}
		for _, orphan := range findOrphans(taskByID, producers, prerequisites) {
			add(CompileError{Code: CodeOrphanTask, TaskID: orphan, Field: "deliverables", Message: "Task 不在任何最终交付物的依赖路径上"})
		}
	}

	estimatedBudget := deriveEstimatedBudget(request.Candidate.EstimatedBudget, tasks)
	checkBudget(request.RunBudget, estimatedBudget, add)
	graphHash, hashErr := hashCandidate(request.Candidate)
	if hashErr != nil {
		add(CompileError{Code: CodeInvalidPlan, Field: "candidate", Message: hashErr.Error()})
	}

	sortCompileErrors(errorsFound)
	if len(errorsFound) > 0 {
		return nil, errorsFound
	}
	candidate, _ := cloneCandidate(request.Candidate)
	compiledTasks := make([]TaskContract, 0, len(topologicalOrder))
	compiledByID := make(map[uuid.UUID]TaskContract, len(candidate.Tasks))
	for _, value := range candidate.Tasks {
		compiledByID[value.ID] = value
	}
	for _, id := range topologicalOrder {
		compiledTasks = append(compiledTasks, compiledByID[id])
	}
	criticalPath := longestPath(topologicalOrder, adjacency, compiledByID)
	return &ExecutablePlan{
		RunID: request.RunID, Tasks: compiledTasks,
		Dependencies: candidate.Dependencies, Deliverables: candidate.Deliverables,
		TopologicalOrder: append([]uuid.UUID(nil), topologicalOrder...),
		CriticalPath:     append([]uuid.UUID(nil), criticalPath...),
		EstimatedBudget:  estimatedBudget, GraphHash: graphHash,
	}, nil
}

func (c *PlanCompiler) validateDeliverables(values []Deliverable, tasks map[uuid.UUID]TaskContract, add func(CompileError)) []uuid.UUID {
	if len(values) == 0 {
		add(CompileError{Code: CodeNoDeliverable, Field: "deliverables", Message: "至少需要一个最终交付物"})
		return nil
	}
	items := append([]Deliverable(nil), values...)
	sort.SliceStable(items, func(i, j int) bool {
		left := items[i].Name + "\x00" + items[i].TaskID.String()
		right := items[j].Name + "\x00" + items[j].TaskID.String()
		return left < right
	})
	seenNames := make(map[string]struct{}, len(items))
	producerSet := make(map[uuid.UUID]struct{}, len(items))
	for _, value := range items {
		name := strings.ToLower(strings.TrimSpace(value.Name))
		if name == "" || strings.TrimSpace(value.Type) == "" {
			add(CompileError{Code: CodeNoDeliverable, TaskID: value.TaskID, Field: "deliverables", Message: "name、type 和有效 producer task 均为必填"})
		}
		if _, exists := seenNames[name]; name != "" && exists {
			add(CompileError{Code: CodeNoDeliverable, TaskID: value.TaskID, Field: "deliverables.name", Message: "交付物名称不能重复"})
		}
		seenNames[name] = struct{}{}
		if _, exists := tasks[value.TaskID]; !exists {
			add(CompileError{Code: CodeDependencyNotFound, TaskID: value.TaskID, Field: "deliverables.task_id", Message: "交付物 producer Task 不存在"})
			continue
		}
		producerSet[value.TaskID] = struct{}{}
	}
	producers := make([]uuid.UUID, 0, len(producerSet))
	for id := range producerSet {
		producers = append(producers, id)
	}
	sortUUIDs(producers)
	return producers
}

func validateBudgetShape(value ResourceBudget, field string, add func(CompileError)) {
	if value.MaxTokens < 0 || value.MaxCostMicros < 0 || value.MaxDurationSeconds < 0 {
		add(CompileError{Code: CodeInvalidPlan, Field: field, Message: "预算各维度不能为负数"})
	}
}

func taskProducesArtifact(value TaskContract, expected string) bool {
	expected = strings.ToLower(strings.TrimSpace(expected))
	for _, artifact := range value.Output.Artifacts {
		if strings.EqualFold(strings.TrimSpace(artifact.Name), expected) ||
			strings.EqualFold(strings.TrimSpace(artifact.Type), expected) {
			return true
		}
	}
	return false
}

func validateAvailableNames(taskID uuid.UUID, field string, required []string, available map[string]bool,
	code CompileErrorCode, add func(CompileError),
) {
	items := append([]string(nil), required...)
	sort.SliceStable(items, func(i, j int) bool { return strings.ToLower(items[i]) < strings.ToLower(items[j]) })
	for _, raw := range items {
		name := strings.ToLower(strings.TrimSpace(raw))
		if name != "" && !available[name] {
			add(CompileError{Code: code, TaskID: taskID, Field: field, Message: fmt.Sprintf("%s 当前不可用", raw)})
		}
	}
}

func normalizeLevels(values map[string]float64) map[string]float64 {
	result := make(map[string]float64, len(values))
	for name, level := range values {
		key := strings.ToLower(strings.TrimSpace(name))
		if level > result[key] {
			result[key] = level
		}
	}
	return result
}

func normalizeAvailability(values map[string]bool) map[string]bool {
	result := make(map[string]bool, len(values))
	for name, enabled := range values {
		key := strings.ToLower(strings.TrimSpace(name))
		result[key] = result[key] || enabled
	}
	return result
}

func stableTopologicalOrder(original map[uuid.UUID]int, adjacency map[uuid.UUID][]uuid.UUID) []uuid.UUID {
	indegree := make(map[uuid.UUID]int, len(original))
	ready := make([]uuid.UUID, 0)
	for id, count := range original {
		indegree[id] = count
		if count == 0 {
			ready = append(ready, id)
		}
	}
	sortUUIDs(ready)
	order := make([]uuid.UUID, 0, len(original))
	for len(ready) > 0 {
		id := ready[0]
		ready = ready[1:]
		order = append(order, id)
		for _, child := range adjacency[id] {
			indegree[child]--
			if indegree[child] == 0 {
				ready = append(ready, child)
				sortUUIDs(ready)
			}
		}
	}
	return order
}

func graphDepth(order []uuid.UUID, prerequisites map[uuid.UUID][]uuid.UUID) int {
	depths := make(map[uuid.UUID]int, len(order))
	maxDepth := 0
	for _, id := range order {
		depth := 1
		for _, parent := range prerequisites[id] {
			if depths[parent]+1 > depth {
				depth = depths[parent] + 1
			}
		}
		depths[id] = depth
		if depth > maxDepth {
			maxDepth = depth
		}
	}
	return maxDepth
}

func findOrphans(tasks map[uuid.UUID]TaskContract, producers []uuid.UUID, prerequisites map[uuid.UUID][]uuid.UUID) []uuid.UUID {
	reachable := make(map[uuid.UUID]struct{}, len(tasks))
	stack := append([]uuid.UUID(nil), producers...)
	for len(stack) > 0 {
		last := len(stack) - 1
		id := stack[last]
		stack = stack[:last]
		if _, seen := reachable[id]; seen {
			continue
		}
		reachable[id] = struct{}{}
		stack = append(stack, prerequisites[id]...)
	}
	orphans := make([]uuid.UUID, 0)
	for id := range tasks {
		if _, ok := reachable[id]; !ok {
			orphans = append(orphans, id)
		}
	}
	sortUUIDs(orphans)
	return orphans
}

func deriveEstimatedBudget(explicit ResourceBudget, tasks []TaskContract) ResourceBudget {
	derived := ResourceBudget{}
	for _, value := range tasks {
		derived.MaxTokens = saturatingAdd(derived.MaxTokens, value.Budget.MaxTokens)
		derived.MaxCostMicros = saturatingAdd(derived.MaxCostMicros, value.Budget.MaxCostMicros)
		derived.MaxDurationSeconds = saturatingAdd(derived.MaxDurationSeconds, value.Budget.MaxDurationSeconds)
	}
	if explicit.MaxTokens > 0 {
		derived.MaxTokens = explicit.MaxTokens
	}
	if explicit.MaxCostMicros > 0 {
		derived.MaxCostMicros = explicit.MaxCostMicros
	}
	if explicit.MaxDurationSeconds > 0 {
		derived.MaxDurationSeconds = explicit.MaxDurationSeconds
	}
	return derived
}

func checkBudget(limit, estimated ResourceBudget, add func(CompileError)) {
	checks := []struct {
		field     string
		maximum   int64
		estimated int64
	}{
		{"run_budget.max_tokens", limit.MaxTokens, estimated.MaxTokens},
		{"run_budget.max_cost_micros", limit.MaxCostMicros, estimated.MaxCostMicros},
		{"run_budget.max_duration_seconds", limit.MaxDurationSeconds, estimated.MaxDurationSeconds},
	}
	for _, check := range checks {
		if check.maximum > 0 && check.estimated > check.maximum {
			add(CompileError{Code: CodePlanOverBudget, Field: check.field,
				Message: fmt.Sprintf("估算 %d 超过 Run 上限 %d", check.estimated, check.maximum)})
		}
	}
}

func saturatingAdd(left, right int64) int64 {
	if right > 0 && left > math.MaxInt64-right {
		return math.MaxInt64
	}
	return left + right
}

func longestPath(order []uuid.UUID, adjacency map[uuid.UUID][]uuid.UUID, tasks map[uuid.UUID]TaskContract) []uuid.UUID {
	distance := make(map[uuid.UUID]int64, len(order))
	paths := make(map[uuid.UUID][]uuid.UUID, len(order))
	for _, id := range order {
		if distance[id] == 0 {
			distance[id] = tasks[id].ExpectedDurationSeconds
			paths[id] = []uuid.UUID{id}
		}
		for _, child := range adjacency[id] {
			candidateDistance := saturatingAdd(distance[id], tasks[child].ExpectedDurationSeconds)
			candidatePath := append(append([]uuid.UUID(nil), paths[id]...), child)
			if candidateDistance > distance[child] ||
				(candidateDistance == distance[child] && pathKey(candidatePath) < pathKey(paths[child])) {
				distance[child] = candidateDistance
				paths[child] = candidatePath
			}
		}
	}
	var best []uuid.UUID
	var bestDistance int64
	for _, id := range order {
		if distance[id] > bestDistance || (distance[id] == bestDistance && pathKey(paths[id]) < pathKey(best)) {
			bestDistance = distance[id]
			best = paths[id]
		}
	}
	return append([]uuid.UUID(nil), best...)
}

func pathKey(values []uuid.UUID) string {
	parts := make([]string, len(values))
	for index, id := range values {
		parts[index] = id.String()
	}
	return strings.Join(parts, "\x00")
}

func sortUUIDs(values []uuid.UUID) {
	sort.SliceStable(values, func(i, j int) bool { return values[i].String() < values[j].String() })
}

func joinUUIDs(values []uuid.UUID) string {
	parts := make([]string, len(values))
	for index, id := range values {
		parts[index] = id.String()
	}
	return strings.Join(parts, ",")
}

func sortCompileErrors(values CompileErrors) {
	sort.SliceStable(values, func(i, j int) bool {
		left := string(values[i].Code) + "\x00" + values[i].TaskID.String() + "\x00" +
			values[i].DependsOnID.String() + "\x00" + values[i].Field + "\x00" + values[i].Message
		right := string(values[j].Code) + "\x00" + values[j].TaskID.String() + "\x00" +
			values[j].DependsOnID.String() + "\x00" + values[j].Field + "\x00" + values[j].Message
		return left < right
	})
}
