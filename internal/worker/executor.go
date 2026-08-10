package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/licy-yu/agent-os/internal/execution"
	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/responses"
)

// Router 保留确定性执行器用于测试/私有模型，并把其他模型交给 OpenAI Responses API。
type Router struct {
	deterministic execution.Executor
	openAI        execution.Executor
}

func NewRouter(openAI execution.Executor) *Router {
	return &Router{deterministic: DeterministicExecutor{}, openAI: openAI}
}

func (r *Router) ForModel(model string) execution.Executor {
	if strings.HasPrefix(strings.ToLower(model), "mock/") {
		return r.deterministic
	}
	return r.openAI
}

// DeterministicExecutor 不伪装成模型：它专门用于无 API Key 的集成测试，
// 但仍走真实 NATS、Attempt、Checkpoint、Tool Gateway 和 Reviewer 链路。
type DeterministicExecutor struct{}

func (DeterministicExecutor) Execute(ctx context.Context, work *execution.Work,
	checkpoints execution.CheckpointWriter, tools execution.ToolCaller,
) (execution.ExecutionResult, error) {
	if err := checkpoints.Save(ctx, "plan", map[string]any{
		"goal": work.Task.Goal, "executor": "mock/deterministic",
	}, nil); err != nil {
		return execution.ExecutionResult{}, err
	}
	toolResult, err := tools.Call(ctx, "echo", map[string]any{
		"task_id": work.Task.ID.String(), "goal": work.Task.Goal,
	})
	if err != nil {
		return execution.ExecutionResult{}, err
	}
	checks := map[string]bool{"execution": true, "output_nonempty": true}
	if work.Task.Acceptance.Build {
		checks["build"] = true
	}
	if work.Task.Acceptance.UnitTest {
		checks["unit_test"] = true
	}
	if work.Task.Acceptance.SecurityReview {
		checks["security_review"] = true
	}
	for _, name := range work.Task.Acceptance.RequiredChecks {
		checks[name] = true
	}
	output := map[string]any{
		"summary": "确定性执行器已完成任务", "goal": work.Task.Goal, "tool_result": toolResult,
	}
	if err := checkpoints.Save(ctx, "completed", map[string]any{"checks": checks, "output": output}, nil); err != nil {
		return execution.ExecutionResult{}, err
	}
	tokensIn, tokensOut := int64(128), int64(64)
	return execution.ExecutionResult{
		Output: output, Checks: checks, QualityScore: 1,
		TokensIn: tokensIn, TokensOut: tokensOut,
		CostMicros: (tokensIn + tokensOut) * work.Template.CostPer1KTokensMicros / 1000,
	}, nil
}

// OpenAIExecutor 使用官方 Go SDK 的 Responses API。API Key 只从 OPENAI_API_KEY 环境变量读取，
// 不进入配置文件或数据库。工具仍由本地 Tool Gateway 控制；本阶段先执行纯文本工作负载。
type OpenAIExecutor struct{ client openai.Client }

func NewOpenAIExecutor() *OpenAIExecutor {
	return &OpenAIExecutor{client: openai.NewClient()}
}

func (e *OpenAIExecutor) Execute(ctx context.Context, work *execution.Work,
	checkpoints execution.CheckpointWriter, _ execution.ToolCaller,
) (execution.ExecutionResult, error) {
	input, err := json.Marshal(map[string]any{
		"task_id": work.Task.ID, "goal": work.Task.Goal, "input": work.Task.Input,
		"acceptance": work.Task.Acceptance,
	})
	if err != nil {
		return execution.ExecutionResult{}, fmt.Errorf("序列化模型输入: %w", err)
	}
	if err := checkpoints.Save(ctx, "model_request", map[string]any{
		"model": work.Template.Model, "input_bytes": len(input),
	}, nil); err != nil {
		return execution.ExecutionResult{}, err
	}
	maxOutput := work.Task.ExecutionPolicy.MaxTokens
	if maxOutput <= 0 || maxOutput > 32_768 {
		maxOutput = 32_768
	}
	response, err := e.client.Responses.New(ctx, responses.ResponseNewParams{
		Model:           work.Template.Model,
		Instructions:    openai.String(work.Template.Prompt + "\n请只完成给定任务，并明确说明结果与验证证据。"),
		Input:           responses.ResponseNewParamsInputUnion{OfString: openai.String(string(input))},
		MaxOutputTokens: openai.Int(maxOutput),
		Store:           openai.Bool(false),
	})
	if err != nil {
		return execution.ExecutionResult{}, fmt.Errorf("OpenAI Responses API 调用失败: %w", err)
	}
	text := response.OutputText()
	if err := checkpoints.Save(ctx, "model_response", map[string]any{
		"response_id": response.ID, "output_chars": len([]rune(text)),
	}, nil); err != nil {
		return execution.ExecutionResult{}, err
	}
	checks := map[string]bool{"execution": true, "output_nonempty": strings.TrimSpace(text) != ""}
	// 构建、测试和安全检查必须由相应工具产生证据，不能根据模型自述伪造为通过。
	cost := (response.Usage.InputTokens + response.Usage.OutputTokens) * work.Template.CostPer1KTokensMicros / 1000
	return execution.ExecutionResult{
		Output: map[string]any{"text": text, "response_id": response.ID},
		Checks: checks, QualityScore: 0.85,
		TokensIn: response.Usage.InputTokens, TokensOut: response.Usage.OutputTokens, CostMicros: cost,
	}, nil
}
