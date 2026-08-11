package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/licy-yu/agent-os/internal/agentloop"
	"github.com/licy-yu/agent-os/internal/contextengine"
	"github.com/licy-yu/agent-os/internal/execution"
	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/responses"
)

// Router 保留确定性执行器用于测试/私有模型，并把其他模型交给 OpenAI Responses API。
// 两条路径都会先包进同一张 Eino Attempt Graph，因此开发模式不会绕过生产 Runtime Guard。
type Router struct {
	deterministic execution.Executor
	openAI        execution.Executor
}

func NewRouter(openAI execution.Executor) (*Router, error) {
	deterministic, err := agentloop.New(DeterministicExecutor{})
	if err != nil {
		return nil, fmt.Errorf("初始化确定性 Eino Runtime: %w", err)
	}
	production, err := agentloop.New(openAI)
	if err != nil {
		return nil, fmt.Errorf("初始化生产 Eino Runtime: %w", err)
	}
	return &Router{deterministic: deterministic, openAI: production}, nil
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
		// output 是默认 V1.5 Plan 的稳定交付字段；summary/goal/tool_result 继续
		// 保留给控制台和旧 E2E。两个内置执行器必须遵守同一最小输出合同，
		// 否则默认 JSON_SCHEMA Gate 会把真实成功误判为缺少 required 字段。
		"output":  "确定性执行器已完成任务",
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
	pack, err := contextengine.Build(work)
	if err != nil {
		return execution.ExecutionResult{}, fmt.Errorf("构建 ContextPack: %w", err)
	}
	input, err := json.Marshal(pack)
	if err != nil {
		return execution.ExecutionResult{}, fmt.Errorf("序列化模型输入: %w", err)
	}
	if err := checkpoints.Save(ctx, "model_request", map[string]any{
		"model": work.Template.Model, "input_bytes": len(input),
		"context_tokens": pack.UsedTokens, "context_sources": len(pack.Sections),
		"omitted_sources": len(pack.Omitted),
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
		// output 与默认 Plan 的 AcceptanceGate 对齐；text 是面向现有客户端的
		// 兼容字段。这里保存模型真实返回，不根据自然语言伪造任何测试证据。
		Output: map[string]any{"output": text, "text": text, "response_id": response.ID},
		Checks: checks, QualityScore: 0.85,
		TokensIn: response.Usage.InputTokens, TokensOut: response.Usage.OutputTokens, CostMicros: cost,
	}, nil
}
