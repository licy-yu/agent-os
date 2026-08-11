package safetycontrol

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/licy-yu/agent-os/internal/effect"
	"github.com/licy-yu/agent-os/internal/interaction"
	"github.com/licy-yu/agent-os/internal/runcontrol"
	"github.com/licy-yu/agent-os/internal/verification"
)

var (
	testTenantA  = uuid.MustParse("10000000-0000-0000-0000-000000000001")
	testTenantB  = uuid.MustParse("10000000-0000-0000-0000-000000000002")
	testRun      = uuid.MustParse("20000000-0000-0000-0000-000000000001")
	testTask     = uuid.MustParse("30000000-0000-0000-0000-000000000001")
	testAttempt  = uuid.MustParse("40000000-0000-0000-0000-000000000001")
	testEffect   = uuid.MustParse("50000000-0000-0000-0000-000000000001")
	testInteract = uuid.MustParse("60000000-0000-0000-0000-000000000001")
	testArtifact = uuid.MustParse("70000000-0000-0000-0000-000000000001")
	testVerify   = uuid.MustParse("80000000-0000-0000-0000-000000000001")
	testManifest = uuid.MustParse("90000000-0000-0000-0000-000000000001")
	testNow      = time.Date(2026, 8, 11, 9, 30, 0, 0, time.UTC)
)

// fakeStore 只模拟应用端口，不复制生产 SQL 逻辑。每个测试只设置关心的函数，
// 从而可以精确断言 Service 是否在写入前完成租户、领域状态与幂等校验。
type fakeStore struct {
	ensureRunTenantFn    func(context.Context, uuid.UUID, uuid.UUID) error
	findMutationFn       func(context.Context, uuid.UUID, string, string) (*MutationResult, error)
	listInteractionsFn   func(context.Context, uuid.UUID, InteractionQuery) ([]InteractionView, error)
	getInteractionFn     func(context.Context, uuid.UUID, uuid.UUID) (*InteractionView, error)
	createInteractionFn  func(context.Context, CreateInteractionRecord) (*InteractionView, error)
	resolveInteractionFn func(context.Context, ResolveInteractionRecord) (*InteractionView, error)
	listArtifactsFn      func(context.Context, uuid.UUID, ArtifactQuery) ([]ArtifactView, error)
	getArtifactFn        func(context.Context, uuid.UUID, uuid.UUID) (*ArtifactView, error)
	getArtifactLineageFn func(context.Context, uuid.UUID, uuid.UUID, ArtifactLineageQuery) (*ArtifactLineageView, error)
	listEffectsFn        func(context.Context, uuid.UUID, EffectQuery) ([]EffectView, error)
	getEffectFn          func(context.Context, uuid.UUID, uuid.UUID) (*EffectView, error)
	transitionEffectFn   func(context.Context, TransitionEffectRecord) (*EffectView, error)
	listTimelineFn       func(context.Context, uuid.UUID, TimelineQuery) ([]TimelineEventView, error)
	listSchedulerFn      func(context.Context, uuid.UUID, SchedulerDecisionQuery) ([]SchedulerDecisionView, error)
	listVerificationsFn  func(context.Context, uuid.UUID, VerificationQuery) ([]VerificationRunView, error)
	getVerificationFn    func(context.Context, uuid.UUID, uuid.UUID) (*VerificationRunView, error)
	getManifestFn        func(context.Context, uuid.UUID, uuid.UUID) (*CompletionManifestView, error)
}

func (f *fakeStore) EnsureRunTenant(ctx context.Context, tenantID, runID uuid.UUID) error {
	if f.ensureRunTenantFn != nil {
		return f.ensureRunTenantFn(ctx, tenantID, runID)
	}
	return nil
}

func (f *fakeStore) FindMutation(ctx context.Context, tenantID uuid.UUID, key, hash string) (*MutationResult, error) {
	if f.findMutationFn != nil {
		return f.findMutationFn(ctx, tenantID, key, hash)
	}
	return nil, nil
}

func (f *fakeStore) ListInteractions(ctx context.Context, tenantID uuid.UUID, query InteractionQuery) ([]InteractionView, error) {
	return f.listInteractionsFn(ctx, tenantID, query)
}

func (f *fakeStore) GetInteraction(ctx context.Context, tenantID, id uuid.UUID) (*InteractionView, error) {
	return f.getInteractionFn(ctx, tenantID, id)
}

func (f *fakeStore) CreateInteraction(ctx context.Context, record CreateInteractionRecord) (*InteractionView, error) {
	return f.createInteractionFn(ctx, record)
}

func (f *fakeStore) ResolveInteraction(ctx context.Context, record ResolveInteractionRecord) (*InteractionView, error) {
	return f.resolveInteractionFn(ctx, record)
}

func (f *fakeStore) ListArtifacts(ctx context.Context, tenantID uuid.UUID, query ArtifactQuery) ([]ArtifactView, error) {
	return f.listArtifactsFn(ctx, tenantID, query)
}

func (f *fakeStore) GetArtifact(ctx context.Context, tenantID, id uuid.UUID) (*ArtifactView, error) {
	return f.getArtifactFn(ctx, tenantID, id)
}

func (f *fakeStore) GetArtifactLineage(ctx context.Context, tenantID, id uuid.UUID, query ArtifactLineageQuery) (*ArtifactLineageView, error) {
	return f.getArtifactLineageFn(ctx, tenantID, id, query)
}

func (f *fakeStore) ListEffects(ctx context.Context, tenantID uuid.UUID, query EffectQuery) ([]EffectView, error) {
	return f.listEffectsFn(ctx, tenantID, query)
}

func (f *fakeStore) GetEffect(ctx context.Context, tenantID, id uuid.UUID) (*EffectView, error) {
	return f.getEffectFn(ctx, tenantID, id)
}

func (f *fakeStore) TransitionEffect(ctx context.Context, record TransitionEffectRecord) (*EffectView, error) {
	return f.transitionEffectFn(ctx, record)
}

func (f *fakeStore) ListTimeline(ctx context.Context, tenantID uuid.UUID, query TimelineQuery) ([]TimelineEventView, error) {
	return f.listTimelineFn(ctx, tenantID, query)
}

func (f *fakeStore) ListSchedulerDecisions(ctx context.Context, tenantID uuid.UUID, query SchedulerDecisionQuery) ([]SchedulerDecisionView, error) {
	return f.listSchedulerFn(ctx, tenantID, query)
}

func (f *fakeStore) ListVerificationRuns(ctx context.Context, tenantID uuid.UUID, query VerificationQuery) ([]VerificationRunView, error) {
	return f.listVerificationsFn(ctx, tenantID, query)
}

func (f *fakeStore) GetVerificationRun(ctx context.Context, tenantID, id uuid.UUID) (*VerificationRunView, error) {
	return f.getVerificationFn(ctx, tenantID, id)
}

func (f *fakeStore) GetCompletionManifest(ctx context.Context, tenantID, id uuid.UUID) (*CompletionManifestView, error) {
	return f.getManifestFn(ctx, tenantID, id)
}

func TestDTOUsesCamelCaseJSON(t *testing.T) {
	raw, err := json.Marshal(InteractionView{InteractionType: interaction.TypeApproval, Version: 3})
	if err != nil {
		t.Fatal(err)
	}
	encoded := string(raw)
	for _, expected := range []string{`"interactionType"`, `"allowedActions"`, `"tenantId"`, `"version"`} {
		if !strings.Contains(encoded, expected) {
			t.Fatalf("DTO 缺少 camelCase 字段 %s: %s", expected, encoded)
		}
	}
	if strings.Contains(encoded, "interaction_type") || strings.Contains(encoded, "tenant_id") {
		t.Fatalf("DTO 不得暴露 snake_case: %s", encoded)
	}

	reconcileRaw, err := json.Marshal(EffectCommandRequest{
		Version: 2, Outcome: effect.StatusSucceeded,
		Result: map[string]any{"resourceState": "ready"},
		Evidence: map[string]any{
			"observedAt": "2026-08-11T09:30:00Z",
		},
		ExternalRef: "deployment-42",
	})
	if err != nil {
		t.Fatal(err)
	}
	reconcileJSON := string(reconcileRaw)
	for _, expected := range []string{`"outcome"`, `"result"`, `"evidence"`, `"externalRef"`, `"observedAt"`} {
		if !strings.Contains(reconcileJSON, expected) {
			t.Fatalf("对账 DTO 缺少 camelCase 字段 %s: %s", expected, reconcileJSON)
		}
	}
	if strings.Contains(reconcileJSON, "external_ref") || strings.Contains(reconcileJSON, "observed_at") {
		t.Fatalf("对账 DTO 不得暴露 snake_case: %s", reconcileJSON)
	}
}

func TestListInteractionsNormalizesPendingAndFailsClosedOnTenantLeak(t *testing.T) {
	var captured InteractionQuery
	store := &fakeStore{
		listInteractionsFn: func(_ context.Context, tenantID uuid.UUID, query InteractionQuery) ([]InteractionView, error) {
			if tenantID != testTenantA {
				t.Fatalf("tenant = %s", tenantID)
			}
			captured = query
			return []InteractionView{{ID: testInteract, TenantID: testTenantA, RunID: testRun}}, nil
		},
	}
	service := NewService(store)
	items, err := service.ListInteractions(testContext(testTenantA, "viewer"), InteractionQuery{
		RunID: testRun, Status: interaction.Status("PENDING"), Limit: 0,
	})
	if err != nil || len(items) != 1 {
		t.Fatalf("ListInteractions() = %#v, %v", items, err)
	}
	if captured.Status != interaction.StatusWaiting || captured.Limit != defaultListLimit {
		t.Fatalf("query 未归一化: %#v", captured)
	}

	store.listInteractionsFn = func(context.Context, uuid.UUID, InteractionQuery) ([]InteractionView, error) {
		return []InteractionView{{TenantID: testTenantB, RunID: testRun}}, nil
	}
	_, err = service.ListInteractions(testContext(testTenantA, "viewer"), InteractionQuery{RunID: testRun})
	if !errors.Is(err, ErrTenantBoundary) {
		t.Fatalf("跨租户返回 err = %v, want ErrTenantBoundary", err)
	}
}

func TestCreateInteractionBindsR3ApprovalAndAuditsActor(t *testing.T) {
	var captured CreateInteractionRecord
	store := &fakeStore{
		getEffectFn: func(_ context.Context, tenantID, id uuid.UUID) (*EffectView, error) {
			return &EffectView{
				ID: id, TenantID: tenantID, RunID: testRun, TaskID: uuidPointer(testTask),
				AttemptID: testAttempt, RiskLevel: effect.RiskR3ProductionDestructive,
				Status: effect.StatusPrepared, Version: 7,
			}, nil
		},
		createInteractionFn: func(_ context.Context, record CreateInteractionRecord) (*InteractionView, error) {
			captured = record
			copy := record.Interaction
			return &copy, nil
		},
	}
	service := NewService(store)
	service.clock = func() time.Time { return testNow }
	service.newID = func() uuid.UUID { return testInteract }
	ctx := testContext(testTenantA, "alice", ScopeInteractionCreate)
	created, err := service.CreateInteraction(ctx, "create-r3-1", CreateInteractionRequest{
		RunID: testRun, TaskID: uuidPointer(testTask), AttemptID: uuidPointer(testAttempt),
		EffectID: uuidPointer(testEffect), InteractionType: interaction.TypeApproval,
		Title: "  发布生产变更  ",
	})
	if err != nil {
		t.Fatalf("CreateInteraction() error = %v", err)
	}
	if created.Status != interaction.StatusWaiting || created.RequestedBy != "alice" || created.Title != "发布生产变更" {
		t.Fatalf("created = %#v", created)
	}
	if captured.Binding == nil || captured.Binding.EffectID != testEffect || captured.Binding.ExpectedEffectVersion != 7 {
		t.Fatalf("R3 binding = %#v", captured.Binding)
	}
	if captured.Mutation.Actor != "alice" || captured.Mutation.IdempotencyKey != "create-r3-1" ||
		!strings.HasPrefix(captured.Mutation.RequestHash, "sha256:") {
		t.Fatalf("mutation = %#v", captured.Mutation)
	}
	if !containsAction(created.AllowedActions, "approve") || !containsAction(created.AllowedActions, "reject") {
		t.Fatalf("approval actions = %#v", created.AllowedActions)
	}
}

func TestCreateInteractionRejectsMissingScopeAndR3NonApproval(t *testing.T) {
	store := &fakeStore{}
	service := NewService(store)
	request := CreateInteractionRequest{
		RunID: testRun, InteractionType: interaction.TypeInput, Title: "input",
	}
	_, err := service.CreateInteraction(testContext(testTenantA, "alice"), "create-1", request)
	if !errors.Is(err, ErrForbidden) {
		t.Fatalf("missing scope err = %v", err)
	}

	store.getEffectFn = func(context.Context, uuid.UUID, uuid.UUID) (*EffectView, error) {
		return &EffectView{
			ID: testEffect, TenantID: testTenantA, RunID: testRun, AttemptID: testAttempt,
			RiskLevel: effect.RiskR3ProductionDestructive, Status: effect.StatusPrepared, Version: 1,
		}, nil
	}
	request.EffectID = uuidPointer(testEffect)
	_, err = service.CreateInteraction(testContext(testTenantA, "alice", ScopeInteractionCreate), "create-2", request)
	if !errors.Is(err, ErrApprovalRequired) {
		t.Fatalf("R3 non-approval err = %v", err)
	}
}

func TestApproveR3ResolvesInteractionAndAuthorizesEffectAtomically(t *testing.T) {
	approvalID := testInteract
	current := waitingApproval(approvalID)
	linked := preparedR3Effect(approvalID)
	var captured ResolveInteractionRecord
	store := &fakeStore{
		getInteractionFn: func(context.Context, uuid.UUID, uuid.UUID) (*InteractionView, error) {
			copy := current
			return &copy, nil
		},
		getEffectFn: func(context.Context, uuid.UUID, uuid.UUID) (*EffectView, error) {
			copy := linked
			return &copy, nil
		},
		resolveInteractionFn: func(_ context.Context, record ResolveInteractionRecord) (*InteractionView, error) {
			captured = record
			copy := current
			copy.Status = interaction.StatusResolved
			copy.Version++
			copy.Response = &record.Response
			copy.ResolvedBy = record.Response.RespondedBy
			return &copy, nil
		},
	}
	service := NewService(store)
	service.clock = func() time.Time { return testNow }
	resolved, err := service.Approve(testContext(testTenantA, "reviewer", ScopeInteractionResolve),
		approvalID, "approve-1", InteractionCommandRequest{Version: current.Version})
	if err != nil {
		t.Fatalf("Approve() error = %v", err)
	}
	if resolved.Response == nil || resolved.Response.RespondedBy != "reviewer" || resolved.Response.Action != "approve" {
		t.Fatalf("resolved = %#v", resolved)
	}
	if captured.Authorization == nil || captured.Rejection != nil {
		t.Fatalf("authorization/rejection = %#v / %#v", captured.Authorization, captured.Rejection)
	}
	if !captured.ResumeWaitingAttempt || captured.FailWaitingAttempt {
		t.Fatalf("waiting attempt action = resume:%v fail:%v",
			captured.ResumeWaitingAttempt, captured.FailWaitingAttempt)
	}
	if captured.Authorization.ExpectedStatus != effect.StatusPrepared ||
		captured.Authorization.NextStatus != effect.StatusAuthorized ||
		captured.Authorization.ExpectedEffectVersion != linked.Version {
		t.Fatalf("authorization = %#v", captured.Authorization)
	}
	if captured.ExpectedVersion != current.Version || captured.Mutation.IdempotencyKey != "approve-1" {
		t.Fatalf("CAS/mutation = %#v", captured)
	}
}

func TestRejectSealsPreparedEffectWithoutInventingTerminalStatus(t *testing.T) {
	current := waitingApproval(testInteract)
	linked := preparedR3Effect(testInteract)
	var captured ResolveInteractionRecord
	store := &fakeStore{
		getInteractionFn: func(context.Context, uuid.UUID, uuid.UUID) (*InteractionView, error) {
			copy := current
			return &copy, nil
		},
		getEffectFn: func(context.Context, uuid.UUID, uuid.UUID) (*EffectView, error) {
			copy := linked
			return &copy, nil
		},
		resolveInteractionFn: func(_ context.Context, record ResolveInteractionRecord) (*InteractionView, error) {
			captured = record
			copy := current
			copy.Status = interaction.StatusResolved
			copy.Response = &record.Response
			return &copy, nil
		},
	}
	service := NewService(store)
	service.clock = func() time.Time { return testNow }
	_, err := service.Reject(testContext(testTenantA, "reviewer", ScopeInteractionResolve),
		testInteract, "reject-1", InteractionCommandRequest{
			Version: current.Version, Resolution: map[string]any{"reason": "风险窗口已关闭"},
		})
	if err != nil {
		t.Fatalf("Reject() error = %v", err)
	}
	if captured.Authorization != nil || captured.Rejection == nil {
		t.Fatalf("authorization/rejection = %#v / %#v", captured.Authorization, captured.Rejection)
	}
	if captured.ResumeWaitingAttempt || !captured.FailWaitingAttempt {
		t.Fatalf("waiting attempt action = resume:%v fail:%v",
			captured.ResumeWaitingAttempt, captured.FailWaitingAttempt)
	}
	if captured.Rejection.ExpectedStatus != effect.StatusPrepared || captured.Rejection.Reason != "风险窗口已关闭" {
		t.Fatalf("rejection = %#v", captured.Rejection)
	}
}

func TestInteractionCommandsRequireVersionKeyAndProtectApprovalAndSecrets(t *testing.T) {
	current := waitingApproval(testInteract)
	writes := 0
	store := &fakeStore{
		getInteractionFn: func(context.Context, uuid.UUID, uuid.UUID) (*InteractionView, error) {
			copy := current
			return &copy, nil
		},
		resolveInteractionFn: func(context.Context, ResolveInteractionRecord) (*InteractionView, error) {
			writes++
			return nil, nil
		},
	}
	service := NewService(store)
	ctx := testContext(testTenantA, "reviewer", ScopeInteractionResolve)
	_, err := service.Approve(ctx, testInteract, "", InteractionCommandRequest{Version: current.Version})
	if !errors.Is(err, ErrIdempotencyRequired) {
		t.Fatalf("missing key err = %v", err)
	}
	_, err = service.Approve(ctx, testInteract, "approve-stale", InteractionCommandRequest{Version: current.Version - 1})
	if !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale version err = %v", err)
	}
	_, err = service.Resolve(ctx, testInteract, "resolve-approval", InteractionCommandRequest{
		Action: "approve", Version: current.Version,
	})
	if !errors.Is(err, ErrApprovalRequired) {
		t.Fatalf("generic approval err = %v", err)
	}

	auth := current
	auth.InteractionType = interaction.TypeAuth
	auth.AllowedActions = []string{"resolve"}
	auth.EffectID = nil
	store.getInteractionFn = func(context.Context, uuid.UUID, uuid.UUID) (*InteractionView, error) {
		copy := auth
		return &copy, nil
	}
	_, err = service.Resolve(ctx, testInteract, "resolve-auth", InteractionCommandRequest{
		Action: "resolve", Version: auth.Version, Resolution: map[string]any{"password": "plaintext"},
	})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("plaintext AUTH err = %v", err)
	}
	if writes != 0 {
		t.Fatalf("无效命令不应写 Store, writes=%d", writes)
	}
}

func TestInteractionIdempotentReplaySkipsChangedAggregate(t *testing.T) {
	replayed := waitingApproval(testInteract)
	replayed.Status = interaction.StatusResolved
	replayed.Response = &InteractionResponseView{Action: "approve", RespondedBy: "reviewer", RespondedAt: testNow}
	getCalls := 0
	store := &fakeStore{
		findMutationFn: func(context.Context, uuid.UUID, string, string) (*MutationResult, error) {
			return &MutationResult{Operation: operationInteractionApprove, Interaction: &replayed}, nil
		},
		getInteractionFn: func(context.Context, uuid.UUID, uuid.UUID) (*InteractionView, error) {
			getCalls++
			return nil, errors.New("不应读取")
		},
	}
	service := NewService(store)
	result, err := service.Approve(testContext(testTenantA, "reviewer", ScopeInteractionResolve),
		testInteract, "approve-retry", InteractionCommandRequest{Version: 4})
	if err != nil || result != &replayed {
		t.Fatalf("idempotent replay = %#v, %v", result, err)
	}
	if getCalls != 0 {
		t.Fatalf("回放命中后仍读取 Aggregate, calls=%d", getCalls)
	}
}

func TestEffectReconcileCompensateStateMachineAndR3Evidence(t *testing.T) {
	current := EffectView{
		ID: testEffect, TenantID: testTenantA, RunID: testRun, AttemptID: testAttempt,
		RiskLevel: effect.RiskR2ExternalReversible, Status: effect.StatusUnknown, Version: 9,
	}
	var captured TransitionEffectRecord
	store := &fakeStore{
		getEffectFn: func(context.Context, uuid.UUID, uuid.UUID) (*EffectView, error) {
			copy := current
			return &copy, nil
		},
		transitionEffectFn: func(_ context.Context, record TransitionEffectRecord) (*EffectView, error) {
			captured = record
			copy := current
			copy.Status = record.NextStatus
			copy.Version++
			return &copy, nil
		},
	}
	service := NewService(store)
	ctx := testContext(testTenantA, "operator", ScopeEffectReconcile, ScopeEffectCompensate)
	result, err := service.Reconcile(ctx, testEffect, "reconcile-1", EffectCommandRequest{Version: 9, Reason: "timeout"})
	if err != nil || result.Status != effect.StatusReconciling {
		t.Fatalf("Reconcile() = %#v, %v", result, err)
	}
	if captured.ExpectedStatus != effect.StatusUnknown || captured.NextStatus != effect.StatusReconciling {
		t.Fatalf("reconcile transition = %#v", captured)
	}

	current.Status, current.Version = effect.StatusReconciling, 10
	result, err = service.Reconcile(ctx, testEffect, "reconcile-finish", EffectCommandRequest{
		Version: 10, Outcome: effect.StatusSucceeded,
		Result:      map[string]any{"confirmed": true},
		Evidence:    map[string]any{"source": "provider-query", "httpStatus": 200},
		ExternalRef: "external-42",
	})
	if err != nil || result.Status != effect.StatusSucceeded {
		t.Fatalf("Finish Reconcile() = %#v, %v", result, err)
	}
	if !captured.ResumeWaitingAttempt || captured.FailWaitingAttempt ||
		captured.ExternalRef != "external-42" || captured.Result["confirmed"] != true ||
		captured.Evidence["source"] != "provider-query" {
		t.Fatalf("reconcile completion = %#v", captured)
	}

	current.Status, current.Version = effect.StatusPrepared, 9
	_, err = service.Reconcile(ctx, testEffect, "reconcile-invalid", EffectCommandRequest{Version: 9})
	if !errors.Is(err, effect.ErrInvalidTransition) {
		t.Fatalf("invalid reconcile err = %v", err)
	}

	current.Status = effect.StatusSucceeded
	result, err = service.Compensate(ctx, testEffect, "compensate-1", EffectCommandRequest{Version: 9})
	if err != nil || result.Status != effect.StatusCompensating {
		t.Fatalf("Compensate() = %#v, %v", result, err)
	}

	current.RiskLevel = effect.RiskR3ProductionDestructive
	current.ApprovalInteractionID = nil
	current.ApprovedBy = ""
	_, err = service.Compensate(ctx, testEffect, "compensate-r3", EffectCommandRequest{Version: 9})
	if !errors.Is(err, ErrApprovalRequired) {
		t.Fatalf("R3 without evidence err = %v", err)
	}
}

func TestEffectReconcileRequiresAuditableFinalEvidence(t *testing.T) {
	current := EffectView{
		ID: testEffect, TenantID: testTenantA, RunID: testRun, AttemptID: testAttempt,
		RiskLevel: effect.RiskR2ExternalReversible, Status: effect.StatusReconciling,
		Version: 10,
	}
	var captured TransitionEffectRecord
	store := &fakeStore{
		getEffectFn: func(context.Context, uuid.UUID, uuid.UUID) (*EffectView, error) {
			copy := current
			return &copy, nil
		},
		transitionEffectFn: func(_ context.Context, record TransitionEffectRecord) (*EffectView, error) {
			captured = record
			copy := current
			copy.Status = record.NextStatus
			copy.Version++
			return &copy, nil
		},
	}
	service := NewService(store)
	ctx := testContext(testTenantA, "operator", ScopeEffectReconcile)

	_, err := service.Reconcile(ctx, testEffect, "missing-evidence", EffectCommandRequest{
		Version: 10, Outcome: effect.StatusSucceeded, ExternalRef: "external-42",
	})
	if !errors.Is(err, ErrInvalidRequest) || !strings.Contains(err.Error(), "evidence") {
		t.Fatalf("缺少 evidence err = %v", err)
	}

	_, err = service.Reconcile(ctx, testEffect, "oversized-evidence", EffectCommandRequest{
		Version: 10, Outcome: effect.StatusFailed, ExternalRef: "external-42",
		Evidence: map[string]any{"providerResponse": strings.Repeat("x", maximumPayloadSize)},
	})
	if !errors.Is(err, ErrInvalidRequest) || !strings.Contains(err.Error(), "不能超过") {
		t.Fatalf("过大 evidence err = %v", err)
	}

	_, err = service.Reconcile(ctx, testEffect, "missing-external-ref", EffectCommandRequest{
		Version: 10, Outcome: effect.StatusSucceeded,
		Evidence: map[string]any{"source": "provider-query"},
	})
	if !errors.Is(err, ErrInvalidRequest) || !strings.Contains(err.Error(), "externalRef") {
		t.Fatalf("缺少 externalRef err = %v", err)
	}

	// Adapter 在进入 UNKNOWN 前可能已经持久化了外部资源 ID。人工对账时
	// 允许不重复提交，但必须明确从账本中读到该引用。
	current.ExternalRef = "persisted-external-42"
	result, err := service.Reconcile(ctx, testEffect, "persisted-external-ref", EffectCommandRequest{
		Version: 10, Outcome: effect.StatusSucceeded,
		Result:   map[string]any{"state": "ready"},
		Evidence: map[string]any{"source": "provider-query", "statusCode": 200},
	})
	if err != nil || result.Status != effect.StatusSucceeded {
		t.Fatalf("使用已持久化 externalRef 收敛 = %#v, %v", result, err)
	}
	if captured.ExternalRef != "" || captured.Evidence["source"] != "provider-query" {
		t.Fatalf("对账写入合同 = %#v", captured)
	}

	current.ExternalRef = ""
	_, err = service.Reconcile(ctx, testEffect, "unknown-without-reason", EffectCommandRequest{
		Version: 10, Outcome: effect.StatusUnknown,
	})
	if !errors.Is(err, ErrInvalidRequest) || !strings.Contains(err.Error(), "reason") {
		t.Fatalf("UNKNOWN 缺少 reason err = %v", err)
	}
	unknown, err := service.Reconcile(ctx, testEffect, "unknown-with-reason", EffectCommandRequest{
		Version: 10, Outcome: effect.StatusUnknown, Reason: "provider query timed out",
	})
	if err != nil || unknown.Status != effect.StatusUnknown || captured.Reason != "provider query timed out" {
		t.Fatalf("UNKNOWN 对账 = %#v, record=%#v, err=%v", unknown, captured, err)
	}
}

func TestEffectReconcileIdempotencyHashIncludesEvidence(t *testing.T) {
	current := EffectView{
		ID: testEffect, TenantID: testTenantA, RunID: testRun, AttemptID: testAttempt,
		RiskLevel: effect.RiskR2ExternalReversible, Status: effect.StatusReconciling,
		ExternalRef: "external-42", Version: 10,
	}
	hashes := make([]string, 0, 2)
	store := &fakeStore{
		findMutationFn: func(_ context.Context, _ uuid.UUID, _ string, hash string) (*MutationResult, error) {
			hashes = append(hashes, hash)
			return nil, nil
		},
		getEffectFn: func(context.Context, uuid.UUID, uuid.UUID) (*EffectView, error) {
			copy := current
			return &copy, nil
		},
		transitionEffectFn: func(_ context.Context, record TransitionEffectRecord) (*EffectView, error) {
			copy := current
			copy.Status = record.NextStatus
			copy.Version++
			return &copy, nil
		},
	}
	service := NewService(store)
	ctx := testContext(testTenantA, "operator", ScopeEffectReconcile)
	base := EffectCommandRequest{
		Version: 10, Outcome: effect.StatusSucceeded,
		Result: map[string]any{"state": "ready"},
	}
	first := base
	first.Evidence = map[string]any{"queryId": "query-a", "statusCode": 200}
	if _, err := service.Reconcile(ctx, testEffect, "evidence-hash-a", first); err != nil {
		t.Fatal(err)
	}
	second := base
	second.Evidence = map[string]any{"queryId": "query-b", "statusCode": 200}
	if _, err := service.Reconcile(ctx, testEffect, "evidence-hash-b", second); err != nil {
		t.Fatal(err)
	}
	if len(hashes) != 2 || hashes[0] == hashes[1] {
		t.Fatalf("证据未进入幂等指纹: %#v", hashes)
	}
}

func TestReadModelsCarryTenantThroughArtifactsTimelineSchedulerAndVerification(t *testing.T) {
	interactionItem := waitingApproval(testInteract)
	artifact := ArtifactView{ID: testArtifact, TenantID: testTenantA, RunID: testRun, Status: "VALID"}
	effectItem := EffectView{
		ID: testEffect, TenantID: testTenantA, RunID: testRun, AttemptID: testAttempt,
		RiskLevel: effect.RiskR2ExternalReversible, Status: effect.StatusSucceeded,
	}
	timeline := TimelineEventView{ID: uuid.New(), TenantID: testTenantA, RunID: testRun, EventType: "TASK_COMPLETED"}
	decision := SchedulerDecisionView{ID: uuid.New(), TenantID: testTenantA, RunID: testRun, TaskID: testTask}
	verificationRun := VerificationRunView{
		ID: testVerify, TenantID: testTenantA, RunID: testRun, Status: "PASSED",
		Results: []GateResultView{{Status: verification.GatePass}},
	}
	manifest := CompletionManifestView{
		ID: testManifest, TenantID: testTenantA, RunID: testRun,
		Status: verification.ManifestAccepted, Artifacts: []ArtifactView{artifact},
	}
	store := &fakeStore{
		getInteractionFn: func(context.Context, uuid.UUID, uuid.UUID) (*InteractionView, error) {
			copy := interactionItem
			return &copy, nil
		},
		listArtifactsFn: func(context.Context, uuid.UUID, ArtifactQuery) ([]ArtifactView, error) {
			return []ArtifactView{artifact}, nil
		},
		getArtifactFn: func(context.Context, uuid.UUID, uuid.UUID) (*ArtifactView, error) {
			copy := artifact
			return &copy, nil
		},
		getArtifactLineageFn: func(context.Context, uuid.UUID, uuid.UUID, ArtifactLineageQuery) (*ArtifactLineageView, error) {
			return &ArtifactLineageView{RootArtifactID: testArtifact, Artifacts: []ArtifactView{artifact}}, nil
		},
		listEffectsFn: func(context.Context, uuid.UUID, EffectQuery) ([]EffectView, error) {
			return []EffectView{effectItem}, nil
		},
		getEffectFn: func(context.Context, uuid.UUID, uuid.UUID) (*EffectView, error) {
			copy := effectItem
			return &copy, nil
		},
		listTimelineFn: func(context.Context, uuid.UUID, TimelineQuery) ([]TimelineEventView, error) {
			return []TimelineEventView{timeline}, nil
		},
		listSchedulerFn: func(context.Context, uuid.UUID, SchedulerDecisionQuery) ([]SchedulerDecisionView, error) {
			return []SchedulerDecisionView{decision}, nil
		},
		listVerificationsFn: func(context.Context, uuid.UUID, VerificationQuery) ([]VerificationRunView, error) {
			return []VerificationRunView{verificationRun}, nil
		},
		getVerificationFn: func(context.Context, uuid.UUID, uuid.UUID) (*VerificationRunView, error) {
			copy := verificationRun
			return &copy, nil
		},
		getManifestFn: func(context.Context, uuid.UUID, uuid.UUID) (*CompletionManifestView, error) {
			copy := manifest
			return &copy, nil
		},
	}
	service := NewService(store)
	ctx := testContext(testTenantA, "viewer")
	if _, err := service.GetInteraction(ctx, testInteract); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ListArtifacts(ctx, ArtifactQuery{RunID: testRun}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.GetArtifact(ctx, testArtifact); err != nil {
		t.Fatal(err)
	}
	if _, err := service.GetArtifactLineage(ctx, testArtifact, ArtifactLineageQuery{}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ListEffects(ctx, EffectQuery{RunID: testRun}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.GetEffect(ctx, testEffect); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ListTimeline(ctx, TimelineQuery{RunID: testRun}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ListSchedulerDecisions(ctx, SchedulerDecisionQuery{RunID: testRun}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ListVerificationRuns(ctx, VerificationQuery{RunID: testRun}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.GetVerificationRun(ctx, testVerify); err != nil {
		t.Fatal(err)
	}
	if _, err := service.GetCompletionManifest(ctx, testManifest); err != nil {
		t.Fatal(err)
	}

	store.listTimelineFn = func(context.Context, uuid.UUID, TimelineQuery) ([]TimelineEventView, error) {
		leaked := timeline
		leaked.TenantID = testTenantB
		return []TimelineEventView{leaked}, nil
	}
	_, err := service.ListTimeline(ctx, TimelineQuery{RunID: testRun})
	if !errors.Is(err, ErrTenantBoundary) {
		t.Fatalf("timeline tenant leak err = %v", err)
	}
}

func waitingApproval(id uuid.UUID) InteractionView {
	return InteractionView{
		ID: id, TenantID: testTenantA, RunID: testRun, TaskID: uuidPointer(testTask),
		AttemptID: uuidPointer(testAttempt), EffectID: uuidPointer(testEffect),
		InteractionType: interaction.TypeApproval, Status: interaction.StatusWaiting,
		Title: "批准生产 Effect", Payload: map[string]any{},
		AllowedActions: []string{"approve", "reject"}, Version: 4,
	}
}

func preparedR3Effect(approvalID uuid.UUID) EffectView {
	return EffectView{
		ID: testEffect, TenantID: testTenantA, RunID: testRun, TaskID: uuidPointer(testTask),
		AttemptID: testAttempt, IdempotencyKey: "effect-1", RequestHash: "sha256:request",
		RiskLevel: effect.RiskR3ProductionDestructive, Status: effect.StatusPrepared,
		ApprovalInteractionID: uuidPointer(approvalID), Version: 7,
	}
}

func testContext(tenantID uuid.UUID, actor string, scopes ...string) context.Context {
	values := make(map[string]bool, len(scopes))
	for _, scope := range scopes {
		values[scope] = true
	}
	return runcontrol.WithPrincipal(context.Background(), runcontrol.Principal{
		TenantID: tenantID, Subject: actor, Scopes: values,
	})
}

func uuidPointer(value uuid.UUID) *uuid.UUID {
	return &value
}
