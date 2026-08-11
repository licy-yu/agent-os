export type JsonObject = Record<string, unknown>

export type Run = {
  id: string
  definitionId?: string
  tenantId?: string
  name: string
  goal: string
  status: string
  priority: number
  currentPlanVersion: number
  tokenBudget: number
  tokenUsed: number
  costBudgetMicros: number
  costUsedMicros: number
  deadlineAt?: string
  createdAt?: string
  startedAt?: string
  finishedAt?: string
  version: number
  legacy?: boolean
  tasks?: Task[]
  taskStatuses?: Record<string, number>
}

// 旧版名称继续导出，便于后端迁移期间保留类型兼容。
export type Swarm = Run

export type Task = {
  id: string
  runId: string
  swarmId: string
  name: string
  goal: string
  taskType?: string
  logicalKey?: string
  status: string
  priority: number
  dependencyIds?: string[]
  assignedAgentId?: string
  attemptCount: number
  updatedAt?: string
  input?: JsonObject
  inputSpec?: JsonObject
  outputSpec?: JsonObject
  requirements?: JsonObject
  acceptance?: JsonObject
  contextPolicy?: JsonObject
  sideEffectPolicy?: JsonObject
  retryPolicy?: JsonObject
  executionPolicy?: JsonObject
}

export type Agent = {
  id: string
  templateId: string
  swarmId: string
  name: string
  status: string
  load: number
  heartbeatAt?: string
}

export type Overview = {
  swarm_id: string
  task_statuses: Record<string, number>
  agent_statuses: Record<string, number>
  total_attempts: number
  running_attempts: number
  pending_outbox: number
  spent_tokens: number
  budget_tokens: number
  spent_cost_micros: number
  budget_cost_micros: number
  updated_at: string
}

export type Checkpoint = {
  sequence: number
  step_name: string
  state: JsonObject
  created_at: string
}

export type ToolCall = {
  id: string
  tool_name: string
  status: string
  risk_level: string
  arguments: JsonObject
  result: JsonObject
  error_message?: string
  started_at: string
}

export type Attempt = {
  id: string
  task_id: string
  agent_id: string
  attempt_no: number
  status: string
  model: string
  worker_id: string
  output: JsonObject
  tokens_in: number
  tokens_out: number
  cost_micros: number
  step_count: number
  tool_call_count: number
  started_at?: string
  finished_at?: string
  heartbeat_at?: string
  error_code?: string
  error_message?: string
  evaluation?: {
    reviewer: string
    machine_pass: boolean
    policy_pass: boolean
    quality_score: number
    decision: string
    findings: string[]
  }
  checkpoints: Checkpoint[]
  tool_calls: ToolCall[]
}

export type TimelineItem = {
  id: string
  eventType: string
  aggregateType?: string
  aggregateId?: string
  actor?: string
  message?: string
  status?: string
  createdAt: string
  publishedAt?: string
  payload: JsonObject
}

export type EventItem = TimelineItem

export type Artifact = {
  id: string
  taskId?: string
  attemptId?: string
  name: string
  artifactType: string
  version: string
  status: string
  contentUri?: string
  contentHash?: string
  schemaVersion?: string
  sizeBytes?: number
  createdAt?: string
  metadata: JsonObject
}

export type SchedulerDecision = {
  id: string
  taskId: string
  taskName?: string
  agentId?: string
  agentName?: string
  outcome: string
  score?: number
  reason: string
  filters: string[]
  createdAt?: string
  details: JsonObject
}

export type Interaction = {
  id: string
  runId?: string
  taskId?: string
  interactionType: string
  status: string
  title: string
  prompt: string
  riskLevel?: string
  createdAt?: string
  expiresAt?: string
  version: number
  metadata: JsonObject
}

export type RunListResult = {
  items: Run[]
  legacy: boolean
}

export type CreateRunInput = {
  name: string
  goal: string
  tokenBudget: number
  costBudgetMicros: number
  maxAgents: number
  executionEngine?: string
  plan?: JsonObject
}

export class APIError extends Error {
  readonly status: number
  readonly path: string
  readonly responseBody: string

  constructor(path: string, status: number, responseBody: string) {
    let detail = responseBody
    try {
      const parsed = JSON.parse(responseBody) as { message?: string }
      detail = parsed.message || responseBody
    } catch {
      // 非 JSON 错误（例如代理返回的纯文本）保持原文，便于现场诊断。
    }
    super(status > 0 ? `HTTP ${status}${detail ? ` · ${detail}` : ''}` : responseBody)
    this.name = 'APIError'
    this.path = path
    this.status = status
    this.responseBody = responseBody
  }
}

const apiKeyStorage = 'swarmos.apiKey'

// API Key 只保存在当前浏览器标签页的 sessionStorage，不写 Cookie、URL 或长期磁盘缓存。
export function getAPIKey() {
  return window.sessionStorage.getItem(apiKeyStorage) ?? ''
}

export function setAPIKey(value: string) {
  const clean = value.trim()
  if (clean) window.sessionStorage.setItem(apiKeyStorage, clean)
  else window.sessionStorage.removeItem(apiKeyStorage)
}

type RequestOptions = Omit<RequestInit, 'body'> & {
  body?: unknown
  timeoutMs?: number
}

export async function request<T>(path: string, options: RequestOptions = {}): Promise<T> {
  const controller = new AbortController()
  const timer = window.setTimeout(() => controller.abort(), options.timeoutMs ?? 8_000)
  const headers = new Headers(options.headers)
  headers.set('Accept', 'application/json')
  if (options.body !== undefined) headers.set('Content-Type', 'application/json')
  const apiKey = getAPIKey()
  if (apiKey) headers.set('Authorization', `Bearer ${apiKey}`)

  try {
    const response = await fetch(path, {
      ...options,
      body: options.body === undefined ? undefined : JSON.stringify(options.body),
      credentials: 'same-origin',
      headers,
      signal: controller.signal,
    })
    const responseBody = response.status === 204 ? '' : await response.text()
    if (!response.ok) throw new APIError(path, response.status, responseBody)
    if (!responseBody) return undefined as T
    return JSON.parse(responseBody) as T
  } catch (error) {
    if (error instanceof APIError) throw error
    if (error instanceof DOMException && error.name === 'AbortError') {
      throw new APIError(path, 0, '请求超时')
    }
    throw error
  } finally {
    window.clearTimeout(timer)
  }
}

export function endpointUnavailable(error: unknown) {
  return error instanceof APIError && [404, 405, 501].includes(error.status)
}

function object(value: unknown): JsonObject {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as JsonObject
    : {}
}

function pick(value: JsonObject, ...keys: string[]): unknown {
  for (const key of keys) {
    if (value[key] !== undefined && value[key] !== null) return value[key]
  }
  return undefined
}

function text(value: unknown, fallback = ''): string {
  if (typeof value === 'string') return value
  if (typeof value === 'number' || typeof value === 'boolean') return String(value)
  return fallback
}

function number(value: unknown, fallback = 0): number {
  const parsed = typeof value === 'number' ? value : Number(value)
  return Number.isFinite(parsed) ? parsed : fallback
}

function texts(value: unknown): string[] {
  return Array.isArray(value) ? value.map((item) => text(item)).filter(Boolean) : []
}

// VM 局域网常通过 HTTP 打开控制台，randomUUID 在非安全上下文可能不可用。
function requestID() {
  if (typeof globalThis.crypto?.randomUUID === 'function') return globalThis.crypto.randomUUID()
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`
}

function normalizeTask(raw: unknown): Task {
  const value = object(raw)
  const runId = text(pick(value, 'runId', 'run_id', 'swarmId', 'swarm_id'))
  return {
    id: text(value.id),
    runId,
    swarmId: text(pick(value, 'swarmId', 'swarm_id'), runId),
    name: text(value.name, text(pick(value, 'logicalKey', 'logical_key'), '未命名任务')),
    goal: text(value.goal),
    taskType: text(pick(value, 'taskType', 'task_type')) || undefined,
    logicalKey: text(pick(value, 'logicalKey', 'logical_key')) || undefined,
    status: text(value.status, 'DRAFT'),
    priority: number(value.priority, 50),
    dependencyIds: texts(pick(value, 'dependencyIds', 'dependency_ids')),
    assignedAgentId: text(pick(value, 'assignedAgentId', 'assigned_agent_id')) || undefined,
    attemptCount: number(pick(value, 'attemptCount', 'attempt_count')),
    updatedAt: text(pick(value, 'updatedAt', 'updated_at')) || undefined,
    input: object(value.input),
    inputSpec: object(pick(value, 'inputSpec', 'input_spec')),
    outputSpec: object(pick(value, 'outputSpec', 'output_spec')),
    requirements: object(value.requirements),
    acceptance: object(value.acceptance),
    contextPolicy: object(pick(value, 'contextPolicy', 'context_policy')),
    sideEffectPolicy: object(pick(value, 'sideEffectPolicy', 'side_effect_policy')),
    retryPolicy: object(pick(value, 'retryPolicy', 'retry_policy')),
    executionPolicy: object(pick(value, 'executionPolicy', 'execution_policy')),
  }
}

export function normalizeRun(raw: unknown, legacy = false): Run {
  const value = object(raw)
  const budget = object(value.budget)
  const timestamps = object(value.timestamps)
  const rawTasks = pick(value, 'tasks', 'taskList', 'task_list')
  return {
    id: text(value.id),
    definitionId: text(pick(value, 'definitionId', 'definition_id', 'swarmId', 'swarm_id')) || undefined,
    tenantId: text(pick(value, 'tenantId', 'tenant_id')) || undefined,
    name: text(value.name, `Run ${text(value.id).slice(0, 8)}`),
    goal: text(value.goal),
    status: text(value.status, legacy ? 'PENDING' : 'CREATED'),
    priority: number(value.priority, 50),
    currentPlanVersion: number(pick(value, 'currentPlanVersion', 'current_plan_version')),
    tokenBudget: number(pick(value, 'tokenBudget', 'token_budget', 'budgetTokens', 'budget_tokens', 'maxTokens', 'max_tokens') ?? pick(budget, 'maxTokens', 'max_tokens')),
    tokenUsed: number(pick(value, 'tokenUsed', 'token_used', 'spentTokens', 'spent_tokens') ?? pick(budget, 'spentTokens', 'spent_tokens')),
    costBudgetMicros: number(pick(value, 'costBudgetMicros', 'cost_budget_micros', 'budgetCostMicros', 'budget_cost_micros', 'maxCostMicros', 'max_cost_micros') ?? pick(budget, 'maxCostMicros', 'max_cost_micros')),
    costUsedMicros: number(pick(value, 'costUsedMicros', 'cost_used_micros', 'spentCostMicros', 'spent_cost_micros') ?? pick(budget, 'spentCostMicros', 'spent_cost_micros')),
    deadlineAt: text(pick(value, 'deadlineAt', 'deadline_at', 'deadline')) || undefined,
    createdAt: text(pick(value, 'createdAt', 'created_at') ?? pick(timestamps, 'createdAt', 'created_at')) || undefined,
    startedAt: text(pick(value, 'startedAt', 'started_at') ?? pick(timestamps, 'startedAt', 'started_at')) || undefined,
    finishedAt: text(pick(value, 'finishedAt', 'finished_at') ?? pick(timestamps, 'finishedAt', 'finished_at')) || undefined,
    version: number(value.version, 1),
    legacy,
    tasks: Array.isArray(rawTasks) ? rawTasks.map(normalizeTask) : undefined,
    taskStatuses: object(pick(value, 'taskStatuses', 'task_statuses')) as Record<string, number>,
  }
}

export async function listRuns(): Promise<RunListResult> {
  try {
    const response = await request<{ items?: unknown[] }>('/api/v1/runs?page_size=100')
    return { items: (response?.items ?? []).map((item) => normalizeRun(item)), legacy: false }
  } catch (error) {
    if (!endpointUnavailable(error)) throw error
    const response = await request<{ items?: unknown[] }>('/api/v1/swarms?page_size=50')
    return { items: (response?.items ?? []).map((item) => normalizeRun(item, true)), legacy: true }
  }
}

export async function getRun(run: Run): Promise<Run> {
  const primaryPath = run.legacy ? `/api/v1/swarms/${run.id}` : `/api/v1/runs/${run.id}`
  try {
    return normalizeRun(await request<unknown>(primaryPath), Boolean(run.legacy))
  } catch (error) {
    if (!run.legacy || !endpointUnavailable(error)) throw error
    return run
  }
}

export async function createRun(input: CreateRunInput, legacy: boolean): Promise<Run> {
  if (legacy) {
    const response = await request<unknown>('/api/v1/swarms', {
      method: 'POST',
      body: {
        name: input.name,
        goal: input.goal,
        budgetTokens: String(input.tokenBudget),
        budgetCostMicros: String(input.costBudgetMicros),
        maxAgents: input.maxAgents,
      },
    })
    return normalizeRun(response, true)
  }
  const response = await request<unknown>('/api/v1/runs', {
    method: 'POST',
    body: {
      name: input.name,
      goal: input.goal,
      budgetTokens: input.tokenBudget,
      budgetCostMicros: input.costBudgetMicros,
      maxAgents: input.maxAgents,
      executionEngine: input.executionEngine,
      plan: input.plan,
    },
  })
  return normalizeRun(response)
}

export async function runCommand(runID: string, command: 'pause' | 'resume' | 'cancel' | 'replan', body?: JsonObject) {
  return request<unknown>(`/api/v1/runs/${runID}:${command}`, {
    method: 'POST',
    headers: { 'Idempotency-Key': requestID() },
    body: body ?? {},
  })
}

export async function listRunTasks(run: Run, embedded: Task[] = []): Promise<Task[]> {
  if (embedded.length > 0) return embedded
  if (!run.legacy) {
    try {
      const response = await request<{ items?: unknown[] }>(`/api/v1/runs/${run.id}/tasks?page_size=100`)
      return (response?.items ?? []).map(normalizeTask)
    } catch (error) {
      if (!endpointUnavailable(error)) throw error
    }
  }
  const response = await request<{ items?: unknown[] }>(`/api/v1/tasks?swarm_id=${encodeURIComponent(run.id)}&page_size=100`)
  return (response?.items ?? []).map(normalizeTask)
}

export async function listRunAgents(runID: string): Promise<Agent[]> {
  const response = await request<{ items?: Agent[] }>(`/api/v1/agents?swarm_id=${encodeURIComponent(runID)}&page_size=100`)
  return response?.items ?? []
}

export async function getLegacyOverview(runID: string): Promise<Overview | undefined> {
  try {
    return await request<Overview>(`/api/v1/console/overview?swarm_id=${encodeURIComponent(runID)}`)
  } catch (error) {
    if (endpointUnavailable(error)) return undefined
    throw error
  }
}

function normalizeTimeline(raw: unknown): TimelineItem {
  const value = object(raw)
  return {
    id: text(value.id, requestID()),
    eventType: text(pick(value, 'eventType', 'event_type', 'type'), 'EVENT'),
    aggregateType: text(pick(value, 'aggregateType', 'aggregate_type')) || undefined,
    aggregateId: text(pick(value, 'aggregateId', 'aggregate_id')) || undefined,
    actor: text(value.actor) || undefined,
    message: text(pick(value, 'message', 'summary', 'description')) || undefined,
    status: text(value.status) || undefined,
    createdAt: text(pick(value, 'createdAt', 'created_at', 'occurredAt', 'occurred_at'), new Date().toISOString()),
    publishedAt: text(pick(value, 'publishedAt', 'published_at')) || undefined,
    payload: object(value.payload),
  }
}

export async function listTimeline(run: Run): Promise<TimelineItem[]> {
  if (!run.legacy) {
    try {
      const response = await request<{ items?: unknown[] }>(`/api/v1/runs/${run.id}/timeline`)
      return (response?.items ?? []).map(normalizeTimeline)
    } catch (error) {
      if (!endpointUnavailable(error)) throw error
    }
  }
  try {
    const response = await request<{ items?: unknown[] }>(`/api/v1/console/events?swarm_id=${encodeURIComponent(run.id)}`)
    return (response?.items ?? []).map(normalizeTimeline)
  } catch (error) {
    if (endpointUnavailable(error)) return []
    throw error
  }
}

function normalizeArtifact(raw: unknown): Artifact {
  const value = object(raw)
  return {
    id: text(value.id),
    taskId: text(pick(value, 'taskId', 'task_id')) || undefined,
    attemptId: text(pick(value, 'attemptId', 'attempt_id')) || undefined,
    name: text(value.name, text(pick(value, 'artifactType', 'artifact_type'), 'Artifact')),
    artifactType: text(pick(value, 'artifactType', 'artifact_type', 'type'), 'FILE'),
    version: text(value.version, '1'),
    status: text(value.status, 'AVAILABLE'),
    contentUri: text(pick(value, 'contentUri', 'content_uri', 'uri')) || undefined,
    contentHash: text(pick(value, 'contentHash', 'content_hash')) || undefined,
    schemaVersion: text(pick(value, 'schemaVersion', 'schema_version')) || undefined,
    sizeBytes: number(pick(value, 'sizeBytes', 'size_bytes')) || undefined,
    createdAt: text(pick(value, 'createdAt', 'created_at')) || undefined,
    metadata: object(value.metadata),
  }
}

export async function listArtifacts(run: Run): Promise<Artifact[]> {
  if (run.legacy) return []
  try {
    const response = await request<{ items?: unknown[] }>(`/api/v1/runs/${run.id}/artifacts`)
    return (response?.items ?? []).map(normalizeArtifact)
  } catch (error) {
    if (endpointUnavailable(error)) return []
    throw error
  }
}

function normalizeDecision(raw: unknown): SchedulerDecision {
  const value = object(raw)
  const filters = pick(value, 'filters', 'filterResults', 'filter_results')
  return {
    id: text(value.id, requestID()),
    taskId: text(pick(value, 'taskId', 'task_id')),
    taskName: text(pick(value, 'taskName', 'task_name')) || undefined,
    agentId: text(pick(value, 'agentId', 'agent_id')) || undefined,
    agentName: text(pick(value, 'agentName', 'agent_name')) || undefined,
    outcome: text(pick(value, 'outcome', 'decision', 'status'), 'EVALUATED'),
    score: pick(value, 'score', 'totalScore', 'total_score') === undefined
      ? undefined
      : number(pick(value, 'score', 'totalScore', 'total_score')),
    reason: text(pick(value, 'reason', 'explanation', 'message'), '调度器未提供解释'),
    filters: Array.isArray(filters)
      ? filters.map((item) => typeof item === 'string' ? item : JSON.stringify(item))
      : [],
    createdAt: text(pick(value, 'createdAt', 'created_at')) || undefined,
    details: object(pick(value, 'details', 'scores')),
  }
}

export async function listSchedulerDecisions(run: Run): Promise<SchedulerDecision[]> {
  if (run.legacy) return []
  try {
    const response = await request<{ items?: unknown[] }>(`/api/v1/runs/${run.id}/scheduler-decisions`)
    return (response?.items ?? []).map(normalizeDecision)
  } catch (error) {
    if (endpointUnavailable(error)) return []
    throw error
  }
}

function normalizeInteraction(raw: unknown): Interaction {
  const value = object(raw)
  const payload = object(value.payload)
  return {
    id: text(value.id),
    runId: text(pick(value, 'runId', 'run_id', 'swarmId', 'swarm_id')) || undefined,
    taskId: text(pick(value, 'taskId', 'task_id')) || undefined,
    interactionType: text(pick(value, 'interactionType', 'interaction_type', 'type'), 'INPUT_REQUIRED'),
    status: text(value.status, 'PENDING'),
    title: text(value.title, text(payload.title, '需要用户处理')),
    prompt: text(pick(value, 'prompt', 'message', 'description') ?? pick(payload, 'prompt', 'message', 'description')),
    riskLevel: text(pick(value, 'riskLevel', 'risk_level') ?? pick(payload, 'riskLevel', 'risk_level')) || undefined,
    createdAt: text(pick(value, 'createdAt', 'created_at')) || undefined,
    expiresAt: text(pick(value, 'expiresAt', 'expires_at')) || undefined,
    version: number(value.version, 1),
    metadata: Object.keys(object(value.metadata)).length ? object(value.metadata) : payload,
  }
}

export async function listInteractions(): Promise<Interaction[]> {
  try {
    const response = await request<{ items?: unknown[] }>('/api/v1/interactions?status=PENDING')
    return (response?.items ?? []).map(normalizeInteraction)
  } catch (error) {
    if (endpointUnavailable(error)) return []
    throw error
  }
}

const pendingMutationKeys = new Map<string, string>()

export async function interactionCommand(item: Pick<Interaction, 'id' | 'version'>, command: 'approve' | 'reject' | 'resolve', resolution = '') {
  const mutation = `${item.id}:${item.version}:${command}`
  const idempotencyKey = pendingMutationKeys.get(mutation) ?? requestID()
  pendingMutationKeys.set(mutation, idempotencyKey)
  try {
    const response = await request<unknown>(`/api/v1/interactions/${item.id}:${command}`, {
      method: 'POST',
      headers: { 'Idempotency-Key': idempotencyKey },
      body: {
        version: item.version,
        ...(resolution ? { resolution: { value: resolution } } : {}),
      },
    })
    pendingMutationKeys.delete(mutation)
    return response
  } catch (error) {
    // 网络超时后保留同一 Key；用户再次点击会安全回放，而不是产生第二次审批。
    throw error
  }
}

export async function listAttempts(taskID: string, runID: string): Promise<Attempt[]> {
  // run_id 不是为了查询方便，而是服务端在读取旧版 Attempt 聚合前执行租户归属校验
  // 所必需的安全上下文。这样即使猜到其他租户的 task_id，也无法越权枚举执行证据。
  const query = new URLSearchParams({ task_id: taskID, run_id: runID })
  const response = await request<{ items?: Attempt[] }>(`/api/v1/console/attempts?${query.toString()}`)
  return response?.items ?? []
}
