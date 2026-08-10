export type Swarm = {
  id: string
  name: string
  goal: string
  status: string
  budgetTokens: string
  budgetCostMicros: string
  maxAgents: number
  createdAt: string
}

export type Task = {
  id: string
  swarmId: string
  name: string
  goal: string
  status: string
  priority: number
  dependencyIds?: string[]
  assignedAgentId?: string
  attemptCount: number
  updatedAt: string
}

export type Agent = {
  id: string
  templateId: string
  swarmId: string
  name: string
  status: string
  load: number
  heartbeatAt: string
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
  state: Record<string, unknown>
  created_at: string
}

export type ToolCall = {
  id: string
  tool_name: string
  status: string
  risk_level: string
  arguments: Record<string, unknown>
  result: Record<string, unknown>
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
  output: Record<string, unknown>
  tokens_in: number
  tokens_out: number
  cost_micros: number
  step_count: number
  tool_call_count: number
  started_at?: string
  finished_at?: string
  error_code?: string
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

export type EventItem = {
  id: string
  aggregate_type: string
  aggregate_id: string
  event_type: string
  aggregate_version: number
  payload: Record<string, unknown>
  attempts: number
  published_at?: string
  last_error?: string
  created_at: string
}

export async function request<T>(path: string): Promise<T> {
  const controller = new AbortController()
  const timer = window.setTimeout(() => controller.abort(), 8_000)
  try {
    const response = await fetch(path, { signal: controller.signal })
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`)
    }
    return (await response.json()) as T
  } finally {
    window.clearTimeout(timer)
  }
}
