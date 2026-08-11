import {
  useCallback,
  useEffect,
  useMemo,
  useState,
  type CSSProperties,
  type FormEvent,
  type ReactNode,
} from 'react'
import {
  Activity,
  AlertTriangle,
  Bot,
  Boxes,
  Braces,
  Check,
  CheckCircle2,
  ChevronRight,
  CircleGauge,
  Clock3,
  Coins,
  Cpu,
  Database,
  FileBox,
  FileCheck2,
  GitBranch,
  Inbox,
  KeyRound,
  Layers3,
  ListTree,
  MessageSquareText,
  PackageOpen,
  Pause,
  Play,
  Plus,
  RefreshCw,
  RotateCcw,
  Search,
  ServerCog,
  ShieldAlert,
  ShieldCheck,
  Sparkles,
  TerminalSquare,
  Workflow,
  X,
  XCircle,
  Zap,
} from 'lucide-react'
import {
  type Agent,
  type Artifact,
  type Attempt,
  type CreateRunInput,
  type Interaction,
  type Overview,
  type Run,
  type SchedulerDecision,
  type Task,
  type TimelineItem,
  createRun,
  getLegacyOverview,
  getAPIKey,
  getRun,
  interactionCommand,
  listArtifacts,
  listAttempts,
  listInteractions,
  listRunAgents,
  listRunTasks,
  listRuns,
  listSchedulerDecisions,
  listTimeline,
  runCommand,
  setAPIKey,
} from './api'

const terminalTaskStatuses = new Set([
  'SUCCEEDED', 'COMPLETED', 'FAILED', 'CANCELED', 'REJECTED', 'SUPERSEDED',
])
const terminalRunStatuses = new Set(['COMPLETED', 'SUCCEEDED', 'FAILED', 'CANCELED'])

function shortID(value?: string) {
  return value ? value.slice(0, 8) : '—'
}

function formatNumber(value: number) {
  return new Intl.NumberFormat('zh-CN', {
    notation: Math.abs(value) > 999_999 ? 'compact' : 'standard',
    maximumFractionDigits: 1,
  }).format(value)
}

function formatCost(micros: number) {
  return new Intl.NumberFormat('zh-CN', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 4,
  }).format(micros / 1_000_000)
}

function formatTime(value?: string) {
  if (!value) return '—'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return '—'
  return new Intl.DateTimeFormat('zh-CN', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  }).format(date)
}

function formatDate(value?: string) {
  if (!value) return '—'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return '—'
  return new Intl.DateTimeFormat('zh-CN', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  }).format(date)
}

function statusTone(status: string) {
  const value = status.toUpperCase()
  if (['SUCCEEDED', 'COMPLETED', 'IDLE', 'ACCEPT', 'ACCEPTED', 'RESOLVED', 'APPROVED', 'AVAILABLE'].includes(value)) return 'success'
  if (['RUNNING', 'ASSIGNED', 'RESERVED', 'REVIEW', 'VERIFYING', 'SCHEDULED', 'LEASED', 'EXECUTING'].includes(value)) return 'active'
  if (['FAILED', 'OFFLINE', 'REJECT', 'REJECTED', 'ABANDONED', 'CANCELED', 'EXPIRED'].includes(value)) return 'danger'
  if (['BLOCKED', 'RETRY_WAIT', 'WAITING_TOOL', 'WAITING_INPUT', 'WAITING_USER', 'WAITING_EXTERNAL', 'PAUSED', 'RECOVERING', 'DEGRADED', 'REPLAN_REQUIRED', 'PENDING'].includes(value)) return 'warning'
  return 'neutral'
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : '未知错误'
}

function safeJSON(value: unknown) {
  try {
    return JSON.stringify(value, null, 2)
  } catch {
    return '无法序列化的数据'
  }
}

function StatusBadge({ status }: { status: string }) {
  return <span className={`status-badge ${statusTone(status)}`}><i />{status}</span>
}

function MetricCard({ icon, label, value, note, tone = 'cyan' }: {
  icon: ReactNode
  label: string
  value: string
  note: string
  tone?: 'cyan' | 'lime' | 'amber' | 'violet'
}) {
  return (
    <article className={`metric-card ${tone}`}>
      <div className="metric-icon">{icon}</div>
      <div><p>{label}</p><strong>{value}</strong><small>{note}</small></div>
    </article>
  )
}

function EmptyState({ icon, children }: { icon: ReactNode; children: ReactNode }) {
  return <div className="empty-state">{icon}<p>{children}</p></div>
}

function RunRail({ runs, selectedID, query, onQuery, onSelect }: {
  runs: Run[]
  selectedID: string
  query: string
  onQuery: (value: string) => void
  onSelect: (id: string) => void
}) {
  const visibleRuns = runs.filter((run) => `${run.name} ${run.goal} ${run.id}`.toLowerCase().includes(query.toLowerCase()))
  return (
    <section className="run-rail" aria-labelledby="run-list-title">
      <div className="run-rail-heading"><p id="run-list-title">RUNS</p><span>{runs.length}</span></div>
      <label className="run-search"><Search size={13} /><span className="sr-only">搜索 Run</span><input value={query} onChange={(event) => onQuery(event.target.value)} placeholder="搜索运行" /></label>
      <div className="run-list">
        {visibleRuns.map((run) => (
          <button
            type="button"
            key={run.id}
            className={selectedID === run.id ? 'selected' : ''}
            aria-pressed={selectedID === run.id}
            onClick={() => onSelect(run.id)}
          >
            <i className={statusTone(run.status)} />
            <span><strong>{run.name}</strong><small>{shortID(run.id)} · {run.status}</small></span>
            <ChevronRight size={13} />
          </button>
        ))}
        {visibleRuns.length === 0 && <p className="run-list-empty">没有匹配的 Run</p>}
      </div>
    </section>
  )
}

function DAGBoard({ tasks, selectedID, onSelect }: {
  tasks: Task[]
  selectedID?: string
  onSelect: (task: Task) => void
}) {
  const layers = useMemo(() => {
    const byID = new Map(tasks.map((task) => [task.id, task]))
    const cache = new Map<string, number>()
    const calculate = (task: Task, trail: Set<string>): number => {
      const cached = cache.get(task.id)
      if (cached !== undefined) return cached
      if (trail.has(task.id)) return 0
      const dependencies = (task.dependencyIds ?? []).map((id) => byID.get(id)).filter(Boolean) as Task[]
      const value = dependencies.length === 0
        ? 0
        : Math.max(...dependencies.map((dependency) => calculate(dependency, new Set([...trail, task.id])))) + 1
      cache.set(task.id, value)
      return value
    }
    const result = new Map<number, Task[]>()
    tasks.forEach((task) => {
      const layer = calculate(task, new Set())
      result.set(layer, [...(result.get(layer) ?? []), task])
    })
    return [...result.entries()].sort(([a], [b]) => a - b)
  }, [tasks])

  if (tasks.length === 0) return <EmptyState icon={<Workflow size={28} />}>当前 Run 还没有 Task DAG</EmptyState>

  return (
    <div className="dag-scroll" aria-label="任务 DAG">
      <div className="dag-board">
        {layers.map(([layer, items], index) => (
          <div className="dag-stage" key={layer}>
            <div className="stage-label"><span>{String(layer + 1).padStart(2, '0')}</span> STAGE</div>
            <div className="stage-nodes">
              {items.map((task) => (
                <button
                  type="button"
                  key={task.id}
                  className={`task-node ${selectedID === task.id ? 'selected' : ''}`}
                  onClick={() => onSelect(task)}
                  aria-label={`查看任务 ${task.name}`}
                >
                  <div className="node-top"><StatusBadge status={task.status} /><span>P{task.priority}</span></div>
                  <strong>{task.name}</strong>
                  <p>{task.goal}</p>
                  <div className="node-meta">
                    <span><GitBranch size={12} />{task.dependencyIds?.length ?? 0}</span>
                    <span><Bot size={12} />{shortID(task.assignedAgentId)}</span>
                    <span><RefreshCw size={12} />{task.attemptCount}</span>
                  </div>
                </button>
              ))}
            </div>
            {index < layers.length - 1 && <div className="flow-arrow"><ChevronRight size={18} /></div>}
          </div>
        ))}
      </div>
    </div>
  )
}

function TaskInspector({ task, attempts, loading }: { task?: Task; attempts: Attempt[]; loading: boolean }) {
  const [selectedAttemptID, setSelectedAttemptID] = useState('')
  useEffect(() => {
    setSelectedAttemptID((current) => attempts.some((item) => item.id === current) ? current : attempts[0]?.id ?? '')
  }, [task?.id, attempts])
  const attempt = attempts.find((item) => item.id === selectedAttemptID) ?? attempts[0]
  const contract = task ? {
    input: task.input,
    inputSpec: task.inputSpec,
    outputSpec: task.outputSpec,
    requirements: task.requirements,
    acceptance: task.acceptance,
    contextPolicy: task.contextPolicy,
    sideEffectPolicy: task.sideEffectPolicy,
    retryPolicy: task.retryPolicy,
    executionPolicy: task.executionPolicy,
  } : undefined

  return (
    <section className="panel inspector-panel" id="task-detail">
      <div className="panel-heading">
        <div><span className="eyebrow">TASK CONTRACT & EVIDENCE</span><h2>任务详情</h2></div>
        {task && <StatusBadge status={task.status} />}
      </div>
      {!task ? (
        <EmptyState icon={<Search size={26} />}>选择 DAG 节点查看合同和执行证据</EmptyState>
      ) : loading ? (
        <div className="loading-lines"><i /><i /><i /></div>
      ) : (
        <div className="inspector-body">
          <div className="task-title-row">
            <div><small>TASK / {shortID(task.id)}</small><h3>{task.name}</h3><p>{task.goal}</p></div>
            <div className="task-assignment"><span>ASSIGNED AGENT</span><strong>{shortID(task.assignedAgentId)}</strong></div>
          </div>
          <div className="task-contract-strip">
            <span><GitBranch size={13} />{task.dependencyIds?.length ?? 0} dependencies</span>
            <span><RefreshCw size={13} />{attempts.length} attempts</span>
            <span><ListTree size={13} />{task.taskType ?? task.logicalKey ?? 'generic'}</span>
          </div>
          <details className="contract-details">
            <summary>查看完整 Task Contract</summary>
            <pre>{safeJSON(contract)}</pre>
          </details>
          {attempts.length > 0 && (
            <div className="attempt-tabs" aria-label="Attempt 历史">
              {attempts.map((item) => (
                <button type="button" key={item.id} className={item.id === attempt?.id ? 'selected' : ''} onClick={() => setSelectedAttemptID(item.id)}>
                  #{item.attempt_no}<span>{item.status}</span>
                </button>
              ))}
            </div>
          )}
          {!attempt ? (
            <EmptyState icon={<Clock3 size={24} />}>任务尚未产生 Attempt</EmptyState>
          ) : (
            <>
              <div className="evidence-grid">
                <div><span>MODEL</span><strong>{attempt.model || '—'}</strong></div>
                <div><span>TOKENS</span><strong>{formatNumber(attempt.tokens_in + attempt.tokens_out)}</strong></div>
                <div><span>STEPS</span><strong>{attempt.step_count}</strong></div>
                <div><span>COST</span><strong>{formatCost(attempt.cost_micros)}</strong></div>
              </div>
              {attempt.evaluation && (
                <div className="review-card">
                  <div className="review-score"><ShieldCheck size={18} /><strong>{Math.round(attempt.evaluation.quality_score * 100)}</strong><span>/100</span></div>
                  <div><small>VERIFICATION DECISION</small><h4>{attempt.evaluation.decision}</h4><p>{attempt.evaluation.machine_pass ? '机器检查通过' : '机器检查未通过'} · {attempt.evaluation.policy_pass ? '策略合规' : '策略拒绝'}</p></div>
                </div>
              )}
              {(attempt.error_code || attempt.error_message) && (
                <div className="failure-card"><AlertTriangle size={17} /><div><strong>{attempt.error_code ?? 'EXECUTION_FAILURE'}</strong><p>{attempt.error_message ?? '执行失败，等待恢复策略处理。'}</p></div></div>
              )}
              <div className="evidence-columns">
                <div className="evidence-section">
                  <h4><Layers3 size={15} />CHECKPOINTS</h4>
                  {(attempt.checkpoints ?? []).length === 0 && <p className="muted">暂无 Checkpoint</p>}
                  {(attempt.checkpoints ?? []).map((checkpoint) => (
                    <div className="checkpoint" key={checkpoint.sequence}><span>{String(checkpoint.sequence).padStart(2, '0')}</span><div><strong>{checkpoint.step_name}</strong><small>{formatTime(checkpoint.created_at)}</small></div><CheckCircle2 size={15} /></div>
                  ))}
                </div>
                <div className="evidence-section">
                  <h4><TerminalSquare size={15} />TOOL CALLS</h4>
                  {(attempt.tool_calls ?? []).length === 0 && <p className="muted">无工具调用</p>}
                  {(attempt.tool_calls ?? []).map((call) => (
                    <div className="tool-call" key={call.id}><Braces size={15} /><div><strong>{call.tool_name}</strong><small>{call.risk_level}</small></div><StatusBadge status={call.status} /></div>
                  ))}
                </div>
              </div>
            </>
          )}
        </div>
      )}
    </section>
  )
}

function TimelinePanel({ items }: { items: TimelineItem[] }) {
  return (
    <section className="panel timeline-panel" id="timeline">
      <div className="panel-heading"><div><span className="eyebrow">RUN EVENT PROJECTION</span><h2>Timeline</h2></div><span className="panel-count">{items.length} EVENTS</span></div>
      {items.length === 0 ? <EmptyState icon={<Activity size={25} />}>尚无 Timeline 事件</EmptyState> : (
        <div className="timeline-list">
          {items.slice(0, 40).map((item) => (
            <article className="timeline-row" key={item.id}>
              <div className={`timeline-marker ${statusTone(item.status ?? (item.publishedAt ? 'SUCCEEDED' : 'RUNNING'))}`}><i /></div>
              <div className="timeline-copy">
                <div><strong>{item.eventType}</strong>{item.status && <StatusBadge status={item.status} />}</div>
                <p>{item.message || `${item.aggregateType ?? 'run'} / ${shortID(item.aggregateId)}`}</p>
              </div>
              <div className="timeline-meta"><time>{formatTime(item.createdAt)}</time><small>{item.actor ?? (item.publishedAt ? 'PUBLISHED' : 'PROJECTED')}</small></div>
            </article>
          ))}
        </div>
      )}
    </section>
  )
}

function ArtifactPanel({ items }: { items: Artifact[] }) {
  return (
    <section className="panel artifact-panel" id="artifacts">
      <div className="panel-heading"><div><span className="eyebrow">VERSIONED OUTPUT</span><h2>Artifacts</h2></div><PackageOpen size={18} /></div>
      {items.length === 0 ? <EmptyState icon={<FileBox size={25} />}>当前 Run 尚无可追踪 Artifact</EmptyState> : (
        <div className="artifact-list">
          {items.slice(0, 16).map((item) => (
            <article className="artifact-row" key={item.id}>
              <div className="artifact-icon"><FileCheck2 size={17} /></div>
              <div><strong>{item.name}</strong><small>{item.artifactType} · v{item.version} · {shortID(item.contentHash)}</small></div>
              <StatusBadge status={item.status} />
            </article>
          ))}
        </div>
      )}
    </section>
  )
}

function SchedulerPanel({ items, selectedTaskID }: { items: SchedulerDecision[]; selectedTaskID?: string }) {
  const visible = selectedTaskID ? items.filter((item) => !item.taskId || item.taskId === selectedTaskID) : items
  return (
    <section className="panel scheduler-panel" id="scheduler-explain">
      <div className="panel-heading"><div><span className="eyebrow">FILTER / SCORE / BIND</span><h2>Scheduler Explain</h2></div><CircleGauge size={18} /></div>
      {visible.length === 0 ? <EmptyState icon={<ListTree size={25} />}>尚无可展示的调度解释</EmptyState> : (
        <div className="decision-list">
          {visible.slice(0, 12).map((item) => (
            <article className="decision-row" key={item.id}>
              <div className="decision-score"><span>SCORE</span><strong>{item.score === undefined ? '—' : item.score.toFixed(1)}</strong></div>
              <div><div><strong>{item.taskName ?? `Task ${shortID(item.taskId)}`}</strong><StatusBadge status={item.outcome} /></div><p>{item.reason}</p><small>{item.agentName ?? shortID(item.agentId)}{item.filters.length ? ` · ${item.filters.join(' · ')}` : ''}</small></div>
            </article>
          ))}
        </div>
      )}
    </section>
  )
}

function interactionLabel(type: string) {
  const value = type.toUpperCase()
  if (value.includes('APPROVAL')) return '执行审批'
  if (value.includes('AUTH')) return '授权请求'
  if (value.includes('BUDGET')) return '预算审批'
  if (value.includes('TAKEOVER')) return '接管请求'
  if (value.includes('CHOICE')) return '需要选择'
  if (value.includes('EDIT')) return '需要编辑'
  return '补充输入'
}

function InteractionInbox({ items, busyID, onAction }: {
  items: Interaction[]
  busyID: string
  onAction: (item: Interaction, action: 'approve' | 'reject' | 'resolve', resolution?: string) => Promise<void>
}) {
  const [responses, setResponses] = useState<Record<string, string>>({})
  return (
    <section className="panel inbox-panel" id="inbox">
      <div className="panel-heading"><div><span className="eyebrow">HUMAN INTERACTION</span><h2>Approval / Input Inbox</h2></div><span className={`inbox-count ${items.length ? 'pending' : ''}`}>{items.length} PENDING</span></div>
      {items.length === 0 ? <EmptyState icon={<Inbox size={27} />}>当前没有待处理的审批或输入</EmptyState> : (
        <div className="interaction-grid">
          {items.map((item) => {
            const type = item.interactionType.toUpperCase()
            const needsResponse = ['INPUT', 'CHOICE', 'EDIT'].some((value) => type.includes(value))
            const busy = busyID.startsWith(`${item.id}:`)
            return (
              <article className="interaction-card" key={item.id}>
                <div className={`interaction-symbol ${needsResponse ? 'input' : 'approval'}`}>{needsResponse ? <MessageSquareText size={19} /> : <ShieldAlert size={19} />}</div>
                <div className="interaction-content">
                  <div className="interaction-title"><div><span>{interactionLabel(item.interactionType)}</span><h3>{item.title}</h3></div><StatusBadge status={item.status} /></div>
                  <p>{item.prompt || '该交互没有附加说明，请根据运行上下文处理。'}</p>
                  <small>RUN {shortID(item.runId)} · TASK {shortID(item.taskId)} · {formatDate(item.createdAt)}</small>
                  {needsResponse && (
                    <label className="interaction-response"><span>回复内容</span><textarea rows={2} value={responses[item.id] ?? ''} onChange={(event) => setResponses((current) => ({ ...current, [item.id]: event.target.value }))} placeholder="输入补充信息后继续运行" /></label>
                  )}
                  <div className="interaction-actions">
                    {needsResponse ? (
                      <button type="button" className="primary" disabled={busy || !(responses[item.id] ?? '').trim()} onClick={() => onAction(item, 'resolve', responses[item.id]?.trim())}><Check size={14} />提交并继续</button>
                    ) : (
                      <button type="button" className="primary" disabled={busy} onClick={() => onAction(item, 'approve')}><Check size={14} />批准</button>
                    )}
                    <button type="button" className="ghost danger" disabled={busy} onClick={() => onAction(item, 'reject')}><X size={14} />拒绝</button>
                  </div>
                </div>
              </article>
            )
          })}
        </div>
      )}
    </section>
  )
}

function CreateRunDialog({ legacy, busy, onClose, onSubmit }: {
  legacy: boolean
  busy: boolean
  onClose: () => void
  onSubmit: (input: CreateRunInput) => Promise<void>
}) {
  const [form, setForm] = useState<CreateRunInput>({
    name: '', goal: '', tokenBudget: 300_000,
    costBudgetMicros: 5_000_000, maxAgents: 4, executionEngine: 'TEMPORAL',
  })
  const submit = (event: FormEvent) => {
    event.preventDefault()
    void onSubmit(form)
  }
  return (
    <div className="modal-backdrop" role="presentation" onMouseDown={(event) => event.target === event.currentTarget && onClose()}>
      <section className="modal" role="dialog" aria-modal="true" aria-labelledby="create-run-title">
        <div className="modal-heading"><div><span className="eyebrow">NEW OPERATION</span><h2 id="create-run-title">创建 {legacy ? 'Legacy Swarm' : 'Run'}</h2></div><button type="button" onClick={onClose} aria-label="关闭"><X size={18} /></button></div>
        <form onSubmit={submit}>
          <label><span>名称</span><input required maxLength={128} autoFocus value={form.name} onChange={(event) => setForm({ ...form, name: event.target.value })} placeholder="例如：发布 SwarmOS V1.5" /></label>
          <label><span>目标</span><textarea required rows={4} value={form.goal} onChange={(event) => setForm({ ...form, goal: event.target.value })} placeholder="描述要交付的结果与验收要求" /></label>
          <div className="form-grid">
            <label><span>最大 Agent</span><input type="number" min={1} max={1000} value={form.maxAgents} onChange={(event) => setForm({ ...form, maxAgents: Number(event.target.value) })} /></label>
            <label><span>Token 预算</span><input type="number" min={0} value={form.tokenBudget} onChange={(event) => setForm({ ...form, tokenBudget: Number(event.target.value) })} /></label>
            <label><span>成本预算（micros）</span><input type="number" min={0} value={form.costBudgetMicros} onChange={(event) => setForm({ ...form, costBudgetMicros: Number(event.target.value) })} /></label>
          </div>
          {!legacy && <label><span>执行引擎</span><select value={form.executionEngine} onChange={(event) => setForm({ ...form, executionEngine: event.target.value })}><option value="TEMPORAL">Durable / Temporal</option><option value="LEGACY">数据库兼容模式</option></select></label>}
          <div className="modal-actions"><button type="button" className="ghost" onClick={onClose}>取消</button><button type="submit" className="primary" disabled={busy}><Plus size={15} />{busy ? '创建中…' : '创建并进入'}</button></div>
        </form>
      </section>
    </div>
  )
}

function ReplanDialog({ currentGoal, busy, onClose, onSubmit }: {
  currentGoal: string
  busy: boolean
  onClose: () => void
  onSubmit: (reason: string, goal: string) => Promise<void>
}) {
  const [reason, setReason] = useState('')
  const [goal, setGoal] = useState(currentGoal)
  const submit = (event: FormEvent) => {
    event.preventDefault()
    void onSubmit(reason.trim(), goal.trim())
  }
  return (
    <div className="modal-backdrop" role="presentation" onMouseDown={(event) => event.target === event.currentTarget && onClose()}>
      <section className="modal" role="dialog" aria-modal="true" aria-labelledby="replan-title">
        <div className="modal-heading"><div><span className="eyebrow">PLAN VERSION</span><h2 id="replan-title">生成新计划</h2></div><button type="button" onClick={onClose} aria-label="关闭"><X size={18} /></button></div>
        <form onSubmit={submit}>
          <label><span>重新规划原因</span><textarea required autoFocus rows={3} value={reason} onChange={(event) => setReason(event.target.value)} placeholder="说明环境变化、失败原因或目标调整" /></label>
          <label><span>更新后的目标（可保持不变）</span><textarea required rows={4} value={goal} onChange={(event) => setGoal(event.target.value)} /></label>
          <div className="modal-actions"><button type="button" className="ghost" onClick={onClose}>取消</button><button type="submit" className="primary" disabled={busy || !reason.trim()}><RotateCcw size={15} />{busy ? '提交中…' : '确认 Replan'}</button></div>
        </form>
      </section>
    </div>
  )
}

export function App() {
  const [runs, setRuns] = useState<Run[]>([])
  const [legacyAPI, setLegacyAPI] = useState(false)
  const [selectedRunID, setSelectedRunID] = useState('')
  const [runDetail, setRunDetail] = useState<Run>()
  const [tasks, setTasks] = useState<Task[]>([])
  const [agents, setAgents] = useState<Agent[]>([])
  const [overview, setOverview] = useState<Overview>()
  const [timeline, setTimeline] = useState<TimelineItem[]>([])
  const [artifacts, setArtifacts] = useState<Artifact[]>([])
  const [decisions, setDecisions] = useState<SchedulerDecision[]>([])
  const [interactions, setInteractions] = useState<Interaction[]>([])
  const [selectedTask, setSelectedTask] = useState<Task>()
  const [attempts, setAttempts] = useState<Attempt[]>([])
  const [runQuery, setRunQuery] = useState('')
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [attemptLoading, setAttemptLoading] = useState(false)
  const [actionBusy, setActionBusy] = useState('')
  const [interactionBusy, setInteractionBusy] = useState('')
  const [showCreate, setShowCreate] = useState(false)
  const [showReplan, setShowReplan] = useState(false)
  const [error, setError] = useState('')
  const [notice, setNotice] = useState<{ tone: 'success' | 'danger'; text: string }>()
  const [lastUpdated, setLastUpdated] = useState<Date>()
  const [apiKeyConfigured, setAPIKeyConfigured] = useState(() => Boolean(getAPIKey()))

  const bootstrap = useCallback(async () => {
    setLoading(true)
    try {
      const result = await listRuns()
      setRuns(result.items)
      setLegacyAPI(result.legacy)
      setSelectedRunID((current) => result.items.some((run) => run.id === current) ? current : result.items[0]?.id ?? '')
      setError('')
    } catch (reason) {
      setError(`无法读取 Run 列表：${errorMessage(reason)}`)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { void bootstrap() }, [bootstrap])

  const loadRuntime = useCallback(async (baseRun: Run, quiet = false) => {
    if (!quiet) setRefreshing(true)
    try {
      const detail = await getRun(baseRun)
      const currentRun = { ...baseRun, ...detail, legacy: baseRun.legacy }
      setRunDetail(currentRun)
      const [taskResult, agentResult, overviewResult, timelineResult, artifactResult, decisionResult, interactionResult] = await Promise.allSettled([
        listRunTasks(currentRun, currentRun.tasks),
        listRunAgents(currentRun.id),
        getLegacyOverview(currentRun.id),
        listTimeline(currentRun),
        listArtifacts(currentRun),
        listSchedulerDecisions(currentRun),
        listInteractions(),
      ] as const)

      if (taskResult.status === 'fulfilled') {
        setTasks(taskResult.value)
        setSelectedTask((current) => taskResult.value.find((item) => item.id === current?.id) ?? taskResult.value[0])
      }
      if (agentResult.status === 'fulfilled') setAgents(agentResult.value)
      if (overviewResult.status === 'fulfilled') setOverview(overviewResult.value)
      if (timelineResult.status === 'fulfilled') setTimeline(timelineResult.value)
      if (artifactResult.status === 'fulfilled') setArtifacts(artifactResult.value)
      if (decisionResult.status === 'fulfilled') setDecisions(decisionResult.value)
      if (interactionResult.status === 'fulfilled') setInteractions(interactionResult.value)

      const rejected = [taskResult, agentResult, overviewResult, timelineResult, artifactResult, decisionResult, interactionResult]
        .find((result) => result.status === 'rejected')
      setError(rejected?.status === 'rejected' ? `部分运行数据读取失败：${errorMessage(rejected.reason)}` : '')
      setLastUpdated(new Date())
    } catch (reason) {
      setError(`Run 详情读取失败：${errorMessage(reason)}`)
    } finally {
      setRefreshing(false)
    }
  }, [])

  useEffect(() => {
    const baseRun = runs.find((run) => run.id === selectedRunID)
    if (!baseRun) {
      setRunDetail(undefined)
      setTasks([])
      setAgents([])
      setTimeline([])
      setArtifacts([])
      setDecisions([])
      setSelectedTask(undefined)
      return
    }
    setRunDetail(undefined)
    setOverview(undefined)
    setAttempts([])
    void loadRuntime(baseRun)
    const timer = window.setInterval(() => void loadRuntime(baseRun, true), 5_000)
    return () => window.clearInterval(timer)
  }, [loadRuntime, runs, selectedRunID])

  const loadTaskAttempts = useCallback(async (taskID: string, runID: string, quiet = false) => {
    if (!quiet) setAttemptLoading(true)
    try {
      setAttempts(await listAttempts(taskID, runID))
    } catch (reason) {
      setAttempts([])
      if (!quiet) setNotice({ tone: 'danger', text: `Attempt 读取失败：${errorMessage(reason)}` })
    } finally {
      setAttemptLoading(false)
    }
  }, [])

  useEffect(() => {
    if (!selectedTask?.id) {
      setAttempts([])
      return
    }
    void loadTaskAttempts(selectedTask.id, selectedRunID)
    const timer = window.setInterval(() => void loadTaskAttempts(selectedTask.id, selectedRunID, true), 5_000)
    return () => window.clearInterval(timer)
  }, [loadTaskAttempts, selectedRunID, selectedTask?.id])

  useEffect(() => {
    if (!notice) return
    const timer = window.setTimeout(() => setNotice(undefined), 5_000)
    return () => window.clearTimeout(timer)
  }, [notice])

  const selectedRun = runDetail?.id === selectedRunID
    ? runDetail
    : runs.find((run) => run.id === selectedRunID)

  const taskStatuses = useMemo(() => {
    if (overview?.task_statuses) return overview.task_statuses
    if (selectedRun?.taskStatuses && Object.keys(selectedRun.taskStatuses).length) return selectedRun.taskStatuses
    return tasks.reduce<Record<string, number>>((result, task) => {
      result[task.status] = (result[task.status] ?? 0) + 1
      return result
    }, {})
  }, [overview?.task_statuses, selectedRun?.taskStatuses, tasks])

  const totalTasks = Object.values(taskStatuses).reduce((total, value) => total + value, 0)
  const completedTasks = (taskStatuses.SUCCEEDED ?? 0) + (taskStatuses.COMPLETED ?? 0)
  const activeTasks = tasks.filter((task) => !terminalTaskStatuses.has(task.status)).length
  const onlineAgents = agents.filter((agent) => !['OFFLINE', 'STOPPED'].includes(agent.status)).length
  const progress = totalTasks > 0 ? Math.round((completedTasks / totalTasks) * 100) : 0
  const tokenBudget = overview?.budget_tokens ?? selectedRun?.tokenBudget ?? 0
  const tokenUsed = overview?.spent_tokens ?? selectedRun?.tokenUsed ?? 0
  const costBudget = overview?.budget_cost_micros ?? selectedRun?.costBudgetMicros ?? 0
  const costUsed = overview?.spent_cost_micros ?? selectedRun?.costUsedMicros ?? 0
  const tokenUsage = tokenBudget ? Math.min(100, Math.round((tokenUsed / tokenBudget) * 100)) : 0
  const selectedRunInteractions = interactions.filter((item) => !item.runId || item.runId === selectedRunID)
  const status = selectedRun?.status.toUpperCase() ?? 'CREATED'
  const supportsCommands = Boolean(selectedRun && !selectedRun.legacy)
  const canPause = supportsCommands && ['RUNNING', 'VERIFYING', 'READY'].includes(status)
  const canResume = supportsCommands && status === 'PAUSED'
  const canMutate = supportsCommands && !terminalRunStatuses.has(status)

  const selectTask = (task: Task) => {
    setSelectedTask(task)
    setAttempts([])
  }

  const handleRunAction = async (command: 'pause' | 'resume' | 'cancel') => {
    if (!selectedRun || selectedRun.legacy) return
    if (command === 'cancel' && !window.confirm('确认取消这个 Run？已完成的 Artifact 会保留。')) return
    setActionBusy(command)
    try {
      await runCommand(selectedRun.id, command)
      setNotice({ tone: 'success', text: `${command.toUpperCase()} 命令已提交` })
      await loadRuntime(selectedRun, true)
    } catch (reason) {
      setNotice({ tone: 'danger', text: `命令提交失败：${errorMessage(reason)}` })
    } finally {
      setActionBusy('')
    }
  }

  const handleReplan = async (reason: string, goal: string) => {
    if (!selectedRun || selectedRun.legacy) return
    setActionBusy('replan')
    try {
      await runCommand(selectedRun.id, 'replan', {
        reason,
        ...(goal !== selectedRun.goal ? { goal } : {}),
      })
      setShowReplan(false)
      setNotice({ tone: 'success', text: 'Replan 已提交，等待生成新的 PlanVersion' })
      await loadRuntime(selectedRun, true)
    } catch (actionError) {
      setNotice({ tone: 'danger', text: `Replan 失败：${errorMessage(actionError)}` })
    } finally {
      setActionBusy('')
    }
  }

  const handleCreate = async (input: CreateRunInput) => {
    setActionBusy('create')
    try {
      const created = await createRun(input, legacyAPI)
      setRuns((current) => [created, ...current.filter((run) => run.id !== created.id)])
      setSelectedRunID(created.id)
      setShowCreate(false)
      setNotice({ tone: 'success', text: `${legacyAPI ? 'Swarm' : 'Run'} 已创建` })
    } catch (createError) {
      setNotice({ tone: 'danger', text: `创建失败：${errorMessage(createError)}` })
    } finally {
      setActionBusy('')
    }
  }

  const handleInteraction = async (item: Interaction, action: 'approve' | 'reject' | 'resolve', resolution = '') => {
    setInteractionBusy(`${item.id}:${action}`)
    try {
      await interactionCommand(item, action, resolution)
      setInteractions(await listInteractions())
      setNotice({ tone: 'success', text: `${interactionLabel(item.interactionType)}已处理` })
    } catch (interactionError) {
      setNotice({ tone: 'danger', text: `交互处理失败：${errorMessage(interactionError)}` })
    } finally {
      setInteractionBusy('')
    }
  }

  const configureAPIKey = () => {
    const value = window.prompt(
      '请输入服务器 .env 中的 SWARMOS_API_KEY。密钥只保存在当前标签页，关闭标签页后自动清除。',
      '',
    )
    if (value === null) return
    setAPIKey(value)
    setAPIKeyConfigured(Boolean(value.trim()))
    setError('')
    void bootstrap()
  }

  return (
    <>
      <div className="app-shell">
        <aside className="sidebar">
          <div className="brand"><div className="brand-mark"><Sparkles size={18} /></div><div><strong>SWARM<span>/OS</span></strong><small>OPERATIONS CONSOLE</small></div></div>
          <nav aria-label="主导航">
            <a className="active" href="#overview"><CircleGauge size={18} /><span>Run 总览</span></a>
            <a href="#dag"><Workflow size={18} /><span>任务编排</span><em>{activeTasks}</em></a>
            <a href="#timeline"><Activity size={18} /><span>Timeline</span></a>
            <a href="#artifacts"><FileBox size={18} /><span>Artifacts</span></a>
            <a href="#inbox"><Inbox size={18} /><span>审批与输入</span>{interactions.length > 0 && <em className="warning">{interactions.length}</em>}</a>
          </nav>
          <RunRail runs={runs} selectedID={selectedRunID} query={runQuery} onQuery={setRunQuery} onSelect={setSelectedRunID} />
          <div className="sidebar-infra">
            <p>INFRASTRUCTURE</p>
            <div><Database size={14} /><span>PostgreSQL</span><i /></div>
            <div><Zap size={14} /><span>Durable Events</span><i /></div>
            <div><Cpu size={14} /><span>Worker Pool</span><i /></div>
          </div>
          <div className="sidebar-foot"><ServerCog size={17} /><div><strong>V1.5 · OPS</strong><small>{legacyAPI ? 'compatibility mode' : 'durable runtime'}</small></div></div>
        </aside>

        <main>
          <header className="topbar">
            <div className="breadcrumb"><span>OPERATIONS</span><ChevronRight size={14} /><strong>{selectedRun?.name ?? '等待 Run'}</strong>{selectedRun && <span className="plan-version">PLAN V{selectedRun.currentPlanVersion}</span>}</div>
            <div className="top-actions">
              <button className={`auth-key-button ${apiKeyConfigured ? 'configured' : ''}`} type="button" onClick={configureAPIKey} title="配置 API Key"><KeyRound size={14} /><span>{apiKeyConfigured ? 'KEY SET' : 'API KEY'}</span></button>
              <label className="run-select"><Boxes size={15} /><span className="sr-only">选择 Run</span><select value={selectedRunID} onChange={(event) => setSelectedRunID(event.target.value)}>{runs.map((run) => <option key={run.id} value={run.id}>{run.name}</option>)}</select></label>
              <button className="new-run-button" type="button" onClick={() => setShowCreate(true)}><Plus size={15} /><span>新建 Run</span></button>
              <button className="refresh-button" type="button" aria-label="刷新数据" disabled={!selectedRun} onClick={() => selectedRun && void loadRuntime(selectedRun)}><RefreshCw size={16} className={refreshing ? 'spin' : ''} /></button>
              <div className="live-pill"><i />LIVE</div>
            </div>
          </header>

          <div className="content" id="overview">
            <div className="aria-status" aria-live="polite">{notice?.text}</div>
            {error && <div className="connection-banner"><XCircle size={17} /><div><strong>部分控制面能力暂不可用</strong><span>{error} · 控制台会自动重试</span></div></div>}
            {notice && <div className={`notice-banner ${notice.tone}`}><div>{notice.tone === 'success' ? <CheckCircle2 size={17} /> : <AlertTriangle size={17} />}<span>{notice.text}</span></div><button type="button" aria-label="关闭提示" onClick={() => setNotice(undefined)}><X size={15} /></button></div>}
            {selectedRun?.legacy && <div className="compat-banner"><AlertTriangle size={16} /><div><strong>Legacy API 降级模式</strong><span>当前后端尚未提供 Run 动作、Artifact、Scheduler Explain 和 Interaction；现有 Swarm 数据仍可只读查看。</span></div></div>}

            <section className="hero-section">
              <div className="hero-copy"><span className="eyebrow">RUN OPERATION / {shortID(selectedRunID)}</span><h1>{selectedRun?.name ?? 'SwarmOS Operations Console'}</h1><p>{selectedRun?.goal ?? (loading ? '正在连接运行控制面…' : '创建第一个 Run，将长期任务纳入可恢复、可验证的执行链路。')}</p>{selectedRun?.deadlineAt && <small><Clock3 size={13} /> DEADLINE {formatDate(selectedRun.deadlineAt)}</small>}</div>
              <div className="hero-right">
                <div className="hero-progress"><div className="progress-ring" style={{ '--progress': `${progress * 3.6}deg` } as CSSProperties}><strong>{progress}%</strong><span>DONE</span></div><div><small>RUN STATUS</small><StatusBadge status={selectedRun?.status ?? 'CREATED'} /><p>{completedTasks}/{totalTasks} tasks completed</p></div></div>
                <div className="run-actions" aria-label="Run 操作">
                  <button type="button" disabled={!canPause || Boolean(actionBusy)} onClick={() => void handleRunAction('pause')}><Pause size={14} />Pause</button>
                  <button type="button" disabled={!canResume || Boolean(actionBusy)} onClick={() => void handleRunAction('resume')}><Play size={14} />Resume</button>
                  <button type="button" disabled={!canMutate || Boolean(actionBusy)} onClick={() => setShowReplan(true)}><RotateCcw size={14} />Replan</button>
                  <button type="button" className="danger" disabled={!canMutate || Boolean(actionBusy)} onClick={() => void handleRunAction('cancel')}><XCircle size={14} />Cancel</button>
                </div>
              </div>
            </section>

            <section className="metrics-grid">
              <MetricCard icon={<Workflow size={21} />} label="TASK GRAPH" value={`${completedTasks} / ${totalTasks}`} note={`${activeTasks} 个任务仍在流转`} />
              <MetricCard icon={<Bot size={21} />} label="AGENT CAPACITY" value={`${onlineAgents} / ${agents.length}`} note={`${overview?.agent_statuses.RUNNING ?? 0} 个实例执行中`} tone="lime" />
              <MetricCard icon={<Cpu size={21} />} label="TOKEN BUDGET" value={`${tokenUsage}%`} note={`${formatNumber(tokenUsed)} / ${formatNumber(tokenBudget)}`} tone="amber" />
              <MetricCard icon={<Coins size={21} />} label="RUN COST" value={formatCost(costUsed)} note={`${formatCost(costBudget)} budget · ${selectedRunInteractions.length} pending`} tone="violet" />
            </section>

            <div className="primary-grid">
              <section className="panel dag-panel" id="dag">
                <div className="panel-heading"><div><span className="eyebrow">DECLARATIVE ORCHESTRATION</span><h2>Task DAG</h2></div><div className="legend"><span className="success">成功</span><span className="active">运行</span><span className="warning">等待</span></div></div>
                {loading && tasks.length === 0 ? <div className="loading-lines"><i /><i /><i /></div> : <DAGBoard tasks={tasks} selectedID={selectedTask?.id} onSelect={selectTask} />}
              </section>
              <TaskInspector task={selectedTask} attempts={attempts} loading={attemptLoading} />
            </div>

            <div className="operations-grid">
              <TimelinePanel items={timeline} />
              <div className="operations-side">
                <ArtifactPanel items={artifacts} />
                <SchedulerPanel items={decisions} selectedTaskID={selectedTask?.id} />
              </div>
            </div>

            <InteractionInbox items={interactions} busyID={interactionBusy} onAction={handleInteraction} />

            <section className="panel agents-panel" id="agents">
              <div className="panel-heading"><div><span className="eyebrow">RUNTIME SLOTS</span><h2>Agent Fleet</h2></div><span className="panel-count">{agents.length} INSTANCES</span></div>
              <div className="agent-list">
                {agents.length === 0 && <EmptyState icon={<Bot size={26} />}>没有注册 Agent</EmptyState>}
                {agents.map((agent) => (
                  <div className="agent-row" key={agent.id}>
                    <div className={`agent-avatar ${statusTone(agent.status)}`}><Bot size={18} /></div>
                    <div className="agent-name"><strong>{agent.name}</strong><small>{shortID(agent.id)} · heartbeat {formatTime(agent.heartbeatAt)}</small></div>
                    <div className="agent-load"><span><i style={{ width: `${Math.max(4, agent.load * 100)}%` }} /></span><small>{Math.round(agent.load * 100)}% LOAD</small></div>
                    <StatusBadge status={agent.status} />
                  </div>
                ))}
              </div>
            </section>
            <footer><span>SWARMOS / V1.5 OPERATIONS CONSOLE</span><span>{lastUpdated ? `UPDATED ${formatTime(lastUpdated.toISOString())}` : 'CONNECTING'} · AUTO REFRESH 5S</span></footer>
          </div>
        </main>
      </div>

      {showCreate && <CreateRunDialog legacy={legacyAPI} busy={actionBusy === 'create'} onClose={() => setShowCreate(false)} onSubmit={handleCreate} />}
      {showReplan && selectedRun && <ReplanDialog currentGoal={selectedRun.goal} busy={actionBusy === 'replan'} onClose={() => setShowReplan(false)} onSubmit={handleReplan} />}
    </>
  )
}
