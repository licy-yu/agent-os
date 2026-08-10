import {
  useCallback,
  useEffect,
  useMemo,
  useState,
  type CSSProperties,
  type ReactNode,
} from 'react'
import {
  Activity,
  Bot,
  Boxes,
  Braces,
  CheckCircle2,
  ChevronRight,
  CircleGauge,
  Clock3,
  Cpu,
  Database,
  GitBranch,
  Layers3,
  RefreshCw,
  Search,
  ServerCog,
  ShieldCheck,
  Sparkles,
  TerminalSquare,
  Workflow,
  XCircle,
  Zap,
} from 'lucide-react'
import {
  type Agent,
  type Attempt,
  type EventItem,
  type Overview,
  type Swarm,
  type Task,
  request,
} from './api'

const terminalTaskStatuses = new Set(['SUCCEEDED', 'FAILED', 'CANCELED', 'REJECTED'])

function shortID(value?: string) {
  return value ? value.slice(0, 8) : '—'
}

function formatNumber(value: number) {
  return new Intl.NumberFormat('zh-CN', { notation: value > 999_999 ? 'compact' : 'standard' }).format(value)
}

function formatTime(value?: string) {
  if (!value) return '—'
  return new Intl.DateTimeFormat('zh-CN', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  }).format(new Date(value))
}

function statusTone(status: string) {
  if (['SUCCEEDED', 'IDLE', 'ACCEPT'].includes(status)) return 'success'
  if (['RUNNING', 'ASSIGNED', 'RESERVED', 'REVIEW'].includes(status)) return 'active'
  if (['FAILED', 'OFFLINE', 'REJECT'].includes(status)) return 'danger'
  if (['BLOCKED', 'RETRY_WAIT', 'WAITING_TOOL', 'WAITING_INPUT'].includes(status)) return 'warning'
  return 'neutral'
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
      <div>
        <p>{label}</p>
        <strong>{value}</strong>
        <small>{note}</small>
      </div>
    </article>
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

  if (tasks.length === 0) {
    return <div className="empty-state"><Workflow size={28} /><p>这个蜂群还没有 Task DAG</p></div>
  }

  return (
    <div className="dag-scroll" aria-label="任务 DAG">
      <div className="dag-board">
        {layers.map(([layer, items], index) => (
          <div className="dag-stage" key={layer}>
            <div className="stage-label"><span>0{layer + 1}</span> STAGE</div>
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

function AttemptInspector({ task, attempts, loading }: { task?: Task; attempts: Attempt[]; loading: boolean }) {
  const attempt = attempts[0]
  return (
    <section className="panel inspector-panel" id="attempts">
      <div className="panel-heading">
        <div><span className="eyebrow">EXECUTION EVIDENCE</span><h2>执行证据</h2></div>
        {task && <StatusBadge status={task.status} />}
      </div>
      {!task ? (
        <div className="empty-state"><Search size={26} /><p>选择 DAG 节点查看 Attempt</p></div>
      ) : loading ? (
        <div className="loading-lines"><i /><i /><i /></div>
      ) : !attempt ? (
        <div className="empty-state"><Clock3 size={26} /><p>任务尚未产生 Attempt</p></div>
      ) : (
        <div className="inspector-body">
          <div className="task-title-row"><div><small>TASK / {shortID(task.id)}</small><h3>{task.name}</h3></div><StatusBadge status={attempt.status} /></div>
          <div className="evidence-grid">
            <div><span>MODEL</span><strong>{attempt.model}</strong></div>
            <div><span>TOKENS</span><strong>{formatNumber(attempt.tokens_in + attempt.tokens_out)}</strong></div>
            <div><span>STEPS</span><strong>{attempt.step_count}</strong></div>
            <div><span>TOOLS</span><strong>{attempt.tool_call_count}</strong></div>
          </div>
          {attempt.evaluation && (
            <div className="review-card">
              <div className="review-score"><ShieldCheck size={18} /><strong>{Math.round(attempt.evaluation.quality_score * 100)}</strong><span>/100</span></div>
              <div><small>REVIEW DECISION</small><h4>{attempt.evaluation.decision}</h4><p>{attempt.evaluation.machine_pass ? '机器检查通过' : '机器检查未通过'} · {attempt.evaluation.policy_pass ? '策略合规' : '策略拒绝'}</p></div>
            </div>
          )}
          <div className="evidence-section">
            <h4><Layers3 size={15} />CHECKPOINTS</h4>
            {attempt.checkpoints.map((checkpoint) => (
              <div className="checkpoint" key={checkpoint.sequence}><span>{String(checkpoint.sequence).padStart(2, '0')}</span><div><strong>{checkpoint.step_name}</strong><small>{formatTime(checkpoint.created_at)}</small></div><CheckCircle2 size={15} /></div>
            ))}
          </div>
          <div className="evidence-section">
            <h4><TerminalSquare size={15} />TOOL CALLS</h4>
            {attempt.tool_calls.length === 0 && <p className="muted">无工具调用</p>}
            {attempt.tool_calls.map((call) => (
              <div className="tool-call" key={call.id}><Braces size={15} /><div><strong>{call.tool_name}</strong><small>{call.risk_level}</small></div><StatusBadge status={call.status} /></div>
            ))}
          </div>
        </div>
      )}
    </section>
  )
}

export function App() {
  const [swarms, setSwarms] = useState<Swarm[]>([])
  const [selectedSwarmID, setSelectedSwarmID] = useState('')
  const [tasks, setTasks] = useState<Task[]>([])
  const [agents, setAgents] = useState<Agent[]>([])
  const [overview, setOverview] = useState<Overview>()
  const [events, setEvents] = useState<EventItem[]>([])
  const [selectedTask, setSelectedTask] = useState<Task>()
  const [attempts, setAttempts] = useState<Attempt[]>([])
  const [loading, setLoading] = useState(true)
  const [attemptLoading, setAttemptLoading] = useState(false)
  const [refreshing, setRefreshing] = useState(false)
  const [error, setError] = useState('')
  const [lastUpdated, setLastUpdated] = useState<Date>()

  const loadSwarms = useCallback(async () => {
    const response = await request<{ items: Swarm[] }>('/api/v1/swarms?page_size=50')
    setSwarms(response.items ?? [])
    setSelectedSwarmID((current) => current || response.items?.[0]?.id || '')
  }, [])

  const loadRuntime = useCallback(async (swarmID: string, quiet = false) => {
    if (!swarmID) return
    if (!quiet) setRefreshing(true)
    try {
      const [taskResponse, agentResponse, nextOverview, eventResponse] = await Promise.all([
        request<{ items: Task[] }>(`/api/v1/tasks?swarm_id=${swarmID}&page_size=100`),
        request<{ items: Agent[] }>(`/api/v1/agents?swarm_id=${swarmID}&page_size=100`),
        request<Overview>(`/api/v1/console/overview?swarm_id=${swarmID}`),
        request<{ items: EventItem[] }>(`/api/v1/console/events?swarm_id=${swarmID}`),
      ])
      setTasks(taskResponse.items ?? [])
      setAgents(agentResponse.items ?? [])
      setOverview(nextOverview)
      setEvents(eventResponse.items ?? [])
      setError('')
      setLastUpdated(new Date())
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : '控制面连接失败')
    } finally {
      setLoading(false)
      setRefreshing(false)
    }
  }, [])

  useEffect(() => {
    loadSwarms().catch((reason: unknown) => {
      setError(reason instanceof Error ? reason.message : '控制面连接失败')
      setLoading(false)
    })
  }, [loadSwarms])

  useEffect(() => {
    setSelectedTask(undefined)
    setAttempts([])
    loadRuntime(selectedSwarmID).catch(() => undefined)
    const timer = window.setInterval(() => loadRuntime(selectedSwarmID, true).catch(() => undefined), 5_000)
    return () => window.clearInterval(timer)
  }, [loadRuntime, selectedSwarmID])

  const selectTask = useCallback(async (task: Task) => {
    setSelectedTask(task)
    setAttemptLoading(true)
    try {
      const response = await request<{ items: Attempt[] }>(`/api/v1/console/attempts?task_id=${task.id}`)
      setAttempts(response.items ?? [])
    } catch {
      setAttempts([])
    } finally {
      setAttemptLoading(false)
    }
  }, [])

  const selectedSwarm = swarms.find((swarm) => swarm.id === selectedSwarmID)
  const totalTasks = Object.values(overview?.task_statuses ?? {}).reduce((total, value) => total + value, 0)
  const completedTasks = overview?.task_statuses.SUCCEEDED ?? 0
  const activeTasks = tasks.filter((task) => !terminalTaskStatuses.has(task.status)).length
  const onlineAgents = agents.filter((agent) => !['OFFLINE', 'STOPPED'].includes(agent.status)).length
  const progress = totalTasks > 0 ? Math.round((completedTasks / totalTasks) * 100) : 0
  const tokenUsage = overview?.budget_tokens
    ? Math.min(100, Math.round((overview.spent_tokens / overview.budget_tokens) * 100))
    : 0

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand"><div className="brand-mark"><Sparkles size={18} /></div><div><strong>SWARM<span>/OS</span></strong><small>CONTROL ROOM</small></div></div>
        <nav aria-label="主导航">
          <a className="active" href="#overview"><CircleGauge size={18} /><span>运行总览</span></a>
          <a href="#dag"><Workflow size={18} /><span>任务编排</span><em>{activeTasks}</em></a>
          <a href="#agents"><Bot size={18} /><span>Agent 舰队</span></a>
          <a href="#attempts"><Layers3 size={18} /><span>执行证据</span></a>
          <a href="#events"><Activity size={18} /><span>事件流</span></a>
        </nav>
        <div className="sidebar-infra">
          <p>INFRASTRUCTURE</p>
          <div><Database size={14} /><span>PostgreSQL</span><i /></div>
          <div><Zap size={14} /><span>JetStream</span><i /></div>
          <div><Cpu size={14} /><span>Redis Lease</span><i /></div>
        </div>
        <div className="sidebar-foot"><ServerCog size={17} /><div><strong>V1 · KRATOS</strong><small>Go 1.24 runtime</small></div></div>
      </aside>

      <main>
        <header className="topbar">
          <div className="breadcrumb"><span>OPERATIONS</span><ChevronRight size={14} /><strong>{selectedSwarm?.name ?? '等待蜂群'}</strong></div>
          <div className="top-actions">
            <label className="swarm-select"><Boxes size={15} /><select aria-label="选择蜂群" value={selectedSwarmID} onChange={(event) => setSelectedSwarmID(event.target.value)}>{swarms.map((swarm) => <option key={swarm.id} value={swarm.id}>{swarm.name}</option>)}</select></label>
            <button className="refresh-button" type="button" aria-label="刷新数据" onClick={() => loadRuntime(selectedSwarmID)}><RefreshCw size={16} className={refreshing ? 'spin' : ''} /></button>
            <div className="live-pill"><i />LIVE</div>
          </div>
        </header>

        <div className="content" id="overview">
          {error && <div className="connection-banner"><XCircle size={17} /><div><strong>控制面暂不可用</strong><span>{error} · 正在自动重连</span></div></div>}
          <section className="hero-section">
            <div><span className="eyebrow">SWARM OPERATION / {shortID(selectedSwarmID)}</span><h1>{selectedSwarm?.name ?? 'SwarmOS Control Room'}</h1><p>{selectedSwarm?.goal ?? '正在连接声明式智能体控制面…'}</p></div>
            <div className="hero-progress"><div className="progress-ring" style={{ '--progress': `${progress * 3.6}deg` } as CSSProperties}><strong>{progress}%</strong><span>DONE</span></div><div><small>OPERATION STATUS</small><StatusBadge status={selectedSwarm?.status ?? 'PENDING'} /><p>{completedTasks}/{totalTasks} tasks completed</p></div></div>
          </section>

          <section className="metrics-grid">
            <MetricCard icon={<Workflow size={21} />} label="TASK GRAPH" value={`${completedTasks} / ${totalTasks}`} note={`${activeTasks} 个任务仍在流转`} />
            <MetricCard icon={<Bot size={21} />} label="AGENT CAPACITY" value={`${onlineAgents} / ${agents.length}`} note={`${overview?.agent_statuses.RUNNING ?? 0} 个实例执行中`} tone="lime" />
            <MetricCard icon={<Cpu size={21} />} label="TOKEN BUDGET" value={`${tokenUsage}%`} note={`${formatNumber(overview?.spent_tokens ?? 0)} / ${formatNumber(overview?.budget_tokens ?? 0)}`} tone="amber" />
            <MetricCard icon={<Layers3 size={21} />} label="ATTEMPTS" value={formatNumber(overview?.total_attempts ?? 0)} note={`${overview?.running_attempts ?? 0} active · ${overview?.pending_outbox ?? 0} outbox`} tone="violet" />
          </section>

          <div className="primary-grid">
            <section className="panel dag-panel" id="dag">
              <div className="panel-heading"><div><span className="eyebrow">DECLARATIVE ORCHESTRATION</span><h2>Task DAG</h2></div><div className="legend"><span className="success">成功</span><span className="active">运行</span><span className="warning">等待</span></div></div>
              {loading ? <div className="loading-lines"><i /><i /><i /></div> : <DAGBoard tasks={tasks} selectedID={selectedTask?.id} onSelect={selectTask} />}
            </section>
            <AttemptInspector task={selectedTask} attempts={attempts} loading={attemptLoading} />
          </div>

          <div className="secondary-grid">
            <section className="panel agents-panel" id="agents">
              <div className="panel-heading"><div><span className="eyebrow">RUNTIME SLOTS</span><h2>Agent Fleet</h2></div><span className="panel-count">{agents.length} INSTANCES</span></div>
              <div className="agent-list">
                {agents.length === 0 && <div className="empty-state"><Bot size={26} /><p>没有注册 Agent</p></div>}
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

            <section className="panel events-panel" id="events">
              <div className="panel-heading"><div><span className="eyebrow">TRANSACTIONAL OUTBOX</span><h2>Event Stream</h2></div><Activity size={18} /></div>
              <div className="event-list">
                {events.slice(0, 12).map((event) => (
                  <div className="event-row" key={event.id}>
                    <div className={`event-dot ${event.published_at ? 'published' : 'pending'}`} />
                    <div><strong>{event.event_type}</strong><small>{event.aggregate_type} / {shortID(event.aggregate_id)} · v{event.aggregate_version}</small></div>
                    <time>{formatTime(event.created_at)}</time>
                  </div>
                ))}
              </div>
            </section>
          </div>
          <footer><span>SWARMOS / CONTROL PLANE</span><span>{lastUpdated ? `UPDATED ${formatTime(lastUpdated.toISOString())}` : 'CONNECTING'} · AUTO REFRESH 5S</span></footer>
        </div>
      </main>
    </div>
  )
}
