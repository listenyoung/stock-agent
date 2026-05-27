import { useEffect, useMemo, useState } from 'react'

import {
  approveToolCall,
  archiveMemory,
  exportTrainingJsonl,
  getEvalSummary,
  listMemories,
  listRunCheckpoints,
  listRunEvents,
  listRuns,
  listTools,
  pinMemory,
  rejectToolCall,
  resumeAgentRun,
  runAgentEval,
  runMemoryMaintenance,
  streamAgentRun,
  submitFeedback,
} from './api/agent'
import { AuthBar } from './components/agent/AuthBar'
import { FeedbackBar } from './components/agent/FeedbackBar'
import { RunHistory, type ConversationItem } from './components/agent/RunHistory'
import { SidePanel } from './components/agent/SidePanel'
import { Composer } from './components/chat/Composer'
import { MessageList, type ChatMessage } from './components/chat/MessageList'
import type {
  AgentCheckpoint,
  AgentRun,
  AgentRunEvent,
  ApprovalRequest,
  EvalDashboardSummary,
  MemoryItem,
  ToolDescriptor,
} from './types'

export default function App() {
  const [tools, setTools] = useState<ToolDescriptor[]>([])
  const [memories, setMemories] = useState<MemoryItem[]>([])
  const [events, setEvents] = useState<AgentRunEvent[]>([])
  const [runs, setRuns] = useState<AgentRun[]>([])
  const [activeRunId, setActiveRunId] = useState<string | undefined>()
  const [checkpoints, setCheckpoints] = useState<AgentCheckpoint[]>([])
  const [pendingApproval, setPendingApproval] = useState<ApprovalRequest | undefined>()
  const [threadId, setThreadId] = useState<string | undefined>()
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [isRunning, setIsRunning] = useState(false)
  const [toolsEnabled, setToolsEnabled] = useState(true)
  const [memoryEnabled, setMemoryEnabled] = useState(true)
  const [username, setUsername] = useState(localStorage.getItem('agent_username') || '')
  const [exportPreview, setExportPreview] = useState('')
  const [evalSummary, setEvalSummary] = useState<EvalDashboardSummary | undefined>()
  const [evalRunning, setEvalRunning] = useState(false)

  useEffect(() => {
    listTools().then(setTools).catch(() => setTools([]))
    listMemories().then(setMemories).catch(() => setMemories([]))
    listRuns().then(setRuns).catch(() => setRuns([]))
    getEvalSummary().then(setEvalSummary).catch(() => setEvalSummary(undefined))
  }, [])

  const status = useMemo(() => {
    const failed = events.find((event) => event.event === 'run_failed')
    if (failed) return 'failed'
    if (isRunning) return 'running'
    if (messages.length) return 'completed'
    return 'idle'
  }, [messages.length, events, isRunning])

  const conversations = useMemo<ConversationItem[]>(() => {
    const groups = new Map<string, AgentRun[]>()
    for (const run of runs) {
      const id = run.thread_id || run.run_id
      groups.set(id, [...(groups.get(id) || []), run])
    }
    return Array.from(groups.entries())
      .map(([id, groupedRuns]) => {
        const sorted = [...groupedRuns].sort(
          (a, b) => new Date(a.started_at || '').getTime() - new Date(b.started_at || '').getTime(),
        )
        const latest = sorted[sorted.length - 1]
        return {
          threadId: id,
          title: sorted[0]?.request?.message || id,
          status: latest?.status || 'unknown',
          updatedAt: latest?.completed_at || latest?.started_at,
          runs: sorted,
        }
      })
      .sort((a, b) => new Date(b.updatedAt || '').getTime() - new Date(a.updatedAt || '').getTime())
  }, [runs])

  function messagesFromRuns(threadRuns: AgentRun[]): ChatMessage[] {
    return threadRuns.flatMap((run) => {
      const items: ChatMessage[] = []
      if (run.request?.message) {
        items.push({
          id: `${run.run_id}-user`,
          role: 'user',
          content: run.request.message,
          status: 'completed',
        })
      }
      if (run.output || run.error) {
        items.push({
          id: `${run.run_id}-assistant`,
          role: 'assistant',
          content: run.output || run.error || '',
          status: run.status === 'failed' ? 'failed' : 'completed',
        })
      }
      return items
    })
  }

  async function runAgent(message: string) {
    setIsRunning(true)
    const userMessageId = `local-user-${Date.now()}`
    const assistantMessageId = `local-assistant-${Date.now()}`
    setMessages((current) => [
      ...current,
      { id: userMessageId, role: 'user', content: message, status: 'completed' },
      { id: assistantMessageId, role: 'assistant', content: '', status: 'streaming' },
    ])
    setEvents([])

    try {
      await streamAgentRun(
        {
          message,
          thread_id: threadId,
          tools_enabled: toolsEnabled,
          memory_enabled: memoryEnabled,
          tool_choice: toolsEnabled ? 'auto' : 'none',
          require_approval: false,
        },
        (event) => {
          setEvents((current) => [...current, event])
          setActiveRunId(event.run_id)
          if (event.event === 'run_completed') {
            listRunCheckpoints(event.run_id).then(setCheckpoints).catch(() => setCheckpoints([]))
          }
          if (event.thread_id) setThreadId(event.thread_id)
          if (event.event === 'model_delta') {
            setMessages((current) =>
              current.map((item) =>
                item.id === assistantMessageId
                  ? { ...item, content: item.content + String(event.data.delta || ''), status: 'streaming' }
                  : item,
              ),
            )
          }
          if (event.event === 'run_completed') {
            const output = typeof event.data.output === 'string' ? event.data.output : ''
            setMessages((current) =>
              current.map((item) =>
                item.id === assistantMessageId
                  ? { ...item, content: item.content || output, status: 'completed' }
                  : item,
              ),
            )
          }
          if (event.event === 'run_failed') {
            const error = typeof event.data.error === 'string' ? event.data.error : '运行失败'
            setMessages((current) =>
              current.map((item) =>
                item.id === assistantMessageId ? { ...item, content: error, status: 'failed' } : item,
              ),
            )
          }
          if (event.event === 'tool_approval_required') {
            setPendingApproval(event.data as unknown as ApprovalRequest)
          }
          if (event.event === 'tool_call_completed') {
            setPendingApproval(undefined)
          }
        },
      )
      listMemories().then(setMemories).catch(() => undefined)
      listRuns().then(setRuns).catch(() => undefined)
    } finally {
      setIsRunning(false)
      setMessages((current) =>
        current.map((item) => (item.id === assistantMessageId ? { ...item, status: item.status === 'streaming' ? 'completed' : item.status } : item)),
      )
    }
  }

  async function selectConversation(selectedThreadId: string) {
    const conversation = conversations.find((item) => item.threadId === selectedThreadId)
    if (!conversation) return
    const latest = conversation.runs[conversation.runs.length - 1]
    const replayEvents = latest ? await listRunEvents(latest.run_id) : []
    const replayCheckpoints = latest ? await listRunCheckpoints(latest.run_id) : []
    setActiveRunId(latest?.run_id)
    setEvents(replayEvents)
    setCheckpoints(replayCheckpoints)
    setThreadId(selectedThreadId)
    setMessages(messagesFromRuns(conversation.runs))
    setPendingApproval(undefined)
  }

  function startNewChat() {
    setThreadId(undefined)
    setActiveRunId(undefined)
    setEvents([])
    setCheckpoints([])
    setMessages([])
    setPendingApproval(undefined)
  }

  async function resumeRun(run: AgentRun) {
    setIsRunning(true)
    setActiveRunId(run.run_id)
    setEvents([])
    setMessages(messagesFromRuns([run]))
    setPendingApproval(undefined)
    try {
      await resumeAgentRun(run.run_id, (event) => {
        setEvents((current) => [...current, event])
        setActiveRunId(event.run_id)
        if (event.thread_id) setThreadId(event.thread_id)
        if (event.event === 'model_delta') {
          setMessages((current) => {
            const last = current[current.length - 1]
            if (last?.role === 'assistant') {
              return current.map((item, index) =>
                index === current.length - 1
                  ? { ...item, content: item.content + String(event.data.delta || ''), status: 'streaming' }
                  : item,
              )
            }
            return [
              ...current,
              {
                id: `${event.run_id}-assistant-resume`,
                role: 'assistant',
                content: String(event.data.delta || ''),
                status: 'streaming',
              },
            ]
          })
        }
        if (event.event === 'tool_approval_required') {
          setPendingApproval(event.data as unknown as ApprovalRequest)
        }
        if (event.event === 'tool_call_completed') {
          setPendingApproval(undefined)
        }
        if (event.event === 'run_completed') {
          listRunCheckpoints(event.run_id).then(setCheckpoints).catch(() => setCheckpoints([]))
        }
      })
      listRuns().then(setRuns).catch(() => undefined)
      listMemories().then(setMemories).catch(() => undefined)
    } finally {
      setIsRunning(false)
    }
  }

  async function rateRun(rating: -1 | 0 | 1) {
    if (!activeRunId) return
    await submitFeedback(activeRunId, rating)
    listRuns().then(setRuns).catch(() => undefined)
  }

  async function exportRuns() {
    const text = await exportTrainingJsonl()
    setExportPreview(text.split('\n').slice(0, 3).join('\n'))
  }

  async function runEvalDashboard() {
    setEvalRunning(true)
    try {
      await runAgentEval(20)
      const summary = await getEvalSummary()
      setEvalSummary(summary)
    } finally {
      setEvalRunning(false)
    }
  }

  async function approvePending(approvalId: string) {
    await approveToolCall(approvalId)
    setPendingApproval(undefined)
  }

  async function rejectPending(approvalId: string) {
    await rejectToolCall(approvalId)
    setPendingApproval(undefined)
  }

  async function togglePinMemory(memoryId: string, pinned: boolean) {
    await pinMemory(memoryId, pinned)
    listMemories().then(setMemories).catch(() => undefined)
  }

  async function archiveSelectedMemory(memoryId: string) {
    await archiveMemory(memoryId)
    listMemories().then(setMemories).catch(() => undefined)
  }

  async function maintainMemoryNow() {
    await runMemoryMaintenance()
    listMemories().then(setMemories).catch(() => undefined)
  }

  return (
    <div className="workspace">
      <header className="topbar">
        <div>
          <strong>StockAgent Workspace</strong>
          <span className={`status ${status}`}>{status}</span>
        </div>
        <div className="toggles">
          <label>
            <input
              type="checkbox"
              checked={toolsEnabled}
              onChange={(event) => setToolsEnabled(event.target.checked)}
            />
            Tools
          </label>
          <label>
            <input
              type="checkbox"
              checked={memoryEnabled}
              onChange={(event) => setMemoryEnabled(event.target.checked)}
            />
            Memory
          </label>
            <AuthBar
            username={username}
            onLogin={(nextUsername) => {
              localStorage.setItem('agent_username', nextUsername)
              setUsername(nextUsername)
              listTools().then(setTools).catch(() => setTools([]))
              listMemories().then(setMemories).catch(() => setMemories([]))
              listRuns().then(setRuns).catch(() => setRuns([]))
              getEvalSummary().then(setEvalSummary).catch(() => setEvalSummary(undefined))
            }}
            onLogout={() => {
              localStorage.removeItem('access_token')
              localStorage.removeItem('refresh_token')
              localStorage.removeItem('agent_username')
              setUsername('')
              setMemories([])
              setRuns([])
              setCheckpoints([])
              setMessages([])
              setEvents([])
              setThreadId(undefined)
              setEvalSummary(undefined)
            }}
          />
        </div>
      </header>

      <div className="layout">
        <aside className="left-panel">
          <RunHistory
            conversations={conversations}
            activeThreadId={threadId}
            onNewChat={startNewChat}
            onSelect={selectConversation}
            onResume={resumeRun}
          />
        </aside>
        <section className="center">
          <MessageList messages={messages} isRunning={isRunning} />
          <FeedbackBar
            runId={activeRunId}
            disabled={isRunning}
            onRate={rateRun}
            onExport={exportRuns}
            exportPreview={exportPreview}
          />
          <Composer disabled={isRunning} onSubmit={runAgent} />
        </section>
        <SidePanel
          tools={tools}
          memories={memories}
          events={events}
          checkpointCount={checkpoints.length}
          approval={pendingApproval}
          evalSummary={evalSummary}
          evalRunning={evalRunning}
          onApprove={approvePending}
          onReject={rejectPending}
          onRunEval={runEvalDashboard}
          onPinMemory={togglePinMemory}
          onArchiveMemory={archiveSelectedMemory}
          onRunMemoryMaintenance={maintainMemoryNow}
        />
      </div>
    </div>
  )
}
