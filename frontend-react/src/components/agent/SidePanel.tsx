import type {
  AgentRunEvent,
  ApprovalRequest,
  EvalDashboardSummary,
  MemoryItem,
  ToolDescriptor,
} from '../../types'
import { ApprovalCard } from './ApprovalCard'
import { EvalPanel } from './EvalPanel'
import { ToolCallCard } from '../chat/ToolCallCard'

interface SidePanelProps {
  tools: ToolDescriptor[]
  memories: MemoryItem[]
  events: AgentRunEvent[]
  checkpointCount: number
  approval?: ApprovalRequest
  evalSummary?: EvalDashboardSummary
  evalRunning: boolean
  onApprove: (approvalId: string) => void
  onReject: (approvalId: string) => void
  onRunEval: () => void
  onPinMemory: (memoryId: string, pinned: boolean) => void
  onArchiveMemory: (memoryId: string) => void
  onRunMemoryMaintenance: () => void
}

export function SidePanel({
  tools,
  memories,
  events,
  checkpointCount,
  approval,
  evalSummary,
  evalRunning,
  onApprove,
  onReject,
  onRunEval,
  onPinMemory,
  onArchiveMemory,
  onRunMemoryMaintenance,
}: SidePanelProps) {
  const toolEvents = events.filter(
    (event) => event.event === 'tool_call_started' || event.event === 'tool_call_completed',
  )

  return (
    <aside className="side-panel">
      <section className="side-focus">
        <ApprovalCard approval={approval} onApprove={onApprove} onReject={onReject} />
      </section>

      <details className="panel-disclosure" open>
        <summary>
          <span>评估</span>
          <small>{evalSummary?.latest?.summary ? '已生成' : '未生成'}</small>
        </summary>
        <EvalPanel summary={evalSummary} running={evalRunning} onRunEval={onRunEval} />
      </details>

      <details className="panel-disclosure">
        <summary>
          <span>可用工具</span>
          <small>{tools.length}</small>
        </summary>
        <div className="chip-list">
          {tools.map((tool) => (
            <span className="chip" key={tool.name}>
              {tool.name}
            </span>
          ))}
        </div>
      </details>

      <details className="panel-disclosure">
        <summary>
          <span>工具调用</span>
          <small>{toolEvents.length}</small>
        </summary>
        <div className="tool-list">
          {toolEvents.map((event) => (
            <ToolCallCard event={event} key={`${event.run_id}-${event.sequence}`} />
          ))}
        </div>
      </details>

      <details className="panel-disclosure">
        <summary>
          <span>检查点</span>
          <small>{checkpointCount}</small>
        </summary>
        <div className="metric-box">
          <strong>{checkpointCount}</strong>
          <p>compact state snapshots</p>
        </div>
      </details>

      <details className="panel-disclosure">
        <summary>
          <span>记忆</span>
          <small>{memories.length}</small>
        </summary>
        <div className="panel-inline-actions">
          <button onClick={onRunMemoryMaintenance}>Maintain</button>
        </div>
        <div className="memory-list">
          {memories.slice(0, 6).map((memory) => (
            <article className="memory-item" key={memory.memory_id}>
              <div className="memory-item-header">
                <strong>{memory.type}</strong>
                <span>{memory.status || 'active'}</span>
              </div>
              <p>{memory.content}</p>
              <div className="memory-meta">
                <span>imp {(memory.importance || 0).toFixed(2)}</span>
                <span>conf {(memory.confidence || 0).toFixed(2)}</span>
                <span>hits {memory.hit_count || 0}</span>
                {memory.pinned && <span>pinned</span>}
                {(memory.conflicts_with?.length || 0) > 0 && <span>conflict</span>}
              </div>
              <div className="memory-actions">
                <button onClick={() => onPinMemory(memory.memory_id, !memory.pinned)}>
                  {memory.pinned ? 'Unpin' : 'Pin'}
                </button>
                <button disabled={memory.pinned} onClick={() => onArchiveMemory(memory.memory_id)}>
                  Archive
                </button>
              </div>
            </article>
          ))}
        </div>
      </details>

      <details className="panel-disclosure">
        <summary>
          <span>运行轨迹</span>
          <small>{events.length}</small>
        </summary>
        <div className="trace-list">
          {events.map((event) => (
            <div className="trace-item" key={`${event.run_id}-${event.sequence}`}>
              <span>{event.sequence}</span>
              <strong>{event.event}</strong>
            </div>
          ))}
        </div>
      </details>
    </aside>
  )
}
