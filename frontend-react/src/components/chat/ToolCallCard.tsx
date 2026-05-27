import type { AgentRunEvent } from '../../types'

interface ToolCallCardProps {
  event: AgentRunEvent
}

export function ToolCallCard({ event }: ToolCallCardProps) {
  const name = String(event.data.name || 'tool')
  const isCompleted = event.event === 'tool_call_completed'
  const success = event.data.success !== false

  return (
    <article className={`tool-card ${isCompleted ? 'completed' : 'started'}`}>
      <div className="tool-card-header">
        <span>{name}</span>
        <strong>{isCompleted ? (success ? '完成' : '失败') : '调用中'}</strong>
      </div>
      <pre>{JSON.stringify(event.data, null, 2)}</pre>
    </article>
  )
}
