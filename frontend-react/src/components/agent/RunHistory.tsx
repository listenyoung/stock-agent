import type { AgentRun } from '../../types'

export interface ConversationItem {
  threadId: string
  title: string
  status: string
  updatedAt?: string
  runs: AgentRun[]
}

interface RunHistoryProps {
  conversations: ConversationItem[]
  activeThreadId?: string
  onNewChat: () => void
  onSelect: (threadId: string) => void
  onResume: (run: AgentRun) => void
}

export function RunHistory({
  conversations,
  activeThreadId,
  onNewChat,
  onSelect,
  onResume,
}: RunHistoryProps) {
  return (
    <section className="run-history">
      <div className="panel-heading">
        <h2>对话</h2>
        <span>{conversations.length}</span>
      </div>
      <button className="new-chat" type="button" onClick={onNewChat}>
        新对话
      </button>
      <div className="run-list">
        {conversations.map((conversation) => {
          const latest = conversation.runs[conversation.runs.length - 1]
          const resumable = latest?.status === 'failed' || latest?.status === 'running'
          return (
            <div
              className={conversation.threadId === activeThreadId ? 'run-item active' : 'run-item'}
              key={conversation.threadId}
            >
              <button type="button" onClick={() => onSelect(conversation.threadId)}>
                <strong>{conversation.status}</strong>
                <span>{conversation.title}</span>
                <small>{conversation.runs.length} 条消息</small>
              </button>
              {resumable && latest ? (
                <button className="resume-run" type="button" onClick={() => onResume(latest)}>
                  Resume
                </button>
              ) : null}
            </div>
          )
        })}
      </div>
    </section>
  )
}
