export interface ChatMessage {
  id: string
  role: 'user' | 'assistant'
  content: string
  status?: 'streaming' | 'failed' | 'completed'
}

interface MessageListProps {
  messages: ChatMessage[]
  isRunning: boolean
}

export function MessageList({ messages, isRunning }: MessageListProps) {
  return (
    <main className="message-list">
      {messages.length === 0 ? (
        <div className="empty-state">
          <h1>StockAgent</h1>
          <p>像聊天一样提问。工具、记忆、轨迹和评估都收在右侧，需要时再展开查看。</p>
        </div>
      ) : null}

      {messages.map((message) => (
        <article
          className={`message ${message.role === 'user' ? 'user-message' : 'assistant-message'} ${
            message.status === 'failed' ? 'error-message' : ''
          }`}
          key={message.id}
        >
          <div className="message-role">{message.role === 'user' ? 'You' : 'StockAgent'}</div>
          <div className="assistant-text">
            {message.content}
            {message.status === 'streaming' || (isRunning && message === messages[messages.length - 1]) ? (
              <span className="cursor" />
            ) : null}
          </div>
        </article>
      ))}
    </main>
  )
}
