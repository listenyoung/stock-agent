import { FormEvent, useState } from 'react'

interface ComposerProps {
  disabled: boolean
  onSubmit: (message: string) => void
}

export function Composer({ disabled, onSubmit }: ComposerProps) {
  const [message, setMessage] = useState('')

  function handleSubmit(event: FormEvent) {
    event.preventDefault()
    const text = message.trim()
    if (!text || disabled) return
    setMessage('')
    onSubmit(text)
  }

  return (
    <form className="composer" onSubmit={handleSubmit}>
      <textarea
        value={message}
        onChange={(event) => setMessage(event.target.value)}
        placeholder="例如：分析一下 000001.SZ 最近走势，结合新闻和风险给出结论"
        disabled={disabled}
      />
      <button type="submit" disabled={disabled || !message.trim()}>
        {disabled ? '运行中' : '发送'}
      </button>
    </form>
  )
}
