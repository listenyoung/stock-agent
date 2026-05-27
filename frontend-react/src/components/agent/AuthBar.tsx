import { FormEvent, useState } from 'react'

import { login, register } from '../../api/auth'

interface AuthBarProps {
  username?: string
  onLogin: (username: string) => void
  onLogout: () => void
}

export function AuthBar({ username, onLogin, onLogout }: AuthBarProps) {
  const [name, setName] = useState('')
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [mode, setMode] = useState<'login' | 'register'>('login')
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)

  async function handleSubmit(event: FormEvent) {
    event.preventDefault()
    if (!name.trim() || !password) return
    if (mode === 'register' && !email.trim()) return
    setLoading(true)
    setError('')
    try {
      if (mode === 'register') {
        await register(name.trim(), email.trim(), password)
      }
      const result = await login(name.trim(), password)
      localStorage.setItem('access_token', result.access_token)
      localStorage.setItem('refresh_token', result.refresh_token)
      onLogin(result.username)
      setPassword('')
      setEmail('')
    } catch (err) {
      setError(err instanceof Error ? err.message : '登录失败')
    } finally {
      setLoading(false)
    }
  }

  if (username) {
    return (
      <div className="auth-bar signed-in">
        <span>{username}</span>
        <button type="button" onClick={onLogout}>
          退出
        </button>
      </div>
    )
  }

  return (
    <form className="auth-bar" onSubmit={handleSubmit}>
      <div className="auth-mode" aria-label="认证模式">
        <button
          type="button"
          className={mode === 'login' ? 'active' : ''}
          onClick={() => {
            setMode('login')
            setError('')
          }}
        >
          登录
        </button>
        <button
          type="button"
          className={mode === 'register' ? 'active' : ''}
          onClick={() => {
            setMode('register')
            setError('')
          }}
        >
          注册
        </button>
      </div>
      <input
        value={name}
        onChange={(event) => setName(event.target.value)}
        placeholder="用户名"
        autoComplete="username"
      />
      {mode === 'register' ? (
        <input
          value={email}
          onChange={(event) => setEmail(event.target.value)}
          placeholder="邮箱"
          type="email"
          autoComplete="email"
        />
      ) : null}
      <input
        value={password}
        onChange={(event) => setPassword(event.target.value)}
        placeholder="密码"
        type="password"
        autoComplete={mode === 'login' ? 'current-password' : 'new-password'}
      />
      <button type="submit" disabled={loading}>
        {loading ? '处理中' : mode === 'login' ? '登录' : '注册并登录'}
      </button>
      {error ? <span className="auth-error">{error}</span> : null}
    </form>
  )
}
