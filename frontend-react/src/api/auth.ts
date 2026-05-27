const API_BASE = import.meta.env.VITE_API_BASE_URL || '/api/v1'

export interface LoginResult {
  access_token: string
  refresh_token: string
  user_id: string
  username: string
}

export interface RegisterResult {
  user_id: string
  username: string
  message: string
}

async function readError(response: Response, fallback: string): Promise<string> {
  try {
    const payload = await response.json()
    if (typeof payload.detail === 'string') return payload.detail
    if (typeof payload.message === 'string') return payload.message
  } catch {
    // Ignore non-JSON error bodies.
  }
  return fallback
}

export async function login(username: string, password: string): Promise<LoginResult> {
  const form = new URLSearchParams()
  form.set('username', username)
  form.set('password', password)

  const response = await fetch(`${API_BASE}/auth/login`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    body: form,
  })
  if (!response.ok) throw new Error(await readError(response, '登录失败'))
  return response.json()
}

export async function register(
  username: string,
  email: string,
  password: string,
): Promise<RegisterResult> {
  const response = await fetch(`${API_BASE}/auth/register`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ username, email, password }),
  })
  if (!response.ok) throw new Error(await readError(response, '注册失败'))
  return response.json()
}
