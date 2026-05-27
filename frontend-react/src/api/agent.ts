import type {
  AgentCheckpoint,
  AgentRun,
  AgentRunEvent,
  EvalDashboardSummary,
  EvalRunSummary,
  MemoryItem,
  ToolDescriptor,
} from '../types'

const API_BASE = import.meta.env.VITE_API_BASE_URL || '/api/v1'

function authHeaders(): HeadersInit {
  const token = localStorage.getItem('access_token')
  return token ? { Authorization: `Bearer ${token}` } : {}
}

export async function listTools(): Promise<ToolDescriptor[]> {
  const response = await fetch(`${API_BASE}/agents/tools`, {
    headers: authHeaders(),
  })
  if (!response.ok) throw new Error('Failed to load tools')
  return response.json()
}

export async function listMemories(): Promise<MemoryItem[]> {
  const response = await fetch(`${API_BASE}/agents/memories`, {
    headers: authHeaders(),
  })
  if (!response.ok) throw new Error('Failed to load memories')
  return response.json()
}

export async function pinMemory(memoryId: string, pinned = true): Promise<void> {
  const response = await fetch(`${API_BASE}/agents/memories/${memoryId}/pin?pinned=${pinned}`, {
    method: 'POST',
    headers: authHeaders(),
  })
  if (!response.ok) throw new Error('Failed to pin memory')
}

export async function archiveMemory(memoryId: string): Promise<void> {
  const response = await fetch(`${API_BASE}/agents/memories/${memoryId}/archive`, {
    method: 'POST',
    headers: authHeaders(),
  })
  if (!response.ok) throw new Error('Failed to archive memory')
}

export async function runMemoryMaintenance(): Promise<void> {
  const response = await fetch(`${API_BASE}/agents/memories/maintenance`, {
    method: 'POST',
    headers: authHeaders(),
  })
  if (!response.ok) throw new Error('Failed to run memory maintenance')
}

export async function listRuns(): Promise<AgentRun[]> {
  const response = await fetch(`${API_BASE}/agents/runs`, {
    headers: authHeaders(),
  })
  if (!response.ok) throw new Error('Failed to load runs')
  return response.json()
}

export async function listRunEvents(runId: string): Promise<AgentRunEvent[]> {
  const response = await fetch(`${API_BASE}/agents/runs/${runId}/events`, {
    headers: authHeaders(),
  })
  if (!response.ok) throw new Error('Failed to load run events')
  return response.json()
}

export async function listRunCheckpoints(runId: string): Promise<AgentCheckpoint[]> {
  const response = await fetch(`${API_BASE}/agents/runs/${runId}/checkpoints`, {
    headers: authHeaders(),
  })
  if (!response.ok) throw new Error('Failed to load checkpoints')
  return response.json()
}

export async function submitFeedback(
  runId: string,
  rating: -1 | 0 | 1,
  comment = '',
): Promise<void> {
  const response = await fetch(`${API_BASE}/agents/runs/${runId}/feedback`, {
    method: 'POST',
    headers: {
      ...authHeaders(),
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ rating, comment }),
  })
  if (!response.ok) throw new Error('Failed to submit feedback')
}

export async function approveToolCall(approvalId: string): Promise<void> {
  const response = await fetch(`${API_BASE}/agents/approvals/${approvalId}/approve`, {
    method: 'POST',
    headers: authHeaders(),
  })
  if (!response.ok) throw new Error('Failed to approve tool call')
}

export async function rejectToolCall(approvalId: string): Promise<void> {
  const response = await fetch(`${API_BASE}/agents/approvals/${approvalId}/reject`, {
    method: 'POST',
    headers: authHeaders(),
  })
  if (!response.ok) throw new Error('Failed to reject tool call')
}

export async function exportTrainingJsonl(): Promise<string> {
  const response = await fetch(`${API_BASE}/agents/exports/training.jsonl`, {
    headers: authHeaders(),
  })
  if (!response.ok) throw new Error('Failed to export training data')
  return response.text()
}

export async function getEvalSummary(): Promise<EvalDashboardSummary> {
  const response = await fetch(`${API_BASE}/agents/evals/summary`, {
    headers: authHeaders(),
  })
  if (!response.ok) throw new Error('Failed to load eval summary')
  return response.json()
}

export async function runAgentEval(limit = 20): Promise<EvalRunSummary> {
  const response = await fetch(`${API_BASE}/agents/evals/run`, {
    method: 'POST',
    headers: {
      ...authHeaders(),
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ limit, use_llm_judge: true }),
  })
  if (!response.ok) throw new Error('Failed to run eval')
  return response.json()
}

async function readSse(response: Response, onEvent: (event: AgentRunEvent) => void): Promise<void> {
  if (!response.ok || !response.body) {
    throw new Error(`Agent stream failed: ${response.status}`)
  }

  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''

  while (true) {
    const { value, done } = await reader.read()
    if (done) break
    buffer += decoder.decode(value, { stream: true })

    const parts = buffer.split('\n\n')
    buffer = parts.pop() || ''

    for (const part of parts) {
      const dataLine = part
        .split('\n')
        .find((line) => line.startsWith('data: '))
      if (!dataLine) continue
      onEvent(JSON.parse(dataLine.slice(6)))
    }
  }
}

export async function resumeAgentRun(
  runId: string,
  onEvent: (event: AgentRunEvent) => void,
): Promise<void> {
  const response = await fetch(`${API_BASE}/agents/runs/${runId}/resume`, {
    method: 'POST',
    headers: {
      ...authHeaders(),
      Accept: 'text/event-stream',
    },
  })
  await readSse(response, onEvent)
}

export async function streamAgentRun(
  body: {
    message: string
    thread_id?: string
    tools_enabled: boolean
    memory_enabled: boolean
    tool_choice: 'auto' | 'none'
    require_approval: boolean
  },
  onEvent: (event: AgentRunEvent) => void,
): Promise<void> {
  const response = await fetch(`${API_BASE}/agents/runs`, {
    method: 'POST',
    headers: {
      ...authHeaders(),
      'Content-Type': 'application/json',
      Accept: 'text/event-stream',
    },
    body: JSON.stringify(body),
  })

  await readSse(response, onEvent)
}
