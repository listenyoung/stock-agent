export type AgentEventName =
  | 'run_started'
  | 'run_resumed'
  | 'job_status_changed'
  | 'memory_loaded'
  | 'context_compressed'
  | 'plan_created'
  | 'agents_assigned'
  | 'sub_agents_started'
  | 'sub_agent_started'
  | 'sub_agent_completed'
  | 'sub_agent_failed'
  | 'sub_agents_completed'
  | 'model_capability_resolved'
  | 'tool_approval_required'
  | 'tool_call_started'
  | 'tool_call_completed'
  | 'reflection_completed'
  | 'critic_completed'
  | 'model_delta'
  | 'run_completed'
  | 'run_failed'

export interface AgentRunEvent {
  event: AgentEventName
  run_id: string
  thread_id: string
  sequence: number
  data: Record<string, unknown>
  created_at: string
}

export interface AgentRun {
  run_id: string
  thread_id: string
  user_id: string
  status: string
  request?: {
    message?: string
  }
  output?: string
  error?: string
  started_at?: string
  completed_at?: string
}

export interface ToolDescriptor {
  name: string
  description: string
  input_schema: Record<string, unknown>
  permission: string
  tags: string[]
}

export interface MemoryItem {
  memory_id: string
  type: string
  content: string
  confidence?: number
  importance: number
  hit_count?: number
  last_accessed_at?: string
  expires_at?: string
  pinned?: boolean
  status?: string
  conflicts_with?: string[]
  updated_at?: string
}

export interface AgentCheckpoint {
  run_id: string
  thread_id: string
  stage: string
  sequence?: number
  state: Record<string, unknown>
  created_at?: string
}

export interface ApprovalRequest {
  approval_id: string
  name: string
  reason: string
  arguments: Record<string, unknown>
}

export interface EvalScores {
  task_completion: number
  tool_call_accuracy: number
  factual_accuracy: number
  context_utilization: number
  risk_disclosure: number
  hallucination_rate: number
  approval_hit_rate: number
  avg_tool_rounds: number
  avg_latency_ms: number
}

export interface EvalSample {
  run_id?: string
  overall: number
  sample_type: string
  question: string
  answer: string
  issues: string[]
  expected_tools: string[]
  called_tools: string[]
}

export interface EvalRunSummary {
  eval_id: string
  total: number
  passed: number
  pass_rate: number
  average_score: number
  metrics: EvalScores
  failed_samples: EvalSample[]
  tool_misuse_samples: EvalSample[]
  low_score_answers: EvalSample[]
}

export interface EvalDashboardSummary {
  latest?: {
    eval_id: string
    status: string
    summary?: EvalRunSummary
  }
  recent: Array<{
    eval_id: string
    status: string
    summary?: EvalRunSummary
  }>
  totals: {
    eval_runs: number
    eval_results: number
  }
}
