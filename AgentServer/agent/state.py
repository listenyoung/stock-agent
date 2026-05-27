"""Shared schemas for agent runs, events, tools, and memory."""

from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


AgentEventType = Literal[
    "run_started",
    "run_resumed",
    "job_status_changed",
    "memory_loaded",
    "context_compressed",
    "plan_created",
    "agents_assigned",
    "sub_agents_started",
    "sub_agent_started",
    "sub_agent_completed",
    "sub_agent_failed",
    "sub_agents_completed",
    "model_capability_resolved",
    "tool_approval_required",
    "tool_call_started",
    "tool_call_completed",
    "reflection_completed",
    "critic_completed",
    "model_delta",
    "run_completed",
    "run_failed",
]


class AgentRunRequest(BaseModel):
    """Request to start an agent run."""

    message: str = Field(..., min_length=1)
    thread_id: Optional[str] = None
    model: Optional[str] = None
    tools_enabled: bool = True
    tool_choice: Literal["auto", "none"] = "auto"
    require_approval: bool = True
    memory_enabled: bool = True
    max_tool_rounds: int = Field(default=4, ge=0, le=8)
    resume_from_run_id: Optional[str] = None


class AgentRunEvent(BaseModel):
    """Streamable event emitted by the agent runtime."""

    event: AgentEventType
    run_id: str
    thread_id: str
    sequence: int
    data: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)


class ToolContext(BaseModel):
    """Context available to tools during a run."""

    run_id: str
    thread_id: str
    user_id: str
    tool_call_id: Optional[str] = None
    idempotency_key: Optional[str] = None


class ToolExecutionResult(BaseModel):
    """Normalized tool result captured for tracing and LLM observation."""

    name: str
    arguments: dict[str, Any]
    result: dict[str, Any]
    success: bool = True
    error_message: Optional[str] = None
    execution_time_ms: float = 0


class MemoryItem(BaseModel):
    """A memory record loaded into context."""

    memory_id: str
    user_id: str
    scope: str = "user"
    type: str = "fact"
    content: str
    confidence: float = 0.8
    importance: float = 0.5
    hit_count: int = 0
    pinned: bool = False
    status: str = "active"
    conflicts_with: list[str] = Field(default_factory=list)
    source_run_id: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    last_accessed_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
