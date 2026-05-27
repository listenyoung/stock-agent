"""Agent runtime API."""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query
from fastapi.responses import PlainTextResponse, StreamingResponse
from pydantic import BaseModel, Field

from agent import (
    AgentEvaluator,
    AgentRunRequest,
    AgentRuntime,
    EvalRunRequest,
    TrainingExporter,
    get_default_agent_graph,
    model_capability_registry,
    tool_registry,
)
from agent.tool_reliability import ToolReliabilityPolicy
from core.managers import mongo_manager

from .auth import get_current_user_id


router = APIRouter()
runtime = AgentRuntime()
training_exporter = TrainingExporter()
evaluator = AgentEvaluator()


class MemoryCreateRequest(BaseModel):
    content: str
    type: str = Field(default="fact", pattern="^(preference|fact|episode|procedure|interaction_summary|thread_summary)$")
    importance: float = 0.5
    confidence: float = Field(default=0.8, ge=0, le=1)
    expires_at: Optional[datetime] = None
    pinned: bool = False


class FeedbackRequest(BaseModel):
    rating: int = Field(..., ge=-1, le=1, description="-1 差，0 一般，1 好")
    comment: str = ""
    tags: list[str] = Field(default_factory=list)
    preferred_answer: Optional[str] = None


@router.post("/runs")
async def create_agent_run(
    body: AgentRunRequest,
    user_id: str = Depends(get_current_user_id),
):
    """Start an agent run and stream Server-Sent Events."""

    async def event_stream():
        async for event in runtime.stream_run(body, user_id=user_id):
            payload = event.model_dump(mode="json")
            yield f"event: {event.event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/runs")
async def list_agent_runs(
    limit: int = Query(default=30, le=100),
    user_id: str = Depends(get_current_user_id),
):
    """List recent runs for replay/debugging."""
    return await mongo_manager.find_many(
        "agent_runs",
        {"user_id": user_id},
        projection={"_id": 0},
        sort=[("started_at", -1)],
        limit=limit,
    )


@router.get("/runs/{run_id}")
async def get_agent_run(
    run_id: str,
    user_id: str = Depends(get_current_user_id),
):
    """Get a single run record."""
    return await mongo_manager.find_one(
        "agent_runs",
        {"run_id": run_id, "user_id": user_id},
        projection={"_id": 0},
    )


@router.get("/runs/{run_id}/events")
async def get_agent_run_events(
    run_id: str,
    user_id: str = Depends(get_current_user_id),
):
    """Get replayable events for a run."""
    run = await mongo_manager.find_one(
        "agent_runs",
        {"run_id": run_id, "user_id": user_id},
        projection={"_id": 0, "run_id": 1},
    )
    if not run:
        return []
    return await mongo_manager.find_many(
        "agent_events",
        {"run_id": run_id},
        projection={"_id": 0},
        sort=[("sequence", 1)],
    )


@router.get("/runs/{run_id}/checkpoints")
async def get_agent_run_checkpoints(
    run_id: str,
    user_id: str = Depends(get_current_user_id),
):
    """Get compact checkpoint snapshots for a run."""
    run = await mongo_manager.find_one(
        "agent_runs",
        {"run_id": run_id, "user_id": user_id},
        projection={"_id": 0, "run_id": 1},
    )
    if not run:
        return []
    return await mongo_manager.find_many(
        "agent_checkpoints",
        {"run_id": run_id},
        projection={"_id": 0},
        sort=[("created_at", 1)],
    )


@router.get("/runs/{run_id}/job")
async def get_agent_job(
    run_id: str,
    user_id: str = Depends(get_current_user_id),
):
    """Get durable job state for a run."""
    return await runtime.jobs.get(run_id, user_id) or {"error": "job not found"}


@router.get("/runs/{run_id}/tool-calls")
async def get_agent_tool_calls(
    run_id: str,
    user_id: str = Depends(get_current_user_id),
):
    """Get idempotent tool call records for a run."""
    return await mongo_manager.find_many(
        "agent_tool_calls",
        {"run_id": run_id, "user_id": user_id},
        projection={"_id": 0},
        sort=[("created_at", 1)],
        limit=200,
    )


@router.post("/runs/{run_id}/resume")
async def resume_agent_run(
    run_id: str,
    user_id: str = Depends(get_current_user_id),
):
    """Resume an interrupted agent run and stream Server-Sent Events."""

    async def event_stream():
        async for event in runtime.stream_resume_run(run_id, user_id=user_id):
            payload = event.model_dump(mode="json")
            yield f"event: {event.event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/runs/{run_id}/eval")
async def evaluate_agent_run(
    run_id: str,
    user_id: str = Depends(get_current_user_id),
):
    """Evaluate a run for quality tracking and training filters."""
    result = await evaluator.evaluate_run(run_id, user_id)
    return result.model_dump(mode="json")


@router.post("/evals/run")
async def run_agent_eval(
    body: EvalRunRequest,
    user_id: str = Depends(get_current_user_id),
):
    """Run an evaluation batch over selected or recent agent runs."""
    return (await evaluator.run_eval(user_id, body)).model_dump(mode="json")


@router.get("/evals/summary")
async def get_agent_eval_summary(
    limit: int = Query(default=5, ge=1, le=20),
    user_id: str = Depends(get_current_user_id),
):
    """Return latest evaluation dashboard summary."""
    return await evaluator.get_summary(user_id, limit=limit)


@router.get("/evals/{eval_id}")
async def get_agent_eval(
    eval_id: str,
    user_id: str = Depends(get_current_user_id),
):
    """Return one evaluation run and its per-sample results."""
    return await evaluator.get_eval(eval_id, user_id)


@router.get("/approvals")
async def list_tool_approvals(
    status: Optional[str] = None,
    limit: int = Query(default=50, le=200),
    user_id: str = Depends(get_current_user_id),
):
    """List tool approval requests."""
    filter_query = {"user_id": user_id}
    if status:
        filter_query["status"] = status
    return await mongo_manager.find_many(
        "agent_tool_approvals",
        filter_query,
        projection={"_id": 0},
        sort=[("created_at", -1)],
        limit=limit,
    )


@router.post("/approvals/{approval_id}/approve")
async def approve_tool_call(
    approval_id: str,
    user_id: str = Depends(get_current_user_id),
):
    """Approve a pending tool call."""
    ok = await runtime.approvals.resolve(approval_id, user_id, approved=True)
    return {"approved": ok}


@router.post("/approvals/{approval_id}/reject")
async def reject_tool_call(
    approval_id: str,
    user_id: str = Depends(get_current_user_id),
):
    """Reject a pending tool call."""
    ok = await runtime.approvals.resolve(approval_id, user_id, approved=False)
    return {"rejected": ok}


@router.post("/runs/{run_id}/feedback")
async def create_agent_feedback(
    run_id: str,
    body: FeedbackRequest,
    user_id: str = Depends(get_current_user_id),
):
    """Attach human feedback to a run for future SFT/DPO/RL data."""
    run = await mongo_manager.find_one(
        "agent_runs",
        {"run_id": run_id, "user_id": user_id},
        projection={"_id": 0, "run_id": 1},
    )
    if not run:
        return {"error": "run not found"}

    feedback_id = uuid.uuid4().hex
    await mongo_manager.insert_one(
        "agent_feedback",
        {
            "feedback_id": feedback_id,
            "run_id": run_id,
            "user_id": user_id,
            "rating": body.rating,
            "comment": body.comment,
            "tags": body.tags,
            "preferred_answer": body.preferred_answer,
        },
    )
    return {"feedback_id": feedback_id}


@router.get("/feedback")
async def list_agent_feedback(
    limit: int = Query(default=50, le=200),
    user_id: str = Depends(get_current_user_id),
):
    """List feedback records for the current user."""
    return await mongo_manager.find_many(
        "agent_feedback",
        {"user_id": user_id},
        projection={"_id": 0},
        sort=[("created_at", -1)],
        limit=limit,
    )


@router.get("/exports/training.jsonl")
async def export_training_jsonl(
    limit: int = Query(default=100, ge=1, le=1000),
    include_failed: bool = False,
    format: str = Query(default="trace", pattern="^(trace|sft|tool-call|dpo|rl-trajectory)$"),
    user_id: str = Depends(get_current_user_id),
):
    """Export replay traces as JSONL training records."""
    content = await training_exporter.export_jsonl(
        user_id=user_id,
        limit=limit,
        include_failed=include_failed,
        format=format,
    )
    return PlainTextResponse(content, media_type="application/x-ndjson")


@router.get("/tools")
async def list_agent_tools():
    """List tools available to the agent."""
    return [item.model_dump(mode="json") for item in tool_registry.list()]


@router.get("/tools/reliability")
async def list_tool_reliability():
    """List reliability policy for each registered tool."""
    return {
        item.name: ToolReliabilityPolicy.get(item.name).model_dump(mode="json")
        for item in tool_registry.list()
    }


@router.get("/model-capabilities")
async def get_model_capabilities(model: Optional[str] = None, provider: Optional[str] = None):
    """Return runtime capability detection for a model/provider."""
    return model_capability_registry.describe(model=model, provider=provider)


@router.get("/graph")
async def get_agent_graph():
    """Return the logical agent graph used by the runtime."""
    return get_default_agent_graph().model_dump(mode="json")


@router.get("/memories")
async def list_memories(
    memory_type: Optional[str] = None,
    include_archived: bool = False,
    limit: int = Query(default=50, le=200),
    user_id: str = Depends(get_current_user_id),
):
    """List long-term memories for the current user."""
    filter_query = {"user_id": user_id}
    if not include_archived:
        filter_query["status"] = {"$ne": "archived"}
    if memory_type:
        filter_query["type"] = memory_type
    return await mongo_manager.find_many(
        "agent_memories",
        filter_query,
        projection={"_id": 0},
        sort=[("pinned", -1), ("importance", -1), ("hit_count", -1), ("updated_at", -1)],
        limit=limit,
    )


@router.get("/profile")
async def get_user_profile(
    user_id: str = Depends(get_current_user_id),
):
    """Get merged long-term user profile."""
    return await mongo_manager.find_one(
        "agent_user_profiles",
        {"user_id": user_id},
        projection={"_id": 0},
    ) or {"user_id": user_id, "profile": ""}


@router.post("/memories")
async def create_memory(
    body: MemoryCreateRequest,
    user_id: str = Depends(get_current_user_id),
):
    """Manually pin a long-term memory."""
    memory_id = await runtime.memory.maybe_write_memory(
        user_id=user_id,
        content=body.content,
        source_run_id="manual",
        memory_type=body.type,
        importance=body.importance,
        confidence=body.confidence,
        expires_at=body.expires_at,
        pinned=body.pinned,
    )
    return {"memory_id": memory_id}


@router.post("/memories/{memory_id}/pin")
async def pin_memory(
    memory_id: str,
    pinned: bool = True,
    user_id: str = Depends(get_current_user_id),
):
    """Pin or unpin a memory so forgetting/decay will not remove it."""
    return {"updated": await runtime.memory.pin_memory(user_id, memory_id, pinned=pinned)}


@router.post("/memories/{memory_id}/archive")
async def archive_memory(
    memory_id: str,
    user_id: str = Depends(get_current_user_id),
):
    """Archive a non-pinned memory without deleting the record."""
    return {"archived": await runtime.memory.archive_memory(user_id, memory_id, reason="manual")}


@router.post("/memories/maintenance")
async def run_memory_maintenance(
    user_id: str = Depends(get_current_user_id),
):
    """Run forgetting, promotion and consolidation maintenance now."""
    archived = await runtime.memory.forget_expired_memories(user_id)
    await runtime.memory.decay_importance(user_id)
    await runtime.memory.promote_hot_episodes(user_id)
    consolidated = await runtime.memory.consolidate_memories(user_id)
    return {"archived": archived, "consolidated_memory_id": consolidated}


@router.delete("/memories/{memory_id}")
async def delete_memory(
    memory_id: str,
    user_id: str = Depends(get_current_user_id),
):
    """Delete a long-term memory."""
    deleted = await mongo_manager.delete_one(
        "agent_memories",
        {"memory_id": memory_id, "user_id": user_id},
    )
    return {"deleted": deleted}
