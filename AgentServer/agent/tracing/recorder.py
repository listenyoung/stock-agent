"""Mongo-backed trace recorder for agent runs."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from core.managers import mongo_manager

from agent.state import AgentRunEvent


class TraceRecorder:
    """Persists runs, events, tool calls, and final outputs for replay."""

    async def start_run(
        self,
        run_id: str,
        thread_id: str,
        user_id: str,
        request: dict[str, Any],
    ) -> None:
        await mongo_manager.insert_one(
            "agent_runs",
            {
                "run_id": run_id,
                "thread_id": thread_id,
                "user_id": user_id,
                "request": request,
                "status": "running",
                "started_at": datetime.utcnow(),
            },
        )

    async def record_event(self, event: AgentRunEvent) -> None:
        await mongo_manager.insert_one("agent_events", event.model_dump(mode="json"))

    async def complete_run(self, run_id: str, output: str) -> None:
        await mongo_manager.update_one(
            "agent_runs",
            {"run_id": run_id},
            {
                "$set": {
                    "status": "completed",
                    "output": output,
                    "completed_at": datetime.utcnow(),
                }
            },
        )

    async def fail_run(self, run_id: str, error: str) -> None:
        await mongo_manager.update_one(
            "agent_runs",
            {"run_id": run_id},
            {
                "$set": {
                    "status": "failed",
                    "error": error,
                    "completed_at": datetime.utcnow(),
                }
            },
        )
