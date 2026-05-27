"""Durable job state machine for long-running agent runs."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field

from core.managers import mongo_manager


class AgentJob(BaseModel):
    job_id: str
    run_id: str
    thread_id: str
    user_id: str
    status: str = "pending"
    current_node: Optional[str] = None
    last_completed_node: Optional[str] = None
    attempts: int = 0
    waiting_for: Optional[dict[str, Any]] = None
    error: Optional[str] = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class AgentJobManager:
    """Stores run progress in Mongo so jobs survive process restarts."""

    async def create(
        self,
        run_id: str,
        thread_id: str,
        user_id: str,
        request: dict[str, Any],
    ) -> AgentJob:
        doc = AgentJob(
            job_id=run_id,
            run_id=run_id,
            thread_id=thread_id,
            user_id=user_id,
            status="pending",
            metadata={"request": request},
        ).model_dump(mode="json")
        update_doc = dict(doc)
        update_doc.pop("attempts", None)
        await mongo_manager.update_one(
            "agent_jobs",
            {"job_id": run_id, "user_id": user_id},
            {"$set": update_doc, "$inc": {"attempts": 1}},
            upsert=True,
        )
        return AgentJob(**{**doc, "attempts": 1})

    async def mark_running(self, run_id: str, user_id: str, current_node: str) -> None:
        await self.update(
            run_id,
            user_id,
            status="running",
            current_node=current_node,
            waiting_for=None,
        )

    async def mark_completed_node(self, run_id: str, user_id: str, node: str) -> None:
        await mongo_manager.update_one(
            "agent_jobs",
            {"job_id": run_id, "user_id": user_id},
            {
                "$set": {
                    "last_completed_node": node,
                    "current_node": node,
                    "status": "running",
                    "waiting_for": None,
                    "heartbeat_at": datetime.utcnow(),
                }
            },
            upsert=True,
        )

    async def mark_waiting(
        self,
        run_id: str,
        user_id: str,
        waiting_for: dict[str, Any],
        current_node: str = "execute_tools",
    ) -> None:
        await self.update(
            run_id,
            user_id,
            status=waiting_for.get("status", "waiting"),
            current_node=current_node,
            waiting_for=waiting_for,
        )

    async def complete(self, run_id: str, user_id: str, output: str) -> None:
        await self.update(
            run_id,
            user_id,
            status="completed",
            current_node="complete_run",
            waiting_for=None,
            metadata={"output_preview": output[:500]},
        )

    async def fail(self, run_id: str, user_id: str, error: str) -> None:
        await self.update(
            run_id,
            user_id,
            status="failed",
            error=error,
            waiting_for=None,
        )

    async def resume(self, run_id: str, user_id: str) -> None:
        await mongo_manager.update_one(
            "agent_jobs",
            {"job_id": run_id, "user_id": user_id},
            {
                "$set": {
                    "status": "resuming",
                    "heartbeat_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                },
                "$inc": {"attempts": 1},
            },
            upsert=True,
        )

    async def get(self, run_id: str, user_id: str) -> Optional[dict[str, Any]]:
        return await mongo_manager.find_one(
            "agent_jobs",
            {"job_id": run_id, "user_id": user_id},
            projection={"_id": 0},
        )

    async def update(
        self,
        run_id: str,
        user_id: str,
        **fields: Any,
    ) -> None:
        fields["heartbeat_at"] = datetime.utcnow()
        await mongo_manager.update_one(
            "agent_jobs",
            {"job_id": run_id, "user_id": user_id},
            {"$set": fields},
            upsert=True,
        )
