"""Approval workflow for tools that have side effects or high compute cost."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Optional

from core.managers import mongo_manager


class ApprovalManager:
    """Persists tool approval requests and waits for user decisions."""

    async def create_request(
        self,
        run_id: str,
        thread_id: str,
        user_id: str,
        tool_name: str,
        arguments: dict[str, Any],
        reason: str,
    ) -> str:
        approval_id = uuid.uuid4().hex
        await mongo_manager.insert_one(
            "agent_tool_approvals",
            {
                "approval_id": approval_id,
                "run_id": run_id,
                "thread_id": thread_id,
                "user_id": user_id,
                "tool_name": tool_name,
                "arguments": arguments,
                "reason": reason,
                "status": "pending",
            },
        )
        return approval_id

    async def resolve(
        self,
        approval_id: str,
        user_id: str,
        approved: bool,
    ) -> bool:
        status = "approved" if approved else "rejected"
        modified = await mongo_manager.update_one(
            "agent_tool_approvals",
            {"approval_id": approval_id, "user_id": user_id, "status": "pending"},
            {
                "$set": {
                    "status": status,
                    "resolved_at": datetime.utcnow(),
                }
            },
        )
        return modified > 0

    async def get_status(self, approval_id: str, user_id: str) -> Optional[str]:
        doc = await mongo_manager.find_one(
            "agent_tool_approvals",
            {"approval_id": approval_id, "user_id": user_id},
            projection={"_id": 0, "status": 1},
        )
        return doc.get("status") if doc else None

    async def wait_for_decision(
        self,
        approval_id: str,
        user_id: str,
        timeout_seconds: int = 300,
        poll_interval: float = 0.5,
    ) -> bool:
        import asyncio
        import time

        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            status = await self.get_status(approval_id, user_id)
            if status == "approved":
                return True
            if status == "rejected":
                return False
            await asyncio.sleep(poll_interval)
        return False
