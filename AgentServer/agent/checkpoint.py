"""Mongo-backed checkpoints for resumable/replayable agent runs."""

from __future__ import annotations

import os
import uuid
from typing import Any, Optional

from core.managers import mongo_manager


class CheckpointRecorder:
    """Stores compact state snapshots at important runtime stages."""

    KEY_STAGES = {
        "run_started",
        "memory_loaded",
        "plan_created",
        "context_compressed",
        "tool_approval_waiting",
        "tool_execution_started",
        "execute_tools_completed",
        "sub_agents_completed",
        "tools_completed",
        "final_prompt_ready",
        "final_answer_completed",
        "critic_completed",
        "memory_written",
        "run_completed",
        "run_failed",
    }

    def __init__(self) -> None:
        self.enabled = os.environ.get("AGENT_CHECKPOINT_ENABLED", "true").lower() != "false"

    async def save(
        self,
        run_id: str,
        thread_id: str,
        user_id: str,
        stage: str,
        state: dict[str, Any],
        sequence: Optional[int] = None,
    ) -> None:
        if not self.enabled or stage not in self.KEY_STAGES:
            return
        await mongo_manager.insert_one(
            "agent_checkpoints",
            {
                "checkpoint_id": uuid.uuid4().hex,
                "run_id": run_id,
                "thread_id": thread_id,
                "user_id": user_id,
                "stage": stage,
                "sequence": sequence,
                "state": self._compact_state(state),
            },
        )

    @staticmethod
    def _compact_state(state: dict[str, Any]) -> dict[str, Any]:
        compact = {}
        for key, value in state.items():
            if key == "event_queue":
                continue
            if key == "request" and hasattr(value, "model_dump"):
                compact[key] = value.model_dump(mode="json")
            elif key == "memories":
                compact[key] = [
                    item.model_dump(mode="json") if hasattr(item, "model_dump") else item
                    for item in value
                ]
            else:
                compact[key] = value
        return compact

    @staticmethod
    def _summarize_sequence(value: Any) -> Any:
        if not isinstance(value, list):
            return value
        tail = value[-3:]
        return {
            "count": len(value),
            "tail": tail,
        }
