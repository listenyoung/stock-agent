"""Training data export helpers for SFT/DPO/RL pipelines."""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any, Optional

from core.managers import mongo_manager


class TrainingExporter:
    """Builds JSONL-ready records from runs, events, and feedback."""

    async def export_jsonl(
        self,
        user_id: str,
        limit: int = 100,
        include_failed: bool = False,
        format: str = "trace",
    ) -> str:
        statuses = ["completed"]
        if include_failed:
            statuses.append("failed")

        runs = await mongo_manager.find_many(
            "agent_runs",
            {"user_id": user_id, "status": {"$in": statuses}},
            projection={"_id": 0},
            sort=[("started_at", -1)],
            limit=limit,
        )

        lines = []
        for run in runs:
            run_id = run["run_id"]
            events = await mongo_manager.find_many(
                "agent_events",
                {"run_id": run_id},
                projection={"_id": 0},
                sort=[("sequence", 1)],
            )
            feedback = await mongo_manager.find_many(
                "agent_feedback",
                {"run_id": run_id, "user_id": user_id},
                projection={"_id": 0},
                sort=[("created_at", -1)],
                limit=10,
            )
            record = self._to_training_record(run, events, feedback)
            lines.append(json.dumps(self._format_record(record, format), ensure_ascii=False))

        export_id = uuid.uuid4().hex
        await mongo_manager.insert_one(
            "agent_training_exports",
            {
                "export_id": export_id,
                "user_id": user_id,
                "format": format,
                "run_count": len(runs),
                "include_failed": include_failed,
                "created_at": datetime.utcnow(),
            },
        )
        return "\n".join(lines)

    def _format_record(self, record: dict[str, Any], format: str) -> dict[str, Any]:
        if format == "sft":
            return {
                "messages": [
                    {"role": "user", "content": record["input"]},
                    {"role": "assistant", "content": record["output"]},
                ],
                "metadata": record["metadata"],
            }
        if format == "tool-call":
            return {
                "input": record["input"],
                "tool_calls": record["tool_calls"],
                "output": record["output"],
                "metadata": record["metadata"],
            }
        if format == "dpo":
            rejected = ""
            for item in record.get("feedback", []):
                if item.get("preferred_answer"):
                    return {
                        "prompt": record["input"],
                        "chosen": item["preferred_answer"],
                        "rejected": record["output"],
                        "metadata": record["metadata"],
                    }
                if item.get("rating", 0) < 0:
                    rejected = record["output"]
            return {
                "prompt": record["input"],
                "chosen": record["output"] if not rejected else "",
                "rejected": rejected,
                "metadata": record["metadata"],
            }
        if format == "rl-trajectory":
            return {
                "state": {"input": record["input"], "metadata": record["metadata"]},
                "actions": record["tool_calls"],
                "reward": self._reward(record),
                "final": record["output"],
            }
        return record

    @staticmethod
    def _reward(record: dict[str, Any]) -> float:
        feedback = record.get("feedback", [])
        if feedback:
            return max(float(item.get("rating", 0)) for item in feedback)
        return float(record.get("metadata", {}).get("eval", {}).get("overall", 0.0) or 0.0)

    def _to_training_record(
        self,
        run: dict[str, Any],
        events: list[dict[str, Any]],
        feedback: list[dict[str, Any]],
    ) -> dict[str, Any]:
        request = run.get("request", {})
        tool_calls = [
            event.get("data", {})
            for event in events
            if event.get("event") == "tool_call_completed"
        ]
        model_delta = "".join(
            str(event.get("data", {}).get("delta", ""))
            for event in events
            if event.get("event") == "model_delta"
        )
        return {
            "schema": "stock_agent_trace_v1",
            "run_id": run.get("run_id"),
            "thread_id": run.get("thread_id"),
            "status": run.get("status"),
            "input": request.get("message", ""),
            "output": run.get("output") or model_delta,
            "tool_calls": tool_calls,
            "feedback": feedback,
            "metadata": {
                "model": request.get("model"),
                "tools_enabled": request.get("tools_enabled"),
                "memory_enabled": request.get("memory_enabled"),
                "started_at": self._iso(run.get("started_at")),
                "completed_at": self._iso(run.get("completed_at")),
            },
        }

    @staticmethod
    def _iso(value: Optional[Any]) -> Optional[str]:
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return value
