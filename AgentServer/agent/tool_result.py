"""Standardized tool observation envelope."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field


class StandardToolObservation(BaseModel):
    tool: str
    status: str
    summary: str
    data: dict[str, Any] = Field(default_factory=dict)
    data_freshness: Optional[str] = None
    freshness_status: str = "unknown"
    cache_hit: bool = False
    retries: int = 0
    cost: float = 0.0
    confidence: float = 0.75
    citations: list[str] = Field(default_factory=list)
    arguments: dict[str, Any] = Field(default_factory=dict)
    execution_time_ms: float = 0
    created_at: datetime = Field(default_factory=datetime.utcnow)


def standardize_tool_result(raw: dict[str, Any]) -> StandardToolObservation:
    name = raw.get("name", "unknown_tool")
    success = bool(raw.get("success", True))
    result = raw.get("result", {}) or {}
    reliability = result.get("reliability", {}) if isinstance(result, dict) else {}
    data = result.get("data", result if isinstance(result, dict) else {"value": result})
    data_count = len(data) if isinstance(data, list) else len(data.keys()) if isinstance(data, dict) else 1
    status = "success" if success else "failed"
    error = raw.get("error_message") or result.get("error_message")
    summary = (
        f"{name} returned {data_count} item(s)"
        if success
        else f"{name} failed: {error or 'unknown error'}"
    )
    freshness = _find_freshness(data)
    confidence = reliability.get("confidence")
    return StandardToolObservation(
        tool=name,
        status=reliability.get("status", status),
        summary=summary,
        data={"raw": data},
        data_freshness=reliability.get("freshness") or freshness,
        freshness_status=reliability.get("freshness_status", "unknown"),
        cache_hit=bool(reliability.get("cache_hit", False)),
        retries=int(reliability.get("retries", 0) or 0),
        cost=float(reliability.get("cost", 0) or 0),
        confidence=float(confidence if confidence is not None else (0.82 if success else 0.2)),
        arguments=raw.get("arguments", {}),
        execution_time_ms=float(reliability.get("latency_ms", raw.get("execution_time_ms", 0)) or 0),
    )


def _find_freshness(data: Any) -> Optional[str]:
    if isinstance(data, list) and data:
        return _find_freshness(data[0])
    if isinstance(data, dict):
        for key in ("trade_date", "end_date", "datetime", "updated_at", "created_at"):
            if data.get(key):
                return str(data[key])
    return None
