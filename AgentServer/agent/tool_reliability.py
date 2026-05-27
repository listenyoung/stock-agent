"""Reliability policy for agent tool execution."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta
from typing import Any, Optional

from pydantic import BaseModel, Field


class ToolReliabilityConfig(BaseModel):
    timeout_s: float = 8.0
    retries: int = 1
    cache_ttl_s: int = 300
    cacheable: bool = True
    fallback_tool: Optional[str] = None
    cost: float = 0.01
    stale_after_days: int = 2
    side_effect: bool = False


class ToolReliabilityMeta(BaseModel):
    tool: str
    status: str = "success"
    freshness: Optional[str] = None
    freshness_status: str = "unknown"
    latency_ms: float = 0
    cache_hit: bool = False
    retries: int = 0
    confidence: float = 0.75
    cost: float = 0.0
    timeout_s: float = 0
    fallback_used: Optional[str] = None
    validation_error: Optional[str] = None


class ToolReliabilityPolicy:
    DEFAULT = ToolReliabilityConfig()
    CONFIGS = {
        "get_stock_basic": ToolReliabilityConfig(cache_ttl_s=3600, cost=0.005, stale_after_days=30),
        "get_stock_daily": ToolReliabilityConfig(cache_ttl_s=600, cost=0.01, stale_after_days=3),
        "get_news_sentiment": ToolReliabilityConfig(cache_ttl_s=300, cost=0.015, stale_after_days=2),
        "get_market_overview": ToolReliabilityConfig(cache_ttl_s=300, cost=0.01, stale_after_days=2),
        "get_market_latest_analysis": ToolReliabilityConfig(cache_ttl_s=600, cost=0.01, stale_after_days=3),
        "get_financial_indicator": ToolReliabilityConfig(cache_ttl_s=3600, cost=0.02, stale_after_days=120),
        "list_backtest_factors": ToolReliabilityConfig(cache_ttl_s=3600, cost=0.005, stale_after_days=30),
        "get_recent_backtest_results": ToolReliabilityConfig(cache_ttl_s=180, cost=0.02, stale_after_days=30),
        "get_backtest_result": ToolReliabilityConfig(cache_ttl_s=60, cost=0.01, stale_after_days=30),
        "submit_backtest": ToolReliabilityConfig(timeout_s=20, retries=0, cacheable=False, cost=1.0, side_effect=True),
        "submit_factor_selection_backtest": ToolReliabilityConfig(timeout_s=20, retries=0, cacheable=False, cost=1.5, side_effect=True),
    }

    @classmethod
    def get(cls, tool_name: str) -> ToolReliabilityConfig:
        return cls.CONFIGS.get(tool_name, cls.DEFAULT)

    @staticmethod
    def cache_key(tool_name: str, arguments: dict[str, Any]) -> str:
        payload = json.dumps(arguments, ensure_ascii=False, sort_keys=True, default=str)
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]
        return f"agent_tool:{tool_name}:{digest}"

    @staticmethod
    def freshness_status(freshness: Optional[str], stale_after_days: int) -> str:
        if not freshness:
            return "unknown"
        parsed = ToolReliabilityPolicy._parse_date(freshness)
        if not parsed:
            return "unknown"
        return "fresh" if datetime.utcnow() - parsed <= timedelta(days=stale_after_days) else "stale"

    @staticmethod
    def confidence(success: bool, cache_hit: bool, freshness_status: str, retries: int) -> float:
        if not success:
            return 0.2
        value = 0.86
        if cache_hit:
            value -= 0.04
        if freshness_status == "stale":
            value -= 0.2
        elif freshness_status == "unknown":
            value -= 0.08
        value -= min(0.18, retries * 0.06)
        return round(max(0.2, min(0.98, value)), 3)

    @staticmethod
    def _parse_date(value: str) -> Optional[datetime]:
        text = str(value)[:19]
        for fmt in ("%Y%m%d", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(text, fmt)
            except Exception:
                continue
        return None
