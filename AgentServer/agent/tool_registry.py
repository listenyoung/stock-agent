"""Unified tool registry for model-native tool calling."""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from typing import Any, Iterable, Optional, Union

from pydantic import BaseModel, Field, ValidationError

from core.base import BaseTool
from core.managers import mongo_manager, redis_manager
from pymongo.errors import DuplicateKeyError

from .tools import (
    BacktestRecentResultsTool,
    FinancialIndicatorTool,
    ListBacktestFactorsTool,
    MarketLatestAnalysisTool,
    MarketOverviewTool,
    QueryBacktestResultTool,
    NewsSentimentTool,
    StockBasicTool,
    StockDailyTool,
    SubmitBacktestTool,
    SubmitFactorSelectionBacktestTool,
)
from .state import ToolContext, ToolExecutionResult
from .tool_reliability import ToolReliabilityMeta, ToolReliabilityPolicy
from .tool_result import standardize_tool_result


class ToolDescriptor(BaseModel):
    """Frontend/API descriptor for a registered tool."""

    name: str
    description: str
    input_schema: dict[str, Any]
    permission: str = "auto"
    tags: list[str] = Field(default_factory=list)


class ToolRegistry:
    """Registry that adapts existing MCP tools to OpenAI-style tool calls."""

    def __init__(self) -> None:
        self._tools: dict[str, BaseTool] = {}
        self._permissions: dict[str, str] = {}
        self._tags: dict[str, list[str]] = {}
        self._register_defaults()

    def _register_defaults(self) -> None:
        self.register(StockBasicTool(), tags=["stock", "fundamental"])
        self.register(StockDailyTool(), tags=["stock", "quote"])
        self.register(NewsSentimentTool(), tags=["news", "sentiment"])
        self.register(MarketOverviewTool(), tags=["market", "overview"])
        self.register(MarketLatestAnalysisTool(), tags=["market", "cycle"])
        self.register(FinancialIndicatorTool(), tags=["stock", "financial"])
        self.register(ListBacktestFactorsTool(), tags=["backtest", "factors"])
        self.register(BacktestRecentResultsTool(), tags=["backtest", "replay"])
        self.register(SubmitBacktestTool(), permission="confirm", tags=["backtest", "action"])
        self.register(SubmitFactorSelectionBacktestTool(), permission="confirm", tags=["backtest", "factor-selection", "action"])
        self.register(QueryBacktestResultTool(), tags=["backtest", "result"])

    def register(
        self,
        tool: BaseTool,
        permission: str = "auto",
        tags: Optional[Iterable[str]] = None,
    ) -> None:
        self._tools[tool.name] = tool
        self._permissions[tool.name] = permission
        self._tags[tool.name] = list(tags or [])

    def list(self) -> list[ToolDescriptor]:
        return [
            ToolDescriptor(
                name=tool.name,
                description=tool.description,
                input_schema=tool.input_model.model_json_schema(),
                permission=self._permissions.get(tool.name, "auto"),
                tags=self._tags.get(tool.name, []),
            )
            for tool in self._tools.values()
        ]

    def openai_tools(self) -> list[dict[str, Any]]:
        """Return tool schema compatible with OpenAI function calling."""
        return [
            {
                "type": "function",
                "function": {
                    "name": descriptor.name,
                    "description": descriptor.description,
                    "parameters": descriptor.input_schema,
                },
            }
            for descriptor in self.list()
        ]

    async def execute(
        self,
        name: str,
        arguments: Optional[Union[dict[str, Any], str]],
        context: ToolContext,
    ) -> ToolExecutionResult:
        started = time.perf_counter()
        config = ToolReliabilityPolicy.get(name)
        if name not in self._tools:
            return ToolExecutionResult(
                name=name,
                arguments={},
                result={},
                success=False,
                error_message=f"Tool not found: {name}",
            )

        tool = self._tools[name]
        try:
            parsed_args = self._parse_arguments(arguments)
            if "user_id" in getattr(tool.input_model, "model_fields", {}):
                parsed_args = {**parsed_args, "user_id": context.user_id}
            idempotency_key = context.idempotency_key or self._idempotency_key(name, parsed_args, context)
            if config.side_effect:
                parsed_args = {**parsed_args, "idempotency_key": idempotency_key}
            input_data = tool.input_model(**parsed_args)
        except (ValidationError, ValueError, TypeError, json.JSONDecodeError) as exc:
            meta = ToolReliabilityMeta(
                tool=name,
                status="validation_error",
                latency_ms=(time.perf_counter() - started) * 1000,
                confidence=0.1,
                cost=0,
                timeout_s=config.timeout_s,
                validation_error=str(exc),
            )
            return ToolExecutionResult(
                name=name,
                arguments=self._safe_arguments(arguments),
                result={"reliability": meta.model_dump(mode="json")},
                success=False,
                error_message=f"Tool argument validation failed: {exc}",
                execution_time_ms=meta.latency_ms,
            )

        if config.side_effect:
            existing = await self._get_existing_tool_call(idempotency_key)
            if existing:
                return self._tool_result_from_idempotent_record(name, parsed_args, existing, started)
            reserved = await self._reserve_tool_call(idempotency_key, name, parsed_args, context)
            if not reserved:
                existing = await self._get_existing_tool_call(idempotency_key)
                if existing:
                    return self._tool_result_from_idempotent_record(name, parsed_args, existing, started)

        cached = await self._read_cache(name, parsed_args, config)
        if cached:
            result_payload = cached["payload"]
            meta = ToolReliabilityMeta(**cached["reliability"])
            meta.cache_hit = True
            meta.latency_ms = (time.perf_counter() - started) * 1000
            meta.confidence = ToolReliabilityPolicy.confidence(
                success=bool(result_payload.get("success", True)),
                cache_hit=True,
                freshness_status=meta.freshness_status,
                retries=meta.retries,
            )
            result_payload["reliability"] = meta.model_dump(mode="json")
            return ToolExecutionResult(
                name=name,
                arguments=parsed_args,
                result=result_payload,
                success=bool(result_payload.get("success", True)),
                error_message=result_payload.get("error_message"),
                execution_time_ms=meta.latency_ms,
            )

        retries_used = 0
        result = None
        last_error = None
        for attempt in range(config.retries + 1):
            retries_used = attempt
            try:
                result = await asyncio.wait_for(tool(input_data), timeout=config.timeout_s)
                if result.success:
                    break
                last_error = result.error_message
            except asyncio.TimeoutError:
                last_error = f"Tool timed out after {config.timeout_s}s"
            except Exception as exc:
                last_error = str(exc)

        fallback_used = None
        if (not result or not result.success) and config.fallback_tool:
            fallback_used = config.fallback_tool
            fallback_result = await self.execute(config.fallback_tool, parsed_args, context)
            payload = fallback_result.result
            payload["reliability"] = {
                **payload.get("reliability", {}),
                "fallback_used": fallback_used,
                "status": "fallback_success" if fallback_result.success else "fallback_failed",
            }
            return ToolExecutionResult(
                name=name,
                arguments=parsed_args,
                result=payload,
                success=fallback_result.success,
                error_message=fallback_result.error_message,
                execution_time_ms=(time.perf_counter() - started) * 1000,
            )

        if result is None:
            result = tool.output_model(
                success=False,
                error_message=last_error or "Tool execution failed",
                execution_time_ms=(time.perf_counter() - started) * 1000,
            )

        payload = result.model_dump(mode="json")
        freshness = standardize_tool_result(
            {
                "name": name,
                "arguments": parsed_args,
                "result": payload,
                "success": payload.get("success", True),
                "execution_time_ms": payload.get("execution_time_ms", 0),
            }
        ).data_freshness
        freshness_status = ToolReliabilityPolicy.freshness_status(freshness, config.stale_after_days)
        meta = ToolReliabilityMeta(
            tool=name,
            status="success" if payload.get("success", True) else "failed",
            freshness=freshness,
            freshness_status=freshness_status,
            latency_ms=(time.perf_counter() - started) * 1000,
            cache_hit=False,
            retries=retries_used,
            confidence=ToolReliabilityPolicy.confidence(
                success=bool(payload.get("success", True)),
                cache_hit=False,
                freshness_status=freshness_status,
                retries=retries_used,
            ),
            cost=config.cost,
            timeout_s=config.timeout_s,
            fallback_used=fallback_used,
        )
        payload["reliability"] = meta.model_dump(mode="json")
        if config.side_effect:
            await self._finish_tool_call(idempotency_key, payload, bool(payload.get("success", True)))
        await self._write_cache(name, parsed_args, payload, meta, config)
        return ToolExecutionResult(
            name=name,
            arguments=parsed_args,
            result=payload,
            success=payload.get("success", True),
            error_message=payload.get("error_message"),
            execution_time_ms=payload.get("execution_time_ms", 0),
        )

    @staticmethod
    def _parse_arguments(arguments: Optional[Union[dict[str, Any], str]]) -> dict[str, Any]:
        if arguments is None:
            return {}
        if isinstance(arguments, dict):
            return arguments
        if not arguments.strip():
            return {}
        return json.loads(arguments)

    @staticmethod
    def _safe_arguments(arguments: Optional[Union[dict[str, Any], str]]) -> dict[str, Any]:
        try:
            return ToolRegistry._parse_arguments(arguments)
        except Exception:
            return {"raw": arguments}

    async def _read_cache(
        self,
        name: str,
        arguments: dict[str, Any],
        config,
    ) -> Optional[dict[str, Any]]:
        if not config.cacheable or not redis_manager.is_initialized:
            return None
        try:
            cached = await redis_manager.cache_get(ToolReliabilityPolicy.cache_key(name, arguments))
            return json.loads(cached) if cached else None
        except Exception:
            return None

    async def _write_cache(
        self,
        name: str,
        arguments: dict[str, Any],
        payload: dict[str, Any],
        meta: ToolReliabilityMeta,
        config,
    ) -> None:
        if not config.cacheable or not redis_manager.is_initialized or not payload.get("success", True):
            return
        try:
            await redis_manager.cache_set(
                ToolReliabilityPolicy.cache_key(name, arguments),
                json.dumps(
                    {"payload": payload, "reliability": meta.model_dump(mode="json")},
                    ensure_ascii=False,
                    default=str,
                ),
                ttl=config.cache_ttl_s,
            )
        except Exception:
            return

    @staticmethod
    def _idempotency_key(name: str, arguments: dict[str, Any], context: ToolContext) -> str:
        payload = json.dumps(arguments, ensure_ascii=False, sort_keys=True, default=str)
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]
        return f"{context.run_id}:{name}:{digest}"

    async def _reserve_tool_call(
        self,
        idempotency_key: str,
        name: str,
        arguments: dict[str, Any],
        context: ToolContext,
    ) -> bool:
        try:
            await mongo_manager.insert_one(
                "agent_tool_calls",
                {
                    "idempotency_key": idempotency_key,
                    "run_id": context.run_id,
                    "thread_id": context.thread_id,
                    "user_id": context.user_id,
                    "tool_call_id": context.tool_call_id,
                    "tool_name": name,
                    "arguments": arguments,
                    "status": "running",
                },
            )
            return True
        except DuplicateKeyError:
            return False
        except Exception:
            return True

    async def _finish_tool_call(
        self,
        idempotency_key: str,
        payload: dict[str, Any],
        success: bool,
    ) -> None:
        status = "completed" if success else "failed"
        external_task_id = payload.get("task_id")
        await mongo_manager.update_one(
            "agent_tool_calls",
            {"idempotency_key": idempotency_key},
            {
                "$set": {
                    "status": status,
                    "result": payload,
                    "external_task_id": external_task_id,
                }
            },
            upsert=True,
        )

    async def _get_existing_tool_call(self, idempotency_key: str) -> Optional[dict[str, Any]]:
        return await mongo_manager.find_one(
            "agent_tool_calls",
            {"idempotency_key": idempotency_key},
            projection={"_id": 0},
        )

    def _tool_result_from_idempotent_record(
        self,
        name: str,
        arguments: dict[str, Any],
        record: dict[str, Any],
        started: float,
    ) -> ToolExecutionResult:
        payload = record.get("result") or {
            "success": False,
            "status": record.get("status", "running"),
            "message": "Tool call is already reserved/running; resume will reuse the existing record.",
        }
        reliability = payload.get("reliability", {}) if isinstance(payload, dict) else {}
        reliability.update(
            {
                "idempotency_key": record.get("idempotency_key"),
                "idempotent_replay": True,
                "latency_ms": (time.perf_counter() - started) * 1000,
            }
        )
        payload["reliability"] = reliability
        return ToolExecutionResult(
            name=name,
            arguments=arguments,
            result=payload,
            success=bool(payload.get("success", record.get("status") == "completed")),
            error_message=payload.get("error_message"),
            execution_time_ms=float(reliability.get("latency_ms", 0) or 0),
        )


tool_registry = ToolRegistry()
