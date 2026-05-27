"""Modern agent runtime with tools, memory, context compression, and tracing."""

from __future__ import annotations

import json
import uuid
import asyncio
from collections.abc import AsyncGenerator
from typing import Any, Optional, Union

from core.managers import llm_manager, milvus_manager
from core.managers import mongo_manager

from .approvals import ApprovalManager
from .checkpoint import CheckpointRecorder
from .critic import Critic
from .guardrails import ToolPermissionPolicy
from .graph_runtime import GraphAgentExecutor
from .jobs import AgentJobManager
from .memory import MemoryManager
from .multi_agent import MultiAgentCoordinator
from .planner import Planner
from .reflection import Reflector
from .state import AgentRunEvent, AgentRunRequest, ToolContext
from .tool_result import standardize_tool_result
from .tool_registry import tool_registry
from .tracing import TraceRecorder


class AgentRuntime:
    """Agent execution runtime."""

    def __init__(self) -> None:
        self.memory = MemoryManager()
        self.tracer = TraceRecorder()
        self.checkpoints = CheckpointRecorder()
        self.jobs = AgentJobManager()
        self.approvals = ApprovalManager()
        self.permissions = ToolPermissionPolicy()
        self.planner = Planner()
        self.reflector = Reflector()
        self.critic = Critic()
        self.coordinator = MultiAgentCoordinator()
        self.graph_executor = GraphAgentExecutor(
            memory=self.memory,
            tracer=self.tracer,
            checkpoints=self.checkpoints,
            approvals=self.approvals,
            permissions=self.permissions,
            planner=self.planner,
            reflector=self.reflector,
            critic=self.critic,
            coordinator=self.coordinator,
            jobs=self.jobs,
        )
        self.graph = self.graph_executor.graph

    async def stream_run(
        self,
        request: AgentRunRequest,
        user_id: str,
    ) -> AsyncGenerator[AgentRunEvent, None]:
        run_id = uuid.uuid4().hex               #本次运行id
        thread_id = request.thread_id or uuid.uuid4().hex             #对话线程id，多个run可以在同一线程中连续运行以共享上下文

        await self._ensure_runtime_managers()
        await self.jobs.create(run_id, thread_id, user_id, request.model_dump(mode="json"))
        event_queue: asyncio.Queue = asyncio.Queue()
        task = asyncio.create_task(
            self.graph_executor.run(
                run_id=run_id,
                thread_id=thread_id,
                user_id=user_id,
                request=request,
                event_queue=event_queue,
            )
        )

        while True:
            event = await event_queue.get()
            if event is None:
                break
            yield event

        await task

    async def stream_resume_run(
        self,
        run_id: str,
        user_id: str,
    ) -> AsyncGenerator[AgentRunEvent, None]:
        await self._ensure_runtime_managers()
        await self.jobs.resume(run_id, user_id)
        run = await mongo_manager.find_one(
            "agent_runs",
            {"run_id": run_id, "user_id": user_id},
            projection={"_id": 0},
        )
        checkpoints = await mongo_manager.find_many(
            "agent_checkpoints",
            {"run_id": run_id, "user_id": user_id},
            projection={"_id": 0},
            sort=[("created_at", -1)],
            limit=20,
        )
        checkpoint = next(
            (item for item in checkpoints if item.get("stage") not in {"run_failed", "run_completed"}),
            checkpoints[0] if checkpoints else None,
        )

        if not run or not checkpoint:
            yield AgentRunEvent(
                event="run_failed",
                run_id=run_id,
                thread_id=run.get("thread_id", "") if run else "",
                sequence=0,
                data={"error": "run or checkpoint not found"},
            )
            return
        if run.get("status") == "completed" or checkpoint.get("stage") == "run_completed":
            yield AgentRunEvent(
                event="run_completed",
                run_id=run_id,
                thread_id=run.get("thread_id", ""),
                sequence=int(checkpoint.get("sequence") or 0) + 1,
                data={"output": run.get("output", ""), "resume_skipped": True},
            )
            return

        checkpoint_state = dict(checkpoint.get("state") or {})
        checkpoint_state.setdefault("run_id", run_id)
        checkpoint_state.setdefault("thread_id", run.get("thread_id"))
        checkpoint_state.setdefault("user_id", user_id)
        checkpoint_state.setdefault("request", run.get("request") or {})
        checkpoint["state"] = checkpoint_state

        event_queue: asyncio.Queue = asyncio.Queue()
        task = asyncio.create_task(self.graph_executor.resume(checkpoint, event_queue))

        while True:
            event = await event_queue.get()
            if event is None:
                break
            yield event

        await task

    async def _run_tool_loop(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        request: AgentRunRequest,
        context: ToolContext,
        user_id: str,
        emit,
    ) -> AsyncGenerator[Union[AgentRunEvent, dict[str, Any]], None]:
        if not tools or request.tool_choice == "none" or request.max_tool_rounds == 0:
            return

        loop_messages = list(messages)

        for _ in range(request.max_tool_rounds):
            response = await llm_manager.create_chat_completion(
                loop_messages,
                model=request.model,
                temperature=0.1,
                max_tokens=1200,
                tools=tools,
                tool_choice="auto",
            )
            message = response.choices[0].message
            tool_calls = getattr(message, "tool_calls", None) or []
            if not tool_calls:
                content = getattr(message, "content", None)
                if content:
                    yield {"model_note": content}
                break

            loop_messages.append(message.model_dump(exclude_none=True))
            for call in tool_calls:
                name = call.function.name
                args = call.function.arguments
                yield await emit(
                    "tool_call_started",
                    {"name": name, "arguments": self._safe_json(args)},
                )

                if request.require_approval and self.permissions.requires_approval(name):
                    approval_id = await self.approvals.create_request(
                        run_id=context.run_id,
                        thread_id=context.thread_id,
                        user_id=user_id,
                        tool_name=name,
                        arguments=self._safe_json(args),
                        reason=self.permissions.approval_reason(name),
                    )
                    yield await emit(
                        "tool_approval_required",
                        {
                            "approval_id": approval_id,
                            "name": name,
                            "arguments": self._safe_json(args),
                            "reason": self.permissions.approval_reason(name),
                        },
                    )
                    approved = await self.approvals.wait_for_decision(approval_id, user_id)
                    if not approved:
                        result = {
                            "name": name,
                            "arguments": self._safe_json(args),
                            "result": {},
                            "success": False,
                            "error_message": "Tool approval rejected or timed out",
                        }
                        yield await emit("tool_call_completed", result)
                        yield result
                        loop_messages.append(
                            {
                                "role": "tool",
                                "tool_call_id": call.id,
                                "content": json.dumps(result, ensure_ascii=False),
                            }
                        )
                        continue

                if not self.permissions.can_auto_run(name) and request.require_approval:
                    # Approved high-cost tools are allowed to continue.
                    pass

                if not self.permissions.can_auto_run(name) and not request.require_approval:
                    result = {
                        "name": name,
                        "arguments": self._safe_json(args),
                        "result": {},
                        "success": False,
                        "error_message": "Tool requires approval",
                    }
                else:
                    tool_result = await tool_registry.execute(name, args, context)
                    result = tool_result.model_dump(mode="json")
                    result["standard_observation"] = standardize_tool_result(result).model_dump(mode="json")

                yield await emit("tool_call_completed", result)
                yield result
                loop_messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": call.id,
                        "content": json.dumps(result, ensure_ascii=False),
                    }
                )

    async def _ensure_runtime_managers(self) -> None:
        if not llm_manager.is_initialized:
            await llm_manager.initialize()
        if not milvus_manager.is_initialized:
            try:
                await milvus_manager.initialize()
            except Exception:
                # Memory still works through Mongo when Milvus is unavailable.
                pass

    async def resume_run(self, run_id: str, user_id: str) -> dict[str, Any]:
        """Return the latest checkpoint state for a run.

        Full continuation is intentionally conservative until the runtime is
        fully graph-driven; this gives the API a stable resume contract.
        """
        run = await mongo_manager.find_one(
            "agent_runs",
            {"run_id": run_id, "user_id": user_id},
            projection={"_id": 0},
        )
        checkpoint = await mongo_manager.find_one(
            "agent_checkpoints",
            {"run_id": run_id, "user_id": user_id},
            projection={"_id": 0},
            sort=[("created_at", -1)],
        )
        return {
            "run": run,
            "checkpoint": checkpoint,
            "resumable": bool(run and checkpoint and run.get("status") == "failed"),
        }

    @staticmethod
    def _safe_json(value: Optional[Union[str, dict[str, Any]]]) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        if not value:
            return {}
        try:
            return json.loads(value)
        except Exception:
            return {"raw": value}
