"""Graph-driven Agent runtime.

This module is the real execution path for Agent runs.  LangGraph owns the
stage orchestration; the outer FastAPI runtime only forwards events from the
graph to the SSE response.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Optional, TypedDict, Union

from langgraph.graph import END, StateGraph

from core.managers import llm_manager, mongo_manager

from .approvals import ApprovalManager
from .checkpoint import CheckpointRecorder
from .critic import Critic
from .guardrails import ToolPermissionPolicy
from .json_tool_parser import json_tool_parser
from .jobs import AgentJobManager
from .memory import MemoryManager
from .model_capabilities import model_capability_registry
from .multi_agent import MultiAgentCoordinator, SubAgentResult
from .planner import Planner
from .reflection import Reflector
from .state import AgentRunEvent, AgentRunRequest, ToolContext
from .tool_result import standardize_tool_result
from .tool_registry import tool_registry
from .tracing import TraceRecorder


class AgentGraphState(TypedDict, total=False):
    run_id: str
    thread_id: str
    user_id: str
    request: AgentRunRequest
    sequence: int
    event_queue: asyncio.Queue
    memories: list[Any]
    history: list[dict[str, Any]]
    tools: list[dict[str, Any]]
    plan: dict[str, Any]
    assignments: list[dict[str, Any]]
    system_prompt: str
    messages: list[dict[str, Any]]
    tool_observations: list[dict[str, Any]]
    sub_agent_results: list[dict[str, Any]]
    reflection: dict[str, Any]
    reflection_tool_attempts: int
    reflection_call_more_tools: bool
    final_messages: list[dict[str, Any]]
    output: str
    critique: dict[str, Any]
    memory_id: str
    error: str


class GraphAgentExecutor:
    """Executes a complete Agent run with LangGraph nodes."""

    NODE_ORDER = [
        "start_run",
        "load_memory",
        "plan",
        "compress_context",
        "execute_tools",
        "run_sub_agents",
        "reflect",
        "final_answer",
        "critic",
        "write_memory",
        "complete_run",
    ]

    STAGE_TO_NODE = {
        "run_started": "start_run",
        "memory_loaded": "load_memory",
        "plan_created": "plan",
        "context_compressed": "compress_context",
        "tool_approval_waiting": "compress_context",
        "tool_execution_started": "compress_context",
        "execute_tools_completed": "execute_tools",
        "sub_agents_completed": "run_sub_agents",
        "tools_completed": "reflect",
        "final_answer_completed": "final_answer",
        "critic_completed": "critic",
        "memory_written": "write_memory",
        "run_completed": "complete_run",
        "run_failed": "complete_run",
    }

    def __init__(
        self,
        memory: MemoryManager,
        tracer: TraceRecorder,
        checkpoints: CheckpointRecorder,
        approvals: ApprovalManager,
        permissions: ToolPermissionPolicy,
        planner: Planner,
        reflector: Reflector,
        critic: Critic,
        coordinator: MultiAgentCoordinator,
        jobs: Optional[AgentJobManager] = None,
    ) -> None:
        self.memory = memory
        self.tracer = tracer
        self.checkpoints = checkpoints
        self.approvals = approvals
        self.permissions = permissions
        self.planner = planner
        self.reflector = reflector
        self.critic = critic
        self.coordinator = coordinator
        self.jobs = jobs or AgentJobManager()
        self.graph = self._build_graph()

    async def run(
        self,
        run_id: str,
        thread_id: str,
        user_id: str,
        request: AgentRunRequest,
        event_queue: asyncio.Queue,
    ) -> AgentGraphState:
        initial_state: AgentGraphState = {
            "run_id": run_id,
            "thread_id": thread_id,
            "user_id": user_id,
            "request": request,
            "sequence": 0,
            "event_queue": event_queue,
        }
        try:
            result = await self.graph.ainvoke(initial_state)
            return result
        except Exception as exc:
            initial_state["error"] = str(exc)
            await self._fail_run(initial_state, exc)
            return initial_state
        finally:
            await event_queue.put(None)

    async def resume(
        self,
        checkpoint: dict[str, Any],
        event_queue: asyncio.Queue,
    ) -> AgentGraphState:
        state = self._restore_checkpoint_state(checkpoint, event_queue)
        try:
            await mongo_manager.update_one(
                "agent_runs",
                {"run_id": state["run_id"], "user_id": state["user_id"]},
                {"$set": {"status": "running", "resumed_from_checkpoint": checkpoint.get("checkpoint_id")}},
            )
            last_node = state.get("last_completed_node") or self.STAGE_TO_NODE.get(checkpoint.get("stage", ""))
            next_nodes = self._remaining_nodes(last_node)
            await self.emit(
                state,
                "run_resumed",
                {
                    "checkpoint_stage": checkpoint.get("stage"),
                    "last_completed_node": last_node,
                    "next_node": next_nodes[0] if next_nodes else None,
                },
            )
            for node_name in next_nodes:
                state = await self._node_callable(node_name)(state)
            return state
        except Exception as exc:
            state["error"] = str(exc)
            await self._fail_run(state, exc)
            return state
        finally:
            await event_queue.put(None)

    def _build_graph(self):
        graph = StateGraph(AgentGraphState)
        graph.add_node("start_run", self.start_run_node)
        graph.add_node("load_memory", self.load_memory_node)
        graph.add_node("plan", self.plan_node)
        graph.add_node("compress_context", self.compress_context_node)
        graph.add_node("execute_tools", self.execute_tools_node)
        graph.add_node("run_sub_agents", self.run_sub_agents_node)
        graph.add_node("reflect", self.reflect_node)
        graph.add_node("final_answer", self.final_answer_node)
        graph.add_node("critic", self.critic_node)
        graph.add_node("write_memory", self.write_memory_node)
        graph.add_node("complete_run", self.complete_run_node)

        graph.set_entry_point("start_run")
        graph.add_edge("start_run", "load_memory")
        graph.add_edge("load_memory", "plan")
        graph.add_edge("plan", "compress_context")
        graph.add_edge("compress_context", "execute_tools")
        graph.add_edge("execute_tools", "run_sub_agents")
        graph.add_edge("run_sub_agents", "reflect")
        graph.add_conditional_edges(
            "reflect",
            self._route_after_reflect,
            {
                "execute_tools": "execute_tools",
                "final_answer": "final_answer",
            },
        )
        graph.add_edge("final_answer", "critic")
        graph.add_edge("critic", "write_memory")
        graph.add_edge("write_memory", "complete_run")
        graph.add_edge("complete_run", END)
        return graph.compile()

    async def start_run_node(self, state: AgentGraphState) -> AgentGraphState:
        request = state["request"]
        await self._set_job_running(state, "start_run")
        await self.tracer.start_run(
            state["run_id"],
            state["thread_id"],
            state["user_id"],
            request.model_dump(mode="json"),
        )
        await self.memory.add_message(
            state["thread_id"],
            state["user_id"],
            "user",
            request.message,
            state["run_id"],
        )
        await self.checkpoints.save(
            state["run_id"],
            state["thread_id"],
            state["user_id"],
            "run_started",
            {"request": request.model_dump(mode="json")},
            sequence=state.get("sequence", 0),
        )
        await self.emit(state, "run_started", {"message": request.message})
        await self._save_resume_checkpoint(state, "run_started", "start_run")
        return state

    async def load_memory_node(self, state: AgentGraphState) -> AgentGraphState:
        await self._set_job_running(state, "load_memory")
        request = state["request"]
        memories = []
        if request.memory_enabled:
            memories = await self.memory.search_memories(state["user_id"], request.message)
        history = await self.memory.get_thread_history(state["thread_id"], state["user_id"])
        state["memories"] = memories
        state["history"] = history
        await self.emit(
            state,
            "memory_loaded",
            {"count": len(memories), "items": [item.model_dump(mode="json") for item in memories]},
        )
        await self.checkpoints.save(
            state["run_id"],
            state["thread_id"],
            state["user_id"],
            "memory_loaded",
            {"memory_count": len(memories)},
            sequence=state.get("sequence", 0),
        )
        await self._save_resume_checkpoint(state, "memory_loaded", "load_memory")
        return state

    async def plan_node(self, state: AgentGraphState) -> AgentGraphState:
        await self._set_job_running(state, "plan")
        request = state["request"]
        tools = tool_registry.openai_tools() if request.tools_enabled else []
        state["tools"] = tools
        plan = await self.planner.create_plan(
            request.message,
            [item.model_dump(mode="json") for item in state.get("memories", [])],
            tools,
            model=request.model,
        )
        assignments = self.coordinator.assign(plan.model_dump(mode="json"))
        state["plan"] = plan.model_dump(mode="json")
        state["assignments"] = [item.model_dump(mode="json") for item in assignments]
        await self.emit(state, "plan_created", state["plan"])
        await self.emit(state, "agents_assigned", {"assignments": state["assignments"]})
        await self.checkpoints.save(
            state["run_id"],
            state["thread_id"],
            state["user_id"],
            "plan_created",
            {"plan": state["plan"], "assignments": state["assignments"]},
            sequence=state.get("sequence", 0),
        )
        await self._save_resume_checkpoint(state, "plan_created", "plan")
        return state

    async def compress_context_node(self, state: AgentGraphState) -> AgentGraphState:
        await self._set_job_running(state, "compress_context")
        request = state["request"]
        tools = state.get("tools", [])
        system_prompt = self.memory.compressor.build_system_prompt(tools)
        messages = [{"role": "system", "content": system_prompt}]
        messages.extend(
            self.memory.compressor.compress(
                request.message,
                state.get("memories", []),
                state.get("history", []),
            )
        )
        state["system_prompt"] = system_prompt
        state["messages"] = messages
        await self.emit(
            state,
            "context_compressed",
            {"messages": len(messages), "tools": [t["function"]["name"] for t in tools]},
        )
        await self.checkpoints.save(
            state["run_id"],
            state["thread_id"],
            state["user_id"],
            "context_compressed",
            {
                "messages": messages,
                "tool_names": [t["function"]["name"] for t in tools],
                "history_count": len(state.get("history", [])),
            },
            sequence=state.get("sequence", 0),
        )
        await self._save_resume_checkpoint(state, "context_compressed", "compress_context")
        return state

    async def execute_tools_node(self, state: AgentGraphState) -> AgentGraphState:
        await self._set_job_running(state, "execute_tools")
        observations = list(state.get("tool_observations", []))
        async for item in self._run_tool_loop(state):
            if isinstance(item, dict):
                observations.append(item)
        state["tool_observations"] = observations
        await self._save_resume_checkpoint(state, "execute_tools_completed", "execute_tools")
        return state

    async def run_sub_agents_node(self, state: AgentGraphState) -> AgentGraphState:
        await self._set_job_running(state, "run_sub_agents")
        request = state["request"]
        if not request.tools_enabled or request.tool_choice == "none":
            state["sub_agent_results"] = []
            await self.emit(state, "sub_agents_completed", {"results": [], "skipped": True})
            await self._save_resume_checkpoint(state, "sub_agents_completed", "run_sub_agents")
            return state

        context = ToolContext(run_id=state["run_id"], thread_id=state["thread_id"], user_id=state["user_id"])

        async def execute_tool(name: str, arguments: dict[str, Any]) -> dict[str, Any]:
            return await self._execute_tool_call(state, context, name, arguments)

        async def emit_span(event: str, data: dict[str, Any]) -> AgentRunEvent:
            return await self.emit(state, event, data)

        results = await self.coordinator.run_parallel(state, execute_tool, emit_span)
        state["sub_agent_results"] = [item.model_dump(mode="json") for item in results]
        await self._save_resume_checkpoint(state, "sub_agents_completed", "run_sub_agents")
        return state

    async def reflect_node(self, state: AgentGraphState) -> AgentGraphState:
        await self._set_job_running(state, "reflect")
        request = state["request"]
        reflection = await self.reflector.reflect(
            request.message,
            state.get("tool_observations", []) + state.get("sub_agent_results", []),
            model=request.model,
        )
        state["reflection"] = reflection.model_dump(mode="json")
        call_more_tools = self._should_call_more_tools(state)
        state["reflection_call_more_tools"] = call_more_tools
        if call_more_tools:
            state["reflection_tool_attempts"] = state.get("reflection_tool_attempts", 0) + 1
            state.setdefault("messages", []).append(
                {
                    "role": "user",
                    "content": (
                        "Reflection 判断当前信息不足。请只围绕缺失信息补充调用必要工具；"
                        "避免重复已经完成且无新增价值的工具调用。"
                        f"\n缺失信息: {state['reflection'].get('missing', [])}"
                        f"\n风险提示: {state['reflection'].get('risk_flags', [])}"
                        f"\n用户原始问题: {request.message}"
                    ),
                }
            )
        await self.emit(state, "reflection_completed", state["reflection"])
        await self.checkpoints.save(
            state["run_id"],
            state["thread_id"],
            state["user_id"],
            "tools_completed",
            {
                "tool_observations": state.get("tool_observations", []),
                "sub_agent_results": state.get("sub_agent_results", []),
                "reflection": state["reflection"],
            },
            sequence=state.get("sequence", 0),
        )
        await self._save_resume_checkpoint(state, "tools_completed", "reflect")
        return state

    def _route_after_reflect(self, state: AgentGraphState) -> str:
        return "execute_tools" if state.get("reflection_call_more_tools", False) else "final_answer"

    @staticmethod
    def _should_call_more_tools(state: AgentGraphState) -> bool:
        request = state["request"]
        reflection = state.get("reflection", {})
        return (
            reflection.get("next_action") == "call_more_tools"
            and state.get("reflection_tool_attempts", 0) < 1
            and request.tools_enabled
            and request.tool_choice != "none"
            and request.max_tool_rounds > 0
        )

    async def final_answer_node(self, state: AgentGraphState) -> AgentGraphState:
        await self._set_job_running(state, "final_answer")
        request = state["request"]
        final_messages = [{"role": "system", "content": state["system_prompt"]}]
        final_messages.extend(
            self.memory.compressor.compress(
                request.message,
                state.get("memories", []),
                state.get("history", []),
                tool_observations=state.get("tool_observations", []),
            )
        )
        if state.get("tool_observations") or state.get("sub_agent_results"):
            final_messages.append(
                {
                    "role": "user",
                    "content": (
                        "请基于上面的工具观测、并行子 Agent 结果、计划和反思给出简洁、可核验、有风险提示的最终回答。"
                        f"\n计划: {state.get('plan', {})}"
                        f"\n子 Agent 结果: {state.get('sub_agent_results', [])}"
                        f"\n反思: {state.get('reflection', {})}"
                    ),
                }
            )
        state["final_messages"] = final_messages
        await self.checkpoints.save(
            state["run_id"],
            state["thread_id"],
            state["user_id"],
            "final_prompt_ready",
            {"final_messages": final_messages},
            sequence=state.get("sequence", 0),
        )

        final_text = ""
        async for chunk in llm_manager.chat_stream(
            final_messages,
            model=request.model,
            temperature=0.3,
        ):
            final_text += chunk
            await self.emit(state, "model_delta", {"delta": chunk})
        state["output"] = final_text
        await self._save_resume_checkpoint(state, "final_answer_completed", "final_answer")
        return state

    async def critic_node(self, state: AgentGraphState) -> AgentGraphState:
        await self._set_job_running(state, "critic")
        request = state["request"]
        critique = await self.critic.critique(
            request.message,
            state.get("output", ""),
            model=request.model,
        )
        state["critique"] = critique.model_dump(mode="json")
        if critique.revised_answer:
            state["output"] = critique.revised_answer
        await self.emit(state, "critic_completed", state["critique"])
        await self._save_resume_checkpoint(state, "critic_completed", "critic")
        return state

    async def write_memory_node(self, state: AgentGraphState) -> AgentGraphState:
        await self._set_job_running(state, "write_memory")
        request = state["request"]
        output = state.get("output", "")
        await self.memory.add_message(state["thread_id"], state["user_id"], "assistant", output, state["run_id"])
        memory_id = await self.memory.maybe_write_memory(
            user_id=state["user_id"],
            content=f"用户问: {request.message}\nAgent答: {output[:1200]}",
            source_run_id=state["run_id"],
        )
        await self.memory.maintain_after_run(
            user_id=state["user_id"],
            thread_id=state["thread_id"],
            run_id=state["run_id"],
            user_message=request.message,
            assistant_output=output,
        )
        state["memory_id"] = memory_id
        await self.checkpoints.save(
            state["run_id"],
            state["thread_id"],
            state["user_id"],
            "memory_written",
            {"memory_id": memory_id, "output_chars": len(output)},
            sequence=state.get("sequence", 0),
        )
        await self._save_resume_checkpoint(state, "memory_written", "write_memory")
        return state

    async def complete_run_node(self, state: AgentGraphState) -> AgentGraphState:
        await self._set_job_running(state, "complete_run")
        await self.tracer.complete_run(state["run_id"], state.get("output", ""))
        await self.jobs.complete(state["run_id"], state["user_id"], state.get("output", ""))
        await self.emit(state, "run_completed", {"output": state.get("output", "")})
        await self.checkpoints.save(
            state["run_id"],
            state["thread_id"],
            state["user_id"],
            "run_completed",
            {"output": state.get("output", "")},
            sequence=state.get("sequence", 0),
        )
        await self._save_resume_checkpoint(state, "run_completed", "complete_run")
        return state

    async def emit(
        self,
        state: AgentGraphState,
        event: str,
        data: Optional[dict[str, Any]] = None,
    ) -> AgentRunEvent:
        state["sequence"] = state.get("sequence", 0) + 1
        item = AgentRunEvent(
            event=event,
            run_id=state["run_id"],
            thread_id=state["thread_id"],
            sequence=state["sequence"],
            data=data or {},
        )
        await self.tracer.record_event(item)
        await state["event_queue"].put(item)
        return item

    async def _fail_run(self, state: AgentGraphState, exc: Exception) -> None:
        run_id = state["run_id"]
        await self.tracer.fail_run(run_id, str(exc))
        await self.jobs.fail(run_id, state["user_id"], str(exc))
        await self.emit(state, "run_failed", {"error": str(exc)})
        await self.checkpoints.save(
            run_id,
            state["thread_id"],
            state["user_id"],
            "run_failed",
            {"error": str(exc)},
            sequence=state.get("sequence", 0),
        )

    async def _save_resume_checkpoint(
        self,
        state: AgentGraphState,
        stage: str,
        node_name: str,
    ) -> None:
        snapshot = dict(state)
        snapshot["last_completed_node"] = node_name
        await self.jobs.mark_completed_node(state["run_id"], state["user_id"], node_name)
        await self.checkpoints.save(
            state["run_id"],
            state["thread_id"],
            state["user_id"],
            stage,
            snapshot,
            sequence=state.get("sequence", 0),
        )

    def _restore_checkpoint_state(
        self,
        checkpoint: dict[str, Any],
        event_queue: asyncio.Queue,
    ) -> AgentGraphState:
        raw_state = dict(checkpoint.get("state") or {})
        request = raw_state.get("request") or {}
        if not isinstance(request, AgentRunRequest):
            request = AgentRunRequest(**request)
        memories = []
        for item in raw_state.get("memories", []) or []:
            if hasattr(item, "model_dump"):
                memories.append(item)
            else:
                from .state import MemoryItem

                memories.append(MemoryItem(**item))
        raw_state["request"] = request
        raw_state["memories"] = memories
        raw_state["event_queue"] = event_queue
        raw_state["sequence"] = int(checkpoint.get("sequence") or raw_state.get("sequence") or 0)
        return raw_state  # type: ignore[return-value]

    def _remaining_nodes(self, last_completed_node: Optional[str]) -> list[str]:
        if not last_completed_node or last_completed_node not in self.NODE_ORDER:
            return list(self.NODE_ORDER)
        index = self.NODE_ORDER.index(last_completed_node)
        return self.NODE_ORDER[index + 1 :]

    def _node_callable(self, node_name: str):
        return {
            "start_run": self.start_run_node,
            "load_memory": self.load_memory_node,
            "plan": self.plan_node,
            "compress_context": self.compress_context_node,
            "execute_tools": self.execute_tools_node,
            "run_sub_agents": self.run_sub_agents_node,
            "reflect": self.reflect_node,
            "final_answer": self.final_answer_node,
            "critic": self.critic_node,
            "write_memory": self.write_memory_node,
            "complete_run": self.complete_run_node,
        }[node_name]

    async def _run_tool_loop(
        self,
        state: AgentGraphState,
    ):
        request = state["request"]
        tools = state.get("tools", [])
        if not tools or request.tool_choice == "none" or request.max_tool_rounds == 0:
            return

        capability = model_capability_registry.resolve(request.model)
        await self.emit(state, "model_capability_resolved", capability.model_dump(mode="json"))
        if capability.tool_calling == "none":
            yield {
                "model_note": "当前模型被识别为 text-only，已跳过工具调用。",
                "capability": capability.model_dump(mode="json"),
            }
            return

        loop_messages = list(state.get("messages", []))
        context = ToolContext(run_id=state["run_id"], thread_id=state["thread_id"], user_id=state["user_id"])
        if capability.tool_calling == "json_fallback":
            async for item in self._run_json_tool_loop(state, loop_messages, tools, context):
                yield item
            return

        try:
            async for item in self._run_native_tool_loop(state, loop_messages, tools, context):
                yield item
        except Exception as exc:
            await self.emit(
                state,
                "model_capability_resolved",
                {
                    **capability.model_dump(mode="json"),
                    "tool_calling": "json_fallback",
                    "source": "runtime_fallback",
                    "reason": f"native tool calling failed: {exc}",
                },
            )
            async for item in self._run_json_tool_loop(state, loop_messages, tools, context):
                yield item

    async def _run_native_tool_loop(
        self,
        state: AgentGraphState,
        loop_messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        context: ToolContext,
    ):
        request = state["request"]

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
                parsed_args = self._safe_json(args)
                result = await self._execute_tool_call(state, context, name, parsed_args, tool_call_id=call.id)
                yield result
                loop_messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": call.id,
                        "content": json.dumps(result, ensure_ascii=False),
                    }
                )

    async def _run_json_tool_loop(
        self,
        state: AgentGraphState,
        loop_messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        context: ToolContext,
    ):
        request = state["request"]
        json_messages = list(loop_messages)
        json_messages.append(
            {
                "role": "system",
                "content": self._json_tool_instruction(tools),
            }
        )

        for _ in range(request.max_tool_rounds):
            content = await llm_manager.chat(
                json_messages,
                model=request.model,
                temperature=0.1,
                max_tokens=1200,
            )
            actions = json_tool_parser.parse(content or "")
            tool_actions = [action for action in actions if action.action == "tool_call" and action.tool]
            if not tool_actions:
                answer = next((action.answer for action in actions if action.answer), content or "")
                if answer:
                    yield {"model_note": answer}
                break

            json_messages.append({"role": "assistant", "content": content})
            for index, action in enumerate(tool_actions):
                synthetic_call_id = f"json_{state['sequence']}_{index}"
                result = await self._execute_tool_call(
                    state,
                    context,
                    action.tool or "",
                    action.arguments,
                    tool_call_id=synthetic_call_id,
                )
                yield result
                json_messages.append(
                    {
                        "role": "user",
                        "content": (
                            "工具执行结果如下。请继续只输出 JSON action；如已足够回答，输出 final_answer。\n"
                            f"{json.dumps(result, ensure_ascii=False)}"
                        ),
                    }
                )

    async def _execute_tool_call(
        self,
        state: AgentGraphState,
        context: ToolContext,
        name: str,
        parsed_args: dict[str, Any],
        tool_call_id: Optional[str] = None,
    ) -> dict[str, Any]:
        request = state["request"]
        idempotency_key = self._tool_idempotency_key(state, name, parsed_args, tool_call_id)
        await self.emit(
            state,
            "tool_call_started",
            {"name": name, "arguments": parsed_args, "idempotency_key": idempotency_key},
        )

        if request.require_approval and self.permissions.requires_approval(name):
            snapshot = dict(state)
            snapshot["pending_tool_call"] = {"name": name, "arguments": parsed_args}
            snapshot["last_completed_node"] = "compress_context"
            await self.checkpoints.save(
                state["run_id"],
                state["thread_id"],
                state["user_id"],
                "tool_approval_waiting",
                snapshot,
                sequence=state.get("sequence", 0),
            )
            await self.jobs.mark_waiting(
                state["run_id"],
                state["user_id"],
                {
                    "status": "waiting_approval",
                    "tool_name": name,
                    "arguments": parsed_args,
                    "idempotency_key": idempotency_key,
                },
            )
            approval_id = await self.approvals.create_request(
                run_id=state["run_id"],
                thread_id=state["thread_id"],
                user_id=state["user_id"],
                tool_name=name,
                arguments=parsed_args,
                reason=self.permissions.approval_reason(name),
            )
            await self.emit(
                state,
                "tool_approval_required",
                {
                    "approval_id": approval_id,
                    "name": name,
                    "arguments": parsed_args,
                    "reason": self.permissions.approval_reason(name),
                },
            )
            approved = await self.approvals.wait_for_decision(approval_id, state["user_id"])
            if not approved:
                result = {
                    "name": name,
                    "arguments": parsed_args,
                    "result": {},
                    "success": False,
                    "error_message": "Tool approval rejected or timed out",
                }
                await self.emit(state, "tool_call_completed", result)
                return result

        if not self.permissions.can_auto_run(name) and not request.require_approval:
            result = {
                "name": name,
                "arguments": parsed_args,
                "result": {},
                "success": False,
                "error_message": "Tool requires approval",
            }
        else:
            snapshot = dict(state)
            snapshot["pending_tool_call"] = {"name": name, "arguments": parsed_args}
            snapshot["last_completed_node"] = "compress_context"
            await self.checkpoints.save(
                state["run_id"],
                state["thread_id"],
                state["user_id"],
                "tool_execution_started",
                snapshot,
                sequence=state.get("sequence", 0),
            )
            await self.jobs.mark_waiting(
                state["run_id"],
                state["user_id"],
                {
                    "status": "waiting_tool",
                    "tool_name": name,
                    "arguments": parsed_args,
                    "idempotency_key": idempotency_key,
                },
            )
            call_context = context.model_copy(
                update={"tool_call_id": tool_call_id, "idempotency_key": idempotency_key}
            )
            tool_result = await tool_registry.execute(name, parsed_args, call_context)
            result = tool_result.model_dump(mode="json")
            result["standard_observation"] = standardize_tool_result(result).model_dump(mode="json")

        await self.emit(state, "tool_call_completed", result)
        return result

    async def _set_job_running(self, state: AgentGraphState, node_name: str) -> None:
        await self.jobs.mark_running(state["run_id"], state["user_id"], node_name)
        await self.emit(state, "job_status_changed", {"status": "running", "current_node": node_name})

    @staticmethod
    def _tool_idempotency_key(
        state: AgentGraphState,
        name: str,
        arguments: dict[str, Any],
        tool_call_id: Optional[str] = None,
    ) -> str:
        import hashlib

        payload = json.dumps(arguments, ensure_ascii=False, sort_keys=True, default=str)
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]
        return f"{state['run_id']}:{name}:{tool_call_id or digest}"

    @staticmethod
    def _json_tool_instruction(tools: list[dict[str, Any]]) -> str:
        compact_tools = [
            {
                "name": tool["function"]["name"],
                "description": tool["function"].get("description", ""),
                "parameters": tool["function"].get("parameters", {}),
            }
            for tool in tools
        ]
        return (
            "当前模型不使用原生 function calling。你必须只输出一个 JSON 对象，不要输出 Markdown。\n"
            "需要调用工具时输出: "
            '{"action":"tool_call","tool":"工具名","arguments":{...}}\n'
            "可以一次调用多个工具时输出: "
            '{"tool_calls":[{"tool":"工具名","arguments":{...}}]}\n'
            "已经足够回答时输出: "
            '{"action":"final_answer","answer":"最终回答"}\n'
            f"可用工具: {json.dumps(compact_tools, ensure_ascii=False)}"
        )

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


def build_agent_state_graph():
    """Compatibility helper for callers that only need a compiled graph."""
    return GraphAgentExecutor(
        memory=MemoryManager(),
        tracer=TraceRecorder(),
        checkpoints=CheckpointRecorder(),
        approvals=ApprovalManager(),
        permissions=ToolPermissionPolicy(),
        planner=Planner(),
        reflector=Reflector(),
        critic=Critic(),
        coordinator=MultiAgentCoordinator(),
    ).graph


from .memory import MemoryManager  # noqa: E402
from .tracing import TraceRecorder  # noqa: E402
from .checkpoint import CheckpointRecorder  # noqa: E402
from .approvals import ApprovalManager  # noqa: E402
from .guardrails import ToolPermissionPolicy  # noqa: E402
from .planner import Planner  # noqa: E402
from .reflection import Reflector  # noqa: E402
from .critic import Critic  # noqa: E402
from .multi_agent import MultiAgentCoordinator  # noqa: E402
