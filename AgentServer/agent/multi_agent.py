"""Domain-specialized sub-agent assignment and parallel execution."""

from __future__ import annotations

import asyncio
import json
import re
from typing import Any, Awaitable, Callable, Optional

from pydantic import BaseModel, Field

from core.managers import llm_manager


ToolExecutor = Callable[[str, dict[str, Any]], Awaitable[dict[str, Any]]]
EventEmitter = Callable[[str, dict[str, Any]], Awaitable[Any]]


class AgentAssignment(BaseModel):
    agent: str
    responsibility: str
    tools: list[str] = Field(default_factory=list)


class SubAgentResult(BaseModel):
    agent: str
    responsibility: str
    findings: str = ""
    tool_results: list[dict[str, Any]] = Field(default_factory=list)
    confidence: float = 0.5
    risks: list[str] = Field(default_factory=list)
    success: bool = True
    error_message: Optional[str] = None


class DomainSubAgent:
    """One specialist agent with its own prompt, tool allowlist and schema."""

    def __init__(self, name: str, responsibility: str, tools: list[str], prompt: str) -> None:
        self.name = name
        self.responsibility = responsibility
        self.tools = tools
        self.prompt = prompt

    async def run(
        self,
        state: dict[str, Any],
        execute_tool: ToolExecutor,
        emit: EventEmitter,
    ) -> SubAgentResult:
        await emit("sub_agent_started", {"agent": self.name, "tools": self.tools})
        try:
            tool_results = []
            for call in await self._planned_tool_calls(state):
                if call.get("tool") not in self.tools:
                    continue
                tool_results.append(await execute_tool(call["tool"], call.get("arguments", {})))

            result = await self._write_result(state, tool_results)
            await emit("sub_agent_completed", result.model_dump(mode="json"))
            return result
        except Exception as exc:
            result = SubAgentResult(
                agent=self.name,
                responsibility=self.responsibility,
                success=False,
                error_message=str(exc),
                risks=[f"{self.name} failed: {exc}"],
            )
            await emit("sub_agent_failed", result.model_dump(mode="json"))
            return result

    async def _planned_tool_calls(self, state: dict[str, Any]) -> list[dict[str, Any]]:
        if not self.tools:
            return []
        request = state["request"]
        symbols = self._symbols(request.message)
        if not llm_manager.is_initialized:
            return self._fallback_tool_calls(symbols)

        try:
            text = await llm_manager.chat(
                [
                    {"role": "system", "content": self._tool_planning_prompt()},
                    {
                        "role": "user",
                        "content": json.dumps(
                            {
                                "question": request.message,
                                "allowed_tools": self.tools,
                                "plan": state.get("plan", {}),
                                "known_symbols": symbols,
                            },
                            ensure_ascii=False,
                        ),
                    },
                ],
                model=request.model,
                temperature=0.1,
                max_tokens=500,
            )
            payload = json.loads(self._strip_json(text))
            calls = payload.get("tool_calls", [])
            return calls if isinstance(calls, list) else []
        except Exception:
            return self._fallback_tool_calls(symbols)

    async def _write_result(
        self,
        state: dict[str, Any],
        tool_results: list[dict[str, Any]],
    ) -> SubAgentResult:
        request = state["request"]
        if not llm_manager.is_initialized:
            return SubAgentResult(
                agent=self.name,
                responsibility=self.responsibility,
                findings=self._fallback_findings(tool_results),
                tool_results=tool_results,
                confidence=0.55 if tool_results else 0.35,
                risks=[],
            )

        try:
            text = await llm_manager.chat(
                [
                    {"role": "system", "content": self._analysis_prompt()},
                    {
                        "role": "user",
                        "content": json.dumps(
                            {
                                "question": request.message,
                                "plan": state.get("plan", {}),
                                "existing_tool_observations": state.get("tool_observations", []),
                                "sub_agent_tool_results": tool_results,
                            },
                            ensure_ascii=False,
                        ),
                    },
                ],
                model=request.model,
                temperature=0.2,
                max_tokens=700,
            )
            payload = json.loads(self._strip_json(text))
            return SubAgentResult(
                agent=self.name,
                responsibility=self.responsibility,
                findings=str(payload.get("findings", "")),
                tool_results=tool_results,
                confidence=self._clamp(payload.get("confidence", 0.6)),
                risks=[str(item) for item in payload.get("risks", []) if item],
            )
        except Exception:
            return SubAgentResult(
                agent=self.name,
                responsibility=self.responsibility,
                findings=self._fallback_findings(tool_results),
                tool_results=tool_results,
                confidence=0.55 if tool_results else 0.35,
            )

    def _tool_planning_prompt(self) -> str:
        return (
            f"你是 {self.name}。职责: {self.responsibility}。"
            "你只能选择 allowed_tools 里的工具。只输出 JSON: "
            '{"tool_calls":[{"tool":"工具名","arguments":{...}}]}。'
            "没有必要调用工具时输出 {\"tool_calls\":[]}。"
        )

    def _analysis_prompt(self) -> str:
        return (
            f"你是 {self.name}。职责: {self.responsibility}。{self.prompt}"
            "只输出 JSON: {\"findings\":\"结论\", \"confidence\":0-1, \"risks\":[...]}。"
            "必须区分事实和推断，不能给确定性收益承诺。"
        )

    def _fallback_tool_calls(self, symbols: list[str]) -> list[dict[str, Any]]:
        calls = []
        first_symbol = symbols[0] if symbols else ""
        for tool in self.tools:
            if tool in {"get_stock_basic", "get_stock_daily", "get_financial_indicator", "get_news_sentiment"}:
                if first_symbol:
                    calls.append({"tool": tool, "arguments": {"ts_code": first_symbol}})
            elif tool in {"get_market_overview", "get_market_latest_analysis", "get_recent_backtest_results"}:
                calls.append({"tool": tool, "arguments": {}})
        return calls[:2]

    def _fallback_findings(self, tool_results: list[dict[str, Any]]) -> str:
        if not tool_results:
            return f"{self.name} 未获得足够工具数据，仅保留职责判断。"
        ok = len([item for item in tool_results if item.get("success")])
        return f"{self.name} 完成 {len(tool_results)} 次工具检查，其中 {ok} 次成功。"

    @staticmethod
    def _symbols(text: str) -> list[str]:
        return re.findall(r"\b\d{6}\.(?:SZ|SH|BJ)\b", text.upper())

    @staticmethod
    def _strip_json(text: str) -> str:
        text = text.strip()
        if text.startswith("```"):
            text = text.strip("`").removeprefix("json").strip()
        return text

    @staticmethod
    def _clamp(value: Any) -> float:
        try:
            return round(max(0.0, min(1.0, float(value))), 3)
        except Exception:
            return 0.5


class MultiAgentCoordinator:
    """Supervisor that assigns and executes domain sub-agents."""

    AGENT_TOOL_MAP = {
        "MarketAgent": ["get_market_overview", "get_market_latest_analysis"],
        "TechnicalAgent": ["get_stock_daily"],
        "FundamentalAgent": ["get_stock_basic", "get_financial_indicator"],
        "NewsAgent": ["get_news_sentiment"],
        "BacktestAgent": [
            "list_backtest_factors",
            "get_recent_backtest_results",
            "get_backtest_result",
            "submit_backtest",
            "submit_factor_selection_backtest",
        ],
        "RiskAgent": [],
    }

    PROMPTS = {
        "MarketAgent": "关注大盘环境、情绪周期、流动性和市场风格。",
        "TechnicalAgent": "关注价格、成交量、趋势、波动和关键位置。",
        "FundamentalAgent": "关注公司基础信息、估值、财务质量和经营约束。",
        "NewsAgent": "关注新闻、舆情、事件驱动和信息时效性。",
        "BacktestAgent": "关注策略历史表现、回撤、稳定性和资源消耗。",
        "RiskAgent": "审查所有结论的风险、证据缺口、合规边界和不确定性。",
    }

    def assign(self, plan: dict[str, Any]) -> list[AgentAssignment]:
        expected_tools = set(plan.get("expected_tools", []))
        assignments: list[AgentAssignment] = []
        for agent, tools in self.AGENT_TOOL_MAP.items():
            if agent == "RiskAgent":
                continue
            matched = [tool for tool in tools if tool in expected_tools]
            selected_tools = matched if expected_tools else tools[:1]
            assignments.append(
                AgentAssignment(
                    agent=agent,
                    responsibility=self._responsibility(agent),
                    tools=selected_tools,
                )
            )
        assignments.append(AgentAssignment(agent="RiskAgent", responsibility=self._responsibility("RiskAgent"), tools=[]))
        return assignments

    async def run_parallel(
        self,
        state: dict[str, Any],
        execute_tool: ToolExecutor,
        emit: EventEmitter,
    ) -> list[SubAgentResult]:
        assignments = [AgentAssignment(**item) for item in state.get("assignments", [])]
        specialists = [item for item in assignments if item.agent != "RiskAgent"]
        risk_assignment = next((item for item in assignments if item.agent == "RiskAgent"), None)

        await emit(
            "sub_agents_started",
            {"agents": [item.agent for item in specialists], "mode": "parallel"},
        )
        results = await asyncio.gather(
            *[
                self._build_agent(item).run(state, execute_tool, emit)
                for item in specialists
            ],
            return_exceptions=True,
        )
        normalized = [
            item if isinstance(item, SubAgentResult) else SubAgentResult(agent="UnknownAgent", responsibility="", success=False, error_message=str(item))
            for item in results
        ]

        if risk_assignment:
            risk_result = await self._run_risk_agent(state, risk_assignment, normalized, emit)
            normalized.append(risk_result)

        await emit(
            "sub_agents_completed",
            {"results": [item.model_dump(mode="json") for item in normalized]},
        )
        return normalized

    def _build_agent(self, assignment: AgentAssignment) -> DomainSubAgent:
        return DomainSubAgent(
            assignment.agent,
            assignment.responsibility,
            assignment.tools,
            self.PROMPTS.get(assignment.agent, ""),
        )

    async def _run_risk_agent(
        self,
        state: dict[str, Any],
        assignment: AgentAssignment,
        specialist_results: list[SubAgentResult],
        emit: EventEmitter,
    ) -> SubAgentResult:
        await emit("sub_agent_started", {"agent": "RiskAgent", "tools": []})
        request = state["request"]
        if not llm_manager.is_initialized:
            result = SubAgentResult(
                agent="RiskAgent",
                responsibility=assignment.responsibility,
                findings="需保留投资风险提示，避免确定性收益承诺。",
                confidence=0.65,
                risks=["市场波动", "数据时效性", "模型推断不确定"],
            )
            await emit("sub_agent_completed", result.model_dump(mode="json"))
            return result
        try:
            text = await llm_manager.chat(
                [
                    {
                        "role": "system",
                        "content": (
                            "你是 RiskAgent。审查其他子 Agent 的结论，只输出 JSON: "
                            "{\"findings\":\"风险审查\", \"confidence\":0-1, \"risks\":[...]}"
                        ),
                    },
                    {
                        "role": "user",
                        "content": json.dumps(
                            {
                                "question": request.message,
                                "specialist_results": [item.model_dump(mode="json") for item in specialist_results],
                            },
                            ensure_ascii=False,
                        ),
                    },
                ],
                model=request.model,
                temperature=0.1,
                max_tokens=700,
            )
            payload = json.loads(DomainSubAgent._strip_json(text))
            result = SubAgentResult(
                agent="RiskAgent",
                responsibility=assignment.responsibility,
                findings=str(payload.get("findings", "")),
                confidence=DomainSubAgent._clamp(payload.get("confidence", 0.7)),
                risks=[str(item) for item in payload.get("risks", []) if item],
            )
        except Exception as exc:
            result = SubAgentResult(
                agent="RiskAgent",
                responsibility=assignment.responsibility,
                findings="风险审查失败，最终回答需保守处理。",
                confidence=0.4,
                risks=[str(exc)],
            )
        await emit("sub_agent_completed", result.model_dump(mode="json"))
        return result

    @staticmethod
    def _responsibility(agent: str) -> str:
        return {
            "MarketAgent": "判断市场环境和情绪周期",
            "TechnicalAgent": "分析价格、成交量和技术走势",
            "FundamentalAgent": "分析基础信息和财务质量",
            "NewsAgent": "检查新闻、舆情和事件驱动因素",
            "BacktestAgent": "读取或提交策略回测",
            "RiskAgent": "审查结论风险和投资建议边界",
        }.get(agent, "协助完成任务")
