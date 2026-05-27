"""Structured planning for agent runs."""

from __future__ import annotations

import json
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field

from core.managers import llm_manager


class PlanStep(BaseModel):
    type: Literal["tool", "reason", "answer"] = "reason"
    tool: Optional[str] = None
    reason: str = ""


class AgentPlan(BaseModel):
    goal: str
    steps: list[PlanStep] = Field(default_factory=list)
    risk_level: Literal["low", "medium", "high"] = "medium"
    expected_tools: list[str] = Field(default_factory=list)


class Planner:
    """Produces a compact structured plan before tool execution."""

    async def create_plan(
        self,
        message: str,
        memories: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        model: Optional[str] = None,
    ) -> AgentPlan:
        tool_names = [item["function"]["name"] for item in tools]
        fallback = AgentPlan(
            goal=message,
            steps=[PlanStep(type="reason", reason="理解用户问题"), PlanStep(type="answer", reason="给出回答")],
            expected_tools=[],
        )
        if not llm_manager.is_initialized:
            return fallback

        prompt = {
            "message": message,
            "memories": memories[:6],
            "tools": tool_names,
        }
        try:
            text = await llm_manager.chat(
                [
                    {
                        "role": "system",
                        "content": (
                            "你是股票研究 Agent 的 Planner。只输出 JSON，不要 markdown。"
                            "字段: goal, steps[{type, tool, reason}], risk_level, expected_tools。"
                            "只有需要真实数据时才选择 tool。"
                        ),
                    },
                    {"role": "user", "content": json.dumps(prompt, ensure_ascii=False)},
                ],
                model=model,
                temperature=0.1,
                max_tokens=800,
            )
            return AgentPlan(**json.loads(self._strip_json(text)))
        except Exception:
            return fallback

    @staticmethod
    def _strip_json(text: str) -> str:
        text = text.strip()
        if text.startswith("```"):
            text = text.strip("`")
            text = text.removeprefix("json").strip()
        return text
