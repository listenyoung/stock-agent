"""Reflection pass for deciding whether the agent has enough information."""

from __future__ import annotations

import json
from typing import Any, Literal
from typing import Optional

from pydantic import BaseModel, Field

from core.managers import llm_manager


class ReflectionResult(BaseModel):
    enough_information: bool = True
    missing: list[str] = Field(default_factory=list)
    risk_flags: list[str] = Field(default_factory=list)
    next_action: Literal["final_answer", "call_more_tools"] = "final_answer"


class Reflector:
    """Reviews tool observations before final answer generation."""

    async def reflect(
        self,
        question: str,
        tool_observations: list[dict[str, Any]],
        model: Optional[str] = None,
    ) -> ReflectionResult:
        if not tool_observations or not llm_manager.is_initialized:
            return ReflectionResult()
        try:
            text = await llm_manager.chat(
                [
                    {
                        "role": "system",
                        "content": (
                            "你是 Agent Reflection 节点。只输出 JSON。判断工具信息是否足够，"
                            "字段: enough_information, missing, risk_flags, next_action。"
                        ),
                    },
                    {
                        "role": "user",
                        "content": json.dumps(
                            {"question": question, "tool_observations": tool_observations},
                            ensure_ascii=False,
                        ),
                    },
                ],
                model=model,
                temperature=0.1,
                max_tokens=600,
            )
            return ReflectionResult(**json.loads(self._strip_json(text)))
        except Exception:
            return ReflectionResult()

    @staticmethod
    def _strip_json(text: str) -> str:
        text = text.strip()
        if text.startswith("```"):
            text = text.strip("`")
            text = text.removeprefix("json").strip()
        return text
