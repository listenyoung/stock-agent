"""Final answer critic and risk guard."""

from __future__ import annotations

import json
from typing import Literal
from typing import Optional

from pydantic import BaseModel, Field

from core.managers import llm_manager


class CriticResult(BaseModel):
    passed: bool = True
    score: float = 0.8
    issues: list[str] = Field(default_factory=list)
    risk_level: Literal["low", "medium", "high"] = "medium"
    revised_answer: Optional[str] = None


class Critic:
    """Checks final answers for completeness, hallucination risk, and finance safety."""

    async def critique(
        self,
        question: str,
        answer: str,
        model: Optional[str] = None,
    ) -> CriticResult:
        if not answer or not llm_manager.is_initialized:
            return CriticResult()
        try:
            text = await llm_manager.chat(
                [
                    {
                        "role": "system",
                        "content": (
                            "你是金融 Agent 的 Critic。只输出 JSON。检查回答是否完整、"
                            "是否区分事实和推断、是否有投资风险提示。字段: passed, score, issues, risk_level, revised_answer。"
                        ),
                    },
                    {
                        "role": "user",
                        "content": json.dumps({"question": question, "answer": answer}, ensure_ascii=False),
                    },
                ],
                model=model,
                temperature=0.1,
                max_tokens=900,
            )
            return CriticResult(**json.loads(self._strip_json(text)))
        except Exception:
            return CriticResult()

    @staticmethod
    def _strip_json(text: str) -> str:
        text = text.strip()
        if text.startswith("```"):
            text = text.strip("`")
            text = text.removeprefix("json").strip()
        return text
