"""Context window compression and prompt assembly."""

from typing import Any, Optional

from agent.state import MemoryItem
from agent.token_counter import TokenBudget, TokenCounter


class ContextCompressor:
    """Builds compact model context from memory, history, and tool outputs."""

    def __init__(self, budget: Optional[TokenBudget] = None) -> None:
        self.budget = budget or TokenBudget()
        self.counter = TokenCounter()

    def build_system_prompt(self, tools: list[dict[str, Any]]) -> str:
        tool_names = ", ".join(
            item["function"]["name"] for item in tools if item.get("type") == "function"
        )
        return (
            "你是 StockAgent 的现代化股票研究 Agent。你可以调用工具查询股票基础信息、"
            "日线行情、新闻舆情和后续扩展的回测工具。回答必须区分事实、推断和风险，"
            "不要给出确定性收益承诺。需要数据时优先调用工具。"
            f"\n\n可用工具: {tool_names or '无'}"
        )

    def compress(
        self,
        message: str,
        memories: list[MemoryItem],
        history: list[dict[str, Any]],
        tool_observations: Optional[list[dict[str, Any]]] = None,
    ) -> list[dict[str, str]]:
        memory_text = self._fit(
            "\n".join(f"- [{m.type}] {m.content}" for m in memories),
            self.budget.memory_tokens,
        )
        history_text = self._fit(
            "\n".join(
                f"{item.get('role', 'unknown')}: {item.get('content', '')}"
                for item in history[-30:]
            ),
            self.budget.history_tokens,
        )
        tool_text = self._fit(
            "\n".join(str(item) for item in (tool_observations or [])),
            self.budget.tool_tokens,
        )

        compact_context = "\n\n".join(
            part
            for part in [
                f"相关长期记忆:\n{memory_text}" if memory_text else "",
                f"近期会话摘要/消息:\n{history_text}" if history_text else "",
                f"工具观测:\n{tool_text}" if tool_text else "",
            ]
            if part
        )

        user_content = message
        if compact_context:
            user_content = f"{compact_context}\n\n用户当前问题:\n{message}"

        return [{"role": "user", "content": user_content}]

    def _fit(self, text: str, max_tokens: int) -> str:
        return self.counter.fit(text, max_tokens)
