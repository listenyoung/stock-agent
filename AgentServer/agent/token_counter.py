"""Approximate token counting and budget allocation."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class TokenBudget:
    max_tokens: int = 32000
    system_tokens: int = 2000
    memory_tokens: int = 5000
    history_tokens: int = 8000
    tool_tokens: int = 10000
    reserve_output_tokens: int = 7000


class TokenCounter:
    """Small dependency-free token estimator.

    Chinese text and JSON tool observations are roughly estimated by chars/2;
    English-ish text by chars/4. This is intentionally conservative.
    """

    def count(self, text: str) -> int:
        if not text:
            return 0
        cjk = sum(1 for ch in text if "\u4e00" <= ch <= "\u9fff")
        non_cjk = len(text) - cjk
        return max(1, cjk // 2 + non_cjk // 4)

    def fit(self, text: str, max_tokens: int) -> str:
        if self.count(text) <= max_tokens:
            return text
        approx_chars = max_tokens * 3
        head = text[: approx_chars // 2]
        tail = text[-approx_chars // 2 :]
        return f"{head}\n...[token budget compressed]...\n{tail}"
