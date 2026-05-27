"""Model capability detection for agent tool calling.

Different OpenAI-compatible providers expose different levels of tool-call
support.  The runtime uses this small capability layer to choose between
native function calling and a JSON action fallback that also works with most
local models.
"""

from __future__ import annotations

import os
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field

from core.settings import settings


ToolCallingMode = Literal["native", "json_fallback", "none"]


class ModelCapability(BaseModel):
    provider: str
    model: str
    tool_calling: ToolCallingMode = "json_fallback"
    streaming: bool = True
    json_mode: bool = False
    context_window: int = 8192
    reason: str = ""
    source: str = "heuristic"
    notes: list[str] = Field(default_factory=list)


class ModelCapabilityRegistry:
    """Resolve practical model capabilities from config, model name and env."""

    def resolve(
        self,
        model: Optional[str] = None,
        provider: Optional[str] = None,
    ) -> ModelCapability:
        provider_name = (provider or settings.llm.provider or "").lower()
        model_name = model or settings.llm.model_name
        normalized = model_name.lower()

        override = os.getenv("AGENT_TOOL_CALLING_MODE", "").strip().lower()
        if override in {"native", "json_fallback", "none"}:
            return ModelCapability(
                provider=provider_name,
                model=model_name,
                tool_calling=override,  # type: ignore[arg-type]
                context_window=self._context_window(normalized),
                reason="AGENT_TOOL_CALLING_MODE override",
                source="env",
            )

        mode = self._infer_tool_calling(provider_name, normalized)
        return ModelCapability(
            provider=provider_name,
            model=model_name,
            tool_calling=mode,
            json_mode=mode == "json_fallback",
            context_window=self._context_window(normalized),
            reason=self._reason(provider_name, normalized, mode),
        )

    def describe(
        self,
        model: Optional[str] = None,
        provider: Optional[str] = None,
    ) -> dict[str, Any]:
        return self.resolve(model=model, provider=provider).model_dump(mode="json")

    @staticmethod
    def _infer_tool_calling(provider: str, model: str) -> ToolCallingMode:
        if provider in {"openai", "deepseek", "dashscope", "zhipu"}:
            return "native"
        if provider == "ollama":
            if any(key in model for key in ("llama3", "qwen2.5", "qwen3", "mistral", "glm", "deepseek")):
                return "json_fallback"
            return "none"

        if model.startswith(("gpt-", "o1", "o3", "o4")):
            return "native"
        if any(key in model for key in ("qwen", "deepseek", "glm", "yi-", "llama", "mistral")):
            return "json_fallback"
        return "json_fallback"

    @staticmethod
    def _context_window(model: str) -> int:
        if any(key in model for key in ("128k", "qwen-plus", "qwen-max", "gpt-4.1", "gpt-4o")):
            return 128000
        if any(key in model for key in ("32k", "deepseek", "qwen2.5")):
            return 32768
        if any(key in model for key in ("16k", "glm-4")):
            return 16384
        return 8192

    @staticmethod
    def _reason(provider: str, model: str, mode: ToolCallingMode) -> str:
        if mode == "native":
            return f"{provider or model} is treated as OpenAI-compatible native tool calling"
        if mode == "json_fallback":
            return "model will emit structured JSON actions and the runtime executes tools"
        return "model is treated as text-only and tools are disabled"


model_capability_registry = ModelCapabilityRegistry()
