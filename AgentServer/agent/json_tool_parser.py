"""Parse JSON tool-call actions emitted by models without native tool calling."""

from __future__ import annotations

import json
import re
from typing import Any, Optional

from pydantic import BaseModel, Field


class JsonToolAction(BaseModel):
    action: str = "final_answer"
    tool: Optional[str] = None
    arguments: dict[str, Any] = Field(default_factory=dict)
    answer: str = ""


class JsonToolParser:
    """Lenient parser for a small action schema used by local models."""

    def parse(self, text: str) -> list[JsonToolAction]:
        payload = self._load_json(text)
        if payload is None:
            return [JsonToolAction(action="final_answer", answer=text.strip())]

        if isinstance(payload, list):
            return [self._coerce_action(item) for item in payload if isinstance(item, dict)]

        if isinstance(payload, dict):
            if isinstance(payload.get("tool_calls"), list):
                return [
                    self._coerce_action({"action": "tool_call", **item})
                    for item in payload["tool_calls"]
                    if isinstance(item, dict)
                ]
            if isinstance(payload.get("tools"), list):
                return [
                    self._coerce_action({"action": "tool_call", **item})
                    for item in payload["tools"]
                    if isinstance(item, dict)
                ]
            return [self._coerce_action(payload)]

        return [JsonToolAction(action="final_answer", answer=text.strip())]

    def _coerce_action(self, payload: dict[str, Any]) -> JsonToolAction:
        action = str(payload.get("action") or payload.get("type") or "").lower()
        tool = payload.get("tool") or payload.get("name") or payload.get("function")
        arguments = payload.get("arguments") or payload.get("args") or payload.get("input") or {}

        if tool and action not in {"final_answer", "answer"}:
            action = "tool_call"
        elif action in {"final", "answer", "final_answer"}:
            action = "final_answer"
        else:
            action = "final_answer"

        if isinstance(arguments, str):
            try:
                arguments = json.loads(arguments)
            except Exception:
                arguments = {"raw": arguments}
        if not isinstance(arguments, dict):
            arguments = {"value": arguments}

        return JsonToolAction(
            action=action,
            tool=str(tool) if tool else None,
            arguments=arguments,
            answer=str(payload.get("answer") or payload.get("content") or ""),
        )

    def _load_json(self, text: str) -> Any:
        cleaned = self._strip_fence(text.strip())
        candidates = [cleaned]
        extracted = self._extract_first_json(cleaned)
        if extracted and extracted != cleaned:
            candidates.append(extracted)
        for candidate in candidates:
            try:
                return json.loads(candidate)
            except Exception:
                continue
        return None

    @staticmethod
    def _strip_fence(text: str) -> str:
        match = re.fullmatch(r"```(?:json)?\s*(.*?)\s*```", text, flags=re.IGNORECASE | re.DOTALL)
        return match.group(1).strip() if match else text

    @staticmethod
    def _extract_first_json(text: str) -> Optional[str]:
        start_positions = [pos for pos in (text.find("{"), text.find("[")) if pos >= 0]
        if not start_positions:
            return None
        start = min(start_positions)
        opener = text[start]
        closer = "}" if opener == "{" else "]"
        depth = 0
        in_string = False
        escape = False
        for idx in range(start, len(text)):
            char = text[idx]
            if in_string:
                if escape:
                    escape = False
                elif char == "\\":
                    escape = True
                elif char == '"':
                    in_string = False
                continue
            if char == '"':
                in_string = True
            elif char == opener:
                depth += 1
            elif char == closer:
                depth -= 1
                if depth == 0:
                    return text[start : idx + 1]
        return None


json_tool_parser = JsonToolParser()
