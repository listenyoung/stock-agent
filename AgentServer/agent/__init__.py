"""
Agent runtime package.

This package adds a modern agent layer beside the existing stock APIs. It is
intentionally thin at first: the runtime owns tool calls, memory loading,
context compression, streaming events, and trace persistence while reusing the
project's current managers and data tools.
"""

from .runtime import AgentRuntime
from .state import AgentRunEvent, AgentRunRequest
from .tool_registry import ToolRegistry, tool_registry
from .graph import AgentGraphSpec, get_default_agent_graph
from .exporter import TrainingExporter
from .evaluator import AgentEvaluator, EvalRunRequest, EvalRunSummary, EvalResult
from .jobs import AgentJob, AgentJobManager
from .model_capabilities import ModelCapability, ModelCapabilityRegistry, model_capability_registry
from .tool_reliability import ToolReliabilityConfig, ToolReliabilityMeta, ToolReliabilityPolicy

__all__ = [
    "AgentRuntime",
    "AgentRunEvent",
    "AgentRunRequest",
    "AgentGraphSpec",
    "TrainingExporter",
    "AgentEvaluator",
    "AgentJob",
    "AgentJobManager",
    "EvalResult",
    "EvalRunRequest",
    "EvalRunSummary",
    "ModelCapability",
    "ModelCapabilityRegistry",
    "ToolReliabilityConfig",
    "ToolReliabilityMeta",
    "ToolReliabilityPolicy",
    "ToolRegistry",
    "get_default_agent_graph",
    "model_capability_registry",
    "tool_registry",
]
