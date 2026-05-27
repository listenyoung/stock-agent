"""
Core: 核心基础设施层

包含:
- settings: 全局配置管理
- managers: 资源管理器 (Manager-Instance 模式)
- base: 基类定义 (BaseTool, BaseCollector)
- protocols: 消息协议定义
"""

from .settings import settings, Settings
from .base import BaseTool, BaseCollector, ToolResult
from .protocols import (
    NodeType,
    TaskType,
    TaskStatus,
    SignalType,
    NodeInfo,
    AgentTask,
    AgentResponse,
    TaskProgress,
    AnalysisResult,
)

__all__ = [
    # 配置
    "settings",
    "Settings",
    # 基类
    "BaseTool",
    "BaseCollector",
    "ToolResult",
    # 协议
    "NodeType",
    "TaskType",
    "TaskStatus",
    "SignalType",
    "NodeInfo",
    "AgentTask",
    "AgentResponse",
    "TaskProgress",
    "AnalysisResult",
]
