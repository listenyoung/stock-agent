"""
分析智能体节点

职责:
- 消费 Redis 任务队列
- 使用 LangGraph 执行分析工作流
- 调用 MCP 工具获取数据
- 推送分析结果
- 支持动态扩容
"""

from .node import InferenceNode

__all__ = ["InferenceNode"]
