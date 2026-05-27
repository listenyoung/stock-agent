"""
MCP 服务节点

职责:
- 作为中间层，为 Inference 节点提供数据访问
- 封装 Tushare/MongoDB 数据获取逻辑
- 实现工具调用协议 (Tool Calling)
- 管理 API 频率限制
"""

from .node import MCPNode

__all__ = ["MCPNode"]
