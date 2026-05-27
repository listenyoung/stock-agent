"""
Web 网关节点

职责:
- REST API 服务
- WebSocket 实时推送
- JWT 认证
- 任务分发到 Redis 队列
"""

from .node import WebNode
from .app import create_app

__all__ = ["WebNode", "create_app"]
