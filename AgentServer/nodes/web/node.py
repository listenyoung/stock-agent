"""
Web 网关节点实现
"""

import asyncio
from typing import Optional, Dict, Any

import uvicorn

from nodes.base import BaseNode
from core.protocols import NodeType
from core.settings import settings
from core.managers import redis_manager
from .app import create_app


class WebNode(BaseNode):
    """
    Web 网关节点
    
    职责:
    - 提供 REST API 和 WebSocket 接口
    - JWT 认证
    - 任务派发到 Redis 队列
    - 结果实时推送给前端
    - 通过 gRPC RPC 通知其他节点
    """
    
    node_type = NodeType.WEB
    DEFAULT_RPC_PORT = 50051  # WebNode 默认 RPC 端口
    
    def __init__(
        self,
        node_id: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        rpc_port: int = 0,
    ):
        super().__init__(node_id, rpc_port or settings.rpc.web_port)
        self.host = host or settings.web.host
        self.port = port or settings.web.port
        self._server: Optional[uvicorn.Server] = None
    
    async def start(self) -> None:
        """启动 Web 服务"""
        # 初始化 Redis（用于 RPC 服务发现）
        await redis_manager.initialize()
        
        # 启动 RPC 服务器
        await self._start_rpc_server()
        
        self.logger.info(
            f"Web node starting on {self.host}:{self.port}, "
            f"rpc={self._rpc_address}"
        )
    
    async def stop(self) -> None:
        """停止 Web 服务"""
        if self._server:
            self._server.should_exit = True
    
    async def run(self) -> None:
        """运行 Web 服务"""
        app = create_app()
        
        config = uvicorn.Config(
            app,
            host=self.host,
            port=self.port,
            log_level="info",
            access_log=True,
        )
        
        self._server = uvicorn.Server(config)
        await self._server.serve()


def main():
    """入口函数"""
    node = WebNode()
    asyncio.run(node.main())


if __name__ == "__main__":
    main()
