"""
gRPC RPC Manager

提供 gRPC 服务器和客户端的封装，支持：
- 异步调用 (asyncio)
- 连接池
- 超时重试
- 断线重连
- TraceID 透传
"""

import asyncio
import json
import logging
import time
from typing import Optional, Dict, Any, Callable, List
from concurrent import futures
from dataclasses import dataclass, field

import grpc
from grpc import aio as grpc_aio

from . import service_pb2
from . import service_pb2_grpc


# ==================== 数据类型 ====================


@dataclass
class RPCMethodHandler:
    """RPC 方法处理器"""
    name: str
    handler: Callable
    is_async: bool = True


@dataclass 
class ConnectionInfo:
    """连接信息"""
    channel: grpc_aio.Channel
    stub: service_pb2_grpc.NodeServiceStub
    address: str
    last_used: float = field(default_factory=time.time)
    is_healthy: bool = True


# ==================== RPC Server ====================


class NodeServiceServicer(service_pb2_grpc.NodeServiceServicer):
    """
    gRPC 服务实现
    
    所有节点共用此服务实现，通过注册不同的方法处理器来扩展功能。
    """
    
    def __init__(self, node_id: str, node_type: str):
        self.node_id = node_id
        self.node_type = node_type
        self.logger = logging.getLogger(f"rpc.server.{node_id}")
        self._start_time = time.time()
        self._methods: Dict[str, RPCMethodHandler] = {}
        self._current_load = 0.0
    
    def register_method(self, name: str, handler: Callable, is_async: bool = True) -> None:
        """注册 RPC 方法"""
        self._methods[name] = RPCMethodHandler(name=name, handler=handler, is_async=is_async)
        self.logger.debug(f"Registered RPC method: {name}")
    
    def set_load(self, load: float) -> None:
        """设置当前负载"""
        self._current_load = min(1.0, max(0.0, load))
    
    async def HealthCheck(
        self,
        request: service_pb2.HealthCheckRequest,
        context: grpc.aio.ServicerContext,
    ) -> service_pb2.HealthCheckResponse:
        """健康检查"""
        uptime = int(time.time() - self._start_time)
        
        return service_pb2.HealthCheckResponse(
            node_id=self.node_id,
            node_type=self.node_type,
            status="healthy",
            uptime_seconds=uptime,
            load=self._current_load,
        )
    
    async def Invoke(
        self,
        request: service_pb2.InvokeRequest,
        context: grpc.aio.ServicerContext,
    ) -> service_pb2.InvokeResponse:
        """通用方法调用"""
        start_time = time.time()
        method_name = request.method
        trace_id = request.trace_id
        
        self.logger.debug(f"[{trace_id}] Invoke: {method_name} from {request.source_node}")
        
        # 查找方法处理器
        handler_info = self._methods.get(method_name)
        if not handler_info:
            return service_pb2.InvokeResponse(
                success=False,
                error=f"Method not found: {method_name}",
                elapsed_ms=int((time.time() - start_time) * 1000),
            )
        
        try:
            # 解析参数
            params = {}
            if request.payload:
                params = json.loads(request.payload.decode("utf-8"))
            
            # 添加 trace_id 到参数
            params["_trace_id"] = trace_id
            params["_source_node"] = request.source_node
            
            # 调用处理器
            if handler_info.is_async:
                result = await handler_info.handler(params)
            else:
                result = handler_info.handler(params)
            
            # 序列化结果
            result_bytes = json.dumps(result, default=str).encode("utf-8") if result else b"{}"
            
            elapsed_ms = int((time.time() - start_time) * 1000)
            self.logger.debug(f"[{trace_id}] Invoke success: {method_name}, elapsed={elapsed_ms}ms")
            
            return service_pb2.InvokeResponse(
                success=True,
                result=result_bytes,
                elapsed_ms=elapsed_ms,
            )
            
        except Exception as e:
            elapsed_ms = int((time.time() - start_time) * 1000)
            self.logger.error(f"[{trace_id}] Invoke failed: {method_name}, error={e}")
            
            return service_pb2.InvokeResponse(
                success=False,
                error=str(e),
                elapsed_ms=elapsed_ms,
            )
    
    async def Stream(
        self,
        request_iterator,
        context: grpc.aio.ServicerContext,
    ):
        """双向流式调用（预留）"""
        async for message in request_iterator:
            # 简单回显，后续可扩展
            yield service_pb2.StreamMessage(
                trace_id=message.trace_id,
                message_type="echo",
                payload=message.payload,
                timestamp=int(time.time() * 1000),
            )


class RPCServer:
    """
    gRPC 服务器
    
    每个节点启动一个 gRPC 服务器，提供 RPC 调用能力。
    
    Example:
        server = RPCServer(node_id="listener-xxx", node_type="listener", port=50053)
        
        # 注册方法
        server.register_method("refresh_strategies", self.handle_refresh)
        
        # 启动
        await server.start()
        
        # 停止
        await server.stop()
    """
    
    def __init__(
        self,
        node_id: str,
        node_type: str,
        host: str = "0.0.0.0",
        port: int = 50051,
        max_workers: int = 10,
    ):
        self.node_id = node_id
        self.node_type = node_type
        self.host = host
        self.port = port
        self.max_workers = max_workers
        
        self.logger = logging.getLogger(f"rpc.server.{node_id}")
        
        self._server: Optional[grpc_aio.Server] = None
        self._servicer = NodeServiceServicer(node_id, node_type)
    
    def register_method(self, name: str, handler: Callable, is_async: bool = True) -> None:
        """
        注册 RPC 方法
        
        Args:
            name: 方法名
            handler: 处理函数，签名为 async def handler(params: dict) -> dict
            is_async: 是否为异步函数
        """
        self._servicer.register_method(name, handler, is_async)
    
    def set_load(self, load: float) -> None:
        """设置当前负载"""
        self._servicer.set_load(load)
    
    async def start(self) -> None:
        """启动 gRPC 服务器"""
        self._server = grpc_aio.server(
            futures.ThreadPoolExecutor(max_workers=self.max_workers),
            options=[
                ("grpc.max_send_message_length", 50 * 1024 * 1024),  # 50MB
                ("grpc.max_receive_message_length", 50 * 1024 * 1024),
                ("grpc.keepalive_time_ms", 30000),
                ("grpc.keepalive_timeout_ms", 10000),
            ],
        )
        
        service_pb2_grpc.add_NodeServiceServicer_to_server(self._servicer, self._server)
        
        listen_addr = f"{self.host}:{self.port}"
        self._server.add_insecure_port(listen_addr)
        
        await self._server.start()
        self.logger.info(f"gRPC server started on {listen_addr}")
    
    async def stop(self, grace_period: float = 5.0) -> None:
        """
        优雅关闭 gRPC 服务器
        
        Args:
            grace_period: 等待现有请求完成的超时时间（秒）
        """
        if self._server:
            self.logger.info("Stopping gRPC server...")
            await self._server.stop(grace_period)
            self._server = None
            self.logger.info("gRPC server stopped")
    
    @property
    def address(self) -> str:
        """获取 gRPC 地址"""
        # 获取本机 IP（简化处理，实际可能需要更复杂的逻辑）
        import socket
        try:
            # 获取本机 IP
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
        except Exception:
            ip = "127.0.0.1"
        
        return f"{ip}:{self.port}"


# ==================== RPC Client ====================


class RPCClient:
    """
    gRPC 客户端
    
    支持：
    - 连接池管理
    - 自动从 Redis 获取目标节点地址
    - 超时重试
    - 断线重连
    
    Example:
        client = RPCClient()
        
        # 调用指定地址
        result = await client.invoke(
            address="192.168.1.100:50053",
            method="refresh_strategies",
            params={"strategy_type": "ma5_buy"},
            trace_id="xxx",
        )
        
        # 调用指定节点 ID（自动从 Redis 查询地址）
        result = await client.invoke_by_node_id(
            node_id="listener-abc123",
            method="refresh_strategies",
        )
        
        # 广播给所有指定类型的节点
        results = await client.broadcast_by_type(
            node_type="listener",
            method="refresh_strategies",
        )
    """
    
    # 连接池配置
    MAX_CONNECTIONS_PER_HOST = 5
    CONNECTION_IDLE_TIMEOUT = 300  # 秒
    
    # 重试配置
    DEFAULT_TIMEOUT = 10.0  # 秒
    MAX_RETRIES = 3
    RETRY_DELAY = 0.5  # 秒
    
    def __init__(self):
        self.logger = logging.getLogger("rpc.client")
        self._connections: Dict[str, ConnectionInfo] = {}
        self._lock = asyncio.Lock()
    
    async def _get_channel(self, address: str) -> ConnectionInfo:
        """
        获取或创建到指定地址的连接
        
        Args:
            address: 目标地址 (host:port)
            
        Returns:
            ConnectionInfo
        """
        async with self._lock:
            # 检查现有连接
            if address in self._connections:
                conn = self._connections[address]
                if conn.is_healthy:
                    conn.last_used = time.time()
                    return conn
                else:
                    # 连接不健康，关闭并重建
                    await conn.channel.close()
                    del self._connections[address]
            
            # 创建新连接
            channel = grpc_aio.insecure_channel(
                address,
                options=[
                    ("grpc.max_send_message_length", 50 * 1024 * 1024),
                    ("grpc.max_receive_message_length", 50 * 1024 * 1024),
                    ("grpc.keepalive_time_ms", 30000),
                    ("grpc.keepalive_timeout_ms", 10000),
                    ("grpc.enable_retries", 1),
                ],
            )
            
            stub = service_pb2_grpc.NodeServiceStub(channel)
            conn = ConnectionInfo(
                channel=channel,
                stub=stub,
                address=address,
            )
            
            self._connections[address] = conn
            self.logger.debug(f"Created new connection to {address}")
            
            return conn
    
    async def invoke(
        self,
        address: str,
        method: str,
        params: Dict[str, Any] = None,
        trace_id: str = None,
        source_node: str = None,
        timeout: float = None,
        retries: int = None,
    ) -> Dict[str, Any]:
        """
        调用远程 RPC 方法
        
        Args:
            address: 目标地址 (host:port)
            method: 方法名
            params: 参数字典
            trace_id: 链路追踪 ID
            source_node: 调用方节点 ID
            timeout: 超时时间（秒）
            retries: 重试次数
            
        Returns:
            {"success": bool, "result": Any, "error": str, "elapsed_ms": int}
        """
        import uuid
        
        trace_id = trace_id or uuid.uuid4().hex
        timeout = timeout or self.DEFAULT_TIMEOUT
        retries = retries if retries is not None else self.MAX_RETRIES
        
        # 序列化参数
        payload = json.dumps(params or {}).encode("utf-8")
        
        request = service_pb2.InvokeRequest(
            trace_id=trace_id,
            source_node=source_node or "unknown",
            method=method,
            payload=payload,
            timeout_ms=int(timeout * 1000),
        )
        
        last_error = None
        
        for attempt in range(retries + 1):
            try:
                conn = await self._get_channel(address)
                
                response = await asyncio.wait_for(
                    conn.stub.Invoke(request),
                    timeout=timeout,
                )
                
                # 解析结果
                result = None
                if response.result:
                    result = json.loads(response.result.decode("utf-8"))
                
                return {
                    "success": response.success,
                    "result": result,
                    "error": response.error if not response.success else None,
                    "elapsed_ms": response.elapsed_ms,
                }
                
            except asyncio.TimeoutError:
                last_error = "Timeout"
                self.logger.warning(
                    f"[{trace_id}] RPC timeout: {method} -> {address}, "
                    f"attempt={attempt + 1}/{retries + 1}"
                )
                
            except grpc.aio.AioRpcError as e:
                last_error = f"gRPC error: {e.code().name} - {e.details()}"
                self.logger.warning(
                    f"[{trace_id}] RPC error: {method} -> {address}, "
                    f"code={e.code().name}, attempt={attempt + 1}/{retries + 1}"
                )
                
                # 标记连接为不健康
                if address in self._connections:
                    self._connections[address].is_healthy = False
                
            except Exception as e:
                last_error = str(e)
                self.logger.error(
                    f"[{trace_id}] RPC exception: {method} -> {address}, "
                    f"error={e}, attempt={attempt + 1}/{retries + 1}"
                )
            
            # 重试前等待
            if attempt < retries:
                await asyncio.sleep(self.RETRY_DELAY * (attempt + 1))
        
        return {
            "success": False,
            "result": None,
            "error": last_error,
            "elapsed_ms": 0,
        }
    
    async def invoke_by_node_id(
        self,
        node_id: str,
        method: str,
        params: Dict[str, Any] = None,
        trace_id: str = None,
        source_node: str = None,
        timeout: float = None,
    ) -> Dict[str, Any]:
        """
        通过节点 ID 调用（自动从 Redis 查询地址）
        
        Args:
            node_id: 目标节点 ID
            其他参数同 invoke()
            
        Returns:
            调用结果
        """
        from core.managers import redis_manager
        
        # 从 Redis 获取节点信息
        node_info = await redis_manager.get_node(node_id)
        
        if not node_info:
            return {
                "success": False,
                "result": None,
                "error": f"Node not found: {node_id}",
                "elapsed_ms": 0,
            }
        
        rpc_address = node_info.get("rpc_address")
        if not rpc_address:
            return {
                "success": False,
                "result": None,
                "error": f"Node {node_id} has no RPC address",
                "elapsed_ms": 0,
            }
        
        return await self.invoke(
            address=rpc_address,
            method=method,
            params=params,
            trace_id=trace_id,
            source_node=source_node,
            timeout=timeout,
        )
    
    async def broadcast_by_type(
        self,
        node_type: str,
        method: str,
        params: Dict[str, Any] = None,
        trace_id: str = None,
        source_node: str = None,
        timeout: float = None,
    ) -> List[Dict[str, Any]]:
        """
        广播给指定类型的所有节点
        
        Args:
            node_type: 目标节点类型 (listener, inference, etc.)
            其他参数同 invoke()
            
        Returns:
            各节点的调用结果列表
        """
        from core.managers import redis_manager
        
        # 获取所有指定类型的节点
        nodes = await redis_manager.get_all_nodes(node_type=node_type)
        
        if not nodes:
            self.logger.warning(f"No nodes found for type: {node_type}")
            return []
        
        # 并行调用所有节点
        tasks = []
        for node in nodes:
            rpc_address = node.get("rpc_address")
            if rpc_address:
                task = self.invoke(
                    address=rpc_address,
                    method=method,
                    params=params,
                    trace_id=trace_id,
                    source_node=source_node,
                    timeout=timeout,
                )
                tasks.append((node.get("node_id", "unknown"), task))
        
        # 等待所有调用完成
        results = []
        for node_id, task in tasks:
            try:
                result = await task
                result["node_id"] = node_id
                results.append(result)
            except Exception as e:
                results.append({
                    "node_id": node_id,
                    "success": False,
                    "error": str(e),
                })
        
        return results
    
    async def health_check(self, address: str, timeout: float = 3.0) -> bool:
        """
        健康检查
        
        Args:
            address: 目标地址
            timeout: 超时时间
            
        Returns:
            True 表示健康
        """
        try:
            conn = await self._get_channel(address)
            
            request = service_pb2.HealthCheckRequest(trace_id="health-check")
            response = await asyncio.wait_for(
                conn.stub.HealthCheck(request),
                timeout=timeout,
            )
            
            return response.status == "healthy"
            
        except Exception as e:
            self.logger.debug(f"Health check failed for {address}: {e}")
            return False
    
    async def close(self) -> None:
        """关闭所有连接"""
        async with self._lock:
            for address, conn in self._connections.items():
                try:
                    await conn.channel.close()
                except Exception:
                    pass
            self._connections.clear()
            self.logger.info("All RPC connections closed")
    
    async def cleanup_idle_connections(self) -> None:
        """清理空闲连接"""
        now = time.time()
        async with self._lock:
            to_remove = []
            for address, conn in self._connections.items():
                if now - conn.last_used > self.CONNECTION_IDLE_TIMEOUT:
                    to_remove.append(address)
            
            for address in to_remove:
                try:
                    await self._connections[address].channel.close()
                except Exception:
                    pass
                del self._connections[address]
                self.logger.debug(f"Closed idle connection to {address}")


# ==================== 全局单例 ====================

rpc_manager = RPCClient()
