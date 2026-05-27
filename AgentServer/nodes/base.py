"""
节点基类

所有节点类型的通用基类，提供:
- 生命周期管理
- 心跳机制 (每 5 秒续期，TTL 15 秒)
- 自动注册到 Redis
- 优雅下线 (主动删除心跳 Key)
- 日志配置 (带 trace_id)
- 健康检查
- gRPC RPC 服务 (节点间通信)
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, Callable
import asyncio
import logging
import signal
import uuid
import socket
import os
from datetime import datetime

from core.settings import settings
from core.managers import redis_manager
from core.protocols import NodeInfo, NodeType
from core.rpc import RPCServer


class TraceContextFilter(logging.Filter):
    """日志过滤器：注入 trace_id"""
    
    def __init__(self):
        super().__init__()
        self._trace_id = ""
    
    def set_trace_id(self, trace_id: str) -> None:
        self._trace_id = trace_id
    
    def clear_trace_id(self) -> None:
        self._trace_id = ""
    
    def filter(self, record: logging.LogRecord) -> bool:
        record.trace_id = self._trace_id or "-"
        return True


# 全局 trace_id 过滤器
trace_filter = TraceContextFilter()


class BaseNode(ABC):
    """
    节点基类
    
    所有节点必须继承此类并实现:
    - start(): 启动节点
    - stop(): 停止节点
    - run(): 节点主循环
    
    心跳机制:
    - 每 5 秒向 Redis 写入节点信息
    - Key: agent:nodes:{node_id}
    - TTL: 15 秒
    - 15 秒内无心跳则认为节点离线
    
    优雅下线:
    - 主动删除 Redis 中的心跳 Key
    - 日志记录 "Node Gracefully Offline"
    
    Example:
        class WebNode(BaseNode):
            node_type = NodeType.WEB
            
            async def _initialize_managers(self) -> None:
                await redis_manager.initialize()
                await mongo_manager.initialize()
            
            async def start(self) -> None:
                await self._initialize_managers()
                # ... 启动 FastAPI
    """
    
    # 节点类型 (子类必须定义)
    node_type: NodeType
    
    # 心跳配置
    HEARTBEAT_INTERVAL = 5   # 心跳间隔 (秒)
    HEARTBEAT_TTL = 15       # 心跳 TTL (秒)
    
    # 优雅下线超时
    SHUTDOWN_TIMEOUT = 30    # 等待任务完成的超时时间 (秒)
    
    # 默认 RPC 端口 (子类可覆盖)
    DEFAULT_RPC_PORT = 50051
    
    def __init__(self, node_id: Optional[str] = None, rpc_port: int = 0):
        self.node_id = node_id or f"{self.node_type.value}-{uuid.uuid4().hex[:8]}"
        self.logger = logging.getLogger(f"node.{self.node_id}")
        self.settings = settings
        
        self._running = False
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._start_time: Optional[datetime] = None
        
        # 负载信息 (Inference 节点使用)
        self._current_tasks = 0
        self._max_tasks = 5
        
        # RPC 服务器
        self._rpc_port = rpc_port or self.DEFAULT_RPC_PORT
        self._rpc_server: Optional[RPCServer] = None
        self._rpc_address: Optional[str] = None
        
        # 配置日志
        self._setup_logging()
    
    def _setup_logging(self) -> None:
        """配置日志 (控制台 + 文件)"""
        from logging.handlers import RotatingFileHandler
        from pathlib import Path
        
        obs_config = settings.observability
        log_level = getattr(logging, obs_config.log_level.upper())
        
        # 格式包含 trace_id
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)s | %(name)s | trace_id=%(trace_id)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        root_logger = logging.getLogger()
        if root_logger.handlers:
            return  # 已配置过，跳过
        
        root_logger.setLevel(log_level)
        
        # 1. 控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.setLevel(log_level)
        console_handler.addFilter(trace_filter)
        root_logger.addHandler(console_handler)
        
        # 2. 文件处理器 (可选)
        if obs_config.log_to_file:
            # 创建日志目录
            log_dir = Path(obs_config.log_dir)
            log_dir.mkdir(parents=True, exist_ok=True)
            
            # 日志文件名: {node_type}.log
            log_file = log_dir / f"{self.node_type.value}.log"
            
            # RotatingFileHandler: 自动轮转，防止文件过大
            file_handler = RotatingFileHandler(
                filename=str(log_file),
                maxBytes=obs_config.log_max_size_mb * 1024 * 1024,
                backupCount=obs_config.log_backup_count,
                encoding="utf-8",
            )
            file_handler.setFormatter(formatter)
            file_handler.setLevel(log_level)
            file_handler.addFilter(trace_filter)
            root_logger.addHandler(file_handler)
            
            # 打印日志文件路径
            print(f"[Logging] Output to file: {log_file.absolute()}")
    
    def set_trace_id(self, trace_id: str) -> None:
        """设置当前 trace_id (用于日志)"""
        trace_filter.set_trace_id(trace_id)
    
    def clear_trace_id(self) -> None:
        """清除 trace_id"""
        trace_filter.clear_trace_id()
    
    # ==================== RPC 服务 ====================
    
    async def _start_rpc_server(self) -> None:
        """
        启动 RPC 服务器
        
        子类可以在 start() 中注册 RPC 方法后调用此方法启动 RPC 服务。
        """
        self._rpc_server = RPCServer(
            node_id=self.node_id,
            node_type=self.node_type.value,
            port=self._rpc_port,
        )
        
        # 注册子类的 RPC 方法
        self._register_rpc_methods()
        
        # 启动服务器
        await self._rpc_server.start()
        self._rpc_address = self._rpc_server.address
        
        self.logger.info(f"RPC server started: {self._rpc_address}")
    
    async def _stop_rpc_server(self) -> None:
        """停止 RPC 服务器"""
        if self._rpc_server:
            await self._rpc_server.stop()
            self._rpc_server = None
            self.logger.info("RPC server stopped")
    
    def _register_rpc_methods(self) -> None:
        """
        注册 RPC 方法
        
        子类可覆盖此方法注册自己的 RPC 方法。
        
        Example:
            def _register_rpc_methods(self) -> None:
                super()._register_rpc_methods()
                self.register_rpc_method("refresh_strategies", self.handle_refresh)
        """
        # 默认注册 ping 方法
        self.register_rpc_method("ping", self._handle_ping)
    
    def register_rpc_method(
        self,
        name: str,
        handler: Callable[[Dict[str, Any]], Any],
        is_async: bool = True,
    ) -> None:
        """
        注册 RPC 方法
        
        Args:
            name: 方法名
            handler: 处理函数，签名为 async def handler(params: dict) -> dict
            is_async: 是否为异步函数
        """
        if self._rpc_server:
            self._rpc_server.register_method(name, handler, is_async)
        else:
            self.logger.warning(f"RPC server not started, cannot register method: {name}")
    
    async def _handle_ping(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """默认 ping 方法"""
        return {
            "pong": True,
            "node_id": self.node_id,
            "node_type": self.node_type.value,
            "timestamp": datetime.utcnow().isoformat(),
        }
    
    @abstractmethod
    async def start(self) -> None:
        """
        启动节点
        
        子类实现应包含:
        1. 初始化所需的 Manager
        2. 其他启动逻辑
        """
        raise NotImplementedError
    
    @abstractmethod
    async def stop(self) -> None:
        """停止节点"""
        raise NotImplementedError
    
    @abstractmethod
    async def run(self) -> None:
        """节点主循环"""
        raise NotImplementedError
    
    # ==================== 心跳机制 ====================
    
    async def _start_heartbeat(self) -> None:
        """
        心跳协程
        
        每 5 秒向 Redis 写入节点信息，TTL 15 秒。
        """
        if not redis_manager.is_initialized:
            self.logger.warning("Redis not initialized, skipping heartbeat")
            return
        
        self.logger.info(
            f"Starting heartbeat: interval={self.HEARTBEAT_INTERVAL}s, ttl={self.HEARTBEAT_TTL}s"
        )
        
        while self._running:
            try:
                node_info = self.get_node_info()
                await redis_manager.register_node(
                    node_id=self.node_id,
                    node_info=node_info.model_dump(mode="json"),
                    ttl=self.HEARTBEAT_TTL,
                )
                self.logger.debug(f"Heartbeat sent: {self.node_id}")
            except Exception as e:
                self.logger.error(f"Heartbeat failed: {e}")
            
            await asyncio.sleep(self.HEARTBEAT_INTERVAL)
    
    async def _unregister(self) -> None:
        """
        注销节点 (优雅下线)
        
        主动删除 Redis 中的心跳 Key。
        """
        if redis_manager.is_initialized:
            try:
                deleted = await redis_manager.unregister_node(self.node_id)
                if deleted:
                    self.logger.info(f"Node Gracefully Offline: {self.node_id}")
                else:
                    self.logger.warning(f"Node heartbeat key not found: {self.node_id}")
            except Exception as e:
                self.logger.error(f"Failed to unregister node: {e}")
    
    def get_node_info(self) -> NodeInfo:
        """获取节点信息"""
        return NodeInfo(
            node_id=self.node_id,
            node_type=self.node_type,
            host=socket.gethostname(),
            port=int(os.environ.get("PORT", settings.web.port)),
            status=self._get_status(),
            last_heartbeat=datetime.utcnow(),
            current_tasks=self._current_tasks,
            max_tasks=self._max_tasks,
            rpc_address=self._rpc_address,  # 新增: RPC 地址
        )
    
    def _get_status(self) -> str:
        """获取节点状态"""
        if not self._running:
            return "offline"
        if self._current_tasks >= self._max_tasks:
            return "busy"
        return "online"
    
    # ==================== 健康检查 ====================
    
    async def health_check(self) -> dict:
        """健康检查"""
        from core.managers import health_check_all
        
        manager_health = await health_check_all()
        
        return {
            "node_id": self.node_id,
            "node_type": self.node_type.value,
            "status": "healthy" if all(manager_health.values()) else "unhealthy",
            "uptime_seconds": (datetime.utcnow() - self._start_time).total_seconds() if self._start_time else 0,
            "managers": manager_health,
        }
    
    # ==================== 主入口 ====================
    
    async def main(self) -> None:
        """主入口"""
        # 注册信号处理 (仅 Unix)
        try:
            loop = asyncio.get_event_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self._shutdown_signal()))
        except NotImplementedError:
            # Windows 不支持 add_signal_handler
            pass
        
        try:
            self._running = True
            self._start_time = datetime.utcnow()
            
            self.logger.info(f"Starting node: {self.node_id} ({self.node_type.value})")
            
            # 启动节点
            await self.start()
            
            self.logger.info(f"Node started: {self.node_id}")
            
            # 启动心跳
            self._heartbeat_task = asyncio.create_task(self._start_heartbeat())
            
            # 运行主循环
            await self.run()
            
        except Exception as e:
            self.logger.exception(f"Node error: {e}")
            raise
        finally:
            await self._graceful_shutdown()
    
    async def _shutdown_signal(self) -> None:
        """处理关闭信号"""
        self.logger.info("Shutdown signal received, initiating graceful shutdown...")
        self._running = False
    
    async def _graceful_shutdown(self) -> None:
        """
        优雅下线
        
        1. 停止心跳
        2. 等待当前任务完成
        3. 注销节点 (删除 Redis Key)
        4. 关闭管理器
        """
        self.logger.info("Initiating graceful shutdown...")
        
        # 1. 停止心跳
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self.logger.debug("Heartbeat stopped")
        
        # 2. 等待当前任务完成
        if self._current_tasks > 0:
            self.logger.info(f"Waiting for {self._current_tasks} tasks to complete...")
            
            start_wait = datetime.utcnow()
            while self._current_tasks > 0:
                elapsed = (datetime.utcnow() - start_wait).total_seconds()
                if elapsed > self.SHUTDOWN_TIMEOUT:
                    self.logger.warning(
                        f"Shutdown timeout ({self.SHUTDOWN_TIMEOUT}s), "
                        f"forcing shutdown with {self._current_tasks} tasks remaining"
                    )
                    break
                await asyncio.sleep(0.5)
        
        # 3. 停止 RPC 服务器
        await self._stop_rpc_server()
        
        # 4. 注销节点 (主动删除 Redis Key)
        await self._unregister()
        
        # 5. 停止节点
        await self.stop()
        
        # 6. 关闭管理器
        from core.managers import shutdown_all_managers
        await shutdown_all_managers()
        
        self.logger.info("Graceful shutdown completed")
