"""
管理器基类

所有资源管理器必须继承此类。
"""

from abc import ABC, abstractmethod
from typing import Optional
import logging


class BaseManager(ABC):
    """
    资源管理器基类
    
    每个 Manager 必须:
    1. 实现 async initialize() 初始化连接
    2. 实现 async shutdown() 关闭连接
    3. 实现 async health_check() 健康检查
    4. 在模块级别创建全局单例
    
    Example:
        class RedisManager(BaseManager):
            async def initialize(self) -> None:
                self._client = await aioredis.from_url(...)
        
        # 模块级别单例
        redis_manager = RedisManager()
    """
    
    def __init__(self):
        self._initialized = False
        self._logger: Optional[logging.Logger] = None
    
    @property
    def logger(self) -> logging.Logger:
        if self._logger is None:
            self._logger = logging.getLogger(f"manager.{self.__class__.__name__}")
        return self._logger
    
    @property
    def is_initialized(self) -> bool:
        return self._initialized
    
    def _ensure_initialized(self) -> None:
        """确保已初始化，否则抛出异常"""
        if not self._initialized:
            raise RuntimeError(
                f"{self.__class__.__name__} not initialized. "
                f"Call 'await {self.__class__.__name__.lower()}.initialize()' first."
            )
    
    @abstractmethod
    async def initialize(self) -> None:
        """
        初始化管理器
        
        子类必须在此方法中建立连接。
        初始化完成后必须设置 self._initialized = True
        """
        raise NotImplementedError
    
    @abstractmethod
    async def shutdown(self) -> None:
        """
        关闭管理器
        
        子类必须在此方法中释放资源。
        """
        raise NotImplementedError
    
    @abstractmethod
    async def health_check(self) -> bool:
        """
        健康检查
        
        Returns:
            True 表示健康，False 表示不健康
        """
        raise NotImplementedError
    
    def get_status(self) -> dict:
        """获取管理器状态"""
        return {
            "name": self.__class__.__name__,
            "initialized": self._initialized,
        }
