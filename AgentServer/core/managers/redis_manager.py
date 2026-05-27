"""
Redis 管理器

负责:
- 任务队列 (LPUSH/BRPOP)
- 结果发布订阅 (PUBLISH/SUBSCRIBE)
- 节点注册与心跳 (SET with TTL)
- 分布式锁 (防止重复抓取)
- 缓存
"""

from typing import Optional, Any, Callable
from contextlib import asynccontextmanager
from datetime import datetime
import json
import asyncio
import uuid

import redis.asyncio as aioredis
from redis.asyncio.client import PubSub
from redis.asyncio.connection import ConnectionPool

from .base import BaseManager
from ..settings import settings


class DistributedLock:
    """
    分布式锁
    
    使用 Redis SETNX 实现，支持自动续期和超时释放。
    
    Example:
        async with redis_manager.dist_lock("sync:daily:20240101", timeout=300):
            # 只有一个节点能执行这里的代码
            await sync_daily_data("20240101")
    """
    
    def __init__(
        self,
        client: aioredis.Redis,
        key: str,
        timeout: int = 60,
        retry_interval: float = 0.1,
        retry_times: int = 3,
    ):
        self._client = client
        self._key = f"lock:{key}"
        self._timeout = timeout
        self._retry_interval = retry_interval
        self._retry_times = retry_times
        self._token = uuid.uuid4().hex
        self._acquired = False
        self._extend_task: Optional[asyncio.Task] = None
    
    async def acquire(self) -> bool:
        """获取锁"""
        for _ in range(self._retry_times):
            # 尝试获取锁
            result = await self._client.set(
                self._key,
                self._token,
                ex=self._timeout,
                nx=True,
            )
            
            if result:
                self._acquired = True
                # 启动自动续期
                self._extend_task = asyncio.create_task(self._auto_extend())
                return True
            
            await asyncio.sleep(self._retry_interval)
        
        return False
    
    async def release(self) -> bool:
        """释放锁"""
        if not self._acquired:
            return False
        
        # 停止续期
        if self._extend_task:
            self._extend_task.cancel()
            try:
                await self._extend_task
            except asyncio.CancelledError:
                pass
        
        # 使用 Lua 脚本确保只删除自己的锁
        lua_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        
        try:
            result = await self._client.eval(lua_script, 1, self._key, self._token)
            self._acquired = False
            return result == 1
        except Exception:
            return False
    
    async def _auto_extend(self) -> None:
        """自动续期 (每 timeout/3 秒)"""
        extend_interval = self._timeout / 3
        
        while self._acquired:
            await asyncio.sleep(extend_interval)
            
            if self._acquired:
                try:
                    # 只有持有锁的情况下才续期
                    current = await self._client.get(self._key)
                    if current == self._token:
                        await self._client.expire(self._key, self._timeout)
                except Exception:
                    pass
    
    async def __aenter__(self):
        if not await self.acquire():
            raise RuntimeError(f"Failed to acquire lock: {self._key}")
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.release()
        return False


class RedisManager(BaseManager):
    """
    Redis 资源管理器
    
    特性:
    - 显式连接池管理 (默认 max_connections=100)
    - 分布式锁支持
    - 任务队列和 Pub/Sub
    """
    
    # 默认连接池大小
    DEFAULT_MAX_CONNECTIONS = 100
    
    def __init__(self):
        super().__init__()
        self._pool: Optional[ConnectionPool] = None
        self._client: Optional[aioredis.Redis] = None
        self._pubsub: Optional[PubSub] = None
        self._config = settings.redis
    
    async def initialize(self) -> None:
        """初始化 Redis 连接池"""
        if self._initialized:
            return
        
        max_connections = self._config.max_connections or self.DEFAULT_MAX_CONNECTIONS
        
        self.logger.info(
            f"Connecting to Redis: {self._config.host}:{self._config.port} "
            f"(pool_size={max_connections})"
        )
        
        # 显式创建连接池
        self._pool = ConnectionPool.from_url(
            self._config.url,
            max_connections=max_connections,
            decode_responses=True,
        )
        
        # 从连接池创建客户端
        self._client = aioredis.Redis(connection_pool=self._pool)
        
        # 测试连接
        await self._client.ping()
        
        self._initialized = True
        self.logger.info(f"Redis connected ✓ (pool_size={max_connections})")
    
    async def shutdown(self) -> None:
        """关闭连接池"""
        if self._pubsub:
            await self._pubsub.close()
            self._pubsub = None
        
        if self._client:
            await self._client.close()
            self._client = None
        
        if self._pool:
            await self._pool.disconnect()
            self._pool = None
        
        self._initialized = False
        self.logger.info("Redis disconnected")
    
    async def health_check(self) -> bool:
        """健康检查"""
        try:
            if self._client:
                await self._client.ping()
                return True
        except Exception:
            pass
        return False
    
    @property
    def client(self) -> aioredis.Redis:
        """获取 Redis 客户端"""
        self._ensure_initialized()
        return self._client
    
    def get_pool_stats(self) -> dict:
        """获取连接池状态"""
        if not self._pool:
            return {"status": "not_initialized"}
        
        return {
            "max_connections": self._pool.max_connections,
            # ConnectionPool 的具体统计取决于 redis-py 版本
        }
    
    # ==================== 分布式锁 ====================
    
    def dist_lock(
        self,
        key: str,
        timeout: int = 60,
        retry_interval: float = 0.1,
        retry_times: int = 3,
    ) -> DistributedLock:
        """
        获取分布式锁
        
        Args:
            key: 锁的唯一标识 (会自动加上 lock: 前缀)
            timeout: 锁超时时间 (秒)，超时后自动释放
            retry_interval: 获取锁失败后的重试间隔 (秒)
            retry_times: 获取锁失败后的重试次数
            
        Returns:
            DistributedLock 对象，用于 async with 语法
            
        Example:
            # 防止多个 Data Agent 重复抓取同一天的行情
            async with redis_manager.dist_lock(f"sync:daily:{trade_date}", timeout=300):
                await sync_daily_data(trade_date)
        """
        self._ensure_initialized()
        return DistributedLock(
            client=self._client,
            key=key,
            timeout=timeout,
            retry_interval=retry_interval,
            retry_times=retry_times,
        )
    
    async def try_lock(
        self,
        key: str,
        timeout: int = 60,
    ) -> Optional[DistributedLock]:
        """
        尝试获取锁 (非阻塞)
        
        Returns:
            成功返回 DistributedLock 对象，失败返回 None
        """
        lock = self.dist_lock(key, timeout=timeout, retry_times=1)
        if await lock.acquire():
            return lock
        return None
    
    # ==================== 任务队列 ====================
    
    async def enqueue_task(self, task_data: dict, queue: Optional[str] = None) -> None:
        """
        将任务推入队列
        
        Args:
            task_data: 任务数据
            queue: 队列名称，默认使用配置的任务队列
        """
        self._ensure_initialized()
        queue = queue or self._config.task_queue
        await self._client.lpush(queue, json.dumps(task_data))
    
    async def dequeue_task(
        self,
        queue: Optional[str] = None,
        timeout: int = 0,
    ) -> Optional[str]:
        """
        从队列获取任务 (阻塞)
        
        Args:
            queue: 队列名称
            timeout: 超时时间 (秒)，0 表示无限等待
            
        Returns:
            任务 JSON 字符串，超时返回 None
        """
        self._ensure_initialized()
        queue = queue or self._config.task_queue
        
        result = await self._client.brpop(queue, timeout=timeout)
        if result:
            return result[1]  # (queue_name, data)
        return None
    
    async def get_queue_length(self, queue: Optional[str] = None) -> int:
        """获取队列长度"""
        self._ensure_initialized()
        queue = queue or self._config.task_queue
        return await self._client.llen(queue)
    
    # ==================== 发布订阅 ====================
    
    async def publish_result(self, task_id: str, result: Any) -> None:
        """
        发布任务结果
        
        Args:
            task_id: 任务 ID
            result: 结果对象 (Pydantic Model 或 dict)
        """
        self._ensure_initialized()
        channel = f"{self._config.result_channel_prefix}:{task_id}"
        
        if hasattr(result, "model_dump"):
            data = result.model_dump(mode="json")
        else:
            data = result
        
        await self._client.publish(channel, json.dumps(data, default=str))
    
    async def subscribe_result(
        self,
        task_id: str,
        callback: Callable[[dict], None],
    ) -> PubSub:
        """
        订阅任务结果
        
        Args:
            task_id: 任务 ID
            callback: 收到消息时的回调函数
            
        Returns:
            PubSub 对象，用于后续取消订阅
        """
        self._ensure_initialized()
        channel = f"{self._config.result_channel_prefix}:{task_id}"
        
        pubsub = self._client.pubsub()
        await pubsub.subscribe(channel)
        
        async def listener():
            async for message in pubsub.listen():
                if message["type"] == "message":
                    data = json.loads(message["data"])
                    callback(data)
        
        asyncio.create_task(listener())
        return pubsub
    
    # ==================== 节点注册与心跳 ====================
    
    async def register_node(
        self,
        node_id: str,
        node_info: dict,
        ttl: int = 15,
    ) -> None:
        """
        注册节点
        
        Args:
            node_id: 节点 ID
            node_info: 节点信息
            ttl: 过期时间 (秒)
        """
        self._ensure_initialized()
        key = f"{self._config.node_registry_prefix}:{node_id}"
        await self._client.setex(key, ttl, json.dumps(node_info, default=str))
    
    async def heartbeat(
        self,
        node_id: str,
        node_info: dict,
        ttl: int = 15,
    ) -> None:
        """
        心跳续期 (与 register_node 相同，语义更清晰)
        """
        await self.register_node(node_id, node_info, ttl)
    
    async def unregister_node(self, node_id: str) -> bool:
        """
        注销节点
        
        Returns:
            True 表示成功删除，False 表示 Key 不存在
        """
        self._ensure_initialized()
        key = f"{self._config.node_registry_prefix}:{node_id}"
        result = await self._client.delete(key)
        return result > 0
    
    async def get_all_nodes(self, node_type: Optional[str] = None) -> list[dict]:
        """
        获取所有活跃节点
        
        Args:
            node_type: 节点类型过滤，None 表示获取所有
            
        Returns:
            节点信息列表
        """
        self._ensure_initialized()
        pattern = f"{self._config.node_registry_prefix}:*"
        
        nodes = []
        async for key in self._client.scan_iter(match=pattern):
            data = await self._client.get(key)
            if data:
                node_info = json.loads(data)
                if node_type is None or node_info.get("node_type") == node_type:
                    nodes.append(node_info)
        
        return nodes
    
    async def get_node(self, node_id: str) -> Optional[dict]:
        """获取指定节点信息"""
        self._ensure_initialized()
        key = f"{self._config.node_registry_prefix}:{node_id}"
        data = await self._client.get(key)
        if data:
            return json.loads(data)
        return None
    
    # ==================== 缓存 ====================
    
    async def cache_get(self, key: str) -> Optional[str]:
        """获取缓存"""
        self._ensure_initialized()
        return await self._client.get(f"cache:{key}")
    
    async def cache_set(
        self,
        key: str,
        value: str,
        ttl: Optional[int] = None,
    ) -> None:
        """设置缓存"""
        self._ensure_initialized()
        cache_key = f"cache:{key}"
        if ttl:
            await self._client.setex(cache_key, ttl, value)
        else:
            await self._client.set(cache_key, value)
    
    async def cache_delete(self, key: str) -> None:
        """删除缓存"""
        self._ensure_initialized()
        await self._client.delete(f"cache:{key}")
    
    # ==================== 实时市场数据 ====================
    
    # Redis Key 前缀
    REALTIME_MARKET_KEY = "realtime:market"
    REALTIME_MARKET_TTL = 3600  # 1小时过期（交易时间会持续更新）
    
    async def set_realtime_market_data(self, data: dict) -> None:
        """
        存储实时市场数据（三大指数 + 涨跌统计）
        
        Args:
            data: {
                "sh_index": 3200.00,
                "sh_change": 1.23,
                "sz_index": 10500.00,
                "sz_change": -0.45,
                "cyb_index": 2100.00,
                "cyb_change": 0.88,
                "up_count": 2500,
                "down_count": 1800,
                "flat_count": 200,
                "limit_up": 45,
                "limit_down": 12,
                "update_time": "2026-02-06 10:30:00"
            }
        """
        self._ensure_initialized()
        await self._client.setex(
            self.REALTIME_MARKET_KEY,
            self.REALTIME_MARKET_TTL,
            json.dumps(data, default=str)
        )
        self.logger.debug(f"Set realtime market data: up={data.get('up_count')}, down={data.get('down_count')}")
    
    async def get_realtime_market_data(self) -> Optional[dict]:
        """
        获取实时市场数据
        
        Returns:
            市场数据字典，不存在返回 None
        """
        self._ensure_initialized()
        data = await self._client.get(self.REALTIME_MARKET_KEY)
        if data:
            return json.loads(data)
        return None
    
    async def delete_realtime_market_data(self) -> None:
        """删除实时市场数据（用于收盘后清理）"""
        self._ensure_initialized()
        await self._client.delete(self.REALTIME_MARKET_KEY)

    # ==================== 热点新闻 ====================
    
    HOT_NEWS_KEY_PREFIX = "hot_news"
    HOT_NEWS_TTL = 7200  # 2小时过期
    
    async def set_hot_news(self, source: str, news_list: list[dict]) -> None:
        """
        存储热点新闻
        
        Args:
            source: 来源标识 (如 baidu, weibo)
            news_list: 新闻列表
        """
        self._ensure_initialized()
        key = f"{self.HOT_NEWS_KEY_PREFIX}:{source}"
        data = {
            "source": source,
            "news": news_list,
            "updated_at": datetime.now().isoformat(),
        }
        await self._client.setex(key, self.HOT_NEWS_TTL, json.dumps(data, ensure_ascii=False))
        self.logger.debug(f"Set hot news: source={source}, count={len(news_list)}")
    
    async def get_hot_news(self, source: str) -> Optional[dict]:
        """
        获取指定来源的热点新闻
        
        Args:
            source: 来源标识
            
        Returns:
            {"source": str, "news": list, "updated_at": str}
        """
        self._ensure_initialized()
        key = f"{self.HOT_NEWS_KEY_PREFIX}:{source}"
        data = await self._client.get(key)
        if data:
            return json.loads(data)
        return None
    
    async def get_all_hot_news(self) -> dict[str, dict]:
        """
        获取所有来源的热点新闻
        
        Returns:
            {source: {"source": str, "news": list, "updated_at": str}}
        """
        self._ensure_initialized()
        pattern = f"{self.HOT_NEWS_KEY_PREFIX}:*"
        
        result = {}
        async for key in self._client.scan_iter(match=pattern):
            source = key.split(":")[-1]
            data = await self._client.get(key)
            if data:
                result[source] = json.loads(data)
        
        return result
    
    async def get_hot_news_stats(self) -> dict:
        """
        获取热点新闻统计
        
        Returns:
            {"stats": [{"source": str, "count": int, "updated_at": str}], "total": int}
        """
        self._ensure_initialized()
        all_news = await self.get_all_hot_news()
        
        stats = []
        total = 0
        
        for source, data in all_news.items():
            count = len(data.get("news", []))
            total += count
            stats.append({
                "source": source,
                "source_name": data.get("news", [{}])[0].get("source_name", source) if data.get("news") else source,
                "count": count,
                "updated_at": data.get("updated_at", ""),
            })
        
        return {"stats": stats, "total": total}
    
    async def delete_hot_news(self, source: Optional[str] = None) -> int:
        """
        删除热点新闻
        
        Args:
            source: 来源标识，None 表示删除所有
            
        Returns:
            删除的 key 数量
        """
        self._ensure_initialized()
        
        if source:
            key = f"{self.HOT_NEWS_KEY_PREFIX}:{source}"
            return await self._client.delete(key)
        else:
            pattern = f"{self.HOT_NEWS_KEY_PREFIX}:*"
            keys = [key async for key in self._client.scan_iter(match=pattern)]
            if keys:
                return await self._client.delete(*keys)
            return 0


# ==================== 全局单例 ====================
redis_manager = RedisManager()
