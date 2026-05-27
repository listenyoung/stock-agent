"""
资源管理器 (单例模式)

所有外部服务必须通过全局 Manager 单例访问。
禁止在业务代码中直接 import 外部库。

Example:
    from core.managers import tushare_manager
    
    # 确保已初始化 (在 main.py lifespan 中完成)
    data = await tushare_manager.get_daily("000001.SZ")
    
使用规范:
    1. 导入: from core.managers import redis_manager, mongo_manager, ...
    2. 初始化: 在 main.py 的 lifespan 中按依赖顺序调用 await xxx_manager.initialize()
    3. 调用: 直接使用全局单例的方法
"""

from .base import BaseManager
from .redis_manager import RedisManager, redis_manager
from .mongo_manager import MongoManager, mongo_manager
from .tushare_manager import TushareManager, tushare_manager
from .llm_manager import LLMManager, llm_manager
from .milvus_manager import MilvusManager, milvus_manager
from .analysis_manager import AnalysisManager, analysis_manager, MarketCycle
from .theme_manager import ThemeManager, theme_manager, ThemeStatus
from .prompt_manager import PromptManager, prompt_manager
from .notification_manager import NotificationManager, notification_manager
from .akshare_manager import AKShareManager, akshare_manager

__all__ = [
    # 基类
    "BaseManager",
    # Manager 类 (用于类型标注)
    "RedisManager",
    "MongoManager",
    "TushareManager",
    "LLMManager",
    "MilvusManager",
    "AnalysisManager",
    "MarketCycle",
    "ThemeManager",
    "ThemeStatus",
    "PromptManager",
    "NotificationManager",
    "AKShareManager",
    # 全局单例 (推荐使用)
    "redis_manager",
    "mongo_manager",
    "tushare_manager",
    "llm_manager",
    "milvus_manager",
    "analysis_manager",
    "theme_manager",
    "prompt_manager",
    "notification_manager",
    "akshare_manager",
]


async def initialize_all_managers() -> None:
    """
    按依赖顺序初始化所有管理器
    
    初始化顺序:
    1. Redis (基础设施)
    2. MongoDB (基础设施)
    3. Tushare (数据源)
    4. LLM (AI 服务)
    5. Milvus (向量数据库)
    6. Prompt (提示词管理)
    7. Notification (通知服务)
    """
    await redis_manager.initialize()
    await mongo_manager.initialize()
    await tushare_manager.initialize()
    await llm_manager.initialize()
    await milvus_manager.initialize()
    await prompt_manager.initialize()
    await notification_manager.initialize()


async def shutdown_all_managers() -> None:
    """关闭所有管理器"""
    await notification_manager.shutdown()
    await prompt_manager.shutdown()
    await milvus_manager.shutdown()
    await llm_manager.shutdown()
    await tushare_manager.shutdown()
    await mongo_manager.shutdown()
    await redis_manager.shutdown()


async def health_check_all() -> dict[str, bool]:
    """检查所有管理器健康状态"""
    return {
        "redis": await redis_manager.health_check(),
        "mongo": await mongo_manager.health_check(),
        "tushare": await tushare_manager.health_check(),
        "llm": await llm_manager.health_check(),
        "milvus": await milvus_manager.health_check(),
        "notification": await notification_manager.health_check(),
    }
