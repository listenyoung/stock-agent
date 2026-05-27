"""
Web API 路由
"""

from .auth import router as auth_router
from .user import router as user_router
from .task import router as task_router
from .stock import router as stock_router
from .market import router as market_router
from .subscription import router as subscription_router
from .backtest import router as backtest_router
from .agent import router as agent_router

__all__ = [
    "auth_router",
    "user_router",
    "task_router",
    "stock_router",
    "market_router",
    "subscription_router",
    "backtest_router",
    "agent_router",
]
