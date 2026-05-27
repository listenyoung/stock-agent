"""
MongoDB 全局模型定义
"""

from common.models.user import User, UserInDB
from common.models.stock import Stock, StockDaily, StockFinancial
from common.models.strategy import Strategy, StrategyResult, AnalysisTask

__all__ = [
    "User",
    "UserInDB",
    "Stock",
    "StockDaily",
    "StockFinancial",
    "Strategy",
    "StrategyResult",
    "AnalysisTask",
]
