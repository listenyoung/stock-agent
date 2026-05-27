"""
数据采集器
"""

from .stock_basic import StockBasicCollector
from .stock_daily import StockDailyCollector
from .daily_basic import DailyBasicCollector
from .index_basic import IndexBasicCollector
from .index_daily import IndexDailyCollector
from .moneyflow_industry import MoneyflowIndustryCollector
from .moneyflow_concept import MoneyflowConceptCollector
from .limit_list import LimitListCollector
from .daily_stats import DailyStatsCollector
from .news import NewsCollector
from .fina_indicator import FinaIndicatorCollector
from .hot_news import HotNewsCollector

__all__ = [
    "StockBasicCollector",
    "StockDailyCollector",
    "DailyBasicCollector",
    "IndexBasicCollector",
    "IndexDailyCollector",
    "MoneyflowIndustryCollector",
    "MoneyflowConceptCollector",
    "LimitListCollector",
    "DailyStatsCollector",
    "NewsCollector",
    "FinaIndicatorCollector",
    "HotNewsCollector",
]
