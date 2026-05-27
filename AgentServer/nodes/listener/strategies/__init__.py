"""
策略执行器

内置策略:
- LimitOpenStrategy: 涨跌停打开策略
- PriceChangeStrategy: 涨跌幅阈值策略
- MA5BuyStrategy: 5日线低吸策略
"""

from .base import BaseStrategy
from .limit_open import LimitOpenStrategy
from .price_change import PriceChangeStrategy
from .ma5_buy import MA5BuyStrategy

__all__ = [
    "BaseStrategy",
    "LimitOpenStrategy",
    "PriceChangeStrategy",
    "MA5BuyStrategy",
]
