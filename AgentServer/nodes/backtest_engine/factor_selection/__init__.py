"""
因子选股回测模块

提供完整的因子选股回测功能:
- UniverseManager: 股票池管理
- FactorLibrary: 内置因子库
- FactorEngine: 因子批量计算
- PortfolioBacktester: 组合回测引擎
"""

from .universe import UniverseManager, UniverseType, ExcludeRule
from .factor_library import FactorLibrary, FactorDefinition, FactorCategory
from .factor_engine import FactorEngine
from .portfolio_backtest import PortfolioBacktester

__all__ = [
    "UniverseManager",
    "UniverseType", 
    "ExcludeRule",
    "FactorLibrary",
    "FactorDefinition",
    "FactorCategory",
    "FactorEngine",
    "PortfolioBacktester",
]
