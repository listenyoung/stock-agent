"""
因子库定义

内置常用的选股因子，支持扩展自定义因子。

因子分类:
- 动量因子: 基于价格趋势
- 价值因子: 基于估值指标
- 质量因子: 基于盈利能力
- 成长因子: 基于增长率
- 波动因子: 基于风险指标
- 流动性因子: 基于交易活跃度
- 技术因子: 基于技术指标
"""

from enum import Enum
from typing import Callable, Dict, Optional, List
from dataclasses import dataclass
import pandas as pd
import numpy as np


class FactorCategory(str, Enum):
    """因子分类"""
    MOMENTUM = "momentum"      # 动量因子
    VALUE = "value"           # 价值因子
    QUALITY = "quality"       # 质量因子
    GROWTH = "growth"         # 成长因子
    VOLATILITY = "volatility" # 波动因子
    LIQUIDITY = "liquidity"   # 流动性因子
    TECHNICAL = "technical"   # 技术因子


@dataclass
class FactorDefinition:
    """因子定义"""
    name: str                          # 因子名称 (唯一标识)
    display_name: str                  # 显示名称
    category: FactorCategory           # 因子分类
    description: str                   # 描述
    direction: str                     # "asc" 越大越好, "desc" 越小越好
    data_source: str                   # 数据来源: "daily" | "daily_basic" | "fina"
    required_fields: List[str]         # 需要的数据字段
    compute_func: Callable             # 计算函数
    lookback_days: int = 60            # 需要的历史数据天数


class FactorLibrary:
    """
    因子库
    
    管理所有内置因子和自定义因子
    """
    
    _factors: Dict[str, FactorDefinition] = {}
    
    @classmethod
    def register(cls, factor_def: FactorDefinition):
        """注册因子"""
        cls._factors[factor_def.name] = factor_def
    
    @classmethod
    def get(cls, name: str) -> Optional[FactorDefinition]:
        """获取因子定义"""
        return cls._factors.get(name)
    
    @classmethod
    def list_factors(cls) -> List[Dict]:
        """列出所有因子"""
        return [
            {
                "name": f.name,
                "display_name": f.display_name,
                "category": f.category.value,
                "description": f.description,
                "direction": f.direction,
                "data_source": f.data_source,
            }
            for f in cls._factors.values()
        ]
    
    @classmethod
    def get_factors_by_category(cls, category: FactorCategory) -> List[FactorDefinition]:
        """按分类获取因子"""
        return [f for f in cls._factors.values() if f.category == category]


# ============== 辅助函数 ==============

def _safe_divide(a: pd.Series, b: pd.Series) -> pd.Series:
    """安全除法，避免除零"""
    return a / b.replace(0, np.nan)


def _compute_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """计算 RSI"""
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    rs = _safe_divide(gain, loss)
    return 100 - (100 / (1 + rs))


# ============== 注册内置因子 ==============

# -------------------- 动量因子 --------------------

FactorLibrary.register(FactorDefinition(
    name="momentum_5d",
    display_name="5日动量",
    category=FactorCategory.MOMENTUM,
    description="过去5个交易日的收益率",
    direction="asc",
    data_source="daily",
    required_fields=["close"],
    lookback_days=10,
    compute_func=lambda df: df["close"].pct_change(5),
))

FactorLibrary.register(FactorDefinition(
    name="momentum_20d",
    display_name="20日动量",
    category=FactorCategory.MOMENTUM,
    description="过去20个交易日的收益率",
    direction="asc",
    data_source="daily",
    required_fields=["close"],
    lookback_days=30,
    compute_func=lambda df: df["close"].pct_change(20),
))

FactorLibrary.register(FactorDefinition(
    name="momentum_60d",
    display_name="60日动量",
    category=FactorCategory.MOMENTUM,
    description="过去60个交易日的收益率",
    direction="asc",
    data_source="daily",
    required_fields=["close"],
    lookback_days=70,
    compute_func=lambda df: df["close"].pct_change(60),
))

# -------------------- 价值因子 --------------------

FactorLibrary.register(FactorDefinition(
    name="pe_ttm",
    display_name="市盈率TTM",
    category=FactorCategory.VALUE,
    description="滚动市盈率，越低越便宜",
    direction="desc",  # 越小越好
    data_source="daily_basic",
    required_fields=["pe_ttm"],
    lookback_days=1,
    compute_func=lambda df: df["pe_ttm"],
))

FactorLibrary.register(FactorDefinition(
    name="pb",
    display_name="市净率",
    category=FactorCategory.VALUE,
    description="市净率，越低越便宜",
    direction="desc",
    data_source="daily_basic",
    required_fields=["pb"],
    lookback_days=1,
    compute_func=lambda df: df["pb"],
))

FactorLibrary.register(FactorDefinition(
    name="ps_ttm",
    display_name="市销率TTM",
    category=FactorCategory.VALUE,
    description="滚动市销率，越低越便宜",
    direction="desc",
    data_source="daily_basic",
    required_fields=["ps_ttm"],
    lookback_days=1,
    compute_func=lambda df: df["ps_ttm"],
))

FactorLibrary.register(FactorDefinition(
    name="dv_ttm",
    display_name="股息率TTM",
    category=FactorCategory.VALUE,
    description="滚动股息率，越高越好",
    direction="asc",
    data_source="daily_basic",
    required_fields=["dv_ttm"],
    lookback_days=1,
    compute_func=lambda df: df["dv_ttm"],
))

# -------------------- 质量因子 --------------------

FactorLibrary.register(FactorDefinition(
    name="roe",
    display_name="ROE",
    category=FactorCategory.QUALITY,
    description="净资产收益率，越高越好",
    direction="asc",
    data_source="fina",
    required_fields=["roe"],
    lookback_days=1,
    compute_func=lambda df: df["roe"],
))

FactorLibrary.register(FactorDefinition(
    name="roa",
    display_name="ROA",
    category=FactorCategory.QUALITY,
    description="总资产收益率，越高越好",
    direction="asc",
    data_source="fina",
    required_fields=["roa"],
    lookback_days=1,
    compute_func=lambda df: df["roa"],
))

FactorLibrary.register(FactorDefinition(
    name="gross_margin",
    display_name="毛利率",
    category=FactorCategory.QUALITY,
    description="毛利率，越高越好",
    direction="asc",
    data_source="fina",
    required_fields=["grossprofit_margin"],
    lookback_days=1,
    compute_func=lambda df: df["grossprofit_margin"],
))

# -------------------- 成长因子 --------------------

FactorLibrary.register(FactorDefinition(
    name="revenue_growth",
    display_name="营收增长率",
    category=FactorCategory.GROWTH,
    description="营业收入同比增长率",
    direction="asc",
    data_source="fina",
    required_fields=["revenue_yoy"],
    lookback_days=1,
    compute_func=lambda df: df["revenue_yoy"],
))

FactorLibrary.register(FactorDefinition(
    name="profit_growth",
    display_name="利润增长率",
    category=FactorCategory.GROWTH,
    description="净利润同比增长率",
    direction="asc",
    data_source="fina",
    required_fields=["netprofit_yoy"],
    lookback_days=1,
    compute_func=lambda df: df["netprofit_yoy"],
))

# -------------------- 波动因子 --------------------

FactorLibrary.register(FactorDefinition(
    name="volatility_20d",
    display_name="20日波动率",
    category=FactorCategory.VOLATILITY,
    description="过去20日收益率标准差，低波动优先",
    direction="desc",  # 低波动优先
    data_source="daily",
    required_fields=["close"],
    lookback_days=30,
    compute_func=lambda df: df["close"].pct_change().rolling(20).std(),
))

FactorLibrary.register(FactorDefinition(
    name="volatility_60d",
    display_name="60日波动率",
    category=FactorCategory.VOLATILITY,
    description="过去60日收益率标准差，低波动优先",
    direction="desc",
    data_source="daily",
    required_fields=["close"],
    lookback_days=70,
    compute_func=lambda df: df["close"].pct_change().rolling(60).std(),
))

# -------------------- 流动性因子 --------------------

FactorLibrary.register(FactorDefinition(
    name="turnover_20d",
    display_name="20日换手率",
    category=FactorCategory.LIQUIDITY,
    description="过去20日平均换手率",
    direction="asc",  # 高换手（流动性好）
    data_source="daily_basic",
    required_fields=["turnover_rate"],
    lookback_days=30,
    compute_func=lambda df: df["turnover_rate"].rolling(20).mean(),
))

FactorLibrary.register(FactorDefinition(
    name="amount_20d",
    display_name="20日成交额",
    category=FactorCategory.LIQUIDITY,
    description="过去20日平均成交额（亿元）",
    direction="asc",
    data_source="daily",
    required_fields=["amount"],
    lookback_days=30,
    compute_func=lambda df: df["amount"].rolling(20).mean() / 100000,  # 千元 -> 亿元
))

FactorLibrary.register(FactorDefinition(
    name="total_mv",
    display_name="总市值",
    category=FactorCategory.LIQUIDITY,
    description="总市值（亿元），可用于大/小盘选择",
    direction="asc",  # 根据策略调整
    data_source="daily_basic",
    required_fields=["total_mv"],
    lookback_days=1,
    compute_func=lambda df: df["total_mv"],  # 已经是亿元
))

# -------------------- 技术因子 --------------------

FactorLibrary.register(FactorDefinition(
    name="ma_deviation_20",
    display_name="20日均线偏离",
    category=FactorCategory.TECHNICAL,
    description="股价与20日均线的偏离程度",
    direction="desc",  # 超跌反弹
    data_source="daily",
    required_fields=["close"],
    lookback_days=30,
    compute_func=lambda df: (df["close"] / df["close"].rolling(20).mean() - 1),
))

FactorLibrary.register(FactorDefinition(
    name="rsi_14",
    display_name="RSI(14)",
    category=FactorCategory.TECHNICAL,
    description="14日相对强弱指标",
    direction="asc",
    data_source="daily",
    required_fields=["close"],
    lookback_days=20,
    compute_func=lambda df: _compute_rsi(df["close"], 14),
))

FactorLibrary.register(FactorDefinition(
    name="price_position",
    display_name="价格位置",
    category=FactorCategory.TECHNICAL,
    description="当前价格在60日高低点中的位置 (0~1)",
    direction="desc",  # 低位买入
    data_source="daily",
    required_fields=["close", "high", "low"],
    lookback_days=70,
    compute_func=lambda df: (
        (df["close"] - df["low"].rolling(60).min()) / 
        (df["high"].rolling(60).max() - df["low"].rolling(60).min() + 1e-10)
    ),
))
