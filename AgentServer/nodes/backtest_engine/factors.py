"""
因子数据系统

支持多类型因子的标准化输入：
- 技术因子 (technical_factors): K线技术指标
- 情绪因子 (sentiment_factors): 舆情/情绪评分
- 基本面因子 (fundamental_factors): 财报/估值指标
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import date

import pandas as pd
import numpy as np


@dataclass
class FactorData:
    """
    标准化因子数据容器
    
    所有因子数据必须以 DataFrame 形式提供，
    索引为日期 (DatetimeIndex)，列名为因子名称。
    
    Example:
        factor_data = FactorData(
            ts_code="000001.SZ",
            start_date="2024-01-01",
            end_date="2024-12-31",
        )
        
        # 添加技术因子
        factor_data.technical_factors = pd.DataFrame({
            "ma5": [...],
            "ma20": [...],
            "rsi": [...],
        }, index=dates)
        
        # 添加情绪因子
        factor_data.sentiment_factors = pd.DataFrame({
            "news_score": [...],
            "social_sentiment": [...],
        }, index=dates)
        
        # 计算综合得分
        scores = factor_data.compute_composite_score(weights)
    """
    
    # 基础信息
    ts_code: str  # 股票代码
    start_date: str  # 开始日期 YYYYMMDD
    end_date: str  # 结束日期 YYYYMMDD
    
    # 行情数据 (必需)
    price_data: Optional[pd.DataFrame] = None
    # 必须包含: open, high, low, close, volume, amount
    # 可选: up_limit, down_limit (涨跌停价)
    
    # 因子数据 (可选)
    technical_factors: Optional[pd.DataFrame] = None
    sentiment_factors: Optional[pd.DataFrame] = None
    fundamental_factors: Optional[pd.DataFrame] = None
    
    # 自定义因子 (扩展用)
    custom_factors: Dict[str, pd.DataFrame] = field(default_factory=dict)
    
    # 元数据
    stock_name: str = ""
    industry: str = ""
    
    def validate(self) -> List[str]:
        """
        验证数据完整性
        
        Returns:
            错误信息列表，为空表示验证通过
        """
        errors = []
        
        # 检查必需的行情数据
        if self.price_data is None:
            errors.append("price_data is required")
        else:
            required_cols = ["open", "high", "low", "close", "volume"]
            missing = [c for c in required_cols if c not in self.price_data.columns]
            if missing:
                errors.append(f"price_data missing columns: {missing}")
            
            # 检查索引类型
            if not isinstance(self.price_data.index, pd.DatetimeIndex):
                errors.append("price_data index must be DatetimeIndex")
        
        # 检查因子数据索引对齐
        if self.price_data is not None:
            price_dates = set(self.price_data.index)
            
            for name, df in [
                ("technical_factors", self.technical_factors),
                ("sentiment_factors", self.sentiment_factors),
                ("fundamental_factors", self.fundamental_factors),
            ]:
                if df is not None and not df.empty:
                    factor_dates = set(df.index)
                    if not factor_dates.issubset(price_dates):
                        extra = factor_dates - price_dates
                        errors.append(f"{name} has dates not in price_data: {len(extra)} extra dates")
        
        return errors
    
    def get_all_factors(self) -> pd.DataFrame:
        """
        合并所有因子为一个 DataFrame
        
        Returns:
            合并后的因子 DataFrame，索引为日期
        """
        frames = []
        
        if self.technical_factors is not None and not self.technical_factors.empty:
            # 添加前缀避免列名冲突
            tech = self.technical_factors.add_prefix("tech_")
            frames.append(tech)
        
        if self.sentiment_factors is not None and not self.sentiment_factors.empty:
            sent = self.sentiment_factors.add_prefix("sent_")
            frames.append(sent)
        
        if self.fundamental_factors is not None and not self.fundamental_factors.empty:
            fund = self.fundamental_factors.add_prefix("fund_")
            frames.append(fund)
        
        for name, df in self.custom_factors.items():
            if df is not None and not df.empty:
                custom = df.add_prefix(f"custom_{name}_")
                frames.append(custom)
        
        if not frames:
            return pd.DataFrame(index=self.price_data.index if self.price_data is not None else None)
        
        # 按日期合并
        result = pd.concat(frames, axis=1)
        
        # 对齐到行情数据的日期
        if self.price_data is not None:
            result = result.reindex(self.price_data.index)
        
        return result
    
    def compute_composite_score(
        self,
        weights: Dict[str, float],
        normalize: bool = True,
    ) -> pd.Series:
        """
        计算综合因子得分
        
        Args:
            weights: 因子权重配置，key 为因子名（含前缀），value 为权重
            normalize: 是否归一化到 [0, 1] 区间
            
        Returns:
            综合得分序列，索引为日期
            
        Example:
            weights = {
                "tech_rsi": 0.3,
                "tech_macd_signal": 0.2,
                "sent_news_score": 0.3,
                "fund_pe_ratio": 0.2,
            }
            scores = factor_data.compute_composite_score(weights)
        """
        all_factors = self.get_all_factors()
        
        if all_factors.empty:
            # 无因子数据，返回全 0.5
            return pd.Series(0.5, index=self.price_data.index if self.price_data is not None else None)
        
        # 过滤存在的因子
        valid_weights = {k: v for k, v in weights.items() if k in all_factors.columns}
        
        if not valid_weights:
            return pd.Series(0.5, index=all_factors.index)
        
        # 计算加权得分
        score = pd.Series(0.0, index=all_factors.index)
        total_weight = sum(valid_weights.values())
        
        # 如果权重总和为0，返回中性得分
        if total_weight <= 0:
            return pd.Series(0.5, index=all_factors.index)
        
        for factor_name, weight in valid_weights.items():
            if weight <= 0:
                continue  # 跳过权重为0的因子
                
            factor_values = all_factors[factor_name].fillna(0)
            
            # 归一化单个因子到 [0, 1]
            if normalize and factor_values.std() > 0:
                factor_min = factor_values.min()
                factor_max = factor_values.max()
                if factor_max > factor_min:
                    factor_values = (factor_values - factor_min) / (factor_max - factor_min)
            
            score += factor_values * (weight / total_weight)
        
        # 最终归一化
        if normalize and score.std() > 0:
            score_min = score.min()
            score_max = score.max()
            if score_max > score_min:
                score = (score - score_min) / (score_max - score_min)
        
        return score
    
    def add_technical_indicators(self) -> None:
        """
        自动计算常用技术指标
        
        计算并添加到 technical_factors:
        - MA5, MA10, MA20, MA60
        - RSI (14日)
        - MACD (12, 26, 9)
        - 布林带
        """
        if self.price_data is None:
            return
        
        close = self.price_data["close"]
        high = self.price_data["high"]
        low = self.price_data["low"]
        volume = self.price_data["volume"]
        
        indicators = pd.DataFrame(index=self.price_data.index)
        
        # 均线
        indicators["ma5"] = close.rolling(5).mean()
        indicators["ma10"] = close.rolling(10).mean()
        indicators["ma20"] = close.rolling(20).mean()
        indicators["ma60"] = close.rolling(60).mean()
        
        # RSI
        delta = close.diff()
        gain = delta.where(delta > 0, 0).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss.replace(0, np.inf)
        indicators["rsi"] = 100 - (100 / (1 + rs))
        
        # MACD
        exp1 = close.ewm(span=12, adjust=False).mean()
        exp2 = close.ewm(span=26, adjust=False).mean()
        indicators["macd"] = exp1 - exp2
        indicators["macd_signal"] = indicators["macd"].ewm(span=9, adjust=False).mean()
        indicators["macd_hist"] = indicators["macd"] - indicators["macd_signal"]
        
        # 布林带
        indicators["boll_mid"] = close.rolling(20).mean()
        indicators["boll_std"] = close.rolling(20).std()
        indicators["boll_upper"] = indicators["boll_mid"] + 2 * indicators["boll_std"]
        indicators["boll_lower"] = indicators["boll_mid"] - 2 * indicators["boll_std"]
        
        # 成交量均线
        indicators["vol_ma5"] = volume.rolling(5).mean()
        indicators["vol_ma20"] = volume.rolling(20).mean()
        
        # 价格位置 (0-1)
        indicators["price_position"] = (close - low.rolling(20).min()) / (
            high.rolling(20).max() - low.rolling(20).min() + 1e-8
        )
        
        self.technical_factors = indicators


class CustomFactorInterface(ABC):
    """
    自定义因子接口
    
    用于扩展自定义因子计算逻辑，包括但不限于：
    - LLM 生成的动态因子结论
    - 外部数据源因子
    - 复杂衍生因子
    
    Example:
        class LLMSentimentFactor(CustomFactorInterface):
            name = "llm_sentiment"
            
            async def compute(self, factor_data: FactorData) -> pd.DataFrame:
                # 调用 LLM 分析新闻情绪
                scores = await self._analyze_news(factor_data.ts_code)
                return pd.DataFrame({"llm_score": scores}, index=dates)
    """
    
    # 因子名称
    name: str = "custom_factor"
    
    # 因子描述
    description: str = ""
    
    @abstractmethod
    async def compute(self, factor_data: FactorData) -> pd.DataFrame:
        """
        计算因子值
        
        Args:
            factor_data: 因子数据容器
            
        Returns:
            因子 DataFrame，索引为日期
        """
        raise NotImplementedError
    
    def validate_output(self, df: pd.DataFrame) -> bool:
        """验证输出格式"""
        if df is None or df.empty:
            return False
        if not isinstance(df.index, pd.DatetimeIndex):
            return False
        return True
