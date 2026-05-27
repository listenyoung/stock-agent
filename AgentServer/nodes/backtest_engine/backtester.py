"""
向量化回测引擎

核心特性：
- 全流程向量化计算，避免 Slow Loop
- A股交易规则适配 (T+1, 涨跌停限制)
- 费用精确计算 (佣金 + 印花税)
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from enum import Enum

import pandas as pd
import numpy as np

from .factors import FactorData


class TradeDirection(Enum):
    """交易方向"""
    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"


@dataclass
class BacktestConfig:
    """
    回测配置
    
    Attributes:
        initial_cash: 初始资金
        entry_threshold: 买入阈值 (综合得分 > 此值时买入)
        exit_threshold: 卖出阈值 (综合得分 < 此值时卖出)
        position_size: 仓位比例 (0-1)
        commission_rate: 佣金费率 (买卖双向)
        stamp_duty_rate: 印花税率 (仅卖出)
        slippage: 滑点 (百分比)
        enable_t1: 是否启用 T+1 限制
        enable_limit_check: 是否启用涨跌停检查
    """
    
    # 资金配置
    initial_cash: float = 100000.0
    
    # 信号阈值
    entry_threshold: float = 0.7  # 得分 > 0.7 买入
    exit_threshold: float = 0.3   # 得分 < 0.3 卖出
    
    # 仓位管理
    position_size: float = 1.0    # 满仓买入
    max_position_pct: float = 1.0  # 最大仓位比例
    
    # 费用配置 (A股标准)
    commission_rate: float = 0.0002   # 万2 佣金
    stamp_duty_rate: float = 0.001    # 千1 印花税 (仅卖出)
    min_commission: float = 5.0       # 最低佣金 5 元
    
    # 滑点
    slippage: float = 0.001  # 0.1% 滑点
    
    # A股规则
    enable_t1: bool = True           # T+1 限制
    enable_limit_check: bool = True  # 涨跌停检查
    
    # 因子权重
    factor_weights: Dict[str, float] = field(default_factory=dict)


@dataclass
class Trade:
    """交易记录"""
    date: datetime
    direction: TradeDirection
    price: float
    shares: int
    amount: float
    commission: float
    stamp_duty: float
    reason: str = ""


@dataclass
class BacktestResult:
    """
    回测结果
    
    包含：
    - 每日净值序列
    - 交易记录列表
    - 持仓变化
    - 绩效指标
    """
    
    # 基础信息
    ts_code: str
    start_date: str
    end_date: str
    config: BacktestConfig
    
    # 净值曲线
    daily_nav: pd.Series = None  # 每日净值 (归一化到 1.0)
    daily_equity: pd.Series = None  # 每日总资产
    daily_cash: pd.Series = None  # 每日现金
    daily_position_value: pd.Series = None  # 每日持仓市值
    
    # 交易记录
    trades: List[Trade] = field(default_factory=list)
    
    # 信号数据
    signal_series: pd.Series = None  # 原始信号
    position_series: pd.Series = None  # 持仓序列 (股数)
    
    # 基准对比
    benchmark_nav: pd.Series = None  # 基准净值 (买入持有)
    
    # 执行状态
    success: bool = True
    error_message: str = ""
    execution_time_ms: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为可序列化的字典"""
        return {
            "ts_code": self.ts_code,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "success": self.success,
            "error_message": self.error_message,
            "execution_time_ms": self.execution_time_ms,
            "config": {
                "initial_cash": self.config.initial_cash,
                "entry_threshold": self.config.entry_threshold,
                "exit_threshold": self.config.exit_threshold,
                "position_size": self.config.position_size,
            },
            "nav_series": self.daily_nav.tolist() if self.daily_nav is not None else [],
            "nav_dates": [d.strftime("%Y-%m-%d") for d in self.daily_nav.index] if self.daily_nav is not None else [],
            "benchmark_nav": self.benchmark_nav.tolist() if self.benchmark_nav is not None else [],
            "trades_count": len(self.trades),
            "trades": [
                {
                    "date": t.date.strftime("%Y-%m-%d"),
                    "direction": t.direction.value,
                    "price": round(t.price, 2),
                    "shares": t.shares,
                    "amount": round(t.amount, 2),
                    "commission": round(t.commission, 2),
                    "reason": t.reason,
                }
                for t in self.trades[:50]  # 限制返回数量
            ],
        }


class VectorizedBacktester:
    """
    向量化回测引擎
    
    使用 Pandas 向量化计算实现高效回测，
    支持 A股交易规则 (T+1, 涨跌停限制)。
    
    Example:
        # 准备因子数据
        factor_data = FactorData(ts_code="000001.SZ", ...)
        factor_data.price_data = price_df
        factor_data.add_technical_indicators()
        
        # 配置回测
        config = BacktestConfig(
            initial_cash=100000,
            entry_threshold=0.7,
            exit_threshold=0.3,
            factor_weights={"tech_rsi": 0.5, "tech_macd_signal": 0.5},
        )
        
        # 执行回测
        backtester = VectorizedBacktester(config)
        result = backtester.run(factor_data)
    """
    
    def __init__(self, config: BacktestConfig):
        self.config = config
    
    def run(self, factor_data: FactorData) -> BacktestResult:
        """
        执行回测
        
        Args:
            factor_data: 因子数据
            
        Returns:
            回测结果
        """
        import time
        start_time = time.time()
        
        result = BacktestResult(
            ts_code=factor_data.ts_code,
            start_date=factor_data.start_date,
            end_date=factor_data.end_date,
            config=self.config,
        )
        
        try:
            # 1. 验证数据
            errors = factor_data.validate()
            if errors:
                result.success = False
                result.error_message = "; ".join(errors)
                return result
            
            # 2. 计算综合因子得分
            score_series = factor_data.compute_composite_score(
                self.config.factor_weights,
                normalize=True,
            )
            result.signal_series = score_series
            
            # 3. 生成交易信号
            signals = self._generate_signals(
                score_series,
                factor_data.price_data,
            )
            
            # 4. 执行模拟交易
            self._simulate_trading(
                factor_data.price_data,
                signals,
                result,
            )
            
            # 5. 计算基准收益 (买入持有)
            result.benchmark_nav = self._compute_benchmark(factor_data.price_data)
            
        except Exception as e:
            result.success = False
            result.error_message = str(e)
        
        result.execution_time_ms = (time.time() - start_time) * 1000
        return result
    
    def _generate_signals(
        self,
        score_series: pd.Series,
        price_data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        生成交易信号
        
        信号规则：
        - score > entry_threshold: 买入信号 (1)
        - score < exit_threshold: 卖出信号 (-1)
        - 其他: 持有 (0)
        
        涨跌停规则：
        - 涨停当日不可买入
        - 跌停当日不可卖出
        """
        signals = pd.DataFrame(index=score_series.index)
        
        # 基础信号
        signals["raw_signal"] = 0
        signals.loc[score_series > self.config.entry_threshold, "raw_signal"] = 1
        signals.loc[score_series < self.config.exit_threshold, "raw_signal"] = -1
        
        # 涨跌停检查 (A股规则)
        if self.config.enable_limit_check and "up_limit" in price_data.columns:
            # 一字涨停板：开盘=最低=涨停价，全天封死在涨停，无法买入
            # 只有一字板才限制买入，普通涨停（盘中打开过）可以排队买入
            is_yizi_limit_up = (
                (price_data["open"] >= price_data["up_limit"] * 0.998) &
                (price_data["low"] >= price_data["up_limit"] * 0.998)
            )
            
            # 跌停：收盘价 <= 跌停价，跌停时可以买入但不能卖出
            is_limit_down = price_data["close"] <= price_data["down_limit"] * 1.002
            
            # 一字涨停不能买入，跌停可以买入
            signals["can_buy"] = ~is_yizi_limit_up
            # 跌停不能卖出
            signals["can_sell"] = ~is_limit_down
        else:
            signals["can_buy"] = True
            signals["can_sell"] = True
        
        # 应用涨跌停限制
        signals["signal"] = signals["raw_signal"]
        signals.loc[(signals["raw_signal"] == 1) & ~signals["can_buy"], "signal"] = 0
        signals.loc[(signals["raw_signal"] == -1) & ~signals["can_sell"], "signal"] = 0
        
        return signals
    
    def _simulate_trading(
        self,
        price_data: pd.DataFrame,
        signals: pd.DataFrame,
        result: BacktestResult,
    ) -> None:
        """
        模拟交易执行
        
        使用向量化计算，但需要处理 T+1 规则，
        因此采用半向量化方式。
        """
        dates = price_data.index
        n_days = len(dates)
        
        # 初始化状态数组
        cash = np.zeros(n_days)
        shares = np.zeros(n_days, dtype=np.int64)
        position_value = np.zeros(n_days)
        equity = np.zeros(n_days)
        
        cash[0] = self.config.initial_cash
        
        # 交易记录
        trades = []
        
        # T+1 追踪：买入当日不可卖出
        last_buy_idx = -2  # -2 表示从未买入
        
        # 遍历每个交易日
        for i in range(n_days):
            date = dates[i]
            close_price = price_data.loc[date, "close"]
            open_price = price_data.loc[date, "open"]
            signal = signals.loc[date, "signal"]
            
            # 继承昨日状态
            if i > 0:
                cash[i] = cash[i - 1]
                shares[i] = shares[i - 1]
            
            # 执行价格 (考虑滑点)
            buy_price = open_price * (1 + self.config.slippage)
            sell_price = open_price * (1 - self.config.slippage)
            
            # 买入逻辑
            if signal == 1 and shares[i] == 0 and cash[i] > 0:
                # 计算可买股数 (100 股整数倍)
                available_cash = cash[i] * self.config.position_size
                max_shares = int(available_cash / buy_price / 100) * 100
                
                if max_shares >= 100:
                    # 计算费用
                    amount = max_shares * buy_price
                    commission = max(amount * self.config.commission_rate, self.config.min_commission)
                    
                    if amount + commission <= cash[i]:
                        shares[i] = max_shares
                        cash[i] -= (amount + commission)
                        last_buy_idx = i
                        
                        trades.append(Trade(
                            date=date,
                            direction=TradeDirection.BUY,
                            price=buy_price,
                            shares=max_shares,
                            amount=amount,
                            commission=commission,
                            stamp_duty=0,
                            reason=f"Score > {self.config.entry_threshold}",
                        ))
            
            # 卖出逻辑
            elif signal == -1 and shares[i] > 0:
                # T+1 检查
                if self.config.enable_t1 and i <= last_buy_idx:
                    pass  # 买入当日不可卖出
                else:
                    sell_shares = shares[i]
                    amount = sell_shares * sell_price
                    commission = max(amount * self.config.commission_rate, self.config.min_commission)
                    stamp_duty = amount * self.config.stamp_duty_rate
                    
                    shares[i] = 0
                    cash[i] += (amount - commission - stamp_duty)
                    
                    trades.append(Trade(
                        date=date,
                        direction=TradeDirection.SELL,
                        price=sell_price,
                        shares=sell_shares,
                        amount=amount,
                        commission=commission,
                        stamp_duty=stamp_duty,
                        reason=f"Score < {self.config.exit_threshold}",
                    ))
            
            # 计算当日资产
            position_value[i] = shares[i] * close_price
            equity[i] = cash[i] + position_value[i]
        
        # 保存结果
        result.daily_cash = pd.Series(cash, index=dates)
        result.daily_position_value = pd.Series(position_value, index=dates)
        result.daily_equity = pd.Series(equity, index=dates)
        result.daily_nav = result.daily_equity / self.config.initial_cash
        result.position_series = pd.Series(shares, index=dates)
        result.trades = trades
    
    def _compute_benchmark(self, price_data: pd.DataFrame) -> pd.Series:
        """
        计算基准收益 (首日买入持有)
        """
        close = price_data["close"]
        return close / close.iloc[0]
    
    def run_with_score_series(
        self,
        price_data: pd.DataFrame,
        score_series: pd.Series,
        ts_code: str = "000000.XX",
    ) -> BacktestResult:
        """
        直接使用评分序列运行回测
        
        Args:
            price_data: 行情数据
            score_series: 0-1 评分序列
            ts_code: 股票代码
            
        Returns:
            回测结果
        """
        # 构造因子数据
        factor_data = FactorData(
            ts_code=ts_code,
            start_date=price_data.index[0].strftime("%Y%m%d"),
            end_date=price_data.index[-1].strftime("%Y%m%d"),
            price_data=price_data,
        )
        
        # 临时存储评分
        factor_data.custom_factors["direct_score"] = pd.DataFrame(
            {"score": score_series},
            index=score_series.index,
        )
        
        # 设置权重
        self.config.factor_weights = {"custom_direct_score_score": 1.0}
        
        return self.run(factor_data)
