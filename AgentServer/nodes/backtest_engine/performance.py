"""
绩效评估与回撤系统

核心功能：
- 每日净值追踪
- 最大回撤计算
- 夏普比率、年化收益率、胜率等风险指标
- 可视化数据输出
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime

import pandas as pd
import numpy as np

from .backtester import BacktestResult, Trade, TradeDirection


@dataclass
class DrawdownInfo:
    """回撤信息"""
    max_drawdown: float  # 最大回撤比例 (0-1)
    max_drawdown_pct: float  # 最大回撤百分比
    peak_date: datetime  # 峰值日期
    trough_date: datetime  # 谷底日期
    recovery_date: Optional[datetime]  # 恢复日期
    drawdown_days: int  # 回撤天数
    recovery_days: Optional[int]  # 恢复天数


@dataclass
class PerformanceMetrics:
    """
    绩效指标
    
    包含策略的核心风险收益指标
    """
    
    # ==================== 收益指标 ====================
    
    # 总收益率
    total_return: float = 0.0  # 总收益率
    total_return_pct: float = 0.0  # 总收益率百分比
    
    # 年化收益率
    annual_return: float = 0.0  # 年化收益率
    annual_return_pct: float = 0.0  # 年化收益率百分比
    
    # 基准对比
    benchmark_return: float = 0.0  # 基准收益率
    alpha: float = 0.0  # 超额收益
    
    # ==================== 风险指标 ====================
    
    # 波动率
    volatility: float = 0.0  # 年化波动率
    daily_volatility: float = 0.0  # 日波动率
    
    # 最大回撤
    max_drawdown: float = 0.0  # 最大回撤
    max_drawdown_pct: float = 0.0  # 最大回撤百分比
    drawdown_info: Optional[DrawdownInfo] = None
    
    # ==================== 风险调整收益 ====================
    
    sharpe_ratio: float = 0.0  # 夏普比率 (假设无风险利率 3%)
    sortino_ratio: float = 0.0  # 索提诺比率
    calmar_ratio: float = 0.0  # 卡玛比率
    
    # ==================== 交易统计 ====================
    
    total_trades: int = 0  # 总交易次数
    winning_trades: int = 0  # 盈利交易次数
    losing_trades: int = 0  # 亏损交易次数
    win_rate: float = 0.0  # 胜率
    
    avg_profit: float = 0.0  # 平均盈利
    avg_loss: float = 0.0  # 平均亏损
    profit_factor: float = 0.0  # 盈亏比
    
    # 最大连续
    max_consecutive_wins: int = 0  # 最大连续盈利次数
    max_consecutive_losses: int = 0  # 最大连续亏损次数
    
    # ==================== 持仓统计 ====================
    
    total_days: int = 0  # 总交易日数
    days_in_market: int = 0  # 持仓天数
    market_exposure: float = 0.0  # 市场暴露比例
    
    avg_holding_days: float = 0.0  # 平均持仓天数
    
    # ==================== 费用统计 ====================
    
    total_commission: float = 0.0  # 总佣金
    total_stamp_duty: float = 0.0  # 总印花税
    total_costs: float = 0.0  # 总交易成本
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为可序列化的字典"""
        return {
            "returns": {
                "total_return_pct": round(self.total_return_pct, 2),
                "annual_return_pct": round(self.annual_return_pct, 2),
                "benchmark_return_pct": round(self.benchmark_return * 100, 2),
                "alpha_pct": round(self.alpha * 100, 2),
            },
            "risk": {
                "volatility_pct": round(self.volatility * 100, 2),
                "max_drawdown_pct": round(self.max_drawdown_pct, 2),
                "sharpe_ratio": round(self.sharpe_ratio, 2),
                "sortino_ratio": round(self.sortino_ratio, 2),
                "calmar_ratio": round(self.calmar_ratio, 2),
            },
            "trades": {
                "total_trades": self.total_trades,
                "win_rate_pct": round(self.win_rate * 100, 2),
                "profit_factor": round(self.profit_factor, 2),
                "avg_profit": round(self.avg_profit, 2),
                "avg_loss": round(self.avg_loss, 2),
                "max_consecutive_wins": self.max_consecutive_wins,
                "max_consecutive_losses": self.max_consecutive_losses,
            },
            "exposure": {
                "total_days": self.total_days,
                "days_in_market": self.days_in_market,
                "market_exposure_pct": round(self.market_exposure * 100, 2),
                "avg_holding_days": round(self.avg_holding_days, 1),
            },
            "costs": {
                "total_commission": round(self.total_commission, 2),
                "total_stamp_duty": round(self.total_stamp_duty, 2),
                "total_costs": round(self.total_costs, 2),
            },
        }


class PerformanceAnalyzer:
    """
    绩效分析器
    
    对回测结果进行全面的绩效评估。
    
    Example:
        result = backtester.run(factor_data)
        
        analyzer = PerformanceAnalyzer(risk_free_rate=0.03)
        metrics = analyzer.analyze(result)
        
        print(f"年化收益: {metrics.annual_return_pct}%")
        print(f"最大回撤: {metrics.max_drawdown_pct}%")
        print(f"夏普比率: {metrics.sharpe_ratio}")
    """
    
    # 交易日数 (A股约 244 天/年)
    TRADING_DAYS_PER_YEAR = 244
    
    def __init__(self, risk_free_rate: float = 0.03):
        """
        Args:
            risk_free_rate: 年化无风险利率 (默认 3%)
        """
        self.risk_free_rate = risk_free_rate
        self.daily_rf = risk_free_rate / self.TRADING_DAYS_PER_YEAR
    
    def analyze(self, result: BacktestResult) -> PerformanceMetrics:
        """
        分析回测结果
        
        Args:
            result: 回测结果
            
        Returns:
            绩效指标
        """
        if not result.success or result.daily_nav is None:
            return PerformanceMetrics()
        
        metrics = PerformanceMetrics()
        nav = result.daily_nav
        
        # ==================== 收益指标 ====================
        
        # 总收益
        metrics.total_return = nav.iloc[-1] - 1.0
        metrics.total_return_pct = metrics.total_return * 100
        
        # 年化收益
        n_days = len(nav)
        n_years = n_days / self.TRADING_DAYS_PER_YEAR
        if n_years > 0:
            metrics.annual_return = (nav.iloc[-1] ** (1 / n_years)) - 1.0
            metrics.annual_return_pct = metrics.annual_return * 100
        
        # 基准收益
        if result.benchmark_nav is not None:
            metrics.benchmark_return = result.benchmark_nav.iloc[-1] - 1.0
            metrics.alpha = metrics.total_return - metrics.benchmark_return
        
        # ==================== 风险指标 ====================
        
        # 日收益率
        daily_returns = nav.pct_change().dropna()
        
        # 波动率
        metrics.daily_volatility = daily_returns.std()
        metrics.volatility = metrics.daily_volatility * np.sqrt(self.TRADING_DAYS_PER_YEAR)
        
        # 最大回撤
        metrics.drawdown_info = self._calculate_max_drawdown(nav)
        if metrics.drawdown_info:
            metrics.max_drawdown = metrics.drawdown_info.max_drawdown
            metrics.max_drawdown_pct = metrics.drawdown_info.max_drawdown_pct
        
        # ==================== 风险调整收益 ====================
        
        # 夏普比率
        excess_returns = daily_returns - self.daily_rf
        if metrics.daily_volatility > 0:
            metrics.sharpe_ratio = (excess_returns.mean() / excess_returns.std()) * np.sqrt(self.TRADING_DAYS_PER_YEAR)
        
        # 索提诺比率 (只考虑下行波动)
        downside_returns = daily_returns[daily_returns < 0]
        if len(downside_returns) > 0:
            downside_std = downside_returns.std()
            if downside_std > 0:
                metrics.sortino_ratio = (daily_returns.mean() - self.daily_rf) / downside_std * np.sqrt(self.TRADING_DAYS_PER_YEAR)
        
        # 卡玛比率
        if metrics.max_drawdown > 0:
            metrics.calmar_ratio = metrics.annual_return / metrics.max_drawdown
        
        # ==================== 交易统计 ====================
        
        self._analyze_trades(result.trades, metrics)
        
        # ==================== 持仓统计 ====================
        
        metrics.total_days = n_days
        if result.position_series is not None:
            metrics.days_in_market = (result.position_series > 0).sum()
            metrics.market_exposure = metrics.days_in_market / n_days if n_days > 0 else 0
        
        # 平均持仓天数
        if metrics.total_trades > 0:
            metrics.avg_holding_days = metrics.days_in_market / (metrics.total_trades / 2)
        
        # ==================== 费用统计 ====================
        
        for trade in result.trades:
            metrics.total_commission += trade.commission
            metrics.total_stamp_duty += trade.stamp_duty
        
        metrics.total_costs = metrics.total_commission + metrics.total_stamp_duty
        
        return metrics
    
    def _calculate_max_drawdown(self, nav: pd.Series) -> Optional[DrawdownInfo]:
        """
        计算最大回撤
        
        Returns:
            回撤信息
        """
        if nav.empty:
            return None
        
        # 历史最高点
        running_max = nav.expanding().max()
        
        # 回撤序列
        drawdown = (nav - running_max) / running_max
        
        # 最大回撤
        max_dd_idx = drawdown.idxmin()
        max_dd = drawdown[max_dd_idx]
        
        # 峰值日期
        peak_idx = nav[:max_dd_idx].idxmax()
        
        # 恢复日期
        recovery_idx = None
        recovery_days = None
        
        after_trough = nav[max_dd_idx:]
        recovery_candidates = after_trough[after_trough >= nav[peak_idx]]
        if len(recovery_candidates) > 0:
            recovery_idx = recovery_candidates.index[0]
            recovery_days = (recovery_idx - max_dd_idx).days
        
        # 回撤天数
        drawdown_days = (max_dd_idx - peak_idx).days
        
        return DrawdownInfo(
            max_drawdown=abs(max_dd),
            max_drawdown_pct=abs(max_dd) * 100,
            peak_date=peak_idx,
            trough_date=max_dd_idx,
            recovery_date=recovery_idx,
            drawdown_days=drawdown_days,
            recovery_days=recovery_days,
        )
    
    def _analyze_trades(self, trades: List[Trade], metrics: PerformanceMetrics) -> None:
        """分析交易记录"""
        if not trades:
            return
        
        # 配对交易 (买入 -> 卖出)
        pairs = []
        buy_trade = None
        
        for trade in trades:
            if trade.direction == TradeDirection.BUY:
                buy_trade = trade
            elif trade.direction == TradeDirection.SELL and buy_trade is not None:
                # 计算盈亏
                profit = (trade.price - buy_trade.price) * trade.shares
                profit -= (buy_trade.commission + trade.commission + trade.stamp_duty)
                pairs.append({
                    "profit": profit,
                    "buy_date": buy_trade.date,
                    "sell_date": trade.date,
                })
                buy_trade = None
        
        if not pairs:
            metrics.total_trades = len(trades)
            return
        
        # 统计盈亏
        profits = [p["profit"] for p in pairs]
        winning = [p for p in profits if p > 0]
        losing = [p for p in profits if p < 0]
        
        metrics.total_trades = len(pairs) * 2  # 买卖各算一次
        metrics.winning_trades = len(winning)
        metrics.losing_trades = len(losing)
        
        if len(pairs) > 0:
            metrics.win_rate = len(winning) / len(pairs)
        
        if winning:
            metrics.avg_profit = sum(winning) / len(winning)
        if losing:
            metrics.avg_loss = abs(sum(losing) / len(losing))
        
        # 盈亏比
        if metrics.avg_loss > 0:
            metrics.profit_factor = metrics.avg_profit / metrics.avg_loss
        
        # 连续盈亏
        metrics.max_consecutive_wins = self._max_consecutive(profits, lambda x: x > 0)
        metrics.max_consecutive_losses = self._max_consecutive(profits, lambda x: x < 0)
    
    @staticmethod
    def _max_consecutive(values: List[float], condition) -> int:
        """计算最大连续次数"""
        max_count = 0
        current_count = 0
        
        for v in values:
            if condition(v):
                current_count += 1
                max_count = max(max_count, current_count)
            else:
                current_count = 0
        
        return max_count
    
    def generate_report(self, result: BacktestResult, metrics: PerformanceMetrics) -> Dict[str, Any]:
        """
        生成完整报告
        
        Args:
            result: 回测结果
            metrics: 绩效指标
            
        Returns:
            完整报告字典，可用于前端渲染
        """
        report = {
            "summary": {
                "ts_code": result.ts_code,
                "start_date": result.start_date,
                "end_date": result.end_date,
                "initial_cash": result.config.initial_cash,
                "final_equity": result.daily_equity.iloc[-1] if result.daily_equity is not None else 0,
                "execution_time_ms": result.execution_time_ms,
            },
            "metrics": metrics.to_dict(),
            "charts": {
                "nav_series": {
                    "dates": [d.strftime("%Y-%m-%d") for d in result.daily_nav.index] if result.daily_nav is not None else [],
                    "strategy": result.daily_nav.tolist() if result.daily_nav is not None else [],
                    "benchmark": result.benchmark_nav.tolist() if result.benchmark_nav is not None else [],
                },
                "drawdown_series": self._compute_drawdown_series(result.daily_nav) if result.daily_nav is not None else {},
            },
            "trades": [
                {
                    "date": t.date.strftime("%Y-%m-%d"),
                    "direction": t.direction.value,
                    "price": round(t.price, 2),
                    "shares": t.shares,
                    "amount": round(t.amount, 2),
                    "commission": round(t.commission, 2),
                    "stamp_duty": round(t.stamp_duty, 2),
                    "reason": t.reason,
                }
                for t in result.trades
            ],
        }
        
        return report
    
    def _compute_drawdown_series(self, nav: pd.Series) -> Dict[str, Any]:
        """计算回撤序列"""
        running_max = nav.expanding().max()
        drawdown = (nav - running_max) / running_max
        
        return {
            "dates": [d.strftime("%Y-%m-%d") for d in drawdown.index],
            "values": (drawdown * 100).round(2).tolist(),
        }
