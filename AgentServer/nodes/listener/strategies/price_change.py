"""
涨跌幅阈值策略

当股票涨跌幅超过设定阈值时触发预警。
"""

from typing import List, Dict, Any, Optional
import logging

from .base import BaseStrategy
from core.protocols import (
    StrategySubscription,
    StrategyAlert,
    MarketSnapshot,
    StrategyType,
)


class PriceChangeStrategy(BaseStrategy):
    """
    涨跌幅阈值策略
    
    策略逻辑:
    - 当股票涨跌幅超过设定阈值时触发预警
    
    参数:
    - threshold: 涨跌幅阈值 (百分比，如 3.0 表示 3%)
    - direction: "up" (仅涨), "down" (仅跌), "both" (涨跌都监控)
    - once_per_day: 每只股票每天只触发一次 (默认 True)
    """
    
    def __init__(self):
        self.logger = logging.getLogger("strategy.price_change")
        # 记录已触发的股票 (key: ts_code, value: 触发时间)
        self._triggered_today: Dict[str, set] = {}
    
    @property
    def strategy_type(self) -> str:
        return StrategyType.PRICE_CHANGE.value
    
    def reset_daily_triggers(self) -> None:
        """
        重置每日触发记录
        
        应在每个交易日开盘前调用。
        """
        self._triggered_today.clear()
        self.logger.info("Daily triggers reset")
    
    async def evaluate(
        self,
        subscription: StrategySubscription,
        snapshot: MarketSnapshot,
        previous_snapshot: Optional[MarketSnapshot] = None,
    ) -> List[StrategyAlert]:
        """
        评估涨跌幅条件
        """
        alerts = []
        
        # 获取参数
        threshold = subscription.params.get("threshold", 3.0)
        direction = subscription.params.get("direction", "both")
        once_per_day = subscription.params.get("once_per_day", True)
        
        watch_stocks = self._get_watch_stocks(subscription, snapshot)
        
        # 获取该策略的触发记录
        strategy_key = subscription.strategy_id
        if strategy_key not in self._triggered_today:
            self._triggered_today[strategy_key] = set()
        triggered_set = self._triggered_today[strategy_key]
        
        for ts_code, quote in watch_stocks.items():
            # 检查是否今日已触发
            if once_per_day and ts_code in triggered_set:
                continue
            
            pct_chg = quote.get("pct_chg", 0)
            if pct_chg is None:
                continue
            
            current_price = quote.get("price", 0)
            stock_name = quote.get("name", ts_code)
            
            triggered = False
            reason = ""
            
            # 检查上涨
            if direction in ("up", "both") and pct_chg >= threshold:
                triggered = True
                reason = f"涨幅 {pct_chg:.2f}% 超过阈值 {threshold}%"
            
            # 检查下跌
            elif direction in ("down", "both") and pct_chg <= -threshold:
                triggered = True
                reason = f"跌幅 {pct_chg:.2f}% 超过阈值 -{threshold}%"
            
            if triggered:
                alert = self._create_alert(
                    subscription=subscription,
                    ts_code=ts_code,
                    stock_name=stock_name,
                    price=current_price,
                    reason=reason,
                    extra_data={
                        "pct_chg": pct_chg,
                        "threshold": threshold,
                        "direction": direction,
                        "vol": quote.get("vol", 0),
                        "amount": quote.get("amount", 0),
                    },
                )
                alerts.append(alert)
                triggered_set.add(ts_code)
                self.logger.info(f"[ALERT] {ts_code} {stock_name}: {reason}")
        
        return alerts
