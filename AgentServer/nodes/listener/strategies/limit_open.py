"""
涨跌停打开策略

检测涨停/跌停股票开板的情况。
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


class LimitOpenStrategy(BaseStrategy):
    """
    涨跌停打开策略
    
    策略逻辑:
    1. 每日开盘时获取涨跌停价格列表 (stk_limit)
    2. 每次轮询时检查:
       - 上次快照: 股票价格 == 涨停价/跌停价 (封板中)
       - 当前快照: 股票价格 != 涨停价/跌停价 (开板了)
    3. 触发预警
    
    参数:
    - limit_type: "up" (涨停打开), "down" (跌停打开), "both" (都监控)
    """
    
    def __init__(self):
        self.logger = logging.getLogger("strategy.limit_open")
    
    @property
    def strategy_type(self) -> str:
        return StrategyType.LIMIT_OPEN.value
    
    async def evaluate(
        self,
        subscription: StrategySubscription,
        snapshot: MarketSnapshot,
        previous_snapshot: Optional[MarketSnapshot] = None,
    ) -> List[StrategyAlert]:
        """
        评估涨跌停打开条件
        
        需要:
        1. snapshot.limit_stocks 包含涨跌停价格信息
        2. previous_snapshot 用于比较上一次的状态
        """
        self.logger.info(
            f"[LIMIT_OPEN] evaluate start: subscription={subscription.strategy_name}, "
            f"has_prev={previous_snapshot is not None}, "
            f"limit_stocks_count={len(snapshot.limit_stocks) if snapshot.limit_stocks else 0}, "
            f"snapshot_quotes_count={len(snapshot.quotes)}, "
            f"watch_list={subscription.watch_list[:5]}{'...' if len(subscription.watch_list) > 5 else ''}, "
            f"is_all_market={subscription.is_all_market()}"
        )
        
        if not previous_snapshot:
            self.logger.warning("[LIMIT_OPEN] No previous snapshot, skip limit open check")
            return []
        
        if not snapshot.limit_stocks:
            self.logger.warning("[LIMIT_OPEN] No limit stocks data in snapshot")
            return []
        
        alerts = []
        limit_type = subscription.params.get("limit_type", "both")
        watch_stocks = self._get_watch_stocks(subscription, snapshot)
        
        self.logger.info(
            f"[LIMIT_OPEN] watch_stocks_count={len(watch_stocks)}, limit_type={limit_type}, "
            f"prev_snapshot_quotes={len(previous_snapshot.quotes)}"
        )
        
        # 统计封板中的股票
        at_limit_up_count = 0
        at_limit_down_count = 0
        checked_count = 0
        no_limit_info_count = 0
        no_price_count = 0
        
        for ts_code, quote in watch_stocks.items():
            # 获取涨跌停价格
            limit_info = snapshot.limit_stocks.get(ts_code)
            if not limit_info:
                no_limit_info_count += 1
                continue
            
            up_limit = limit_info.get("up_limit", 0)
            down_limit = limit_info.get("down_limit", 0)
            current_price = quote.get("price", 0)
            
            # 获取上一次的价格
            prev_quote = previous_snapshot.quotes.get(ts_code, {})
            prev_price = prev_quote.get("price", 0)
            
            if not current_price or not prev_price:
                no_price_count += 1
                continue
            
            checked_count += 1
            stock_name = quote.get("name", ts_code)
            
            # 检测当前是否在涨跌停价（直接比较，无需容差）
            is_at_up_limit = up_limit > 0 and current_price >= up_limit
            is_at_down_limit = down_limit > 0 and current_price <= down_limit
            was_at_up_limit = up_limit > 0 and prev_price >= up_limit
            was_at_down_limit = down_limit > 0 and prev_price <= down_limit
            
            if is_at_up_limit:
                at_limit_up_count += 1
            if is_at_down_limit:
                at_limit_down_count += 1
            
            # 检查涨停打开：前一帧在涨停价，当前帧低于涨停价
            if limit_type in ("up", "both"):
                if was_at_up_limit and current_price < up_limit:
                    self.logger.info(
                        f"[LIMIT_OPEN] ★ 涨停打开检测到: {ts_code} {stock_name}, "
                        f"prev={prev_price:.2f}, curr={current_price:.2f}, up_limit={up_limit:.2f}"
                    )
                    alert = self._create_alert(
                        subscription=subscription,
                        ts_code=ts_code,
                        stock_name=stock_name,
                        price=current_price,
                        reason=f"涨停打开，前价格 {prev_price:.2f}，当前 {current_price:.2f}",
                        extra_data={
                            "limit_type": "up",
                            "up_limit": up_limit,
                            "prev_price": prev_price,
                            "pct_chg": quote.get("pct_chg", 0),
                        },
                    )
                    alerts.append(alert)
                elif is_at_up_limit:
                    # 记录当前封板中的股票（用于调试）
                    self.logger.debug(
                        f"[LIMIT_OPEN] 涨停封板中: {ts_code} {stock_name}, "
                        f"curr={current_price:.2f}, up_limit={up_limit:.2f}"
                    )
            
            # 检查跌停打开：前一帧在跌停价，当前帧高于跌停价
            if limit_type in ("down", "both"):
                if was_at_down_limit and current_price > down_limit:
                    self.logger.info(
                        f"[LIMIT_OPEN] ★ 跌停打开检测到: {ts_code} {stock_name}, "
                        f"prev={prev_price:.2f}, curr={current_price:.2f}, down_limit={down_limit:.2f}"
                    )
                    alert = self._create_alert(
                        subscription=subscription,
                        ts_code=ts_code,
                        stock_name=stock_name,
                        price=current_price,
                        reason=f"跌停打开，前价格 {prev_price:.2f}，当前 {current_price:.2f}",
                        extra_data={
                            "limit_type": "down",
                            "down_limit": down_limit,
                            "prev_price": prev_price,
                            "pct_chg": quote.get("pct_chg", 0),
                        },
                    )
                    alerts.append(alert)
        
        self.logger.info(
            f"[LIMIT_OPEN] evaluate done: checked={checked_count}, "
            f"at_limit_up={at_limit_up_count}, at_limit_down={at_limit_down_count}, "
            f"alerts={len(alerts)}, "
            f"no_limit_info={no_limit_info_count}, no_price={no_price_count}"
        )
        
        return alerts
    
    def _is_limit_opened(
        self,
        prev_price: float,
        current_price: float,
        limit_price: float,
        limit_type: str,
    ) -> bool:
        """
        判断是否开板
        
        Args:
            prev_price: 上一次价格
            current_price: 当前价格
            limit_price: 涨跌停价格
            limit_type: "up" 或 "down"
            
        Returns:
            True 表示开板
        """
        if limit_price <= 0:
            return False
        
        if limit_type == "up":
            # 涨停打开：前一帧 >= 涨停价，当前帧 < 涨停价
            was_at_limit = prev_price >= limit_price
            is_opened = current_price < limit_price
        else:
            # 跌停打开：前一帧 <= 跌停价，当前帧 > 跌停价
            was_at_limit = prev_price <= limit_price
            is_opened = current_price > limit_price
        
        return was_at_limit and is_opened
