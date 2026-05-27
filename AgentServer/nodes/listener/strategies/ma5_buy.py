"""
5日线低吸策略

策略逻辑:
1. 股价从上方回落到5日线附近
2. 在5日线上方企稳（连续2个轮询周期站稳）

MA5计算方式:
- MA5 = (前4天收盘价 + 今日开盘价) / 5
- 盘中MA5固定，不随实时价格变化

企稳判断 (状态机):
- 状态0: 初始/正常状态
- 状态1: 首次发现回落到5日线附近 (从上方回落)
- 状态2: 连续N个周期在5日线上方 → 触发通知
"""

from typing import List, Dict, Any, Optional
from datetime import date, datetime
from enum import IntEnum
import logging

from .base import BaseStrategy
from core.protocols import (
    StrategySubscription,
    StrategyAlert,
    MarketSnapshot,
    StrategyType,
)
from core.managers import mongo_manager


class StockState(IntEnum):
    """股票状态"""
    NORMAL = 0      # 正常状态
    TOUCHED = 1     # 已触及5日线
    STABILIZED = 2  # 企稳确认


class StockTracker:
    """单只股票的状态追踪"""
    
    def __init__(self):
        self.state: StockState = StockState.NORMAL
        self.touch_time: Optional[datetime] = None  # 首次触及时间
        self.stable_count: int = 0  # 在5日线上方的连续次数
        self.alerted_today: bool = False  # 今日是否已触发
    
    def reset(self):
        """重置状态"""
        self.state = StockState.NORMAL
        self.touch_time = None
        self.stable_count = 0


class MA5BuyStrategy(BaseStrategy):
    """
    5日线低吸策略
    
    参数:
    - touch_range: 触及均线的范围 (默认 0.02，即 ±2%)
    - stable_periods: 企稳需要的连续周期数 (默认 2)
    - once_per_day: 每只股票每天只触发一次 (默认 True)
    """
    
    def __init__(self):
        self.logger = logging.getLogger("strategy.ma5_buy")
        
        # 股票数据缓存 {ts_code: {ma5, prev_close, open_price, ...}}
        self._stock_data: Dict[str, Dict[str, Any]] = {}
        
        # 股票状态追踪 {ts_code: StockTracker}
        self._trackers: Dict[str, StockTracker] = {}
        
        # 缓存日期
        self._cache_date: Optional[str] = None
    
    @property
    def strategy_type(self) -> str:
        return StrategyType.MA5_BUY.value
    
    async def evaluate(
        self,
        subscription: StrategySubscription,
        snapshot: MarketSnapshot,
        previous_snapshot: Optional[MarketSnapshot] = None,
    ) -> List[StrategyAlert]:
        """评估5日线低吸条件"""
        alerts = []
        
        # 获取参数
        touch_range = subscription.params.get("touch_range", 0.02)
        stable_periods = subscription.params.get("stable_periods", 2)
        once_per_day = subscription.params.get("once_per_day", True)
        
        # 确保缓存数据是今天的
        await self._ensure_cache_updated()
        
        # 全市场监听时自动过滤 ST 股票
        exclude_st = subscription.is_all_market()
        watch_stocks = self._get_watch_stocks(subscription, snapshot, exclude_st=exclude_st)
        
        for ts_code, quote in watch_stocks.items():
            try:
                alert = await self._evaluate_stock(
                    ts_code=ts_code,
                    quote=quote,
                    subscription=subscription,
                    touch_range=touch_range,
                    stable_periods=stable_periods,
                    once_per_day=once_per_day,
                )
                if alert:
                    alerts.append(alert)
                    
            except Exception as e:
                self.logger.error(f"Error evaluating {ts_code}: {e}")
        
        return alerts
    
    async def _evaluate_stock(
        self,
        ts_code: str,
        quote: Dict[str, Any],
        subscription: StrategySubscription,
        touch_range: float,
        stable_periods: int,
        once_per_day: bool,
    ) -> Optional[StrategyAlert]:
        """评估单只股票"""
        
        # 确保有数据缓存
        if ts_code not in self._stock_data:
            await self._load_stock_data(ts_code)
        
        data = self._stock_data.get(ts_code)
        if not data:
            return None
        
        ma5 = data.get("ma5", 0)
        prev_close = data.get("prev_close", 0)
        
        if not ma5 or not prev_close:
            return None
        
        # 获取或创建追踪器
        if ts_code not in self._trackers:
            self._trackers[ts_code] = StockTracker()
        tracker = self._trackers[ts_code]
        
        # 检查是否今日已触发
        if once_per_day and tracker.alerted_today:
            return None
        
        # 当前价格
        current_price = quote.get("price", 0)
        if not current_price:
            return None
        
        stock_name = quote.get("name", ts_code)
        
        # 计算与MA5的距离
        distance_pct = (current_price / ma5) - 1
        is_near_ma5 = abs(distance_pct) <= touch_range
        is_above_ma5 = current_price >= ma5
        was_above_ma5 = prev_close > ma5  # 昨天在MA5上方
        
        # 状态机逻辑
        if tracker.state == StockState.NORMAL:
            # 条件: 昨天在MA5上方，今天回落到MA5附近
            if was_above_ma5 and is_near_ma5:
                tracker.state = StockState.TOUCHED
                tracker.touch_time = datetime.now()
                tracker.stable_count = 1 if is_above_ma5 else 0
                self.logger.info(
                    f"[{ts_code}] 触及5日线: price={current_price:.2f}, "
                    f"MA5={ma5:.2f}, distance={distance_pct*100:.1f}%"
                )
        
        elif tracker.state == StockState.TOUCHED:
            if is_above_ma5:
                # 在5日线上方，计数+1
                tracker.stable_count += 1
                self.logger.debug(
                    f"[{ts_code}] 企稳计数: {tracker.stable_count}/{stable_periods}"
                )
                
                # 达到企稳条件
                if tracker.stable_count >= stable_periods:
                    tracker.state = StockState.STABILIZED
                    tracker.alerted_today = True
                    
                    self.logger.info(
                        f"[ALERT] {ts_code} 5日线企稳! "
                        f"price={current_price:.2f}, MA5={ma5:.2f}"
                    )
                    
                    # 创建预警
                    return self._create_alert(
                        subscription=subscription,
                        ts_code=ts_code,
                        stock_name=stock_name,
                        price=current_price,
                        reason=f"回落5日线后企稳，连续{stable_periods}个周期站稳",
                        extra_data={
                            "ma5": round(ma5, 2),
                            "prev_close": round(prev_close, 2),
                            "distance_to_ma5": round(distance_pct * 100, 2),
                            "stable_count": tracker.stable_count,
                            "touch_time": tracker.touch_time.strftime("%H:%M:%S") if tracker.touch_time else "",
                        },
                    )
            else:
                # 跌破5日线，重置状态
                if current_price < ma5 * (1 - touch_range):
                    self.logger.debug(f"[{ts_code}] 跌破5日线，重置状态")
                    tracker.reset()
        
        elif tracker.state == StockState.STABILIZED:
            # 已触发，等待下一个交易日
            pass
        
        return None
    
    async def _ensure_cache_updated(self) -> None:
        """确保缓存数据是今天的"""
        today = date.today().strftime("%Y%m%d")
        
        if self._cache_date != today:
            self._stock_data.clear()
            # 重置所有追踪器
            for tracker in self._trackers.values():
                tracker.reset()
                tracker.alerted_today = False
            self._cache_date = today
            self.logger.info(f"Cache reset for new trading day: {today}")
    
    async def _load_stock_data(self, ts_code: str) -> None:
        """
        加载股票历史数据并计算MA5
        
        MA5 = (前4天收盘价 + 今日开盘价) / 5
        """
        try:
            # 获取最近5个交易日的日线数据
            records = await mongo_manager.find_many(
                "stock_daily",
                {"ts_code": ts_code},
                sort=[("trade_date", -1)],
                limit=5,
            )
            
            if not records or len(records) < 4:
                self.logger.debug(f"Insufficient data for {ts_code}, got {len(records) if records else 0} records")
                return
            
            # 按日期排序（最新在前）
            records.sort(key=lambda x: x.get("trade_date", ""), reverse=True)
            
            # 获取今日开盘价（从最新一条记录，可能是昨天的数据）
            # 如果有今天的数据，用今天的开盘价；否则用昨收作为参考
            today_str = date.today().strftime("%Y%m%d")
            
            if records[0].get("trade_date") == today_str:
                # 有今天的数据
                today_open = records[0].get("open", 0)
                prev_4_closes = [r.get("close", 0) for r in records[1:5]]
                prev_close = records[1].get("close", 0) if len(records) > 1 else 0
            else:
                # 没有今天的数据，用昨天的收盘价作为今日开盘价估算
                today_open = records[0].get("close", 0)
                prev_4_closes = [r.get("close", 0) for r in records[0:4]]
                prev_close = records[0].get("close", 0)
            
            # 计算 MA5 = (前4天收盘价 + 今日开盘价 * 1.05) / 5
            # 使用开盘价的5%涨幅作为预估收盘价
            if len(prev_4_closes) >= 4 and today_open:
                estimated_close = today_open * 1.05
                ma5 = (sum(prev_4_closes[:4]) + estimated_close) / 5
            else:
                ma5 = 0
            
            self._stock_data[ts_code] = {
                "ma5": ma5,
                "prev_close": prev_close,
                "today_open": today_open,
                "prev_4_closes": prev_4_closes[:4],
            }
            
            self.logger.debug(
                f"Loaded {ts_code}: MA5={ma5:.2f}, prev_close={prev_close:.2f}, "
                f"today_open={today_open:.2f}"
            )
            
        except Exception as e:
            self.logger.error(f"Failed to load data for {ts_code}: {e}")
    
    async def preload_watch_list(self, ts_codes: List[str]) -> None:
        """
        预加载监听列表的历史数据
        
        可在每日开盘前调用，减少盘中数据库查询
        """
        self.logger.info(f"Preloading data for {len(ts_codes)} stocks...")
        
        await self._ensure_cache_updated()
        
        for ts_code in ts_codes:
            await self._load_stock_data(ts_code)
        
        self.logger.info(f"Preloaded {len(self._stock_data)} stocks")
    
    def get_tracker_status(self, ts_code: str) -> Dict[str, Any]:
        """获取股票追踪状态（调试用）"""
        tracker = self._trackers.get(ts_code)
        if not tracker:
            return {"state": "not_tracked"}
        
        return {
            "state": tracker.state.name,
            "touch_time": tracker.touch_time.isoformat() if tracker.touch_time else None,
            "stable_count": tracker.stable_count,
            "alerted_today": tracker.alerted_today,
        }
