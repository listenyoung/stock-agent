"""
策略基类

所有策略必须继承此类并实现 evaluate 方法。
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

from core.protocols import (
    StrategySubscription,
    StrategyAlert,
    MarketSnapshot,
)


class BaseStrategy(ABC):
    """
    策略基类
    
    子类必须实现:
    - evaluate(): 评估策略条件是否满足
    
    Example:
        class MyStrategy(BaseStrategy):
            async def evaluate(
                self,
                subscription: StrategySubscription,
                snapshot: MarketSnapshot,
                previous_snapshot: Optional[MarketSnapshot],
            ) -> List[StrategyAlert]:
                # 实现策略逻辑
                alerts = []
                # ...
                return alerts
    """
    
    @property
    @abstractmethod
    def strategy_type(self) -> str:
        """策略类型标识"""
        raise NotImplementedError
    
    @abstractmethod
    async def evaluate(
        self,
        subscription: StrategySubscription,
        snapshot: MarketSnapshot,
        previous_snapshot: Optional[MarketSnapshot] = None,
    ) -> List[StrategyAlert]:
        """
        评估策略条件
        
        Args:
            subscription: 策略订阅配置
            snapshot: 当前市场快照
            previous_snapshot: 上一次市场快照 (可选，用于比较)
            
        Returns:
            触发的预警列表
        """
        raise NotImplementedError
    
    def _get_watch_stocks(
        self,
        subscription: StrategySubscription,
        snapshot: MarketSnapshot,
        exclude_st: bool = False,
    ) -> Dict[str, Dict[str, Any]]:
        """
        获取需要监听的股票数据
        
        Args:
            subscription: 订阅配置
            snapshot: 市场快照
            exclude_st: 是否排除 ST 股票
            
        Returns:
            符合 watch_list 的股票数据
        """
        if subscription.is_all_market():
            stocks = snapshot.quotes
        else:
            stocks = {
                ts_code: quote
                for ts_code, quote in snapshot.quotes.items()
                if ts_code in subscription.watch_list
            }
        
        # 过滤 ST 股票
        if exclude_st:
            stocks = {
                ts_code: quote
                for ts_code, quote in stocks.items()
                if not self._is_st_stock(quote)
            }
        
        return stocks
    
    def _is_st_stock(self, quote: Dict[str, Any]) -> bool:
        """
        判断是否为 ST 股票
        
        根据股票名称判断，包含 ST、*ST、S*ST 等
        """
        name = quote.get("name", "")
        if not name:
            return False
        
        # ST 股票名称特征
        st_patterns = ["ST", "*ST", "S*ST", "SST", "S"]
        name_upper = name.upper()
        
        for pattern in st_patterns:
            if name_upper.startswith(pattern):
                return True
        
        return False
    
    def _create_alert(
        self,
        subscription: StrategySubscription,
        ts_code: str,
        stock_name: str,
        price: float,
        reason: str,
        extra_data: Optional[Dict[str, Any]] = None,
    ) -> StrategyAlert:
        """
        创建预警对象
        
        Args:
            subscription: 订阅配置
            ts_code: 股票代码
            stock_name: 股票名称
            price: 当前价格
            reason: 触发原因
            extra_data: 额外数据
            
        Returns:
            预警对象
        """
        return StrategyAlert(
            subscription_id=subscription.subscription_id,
            strategy_id=subscription.strategy_id,
            strategy_name=subscription.strategy_name,
            ts_code=ts_code,
            stock_name=stock_name,
            trigger_price=price,
            trigger_reason=reason,
            extra_data=extra_data or {},
        )
