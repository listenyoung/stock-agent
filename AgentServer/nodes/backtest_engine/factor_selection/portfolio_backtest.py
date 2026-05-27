"""
组合回测引擎

支持:
- 定期调仓
- 多种权重方法
- 交易成本
- 基准对比
- 绩效统计
"""

from typing import List, Dict, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime
import pandas as pd
import numpy as np
import logging

from core.managers import mongo_manager
from .universe import UniverseManager, UniverseType, ExcludeRule
from .factor_engine import FactorEngine


logger = logging.getLogger(__name__)


@dataclass
class RebalanceRecord:
    """调仓记录"""
    date: str
    action: str  # "buy" | "sell"
    ts_code: str
    shares: int
    price: float
    amount: float
    reason: str


@dataclass
class PortfolioSnapshot:
    """组合快照"""
    date: str
    cash: float
    holdings: Dict[str, int]  # {ts_code: shares}
    prices: Dict[str, float]  # {ts_code: price}
    market_value: float
    total_value: float


class PortfolioBacktester:
    """
    组合回测引擎
    
    支持:
    - 定期调仓 (日/周/月/季)
    - 多种权重方法 (等权/因子加权)
    - 交易成本 (佣金+印花税)
    - 基准对比
    """
    
    # 交易成本
    BUY_COMMISSION = 0.0002     # 买入佣金 万2
    SELL_COMMISSION = 0.0002   # 卖出佣金 万2
    STAMP_TAX = 0.001          # 印花税 千1 (卖出)
    MIN_COMMISSION = 5         # 最低佣金 5元
    
    def __init__(self):
        self.universe_mgr = UniverseManager()
        self.factor_engine = FactorEngine()
    
    async def run(self, config: Dict) -> Dict:
        """
        运行组合回测
        
        Args:
            config: {
                "universe": "all_a",
                "start_date": "20230101",
                "end_date": "20260101",
                "initial_cash": 1000000,
                "rebalance_freq": "monthly",
                "top_n": 20,
                "weight_method": "equal",
                "factors": [
                    {"name": "momentum_20d", "weight": 0.3},
                    {"name": "pb", "weight": 0.3},
                    {"name": "roe", "weight": 0.4},
                ],
                "exclude": ["st", "new_stock"],
                "benchmark": "000300.SH"
            }
        """
        logger.info(f"Starting portfolio backtest: {config['start_date']} -> {config['end_date']}")
        
        # 初始化
        initial_cash = config.get("initial_cash", 1000000)
        top_n = config.get("top_n", 20)
        weight_method = config.get("weight_method", "equal")
        benchmark_code = config.get("benchmark", "000300.SH")
        
        # 解析排除规则
        exclude_rules = [ExcludeRule(r) for r in config.get("exclude", [])]
        
        # 获取调仓日期
        rebalance_dates = await self.universe_mgr.get_rebalance_dates(
            config["start_date"],
            config["end_date"],
            config.get("rebalance_freq", "monthly"),
        )
        
        if not rebalance_dates:
            return {"error": "No rebalance dates found"}
        
        # 获取所有交易日
        all_trade_dates = await self.universe_mgr.get_all_trade_dates(
            config["start_date"], config["end_date"]
        )
        
        if not all_trade_dates:
            return {"error": "No trade dates found"}
        
        logger.info(f"Rebalance dates: {len(rebalance_dates)}, Trade dates: {len(all_trade_dates)}")
        
        # 加载基准数据
        benchmark_data = await self._load_benchmark(benchmark_code, config["start_date"], config["end_date"])
        
        # 初始化组合状态
        cash = initial_cash
        holdings: Dict[str, int] = {}  # {ts_code: shares}
        
        # 记录
        daily_values: List[Dict] = []
        rebalance_records: List[RebalanceRecord] = []
        selection_history: List[Dict] = []
        
        # 逐日模拟
        rebalance_set = set(rebalance_dates)
        total_days = len(all_trade_dates)
        
        for idx, trade_date in enumerate(all_trade_dates):
            # 每 20 天打印一次进度
            if idx % 20 == 0:
                logger.info(f"Processing day {idx+1}/{total_days}: {trade_date}")
            
            # 检查是否是调仓日
            if trade_date in rebalance_set:
                logger.info(f"Rebalancing on {trade_date} ({idx+1}/{total_days})")
                
                # 1. 获取当日股票池
                universe = await self.universe_mgr.get_universe(
                    UniverseType.ALL_A,
                    trade_date,
                    exclude_rules,
                )
                
                if not universe:
                    logger.warning(f"No stocks in universe for {trade_date}")
                    continue
                
                # 2. 计算因子 & 选股
                factor_df = await self.factor_engine.compute_factors(
                    universe, trade_date, config["factors"]
                )
                target_stocks = self.factor_engine.select_top_stocks(factor_df, top_n)
                
                if not target_stocks:
                    logger.warning(f"No stocks selected for {trade_date}")
                    continue
                
                # 记录选股结果
                selection_history.append({
                    "date": trade_date,
                    "stocks": target_stocks,
                    "universe_size": len(universe),
                })
                
                # 3. 计算目标权重
                target_weights = self._compute_weights(
                    target_stocks, factor_df, weight_method
                )
                
                # 4. 获取价格
                prices = await self._get_prices(
                    set(holdings.keys()) | set(target_weights.keys()),
                    trade_date,
                )
                
                # 5. 执行调仓
                cash, holdings, records = self._rebalance(
                    trade_date, cash, holdings, target_weights, prices
                )
                rebalance_records.extend(records)
            
            # 计算当日市值
            prices = await self._get_prices(set(holdings.keys()), trade_date)
            market_value = sum(
                holdings.get(ts_code, 0) * prices.get(ts_code, 0)
                for ts_code in holdings
            )
            total_value = cash + market_value
            
            # 基准净值
            benchmark_nav = benchmark_data.get(trade_date, 1.0)
            
            daily_values.append({
                "date": trade_date,
                "cash": cash,
                "market_value": market_value,
                "total_value": total_value,
                "benchmark_value": benchmark_nav * initial_cash,
                "return_pct": (total_value / initial_cash - 1) * 100,
            })
        
        # 计算绩效指标
        performance = self._compute_performance(daily_values, initial_cash)
        
        logger.info(f"Backtest completed: return={performance.get('total_return', 0):.2f}%")
        
        # 获取所有选中过的股票名称
        all_selected_stocks = set()
        for item in selection_history:
            all_selected_stocks.update(item["stocks"])
        stock_names = await self._get_stock_names(list(all_selected_stocks))
        
        # 为 selection_history 添加股票名称
        for item in selection_history:
            item["stock_details"] = [
                {"code": ts_code, "name": stock_names.get(ts_code, ts_code.replace(".SH", "").replace(".SZ", ""))}
                for ts_code in item["stocks"]
            ]
        
        # 为 rebalance_records 添加股票名称
        rebalance_records_with_names = [
            {
                "date": r.date,
                "action": r.action,
                "ts_code": r.ts_code,
                "stock_name": stock_names.get(r.ts_code, r.ts_code),
                "shares": r.shares,
                "price": r.price,
                "amount": r.amount,
                "reason": r.reason,
            }
            for r in rebalance_records
        ]
        
        return {
            "config": config,
            "performance": performance,
            "daily_values": daily_values,
            "rebalance_records": rebalance_records_with_names,
            "selection_history": selection_history,
            "final_holdings": holdings,
            "final_cash": cash,
        }
    
    def _compute_weights(
        self,
        stocks: List[str],
        factor_df: pd.DataFrame,
        method: str,
    ) -> Dict[str, float]:
        """计算目标权重"""
        n = len(stocks)
        if n == 0:
            return {}
        
        if method == "equal":
            # 等权重
            weight = 1.0 / n
            return {s: weight for s in stocks}
        
        elif method == "factor_weighted":
            # 因子加权 (得分越高权重越大)
            if factor_df.empty or "composite_score" not in factor_df.columns:
                return {s: 1.0/n for s in stocks}
            
            scores = factor_df[factor_df["ts_code"].isin(stocks)].set_index("ts_code")["composite_score"]
            total_score = scores.sum()
            
            if total_score > 0:
                return (scores / total_score).to_dict()
            return {s: 1.0/n for s in stocks}
        
        return {s: 1.0/n for s in stocks}
    
    async def _get_prices(
        self,
        stocks: Set[str],
        trade_date: str,
    ) -> Dict[str, float]:
        """获取股票价格"""
        if not stocks:
            return {}
        
        result = await mongo_manager.find_many(
            "stock_daily",
            {"ts_code": {"$in": list(stocks)}, "trade_date": trade_date},
            projection={"ts_code": 1, "close": 1},
        )
        
        return {doc["ts_code"]: doc["close"] for doc in result if doc.get("close")}
    
    def _rebalance(
        self,
        trade_date: str,
        cash: float,
        holdings: Dict[str, int],
        target_weights: Dict[str, float],
        prices: Dict[str, float],
    ) -> tuple:
        """
        执行调仓
        
        Returns:
            (new_cash, new_holdings, records)
        """
        records = []
        
        # 计算当前总资产
        current_value = cash + sum(
            holdings.get(ts_code, 0) * prices.get(ts_code, 0)
            for ts_code in holdings
        )
        
        # 1. 先卖出不在目标池的股票
        stocks_to_sell = set(holdings.keys()) - set(target_weights.keys())
        for ts_code in stocks_to_sell:
            shares = holdings[ts_code]
            price = prices.get(ts_code, 0)
            
            if price > 0 and shares > 0:
                amount = shares * price
                commission = max(amount * self.SELL_COMMISSION, self.MIN_COMMISSION)
                tax = amount * self.STAMP_TAX
                cash += amount - commission - tax
                
                records.append(RebalanceRecord(
                    date=trade_date, action="sell", ts_code=ts_code,
                    shares=shares, price=price, amount=amount,
                    reason="not_in_target",
                ))
        
        # 清理已卖出的持仓
        holdings = {k: v for k, v in holdings.items() if k in target_weights}
        
        # 2. 调整持仓到目标权重
        for ts_code, target_weight in target_weights.items():
            target_value = current_value * target_weight
            current_shares = holdings.get(ts_code, 0)
            price = prices.get(ts_code, 0)
            
            if price <= 0:
                continue
            
            current_value_in_stock = current_shares * price
            diff_value = target_value - current_value_in_stock
            
            if diff_value > 100:  # 需要买入 (至少买 100 元)
                # A股 100 股整数倍
                buy_shares = int(diff_value / price / 100) * 100
                if buy_shares > 0:
                    buy_amount = buy_shares * price
                    commission = max(buy_amount * self.BUY_COMMISSION, self.MIN_COMMISSION)
                    
                    if cash >= buy_amount + commission:
                        cash -= buy_amount + commission
                        holdings[ts_code] = current_shares + buy_shares
                        
                        records.append(RebalanceRecord(
                            date=trade_date, action="buy", ts_code=ts_code,
                            shares=buy_shares, price=price, amount=buy_amount,
                            reason="rebalance",
                        ))
            
            elif diff_value < -100:  # 需要卖出
                sell_shares = min(current_shares, int(-diff_value / price / 100) * 100)
                if sell_shares > 0:
                    sell_amount = sell_shares * price
                    commission = max(sell_amount * self.SELL_COMMISSION, self.MIN_COMMISSION)
                    tax = sell_amount * self.STAMP_TAX
                    cash += sell_amount - commission - tax
                    holdings[ts_code] = current_shares - sell_shares
                    
                    records.append(RebalanceRecord(
                        date=trade_date, action="sell", ts_code=ts_code,
                        shares=sell_shares, price=price, amount=sell_amount,
                        reason="rebalance",
                    ))
        
        # 清理持仓为 0 的股票
        holdings = {k: v for k, v in holdings.items() if v > 0}
        
        return cash, holdings, records
    
    async def _load_benchmark(
        self,
        benchmark_code: str,
        start_date: str,
        end_date: str,
    ) -> Dict[str, float]:
        """加载基准数据，返回归一化净值"""
        result = await mongo_manager.find_many(
            "index_daily",
            {
                "ts_code": benchmark_code,
                "trade_date": {"$gte": start_date, "$lte": end_date},
            },
            projection={"trade_date": 1, "close": 1},
        )
        
        if not result:
            return {}
        
        # 按日期排序
        result = sorted(result, key=lambda x: x["trade_date"])
        
        # 归一化
        base_price = result[0]["close"]
        return {
            doc["trade_date"]: doc["close"] / base_price
            for doc in result
        }
    
    async def _get_stock_names(self, ts_codes: List[str]) -> Dict[str, str]:
        """获取股票名称映射"""
        if not ts_codes:
            return {}
        
        result = await mongo_manager.find_many(
            "stock_basic",
            {"ts_code": {"$in": ts_codes}},
            projection={"ts_code": 1, "name": 1},
        )
        return {doc["ts_code"]: doc.get("name", doc["ts_code"]) for doc in result}
    
    def _compute_performance(
        self,
        daily_values: List[Dict],
        initial_cash: float,
    ) -> Dict:
        """计算绩效指标"""
        if not daily_values:
            return {}
        
        values = pd.Series([d["total_value"] for d in daily_values])
        benchmark_values = pd.Series([d["benchmark_value"] for d in daily_values])
        
        # 收益率
        total_return = (values.iloc[-1] / initial_cash - 1) * 100
        benchmark_return = (benchmark_values.iloc[-1] / initial_cash - 1) * 100
        excess_return = total_return - benchmark_return
        
        # 年化收益
        days = len(daily_values)
        annual_return = ((1 + total_return/100) ** (252/days) - 1) * 100 if days > 0 else 0
        
        # 最大回撤
        peak = values.expanding().max()
        drawdown = (values - peak) / peak
        max_drawdown = abs(drawdown.min()) * 100
        
        # 最大回撤天数
        max_dd_idx = drawdown.idxmin()
        peak_idx = values[:max_dd_idx+1].idxmax()
        max_dd_days = max_dd_idx - peak_idx
        
        # 波动率
        daily_returns = values.pct_change().dropna()
        volatility = daily_returns.std() * np.sqrt(252) * 100
        
        # 夏普比率 (假设无风险利率 3%)
        risk_free_rate = 0.03
        sharpe = (annual_return/100 - risk_free_rate) / (volatility/100) if volatility > 0 else 0
        
        # 胜率 (正收益天数比例)
        win_rate = (daily_returns > 0).sum() / len(daily_returns) * 100 if len(daily_returns) > 0 else 0
        
        return {
            "total_return": round(total_return, 2),
            "benchmark_return": round(benchmark_return, 2),
            "excess_return": round(excess_return, 2),
            "annual_return": round(annual_return, 2),
            "max_drawdown": round(max_drawdown, 2),
            "max_drawdown_days": int(max_dd_days),
            "volatility": round(volatility, 2),
            "sharpe_ratio": round(sharpe, 2),
            "win_rate": round(win_rate, 2),
            "trade_days": days,
            "start_date": daily_values[0]["date"],
            "end_date": daily_values[-1]["date"],
            "final_value": round(values.iloc[-1], 2),
        }
