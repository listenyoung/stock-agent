"""
因子计算引擎

批量计算全市场股票的因子值，支持:
- 多数据源整合 (daily, daily_basic, fina)
- 因子标准化 (Z-Score / 排名)
- 综合打分
"""

from typing import List, Dict, Set, Optional, Any
import pandas as pd
import numpy as np
import logging

from core.managers import mongo_manager
from .factor_library import FactorLibrary, FactorDefinition


logger = logging.getLogger(__name__)


class FactorEngine:
    """
    因子计算引擎
    
    职责:
    1. 批量加载股票数据
    2. 计算因子值
    3. 因子标准化
    4. 综合打分
    """
    
    async def compute_factors(
        self,
        stocks: Set[str],
        trade_date: str,
        factor_configs: List[Dict],
        lookback_days: int = 120,
    ) -> pd.DataFrame:
        """
        计算所有股票的因子值
        
        Args:
            stocks: 股票代码集合
            trade_date: 计算日期
            factor_configs: [{"name": "momentum_20d", "weight": 0.3, "direction": "asc"}, ...]
            lookback_days: 回溯天数 (用于计算滚动指标)
            
        Returns:
            DataFrame, columns = ["ts_code", "factor1", "factor2", ..., "composite_score"]
        """
        if not stocks:
            return pd.DataFrame()
        
        stocks_list = list(stocks)
        logger.info(f"Computing factors for {len(stocks_list)} stocks on {trade_date}")
        
        # 1. 收集所需数据
        factor_defs = [FactorLibrary.get(cfg["name"]) for cfg in factor_configs]
        factor_defs = [f for f in factor_defs if f is not None]
        
        if not factor_defs:
            logger.warning("No valid factors found")
            return pd.DataFrame({"ts_code": stocks_list})
        
        logger.debug(f"Loading data for {len(factor_defs)} factors...")
        
        # 2. 加载数据
        data = await self._load_all_data(stocks_list, trade_date, factor_defs, lookback_days)
        logger.debug(f"Data loaded, computing factor values...")
        
        # 3. 计算每个因子
        factor_values = {}
        for factor_def in factor_defs:
            values = self._compute_single_factor(data, factor_def, trade_date)
            factor_values[factor_def.name] = values
        
        # 4. 组装 DataFrame
        result = pd.DataFrame({"ts_code": stocks_list})
        for factor_name, values in factor_values.items():
            result[factor_name] = result["ts_code"].map(values)
        
        # 5. 标准化 & 综合打分
        result = self._normalize_factors(result, factor_configs)
        result = self._compute_composite_score(result, factor_configs)
        
        return result
    
    async def _load_all_data(
        self,
        stocks: List[str],
        end_date: str,
        factor_defs: List[FactorDefinition],
        lookback_days: int,
    ) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        加载所有需要的数据
        
        Returns:
            {
                "daily": {ts_code: DataFrame},
                "daily_basic": {ts_code: DataFrame},
                "fina": {ts_code: DataFrame},
            }
        """
        # 确定需要的数据源
        data_sources = set(f.data_source for f in factor_defs)
        
        # 计算开始日期 (简单估算)
        from datetime import datetime, timedelta
        end_dt = datetime.strptime(end_date, "%Y%m%d")
        start_dt = end_dt - timedelta(days=lookback_days * 2)  # 预留空间
        start_date = start_dt.strftime("%Y%m%d")
        
        data = {}
        
        # 加载日线数据
        if "daily" in data_sources:
            data["daily"] = await self._load_daily_data(stocks, start_date, end_date)
        
        # 加载 daily_basic 数据
        if "daily_basic" in data_sources:
            data["daily_basic"] = await self._load_daily_basic_data(stocks, start_date, end_date)
        
        # 加载财务数据
        if "fina" in data_sources:
            data["fina"] = await self._load_fina_data(stocks)
        
        return data
    
    async def _load_daily_data(
        self,
        stocks: List[str],
        start_date: str,
        end_date: str,
    ) -> Dict[str, pd.DataFrame]:
        """加载日线数据"""
        result = await mongo_manager.find_many(
            "stock_daily",
            {
                "ts_code": {"$in": stocks},
                "trade_date": {"$gte": start_date, "$lte": end_date},
            },
            projection={
                "ts_code": 1, "trade_date": 1, 
                "open": 1, "high": 1, "low": 1, "close": 1, 
                "vol": 1, "amount": 1,
            },
        )
        
        # 按股票分组
        stock_data = {}
        for doc in result:
            ts_code = doc["ts_code"]
            if ts_code not in stock_data:
                stock_data[ts_code] = []
            stock_data[ts_code].append(doc)
        
        # 转换为 DataFrame
        return {
            ts_code: pd.DataFrame(docs).sort_values("trade_date").set_index("trade_date")
            for ts_code, docs in stock_data.items()
        }
    
    async def _load_daily_basic_data(
        self,
        stocks: List[str],
        start_date: str,
        end_date: str,
    ) -> Dict[str, pd.DataFrame]:
        """加载 daily_basic 数据"""
        result = await mongo_manager.find_many(
            "daily_basic",
            {
                "ts_code": {"$in": stocks},
                "trade_date": {"$gte": start_date, "$lte": end_date},
            },
            projection={
                "ts_code": 1, "trade_date": 1,
                "pe": 1, "pe_ttm": 1, "pb": 1, "ps": 1, "ps_ttm": 1,
                "dv_ratio": 1, "dv_ttm": 1,
                "turnover_rate": 1, "turnover_rate_f": 1, "volume_ratio": 1,
                "total_mv": 1, "circ_mv": 1,
            },
        )
        
        # 按股票分组
        stock_data = {}
        for doc in result:
            ts_code = doc["ts_code"]
            if ts_code not in stock_data:
                stock_data[ts_code] = []
            stock_data[ts_code].append(doc)
        
        return {
            ts_code: pd.DataFrame(docs).sort_values("trade_date").set_index("trade_date")
            for ts_code, docs in stock_data.items()
        }
    
    async def _load_fina_data(
        self,
        stocks: List[str],
    ) -> Dict[str, pd.DataFrame]:
        """加载最新财务数据"""
        # 从 fina_indicator 获取最新的财务数据
        result = await mongo_manager.find_many(
            "fina_indicator",
            {"ts_code": {"$in": stocks}},
            projection={
                "ts_code": 1, "end_date": 1,
                "roe": 1, "roa": 1, 
                "grossprofit_margin": 1,
                "revenue_yoy": 1, "netprofit_yoy": 1,
            },
        )
        
        # 每只股票取最新的一条
        stock_data = {}
        for doc in result:
            ts_code = doc["ts_code"]
            end_date = doc.get("end_date", "")
            
            if ts_code not in stock_data:
                stock_data[ts_code] = doc
            elif end_date > stock_data[ts_code].get("end_date", ""):
                stock_data[ts_code] = doc
        
        return {
            ts_code: pd.DataFrame([doc])
            for ts_code, doc in stock_data.items()
        }
    
    def _compute_single_factor(
        self,
        data: Dict[str, Dict[str, pd.DataFrame]],
        factor_def: FactorDefinition,
        trade_date: str,
    ) -> Dict[str, float]:
        """计算单个因子的值"""
        result = {}
        source_data = data.get(factor_def.data_source, {})
        
        for ts_code, df in source_data.items():
            try:
                if df.empty:
                    continue
                
                # 计算因子值
                factor_series = factor_def.compute_func(df)
                
                # 获取指定日期的值
                if factor_def.data_source == "fina":
                    # 财务数据取最新值
                    value = factor_series.iloc[-1] if len(factor_series) > 0 else np.nan
                else:
                    # 日线数据取指定日期
                    if trade_date in factor_series.index:
                        value = factor_series.loc[trade_date]
                    elif len(factor_series) > 0:
                        value = factor_series.iloc[-1]
                    else:
                        value = np.nan
                
                # 处理 NaN
                if pd.isna(value):
                    continue
                    
                result[ts_code] = float(value)
                
            except Exception as e:
                logger.debug(f"Failed to compute {factor_def.name} for {ts_code}: {e}")
                continue
        
        return result
    
    def _normalize_factors(
        self,
        df: pd.DataFrame,
        factor_configs: List[Dict],
    ) -> pd.DataFrame:
        """
        因子标准化 (Z-Score)
        
        去极值 + 标准化
        """
        for config in factor_configs:
            factor_name = config["name"]
            if factor_name not in df.columns:
                continue
            
            values = df[factor_name].copy()
            
            # 跳过全空的因子
            if values.isna().all():
                df[f"{factor_name}_norm"] = np.nan
                continue
            
            # 去极值 (MAD 方法)
            median = values.median()
            mad = (values - median).abs().median()
            if mad > 0:
                upper = median + 3 * 1.4826 * mad
                lower = median - 3 * 1.4826 * mad
                values = values.clip(lower, upper)
            
            # Z-Score 标准化
            mean = values.mean()
            std = values.std()
            if std > 0:
                values = (values - mean) / std
            else:
                values = 0.0
            
            df[f"{factor_name}_norm"] = values
        
        return df
    
    def _compute_composite_score(
        self,
        df: pd.DataFrame,
        factor_configs: List[Dict],
    ) -> pd.DataFrame:
        """
        计算综合得分
        
        根据权重加权求和，考虑因子方向
        """
        total_weight = sum(c.get("weight", 1.0) for c in factor_configs)
        
        if total_weight == 0:
            df["composite_score"] = 0.5
            return df
        
        composite = pd.Series(0.0, index=df.index)
        valid_factors = 0
        
        for config in factor_configs:
            factor_name = config["name"]
            norm_col = f"{factor_name}_norm"
            
            if norm_col not in df.columns:
                continue
            
            weight = config.get("weight", 1.0) / total_weight
            direction = config.get("direction")
            
            # 如果没有指定方向，从因子库获取
            if direction is None:
                factor_def = FactorLibrary.get(factor_name)
                direction = factor_def.direction if factor_def else "asc"
            
            factor_value = df[norm_col].fillna(0)
            
            # direction="desc" 表示越小越好，需要取反
            if direction == "desc":
                factor_value = -factor_value
            
            composite += weight * factor_value
            valid_factors += 1
        
        if valid_factors == 0:
            df["composite_score"] = 0.5
        else:
            # 转换为 0~1 排名分
            df["composite_score"] = composite.rank(pct=True)
        
        return df
    
    def select_top_stocks(
        self,
        factor_df: pd.DataFrame,
        top_n: int = 20,
    ) -> List[str]:
        """
        选出得分最高的 N 只股票
        """
        if factor_df.empty or "composite_score" not in factor_df.columns:
            return []
        
        # 过滤掉 NaN
        valid_df = factor_df.dropna(subset=["composite_score"])
        
        if valid_df.empty:
            return []
        
        # 按得分排序，选 Top N
        top_stocks = valid_df.nlargest(top_n, "composite_score")["ts_code"].tolist()
        
        return top_stocks
