"""
Tushare 管理器 (数据 Provider)

负责:
- Tushare API 调用
- 令牌桶算法频率限制
- 数据格式标准化
"""

import asyncio
from typing import Optional, List, Dict, Any
from datetime import datetime, date, timedelta
import time

import pandas as pd

from .base import BaseManager
from ..settings import settings


class TokenBucket:
    """
    令牌桶算法实现
    
    用于控制 API 调用频率。
    """
    
    def __init__(self, rate: float, capacity: int):
        """
        Args:
            rate: 每秒产生的令牌数
            capacity: 令牌桶最大容量
        """
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_time = time.monotonic()
        self._lock = asyncio.Lock()
    
    async def acquire(self, tokens: int = 1) -> float:
        """
        获取令牌
        
        Args:
            tokens: 需要的令牌数
            
        Returns:
            等待时间 (秒)
        """
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_time
            
            # 补充令牌
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_time = now
            
            if self.tokens >= tokens:
                self.tokens -= tokens
                return 0.0
            
            # 计算需要等待的时间
            wait_time = (tokens - self.tokens) / self.rate
            return wait_time
    
    async def wait_and_acquire(self, tokens: int = 1) -> None:
        """等待并获取令牌"""
        wait_time = await self.acquire(tokens)
        if wait_time > 0:
            await asyncio.sleep(wait_time)
            await self.acquire(tokens)


class TushareManager(BaseManager):
    """
    Tushare 数据 Provider
    
    所有 Tushare API 调用必须通过此 Manager。
    内置令牌桶算法控制 QPS。
    """
    
    def __init__(self):
        super().__init__()
        self._ts = None   # tushare 模块引用 (用于非 pro 接口如 realtime_quote)
        self._pro = None  # tushare pro api
        self._config = settings.tushare
        self._bucket: Optional[TokenBucket] = None
    
    async def initialize(self) -> None:
        """初始化 Tushare 连接"""
        if self._initialized:
            return
        
        if not self._config.is_configured:
            self.logger.warning("Tushare token not configured, skipping initialization")
            return
        
        self.logger.info("Initializing Tushare Pro API...")
        
        import tushare as ts
        
        token = self._config.token.get_secret_value()
        ts.set_token(token)
        self._ts = ts  # 保存 tushare 模块引用，用于非 pro 接口
        self._pro = ts.pro_api()
        
        # 频率控制（暂时禁用）
        # rate_per_second = self._config.rate_limit / 60.0
        # self._bucket = TokenBucket(rate=rate_per_second, capacity=20)
        self._bucket = None
        
        self._initialized = True
        self.logger.info(f"Tushare initialized, rate_limit={self._config.rate_limit}/min ✓")
    
    async def shutdown(self) -> None:
        """关闭"""
        self._pro = None
        self._bucket = None
        self._initialized = False
        self.logger.info("Tushare shutdown")
    
    async def health_check(self) -> bool:
        """健康检查"""
        if not self._initialized or self._pro is None:
            return False
        
        try:
            # 简单调用测试
            await self._call_api("trade_cal", start_date="20240101", end_date="20240101")
            return True
        except Exception:
            return False
    
    async def _call_api(self, api_name: str, **kwargs) -> pd.DataFrame:
        """
        调用 Tushare API
        
        Args:
            api_name: API 名称
            **kwargs: API 参数
            
        Returns:
            DataFrame 结果
        """
        self._ensure_initialized()
        
        # 在线程池中执行同步调用
        loop = asyncio.get_event_loop()
        api_func = getattr(self._pro, api_name)
        result = await loop.run_in_executor(None, lambda: api_func(**kwargs))
        return result
    
    # ==================== 股票基础信息 ====================
    
    async def get_stock_basic(
        self,
        ts_code: Optional[str] = None,
        list_status: str = "L",
    ) -> List[Dict[str, Any]]:
        """
        获取股票基础信息
        
        Args:
            ts_code: 股票代码，None 表示获取所有
            list_status: L-上市 D-退市 P-暂停上市
            
        Returns:
            股票基础信息列表
        """
        params = {"list_status": list_status}
        if ts_code:
            params["ts_code"] = ts_code
        
        # 不限制字段，获取所有可用信息
        df = await self._call_api("stock_basic", **params)
        return df.to_dict("records") if not df.empty else []
    
    # ==================== 日线数据 ====================
    
    async def get_daily(
        self,
        ts_code: Optional[str] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        adj: str = "qfq",
    ) -> List[Dict[str, Any]]:
        """
        获取日线数据
        
        Args:
            ts_code: 股票代码 (可选，支持逗号分隔多个)
            trade_date: 交易日期 (可选，查询该日所有股票)
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            adj: 复权类型 qfq-前复权 hfq-后复权 None-不复权
            
        Returns:
            日线数据列表
            
        Note:
            - 按 ts_code 查询：获取某只股票的历史数据
            - 按 trade_date 查询：获取某天所有股票的数据（增量同步推荐）
        """
        params = {"adj": adj}
        
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        
        df = await self._call_api("daily", **params)
        
        if df.empty:
            return []
        
        # 标准化字段
        records = df.to_dict("records")
        return records
    
    async def get_daily_basic(
        self,
        ts_code: Optional[str] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        获取每日指标 (PE, PB, 换手率, 市值等)
        
        不限制字段，获取所有可用指标。
        
        Args:
            ts_code: 股票代码 (支持逗号分隔批量查询)
            trade_date: 交易日期
            start_date: 开始日期
            end_date: 结束日期
        """
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        
        df = await self._call_api("daily_basic", **params)
        return df.to_dict("records") if not df.empty else []
    
    # ==================== 财务数据 ====================
    
    # 财务指标核心字段 (精简版，避免超长字符串)
    FINA_INDICATOR_FIELDS = ",".join([
        # 基础信息
        "ts_code", "ann_date", "end_date",
        # 每股指标
        "eps", "dt_eps", "bps", "cfps", "ocfps",
        # 盈利能力
        "roe", "roe_waa", "roe_dt", "roa", "roic",
        "netprofit_margin", "grossprofit_margin", "profit_to_gr",
        # 成长能力
        "tr_yoy", "or_yoy", "netprofit_yoy", "dt_netprofit_yoy",
        "basic_eps_yoy", "roe_yoy", "bps_yoy", "assets_yoy",
        # 营运能力
        "assets_turn", "ca_turn", "fa_turn", "inv_turn", "ar_turn",
        # 偿债能力
        "current_ratio", "quick_ratio", "cash_ratio",
        "debt_to_assets", "debt_to_eqt", "eqt_to_debt",
        # 现金流
        "ocf_to_or", "ocf_to_profit", "fcff", "fcfe",
        # 杜邦分析
        "ebit", "ebitda", "op_income",
        # 季度同比/环比
        "q_netprofit_yoy", "q_netprofit_qoq", "q_sales_yoy", "q_sales_qoq",
    ])
    
    async def get_financial_indicator(
        self,
        ts_code: Optional[str] = None,
        period: Optional[str] = None,
        fields: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        获取财务指标
        
        Args:
            ts_code: 股票代码 (必传)
            period: 报告期 (YYYYMMDD，如 20231231，可选)
            fields: 自定义字段，默认使用精简版核心字段
            limit: 返回记录数限制 (不传则返回所有历史数据)
            
        Returns:
            财务指标列表
            
        Note:
            Tushare fina_indicator 接口只支持 ts_code 和 period 参数
            不支持 start_date/end_date
        """
        if not ts_code:
            self.logger.warning("fina_indicator requires ts_code parameter")
            return []
        
        params = {
            "ts_code": ts_code,
            "fields": fields or self.FINA_INDICATOR_FIELDS,
        }
        if period:
            params["period"] = period
        
        try:
            df = await self._call_api("fina_indicator", **params)
            records = df.to_dict("records") if not df.empty else []
            
            # 按 end_date 降序排序
            records.sort(key=lambda x: x.get("end_date", ""), reverse=True)
            
            # 限制返回数量
            if limit and len(records) > limit:
                records = records[:limit]
            
            return records
        except Exception as e:
            self.logger.warning(f"Failed to get fina_indicator for {ts_code}: {e}")
            return []
    
    async def get_financial_indicator_batch(
        self,
        ts_codes: List[str],
        limit: int = 8,
        batch_size: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        批量获取多只股票的财务指标
        
        Args:
            ts_codes: 股票代码列表
            limit: 每只股票获取的记录数限制
            batch_size: 每批处理数量
            
        Returns:
            财务指标列表
        """
        all_records = []
        
        for i in range(0, len(ts_codes), batch_size):
            batch = ts_codes[i:i + batch_size]
            for ts_code in batch:
                try:
                    records = await self.get_financial_indicator(
                        ts_code=ts_code,
                        limit=limit,
                    )
                    all_records.extend(records)
                except Exception as e:
                    self.logger.warning(f"Failed to get fina_indicator for {ts_code}: {e}")
                
                # 避免频率限制
                await asyncio.sleep(0.1)
        
        return all_records
    
    async def get_income_statement(
        self,
        ts_code: str,
        period: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        获取利润表数据
        
        Args:
            ts_code: 股票代码
            period: 报告期 (YYYYMMDD)
            limit: 返回记录数限制
        """
        if not ts_code:
            return []
        
        params = {"ts_code": ts_code}
        if period:
            params["period"] = period
        
        try:
            df = await self._call_api("income", **params)
            records = df.to_dict("records") if not df.empty else []
            records.sort(key=lambda x: x.get("end_date", ""), reverse=True)
            if limit and len(records) > limit:
                records = records[:limit]
            return records
        except Exception as e:
            self.logger.warning(f"Failed to get income for {ts_code}: {e}")
            return []
    
    async def get_balance_sheet(
        self,
        ts_code: str,
        period: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        获取资产负债表数据
        
        Args:
            ts_code: 股票代码
            period: 报告期 (YYYYMMDD)
            limit: 返回记录数限制
        """
        if not ts_code:
            return []
        
        params = {"ts_code": ts_code}
        if period:
            params["period"] = period
        
        try:
            df = await self._call_api("balancesheet", **params)
            records = df.to_dict("records") if not df.empty else []
            records.sort(key=lambda x: x.get("end_date", ""), reverse=True)
            if limit and len(records) > limit:
                records = records[:limit]
            return records
        except Exception as e:
            self.logger.warning(f"Failed to get balancesheet for {ts_code}: {e}")
            return []
    
    async def get_cashflow_statement(
        self,
        ts_code: str,
        period: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        获取现金流量表数据
        
        Args:
            ts_code: 股票代码
            period: 报告期 (YYYYMMDD)
            limit: 返回记录数限制
        """
        if not ts_code:
            return []
        
        params = {"ts_code": ts_code}
        if period:
            params["period"] = period
        
        try:
            df = await self._call_api("cashflow", **params)
            records = df.to_dict("records") if not df.empty else []
            records.sort(key=lambda x: x.get("end_date", ""), reverse=True)
            if limit and len(records) > limit:
                records = records[:limit]
            return records
        except Exception as e:
            self.logger.warning(f"Failed to get cashflow for {ts_code}: {e}")
            return []
    
    async def get_financial_data(
        self,
        ts_code: str,
        limit: int = 4,
    ) -> Dict[str, Any]:
        """
        获取完整的财务数据 (三大报表 + 财务指标)
        
        Args:
            ts_code: 股票代码
            limit: 每类数据获取的记录数限制 (默认4个季度)
            
        Returns:
            包含以下数据的字典:
            - income_statement: 利润表
            - balance_sheet: 资产负债表
            - cashflow_statement: 现金流量表
            - financial_indicators: 财务指标
        """
        financial_data = {"ts_code": ts_code}
        
        # 并行获取四类财务数据
        income_task = self.get_income_statement(ts_code, limit=limit)
        balance_task = self.get_balance_sheet(ts_code, limit=limit)
        cashflow_task = self.get_cashflow_statement(ts_code, limit=limit)
        indicator_task = self.get_financial_indicator(ts_code, limit=limit)
        
        results = await asyncio.gather(
            income_task, balance_task, cashflow_task, indicator_task,
            return_exceptions=True
        )
        
        # 处理结果
        if isinstance(results[0], list):
            financial_data["income_statement"] = results[0]
        if isinstance(results[1], list):
            financial_data["balance_sheet"] = results[1]
        if isinstance(results[2], list):
            financial_data["cashflow_statement"] = results[2]
        if isinstance(results[3], list):
            financial_data["financial_indicators"] = results[3]
        
        return financial_data
    
    # ==================== 新闻舆情 ====================
    
    async def get_news(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        src: str = "",
    ) -> List[Dict[str, Any]]:
        """
        获取新闻资讯
        """
        params = {}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if src:
            params["src"] = src
        
        try:
            df = await self._call_api("news", **params)
            return df.to_dict("records") if not df.empty else []
        except Exception as e:
            self.logger.warning(f"Failed to get news: {e}")
            return []
    
    # ==================== 指数基础信息 ====================
    
    async def get_index_basic(
        self,
        market: str = "SW",
        ts_code: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        获取指数基础信息
        
        Args:
            market: 市场类型
                - SSE: 上交所
                - SZSE: 深交所
                - SW: 申万指数
                - MSCI: MSCI指数
                - CSI: 中证指数
                - CICC: 中金指数
                - OTH: 其他指数
            ts_code: 指数代码，None 表示获取所有
            
        Returns:
            指数基础信息列表
        """
        params = {
            "market": market,
            "fields": "ts_code,name,fullname,market,publisher,index_type,category,base_date,base_point,list_date,weight_rule,desc,exp_date",
        }
        if ts_code:
            params["ts_code"] = ts_code
        
        df = await self._call_api("index_basic", **params)
        return df.to_dict("records") if not df.empty else []
    
    async def get_all_index_basic(self) -> List[Dict[str, Any]]:
        """
        获取所有主要市场的指数基础信息
        
        Returns:
            所有指数基础信息列表
        """
        markets = ["SSE", "SZSE", "SW", "CSI"]
        all_records = []
        
        for market in markets:
            try:
                records = await self.get_index_basic(market=market)
                all_records.extend(records)
                self.logger.info(f"Fetched {len(records)} indices from {market}")
            except Exception as e:
                self.logger.warning(f"Failed to fetch index_basic for {market}: {e}")
        
        return all_records
    
    # ==================== 指数日线数据 ====================
    
    async def get_index_daily(
        self,
        ts_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        获取指数日线数据
        
        注意：index_daily 接口必须要 ts_code，不支持逗号拼接多个，
        也不支持只用 trade_date 获取全部。
        
        Args:
            ts_code: 指数代码 (必填，不支持逗号拼接)
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            
        Returns:
            指数日线数据列表
        """
        params = {"ts_code": ts_code}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        
        df = await self._call_api("index_daily", **params)
        
        if df.empty:
            return []
        
        return df.to_dict("records")
    
    # ==================== 行业资金流向 ====================
    
    async def get_moneyflow_ind_ths(
        self,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        获取同花顺行业资金流向
        
        Args:
            trade_date: 交易日期 (YYYYMMDD)
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            
        Returns:
            行业资金流向列表
            
        字段说明:
            - ts_code: 行业代码
            - trade_date: 交易日期
            - name: 行业名称
            - pct_change: 涨跌幅
            - close: 收盘价
            - net_amount: 净流入金额
            - net_amount_rate: 净流入占比
            - buy_elg_amount: 特大单买入金额
            - buy_lg_amount: 大单买入金额
            - buy_md_amount: 中单买入金额
            - buy_sm_amount: 小单买入金额
            - sell_elg_amount: 特大单卖出金额
            - sell_lg_amount: 大单卖出金额
            - sell_md_amount: 中单卖出金额
            - sell_sm_amount: 小单卖出金额
        """
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        
        df = await self._call_api("moneyflow_ind_ths", **params)
        return df.to_dict("records") if not df.empty else []
    
    # ==================== 概念板块资金流向 ====================
    
    async def get_moneyflow_cnt_ths(
        self,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        获取同花顺概念板块资金流向
        
        Args:
            trade_date: 交易日期 (YYYYMMDD)
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            
        Returns:
            概念板块资金流向列表
            
        字段说明:
            - ts_code: 概念板块代码
            - trade_date: 交易日期
            - name: 板块名称
            - pct_change: 涨跌幅
            - close: 收盘价
            - net_amount: 净流入金额
            - net_amount_rate: 净流入占比
            - buy_elg_amount: 特大单买入金额
            - buy_lg_amount: 大单买入金额
            - buy_md_amount: 中单买入金额
            - buy_sm_amount: 小单买入金额
            - sell_elg_amount: 特大单卖出金额
            - sell_lg_amount: 大单卖出金额
            - sell_md_amount: 中单卖出金额
            - sell_sm_amount: 小单卖出金额
        """
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        
        df = await self._call_api("moneyflow_cnt_ths", **params)
        return df.to_dict("records") if not df.empty else []
    
    # ==================== 沪深港通资金流向 ====================
    
    async def get_moneyflow_hsgt(
        self,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        获取沪深港通资金流向
        
        Args:
            trade_date: 交易日期 (YYYYMMDD)
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            
        Returns:
            沪深港通资金流向列表
            
        字段说明:
            - trade_date: 交易日期
            - ggt_ss: 港股通(上海)
            - ggt_sz: 港股通(深圳)
            - hgt: 沪股通（百万元）
            - sgt: 深股通（百万元）
            - north_money: 北向资金（百万元）
            - south_money: 南向资金（百万元）
        """
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        
        df = await self._call_api("moneyflow_hsgt", **params)
        return df.to_dict("records") if not df.empty else []
    
    # ==================== 涨跌停数据 ====================
    
    async def get_limit_list_d(
        self,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        ts_code: Optional[str] = None,
        limit_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        获取每日涨跌停统计数据
        
        Args:
            trade_date: 交易日期 (YYYYMMDD)
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            ts_code: 股票代码
            limit_type: 涨跌停类型 U-涨停 D-跌停
            
        Returns:
            涨跌停统计列表
            
        字段说明:
            - trade_date: 交易日期
            - ts_code: 股票代码
            - industry: 所属行业
            - name: 股票名称
            - close: 收盘价
            - pct_chg: 涨跌幅
            - amount: 成交额（千元）
            - limit_amount: 板上成交额（千元）
            - float_mv: 流通市值
            - total_mv: 总市值
            - turnover_ratio: 换手率
            - fd_amount: 封单金额
            - first_time: 首次涨停时间
            - last_time: 最后涨停时间
            - open_times: 打开次数
            - up_stat: 涨停统计 (N/T 连续 N 天内 T 天涨停)
            - limit_times: 连续涨停次数
            - limit: 涨跌停状态 U-涨停 D-跌停
        """
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if ts_code:
            params["ts_code"] = ts_code
        if limit_type:
            params["limit_type"] = limit_type
        
        df = await self._call_api("limit_list_d", **params)
        return df.to_dict("records") if not df.empty else []
    
    # ==================== 交易日历 ====================
    
    async def get_trade_cal(
        self,
        start_date: str,
        end_date: str,
    ) -> List[str]:
        """
        获取交易日历
        
        Returns:
            交易日列表 (YYYYMMDD 格式)
        """
        df = await self._call_api(
            "trade_cal",
            start_date=start_date,
            end_date=end_date,
            is_open="1",
        )
        
        if df.empty:
            return []
        
        return df["cal_date"].tolist()
    
    async def get_latest_trade_date(self) -> str:
        """
        获取最近可用数据的交易日
        
        规则：
        - 18:00 之前：当天数据还未同步完成，返回昨天或之前最近的交易日
        - 18:00 之后：当天数据已可用，返回今天或之前最近的交易日
        """
        from datetime import datetime
        
        now = datetime.now()
        today = now.strftime("%Y%m%d")
        
        # 18点之前，数据还没出来，截止日期用昨天
        if now.hour < 18:
            cutoff_date = (now - timedelta(days=1)).strftime("%Y%m%d")
            self.logger.info(f"Before 18:00, using yesterday as cutoff: {cutoff_date}")
        else:
            cutoff_date = today
            self.logger.info(f"After 18:00, using today as cutoff: {cutoff_date}")
        
        # 查询最近60天的交易日历
        start_date = (date.today() - timedelta(days=60)).strftime("%Y%m%d")
        
        try:
            df = await self._call_api(
                "trade_cal",
                exchange="SSE",
                start_date=start_date,
                end_date=cutoff_date,
                is_open="1",
            )
            
            if not df.empty:
                # 获取所有交易日并排序
                dates = sorted(df["cal_date"].tolist())
                self.logger.info(f"Trade calendar: {len(dates)} days, range: {dates[0] if dates else 'N/A'} ~ {dates[-1] if dates else 'N/A'}")
                
                # 过滤掉超过截止日期的
                valid_dates = [d for d in dates if d <= cutoff_date]
                if valid_dates:
                    latest = valid_dates[-1]
                    self.logger.info(f"Latest available trade date: {latest}")
                    return latest
                else:
                    self.logger.warning(f"No valid trade dates found (<= {cutoff_date})")
            else:
                self.logger.warning("Trade calendar returned empty dataframe")
        except Exception as e:
            self.logger.error(f"Failed to get trade calendar: {e}")
        
        # 兜底: 返回截止日期
        self.logger.warning(f"Using cutoff date as fallback: {cutoff_date}")
        return cutoff_date
    
    # ==================== 实时行情 (Listener 节点使用) ====================
    
    async def get_realtime_quote(
        self,
        ts_codes: List[str],
        batch_size: int = 50,
        timeout: float = 2.0,
    ) -> List[Dict[str, Any]]:
        """
        获取实时行情 (盘中) - 分批获取，超时跳过
        
        注意: 此接口需要较高权限，请确认 Tushare 积分足够。
        
        Args:
            ts_codes: 股票代码列表
            batch_size: 每批获取数量，默认50
            timeout: 单批超时时间(秒)，超时则跳过，默认2秒
            
        Returns:
            实时行情数据列表，包含字段:
            - ts_code: 股票代码
            - name: 股票名称
            - price: 当前价
            - open: 开盘价
            - high: 最高价
            - low: 最低价
            - pre_close: 昨收价
            - change: 涨跌额
            - pct_chg: 涨跌幅
            - vol: 成交量
            - amount: 成交额
        """
        if not ts_codes:
            return []
        
        all_records: List[Dict[str, Any]] = []
        total_batches = (len(ts_codes) + batch_size - 1) // batch_size
        skipped = 0
        
        for i in range(0, len(ts_codes), batch_size):
            batch_codes = ts_codes[i:i + batch_size]
            batch_num = i // batch_size + 1
            
            try:
                # 使用 ts.realtime_quote (非 pro 接口)，传入逗号分隔字符串
                ts_code_str = ",".join(batch_codes)
                loop = asyncio.get_event_loop()
                
                # 超时则跳过
                df = await asyncio.wait_for(
                    loop.run_in_executor(
                        None,
                        lambda code_str=ts_code_str: self._ts.realtime_quote(ts_code=code_str)
                    ),
                    timeout=timeout
                )
                
                if df is not None and not df.empty:
                    records = df.to_dict("records")
                    all_records.extend(records)
                    self.logger.debug(
                        f"Realtime quote batch {batch_num}/{total_batches}: "
                        f"got {len(records)} stocks"
                    )
            
            except asyncio.TimeoutError:
                skipped += 1
                self.logger.warning(
                    f"Realtime quote batch {batch_num}/{total_batches} timeout, skipped"
                )
                
            except Exception as e:
                skipped += 1
                self.logger.error(
                    f"Failed to get realtime quote batch {batch_num}/{total_batches}: {e}"
                )
            
            # 批次间隔，避免触发频率限制
            if i + batch_size < len(ts_codes):
                await asyncio.sleep(0.1)
        
        self.logger.info(
            f"Realtime quote completed: {len(all_records)}/{len(ts_codes)} stocks"
            + (f" (skipped {skipped} batches)" if skipped > 0 else "")
        )
        return all_records
    
    # 三大核心指数代码
    CORE_INDEX_CODES = [
        "000001.SH",  # 上证指数
        "399001.SZ",  # 深证成指
        "399006.SZ",  # 创业板指
    ]
    
    async def get_realtime_index_quote(self) -> Dict[str, Dict[str, Any]]:
        """
        获取三大指数实时行情
        
        Returns:
            {
                "000001.SH": {"ts_code": "000001.SH", "name": "上证指数", "close": 3200.00, "pct_chg": 1.23, ...},
                "399001.SZ": {"ts_code": "399001.SZ", "name": "深证成指", ...},
                "399006.SZ": {"ts_code": "399006.SZ", "name": "创业板指", ...}
            }
        """
        result = {}
        
        try:
            # 使用 ts.realtime_quote 获取指数实时数据
            ts_code_str = ",".join(self.CORE_INDEX_CODES)
            loop = asyncio.get_event_loop()
            df = await loop.run_in_executor(
                None,
                lambda: self._ts.realtime_quote(ts_code=ts_code_str)
            )
            
            if df is not None and not df.empty:
                for record in df.to_dict("records"):
                    ts_code = record.get("TS_CODE") or record.get("ts_code", "")
                    if ts_code:
                        # 统一字段名为小写
                        normalized = {k.lower(): v for k, v in record.items()}
                        result[ts_code] = normalized
                        
                self.logger.debug(f"Got realtime index quotes: {list(result.keys())}")
            else:
                self.logger.warning("No realtime index data returned")
                
        except Exception as e:
            self.logger.error(f"Failed to get realtime index quote: {e}")
        
        return result
    
    async def get_stk_limit(
        self,
        trade_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        获取每日涨跌停价格 (stk_limit 接口)
        
        返回当日所有股票的涨停价、跌停价。
        
        Args:
            trade_date: 交易日期 (YYYYMMDD)，默认为今天
            
        Returns:
            涨跌停价格列表，包含字段:
            - ts_code: 股票代码
            - trade_date: 交易日期
            - pre_close: 昨日收盘价
            - up_limit: 涨停价
            - down_limit: 跌停价
        """
        if not trade_date:
            trade_date = date.today().strftime("%Y%m%d")
        
        try:
            df = await self._call_api(
                "stk_limit",
                trade_date=trade_date,
            )
            
            if df.empty:
                return []
            
            return df.to_dict("records")
            
        except Exception as e:
            self.logger.error(f"Failed to get stk_limit: {e}")
            return []
    
    async def is_trading_time(self) -> bool:
        """
        检查当前是否为交易时间
        
        交易时间:
        - 上午: 09:30 - 11:30
        - 下午: 13:00 - 15:00
        - 仅限交易日
        
        Returns:
            True 表示在交易时间内
        """
        from datetime import datetime
        
        now = datetime.now()
        today = now.strftime("%Y%m%d")
        
        # 检查是否为交易日
        try:
            df = await self._call_api(
                "trade_cal",
                exchange="SSE",
                start_date=today,
                end_date=today,
            )
            
            if df.empty or df.iloc[0]["is_open"] != 1:
                return False
        except Exception:
            # 无法确认，假设是交易日
            pass
        
        # 检查时间
        current_time = now.hour * 100 + now.minute
        
        # 上午 09:30 - 11:30
        if 930 <= current_time <= 1130:
            return True
        
        # 下午 13:00 - 15:00
        if 1300 <= current_time <= 1500:
            return True
        
        return False


# ==================== 全局单例 ====================
tushare_manager = TushareManager()