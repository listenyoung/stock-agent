"""
每日指标数据采集器

采集每日指标数据（PE/PB/换手率/市值等），用于因子选股回测。
参考 stock_basic.py 的数据清洗逻辑。

字段说明：
- ts_code: 股票代码
- trade_date: 交易日期
- close: 收盘价
- turnover_rate: 换手率（%）
- turnover_rate_f: 换手率（自由流通股）
- volume_ratio: 量比
- pe: 市盈率（总市值/净利润，亏损为空）
- pe_ttm: 市盈率TTM（总市值/滚动净利润）
- pb: 市净率
- ps: 市销率
- ps_ttm: 市销率TTM
- dv_ratio: 股息率（%）
- dv_ttm: 股息率TTM（%）
- total_share: 总股本（万股）
- float_share: 流通股本（万股）
- free_share: 自由流通股本（万股）
- total_mv: 总市值（亿元，已转换）
- circ_mv: 流通市值（亿元，已转换）
"""

from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta

from core.base import BaseCollector
from core.settings import settings
from core.managers import tushare_manager, mongo_manager


def _clean_daily_basic_record(record: Dict) -> Dict:
    """
    清洗 daily_basic 记录
    
    参考 stock_basic.py 的 _add_financial_metrics 逻辑:
    - 市值：从万元转换为亿元
    - 估值：过滤 NaN/None，亏损公司保留 None
    - 交易指标：过滤 NaN
    """
    cleaned = {
        "ts_code": record.get("ts_code"),
        "trade_date": record.get("trade_date"),
    }
    
    # 收盘价
    if "close" in record and record["close"] is not None:
        try:
            value = float(record["close"])
            if value == value:  # 非 NaN
                cleaned["close"] = value
        except (ValueError, TypeError):
            pass
    
    # 市值（万元 -> 亿元）
    for field in ["total_mv", "circ_mv"]:
        if field in record and record[field] is not None:
            try:
                value = float(record[field])
                if value == value:  # 非 NaN
                    cleaned[field] = value / 10000  # 转换为亿元
            except (ValueError, TypeError):
                pass
    
    # 估值指标 (保留字段，即使为空也存储 None，因为亏损公司 PE 就是空)
    for field in ["pe", "pb", "pe_ttm", "ps", "ps_ttm", "dv_ratio", "dv_ttm"]:
        if field in record:
            val = record[field]
            if val is None:
                cleaned[field] = None
            else:
                try:
                    value = float(val)
                    if value == value:  # 非 NaN
                        cleaned[field] = value
                    else:
                        cleaned[field] = None
                except (ValueError, TypeError):
                    cleaned[field] = None
    
    # 交易指标
    for field in ["turnover_rate", "turnover_rate_f", "volume_ratio"]:
        if field in record and record[field] is not None:
            try:
                value = float(record[field])
                if value == value:  # 非 NaN
                    cleaned[field] = value
            except (ValueError, TypeError):
                pass
    
    # 股本数据（万股，保持原单位）
    for field in ["total_share", "float_share", "free_share"]:
        if field in record and record[field] is not None:
            try:
                value = float(record[field])
                if value == value:  # 非 NaN
                    cleaned[field] = value
            except (ValueError, TypeError):
                pass
    
    return cleaned


class DailyBasicCollector(BaseCollector):
    """
    每日指标数据采集器
    
    采集所有股票的每日指标数据（PE/PB/换手率/市值等）。
    
    同步策略:
    - 首次同步：从 2018-01-01 开始同步历史数据
    - 增量同步：从上次同步日期的下一天同步到今天
    - 已同步：如果今天已同步则跳过
    
    数据清洗:
    - 市值从万元转换为亿元
    - 过滤 NaN 值
    - 亏损公司 PE 保留 None
    
    Tushare API 特点:
    - daily_basic 接口按日期获取全市场数据
    - 单次调用可获取 ~5000 只股票
    
    调度时间:
    - 可通过 SYNC_DAILY_BASIC_SCHEDULE 环境变量配置
    - 默认: 每个交易日 16:00 (收盘后，在 stock_daily 之后)
    """
    
    name = "daily_basic"
    description = "采集股票每日指标数据（PE/PB/换手率/市值等）"
    default_schedule = "0 16 * * 1-5"  # 默认: 每个交易日 16:00
    
    # 历史数据起始日期
    HISTORY_START_DATE = "20180101"
    
    # 配置
    WRITE_BATCH_SIZE = 5000  # 每批写入条数
    
    # 判断为"历史同步"的天数阈值
    HISTORY_SYNC_DAYS_THRESHOLD = 30
    
    @property
    def schedule(self) -> str:
        """从配置读取调度时间，未配置则使用默认值"""
        return getattr(settings.data_sync, 'daily_basic_schedule', None) or self.default_schedule
    
    async def collect(self) -> Dict[str, Any]:
        """执行采集"""
        # 获取最新交易日
        latest_trade_date = await tushare_manager.get_latest_trade_date()
        
        if not latest_trade_date:
            return {"count": 0, "message": "Cannot get latest trade date"}
        
        # 检查是否已同步
        if await mongo_manager.is_synced(self.name, latest_trade_date):
            self.logger.info(f"Daily basic {latest_trade_date} already synced, skipping")
            return {"count": 0, "message": f"Already synced {latest_trade_date}", "skipped": True}
        
        # 确定同步日期范围（支持历史数据补充）
        sync_info = await self._determine_sync_range(latest_trade_date)
        
        if sync_info is None:
            return {"count": 0, "message": f"Already synced {latest_trade_date}", "skipped": True}
        
        start_date, end_date, is_history_sync = sync_info
        
        sync_type_desc = "历史同步" if is_history_sync else "增量同步"
        self.logger.info(
            f"[{sync_type_desc}] Syncing daily_basic data: {start_date} -> {end_date}"
        )
        
        total_count = 0
        failed_dates = []
        
        # 获取交易日历
        trade_dates = await self._get_trade_dates(start_date, end_date)
        
        if not trade_dates:
            return {"count": 0, "message": "No trade dates found in range"}
        
        self.logger.info(f"Found {len(trade_dates)} trade dates to sync")
        
        import time
        
        # 按日期逐日采集
        for i, trade_date in enumerate(trade_dates):
            try:
                t1 = time.time()
                
                # Step 1: 获取当日全市场 daily_basic 数据
                records = await tushare_manager.get_daily_basic(trade_date=trade_date)
                t2 = time.time()
                
                if records:
                    # Step 2: 清洗数据
                    cleaned_records = []
                    for record in records:
                        cleaned = _clean_daily_basic_record(record)
                        cleaned["updated_at"] = datetime.utcnow()
                        cleaned_records.append(cleaned)
                    
                    # Step 3: 批量写入
                    result = await mongo_manager.bulk_upsert(
                        collection="daily_basic",
                        documents=cleaned_records,
                        key_fields=["ts_code", "trade_date"],
                        batch_size=self.WRITE_BATCH_SIZE,
                    )
                    
                    count = result["upserted"] + result["modified"]
                    total_count += count
                    
                    # 每 10 天或最后一天输出日志
                    if (i + 1) % 10 == 0 or i == len(trade_dates) - 1:
                        self.logger.info(
                            f"Progress: {i+1}/{len(trade_dates)} dates, "
                            f"date={trade_date}, records={len(records)}, API={t2-t1:.2f}s"
                        )
                else:
                    self.logger.warning(f"No data for {trade_date}")
                    
            except Exception as e:
                failed_dates.append(trade_date)
                self.logger.warning(f"Failed to fetch {trade_date}: {e}")
        
        # 记录同步完成
        await mongo_manager.record_sync(
            sync_type=self.name,
            sync_date=end_date,
            count=total_count,
        )
        
        result_msg = f"[{sync_type_desc}] Synced {total_count} daily_basic records from {start_date} to {end_date}"
        if failed_dates:
            result_msg += f", {len(failed_dates)} dates failed"
        
        return {
            "count": total_count,
            "failed_dates": failed_dates,
            "start_date": start_date,
            "end_date": end_date,
            "sync_type": sync_type_desc,
            "message": result_msg,
        }
    
    async def _get_trade_dates(self, start_date: str, end_date: str) -> List[str]:
        """获取日期范围内的交易日列表"""
        try:
            # 使用 tushare 的交易日历 (已过滤 is_open=1)
            trade_dates = await tushare_manager.get_trade_cal(
                start_date=start_date,
                end_date=end_date,
            )
            return sorted(trade_dates)  # 确保按日期排序
        except Exception as e:
            self.logger.warning(f"Failed to get trade calendar: {e}")
            # 降级：生成日期列表（可能包含非交易日）
            dates = []
            current = datetime.strptime(start_date, "%Y%m%d")
            end = datetime.strptime(end_date, "%Y%m%d")
            while current <= end:
                # 跳过周末
                if current.weekday() < 5:
                    dates.append(current.strftime("%Y%m%d"))
                current += timedelta(days=1)
            return dates
    
    async def _determine_sync_range(
        self,
        latest_trade_date: str,
    ) -> Optional[Tuple[str, str, bool]]:
        """
        确定同步日期范围和同步类型
        
        Args:
            latest_trade_date: 最新交易日
            
        Returns:
            (start_date, end_date, is_history_sync) 元组
            is_history_sync: True 表示历史同步
            如果不需要同步返回 None
        """
        # 获取最后同步日期
        last_sync_date = await mongo_manager.get_last_sync_date(self.name)
        
        if last_sync_date is None:
            # 从未同步过，从历史起始日期开始
            self.logger.info(f"First sync, starting from {self.HISTORY_START_DATE}")
            return (self.HISTORY_START_DATE, latest_trade_date, True)
        
        if last_sync_date >= latest_trade_date:
            # 已同步到最新，无需再同步
            return None
        
        # 计算需要同步的天数
        last_sync_dt = datetime.strptime(last_sync_date, "%Y%m%d")
        latest_dt = datetime.strptime(latest_trade_date, "%Y%m%d")
        days_diff = (latest_dt - last_sync_dt).days
        
        # 判断是否为历史同步
        is_history_sync = days_diff > self.HISTORY_SYNC_DAYS_THRESHOLD
        
        # 增量同步：从上次同步日期的下一天开始
        next_day = (last_sync_dt + timedelta(days=1)).strftime("%Y%m%d")
        
        sync_type = "历史" if is_history_sync else "增量"
        self.logger.info(
            f"{sync_type}同步: last_sync={last_sync_date}, "
            f"syncing {next_day} -> {latest_trade_date} ({days_diff} days)"
        )
        
        return (next_day, latest_trade_date, is_history_sync)
