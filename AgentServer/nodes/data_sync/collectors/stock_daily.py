"""
日线数据采集器

使用高效的批量采集和 BulkWrite 写入。
支持增量同步：
- 首次同步：从 2003-01-27 开始同步所有历史数据（单只股票逐个获取）
- 增量同步：从上次同步日期同步到今天（可批量获取）
- 已同步：如果今天已同步则跳过

注意：Tushare API 每次调用最多返回 6000 条数据，
历史数据（20多年）可能接近此限制，因此首次同步需要逐只股票获取。
"""

from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta

from core.base import BaseCollector
from core.settings import settings
from core.managers import tushare_manager, mongo_manager


class StockDailyCollector(BaseCollector):
    """
    日线数据采集器
    
    采集所有股票的日线行情数据。
    
    同步策略:
    - 首次同步：从 2003-01-27 开始同步所有历史数据
    - 增量同步：从上次同步日期的下一天同步到今天
    - 已同步：如果今天已同步则跳过
    
    Tushare API 限制:
    - 每次调用最多返回 6000 条数据
    - 历史数据跨度大时，需要单只股票逐个获取
    - 增量同步（少量天数）可以批量获取
    
    调度时间:
    - 可通过 SYNC_STOCK_DAILY_SCHEDULE 环境变量配置
    - 默认: 每个交易日 15:30 (收盘后)
    """
    
    name = "stock_daily"
    description = "采集股票日线数据"
    default_schedule = "30 15 * * 1-5"  # 默认: 每个交易日 15:30
    
    # 历史数据起始日期
    HISTORY_START_DATE = "20030127"
    
    # Tushare API 限制
    TUSHARE_MAX_RECORDS = 6000  # Tushare 单次最多返回条数
    
    # 配置 - 根据同步类型动态调整
    FETCH_BATCH_SIZE_HISTORY = 1     # 历史同步：每批 1 只股票（数据量大）
    FETCH_BATCH_SIZE_INCREMENTAL = 500  # 增量同步：每批 100 只股票（数据量小）
    WRITE_BATCH_SIZE = 1000          # 累积多少条后写入数据库
    
    # 判断为"历史同步"的天数阈值
    HISTORY_SYNC_DAYS_THRESHOLD = 30  # 超过 30 天视为历史同步
    
    @property
    def schedule(self) -> str:
        """从配置读取调度时间，未配置则使用默认值"""
        return settings.data_sync.stock_daily_schedule or self.default_schedule
    
    async def collect(self) -> Dict[str, Any]:
        """执行采集"""
        # 获取最新交易日
        latest_trade_date = await tushare_manager.get_latest_trade_date()
        
        # 检查是否已同步
        if await mongo_manager.is_synced(self.name, latest_trade_date):
            self.logger.info(f"Stock daily {latest_trade_date} already synced, skipping")
            return {"count": 0, "message": f"Already synced {latest_trade_date}", "skipped": True}
        
        # 获取所有股票代码
        stocks = await mongo_manager.find_many(
            "stock_basic",
            {"list_status": "L"},
            projection={"ts_code": 1},
        )
        
        if not stocks:
            return {"count": 0, "message": "No stocks found"}
        
        ts_codes = [s["ts_code"] for s in stocks]
        
        # 确定同步日期范围和同步类型（支持历史数据补充）
        sync_info = await self._determine_sync_range(latest_trade_date)
        
        if sync_info is None:
            return {"count": 0, "message": f"Already synced {latest_trade_date}", "skipped": True}
        
        start_date, end_date, is_history_sync = sync_info
        
        # 根据同步类型选择批次大小
        fetch_batch_size = (
            self.FETCH_BATCH_SIZE_HISTORY 
            if is_history_sync 
            else self.FETCH_BATCH_SIZE_INCREMENTAL
        )
        
        sync_type_desc = "历史同步" if is_history_sync else "增量同步"
        self.logger.info(
            f"[{sync_type_desc}] Syncing stock daily data: {start_date} -> {end_date} "
            f"({len(ts_codes)} stocks, batch_size={fetch_batch_size})"
        )
        
        total_count = 0
        failed_count = 0
        buffer: List[Dict[str, Any]] = []
        
        import time
        
        # 统一逻辑：按批次获取（多个ts_code用逗号拼接）
        for i in range(0, len(ts_codes), fetch_batch_size):
            batch = ts_codes[i:i + fetch_batch_size]
            batch_start = i + 1
            batch_end = min(i + fetch_batch_size, len(ts_codes))
            
            # 多个股票代码用逗号拼接
            ts_code_str = ",".join(batch)
            
            self.logger.info(f"Processing stocks {batch_start}-{batch_end}/{len(ts_codes)} ({len(batch)} codes)")
            
            try:
                t1 = time.time()
                records = await tushare_manager.get_daily(
                    ts_code=ts_code_str,
                    start_date=start_date,
                    end_date=end_date,
                )
                t2 = time.time()
                
                if records:
                    buffer.extend(records)
                    self.logger.info(f"  Batch {batch_start}-{batch_end}: {len(records)} rows, API={t2-t1:.2f}s")
                    
            except Exception as e:
                failed_count += len(batch)
                self.logger.warning(f"Failed to fetch batch {batch_start}-{batch_end}: {e}")
            
            # 缓冲区满时写入
            if len(buffer) >= self.WRITE_BATCH_SIZE:
                result = await self._write_buffer(buffer)
                total_count += result
                buffer.clear()
        
        # 写入剩余数据
        if buffer:
            result = await self._write_buffer(buffer)
            total_count += result
        
        # 记录同步完成
        await mongo_manager.record_sync(
            sync_type=self.name,
            sync_date=end_date,
            count=total_count,
        )
        
        return {
            "count": total_count,
            "failed": failed_count,
            "start_date": start_date,
            "end_date": end_date,
            "sync_type": sync_type_desc,
            "message": f"[{sync_type_desc}] Synced {total_count} daily records from {start_date} to {end_date}, {failed_count} failed",
        }
    
    async def _write_buffer(self, buffer: List[Dict[str, Any]]) -> int:
        """写入缓冲区数据到数据库"""
        import time
        t_start = time.time()
        result = await mongo_manager.bulk_upsert(
            collection="stock_daily",
            documents=buffer,
            key_fields=["ts_code", "trade_date"],
            batch_size=self.WRITE_BATCH_SIZE,
        )
        t_end = time.time()
        self.logger.info(
            f"Bulk write: {len(buffer)} records in {t_end-t_start:.2f}s, "
            f"upserted={result['upserted']}, modified={result['modified']}"
        )
        return result["upserted"] + result["modified"]
    
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
            is_history_sync: True 表示历史同步（需要逐只获取）
            如果不需要同步返回 None
        """
        # 获取最后同步日期
        last_sync_date = await mongo_manager.get_last_sync_date(self.name)
        
        if last_sync_date is None:
            # 从未同步过，从历史起始日期开始（历史同步）
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