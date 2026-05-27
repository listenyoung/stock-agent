"""
指数日线数据采集器

注意：Tushare index_daily 接口必须要 ts_code 参数，不支持多个拼接，
也不支持只用 trade_date 获取全部，只能逐个指数获取。
只同步三个核心指数：
- 000001.SH 上证指数
- 399001.SZ 深证成指  
- 399006.SZ 创业板指

支持增量同步：
- 首次同步：从 HISTORY_START_DATE 开始逐个指数同步历史数据
- 增量同步：从上次同步日期逐个指数同步到今天
- 已同步：如果今天已同步则跳过
"""

from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import time

from core.base import BaseCollector
from core.settings import settings
from core.managers import tushare_manager, mongo_manager


class IndexDailyCollector(BaseCollector):
    """
    指数日线数据采集器
    
    采集所有指数的日线行情数据。
    
    同步策略:
    - 首次同步：从 2003-01-27 开始同步所有历史数据
    - 增量同步：从上次同步日期的下一天同步到今天
    - 已同步：如果今天已同步则跳过
    
    Tushare API 限制:
    - 每次调用最多返回 6000 条数据
    - 历史数据跨度大时，需要单只指数逐个获取
    - 增量同步（少量天数）可以批量获取
    
    调度时间:
    - 可通过 SYNC_INDEX_DAILY_SCHEDULE 环境变量配置
    - 默认: 每个交易日 15:35 (收盘后)
    """
    
    name = "index_daily"
    description = "采集指数日线数据"
    default_schedule = "35 15 * * 1-5"  # 默认: 每个交易日 15:35
    
    # 历史数据起始日期
    HISTORY_START_DATE = "20030127"
    
    # 写入批次大小
    WRITE_BATCH_SIZE = 1000  # 累积多少条后写入数据库
    
    # 判断为"历史同步"的天数阈值（用于日志标识）
    HISTORY_SYNC_DAYS_THRESHOLD = 30
    
    @property
    def schedule(self) -> str:
        """从配置读取调度时间，未配置则使用默认值"""
        return settings.data_sync.index_daily_schedule or self.default_schedule
    
    # 核心指数列表（只同步这三个）
    CORE_INDICES = [
        "000001.SH",  # 上证指数
        "399001.SZ",  # 深证成指
        "399006.SZ",  # 创业板指
    ]
    
    async def collect(self) -> Dict[str, Any]:
        """执行采集"""
        # 获取最新交易日
        latest_trade_date = await tushare_manager.get_latest_trade_date()
        
        # 检查是否已同步
        if await mongo_manager.is_synced(self.name, latest_trade_date):
            self.logger.info(f"Index daily {latest_trade_date} already synced, skipping")
            return {"count": 0, "message": f"Already synced {latest_trade_date}", "skipped": True}
        
        # 只同步核心指数
        ts_codes = self.CORE_INDICES
        
        # 确定同步日期范围和同步类型（支持历史数据补充）
        sync_info = await self._determine_sync_range(latest_trade_date)
        
        if sync_info is None:
            return {"count": 0, "message": f"Already synced {latest_trade_date}", "skipped": True}
        
        start_date, end_date, is_history_sync = sync_info
        sync_type_desc = "历史同步" if is_history_sync else "增量同步"
        self.logger.info(
            f"[{sync_type_desc}] Syncing index daily data: {start_date} -> {end_date} "
            f"({len(ts_codes)} indices)"
        )
        
        total_count = 0
        failed_count = 0
        buffer: List[Dict[str, Any]] = []
        
        # index_daily 接口必须要 ts_code，不支持只用 trade_date 获取全部
        # 只能逐个指数获取
        for i, ts_code in enumerate(ts_codes):
            try:
                t1 = time.time()
                records = await tushare_manager.get_index_daily(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date,
                )
                t2 = time.time()
                
                if records:
                    buffer.extend(records)
                    if (i + 1) % 100 == 0 or i == len(ts_codes) - 1:
                        self.logger.info(f"  Progress: {i+1}/{len(ts_codes)}, buffer={len(buffer)}")
                    
            except Exception as e:
                failed_count += 1
                self.logger.warning(f"Failed to fetch {ts_code}: {e}")
            
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
            "message": f"[{sync_type_desc}] Synced {total_count} index daily records from {start_date} to {end_date}, {failed_count} failed",
        }
    
    async def _write_buffer(self, buffer: List[Dict[str, Any]]) -> int:
        """写入缓冲区数据到数据库"""
        t_start = time.time()
        result = await mongo_manager.bulk_upsert(
            collection="index_daily",
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
