"""
行业资金流向采集器

使用日期同步策略：
- 如果没有同步记录，从前30天开始同步
- 增量同步：从上次同步日期的下一天同步到今天
- 已同步：如果今天已同步则跳过

数据来源: 同花顺行业资金流向 (moneyflow_ind_ths)
"""

from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta, date
import time

from core.base import BaseCollector
from core.settings import settings
from core.managers import tushare_manager, mongo_manager


class MoneyflowIndustryCollector(BaseCollector):
    """
    行业资金流向采集器
    
    采集同花顺行业资金流向数据。
    
    同步策略:
    - 首次同步：从前30天开始同步
    - 增量同步：从上次同步日期的下一天同步到今天
    - 已同步：如果今天已同步则跳过
    
    调度时间:
    - 可通过 SYNC_MONEYFLOW_INDUSTRY_SCHEDULE 环境变量配置
    - 默认: 每个交易日 16:00 (收盘后)
    """
    
    name = "moneyflow_industry"
    description = "采集行业资金流向数据"
    default_schedule = "0 16 * * 1-5"  # 默认: 每个交易日 16:00
    
    # 首次同步天数
    INITIAL_SYNC_DAYS = 30
    
    # 批量写入大小
    WRITE_BATCH_SIZE = 1000
    
    @property
    def schedule(self) -> str:
        """从配置读取调度时间，未配置则使用默认值"""
        return settings.data_sync.moneyflow_industry_schedule or self.default_schedule
    
    async def collect(self) -> Dict[str, Any]:
        """执行采集"""
        # 获取最新交易日
        latest_trade_date = await tushare_manager.get_latest_trade_date()
        
        # 检查是否已同步
        if await mongo_manager.is_synced(self.name, latest_trade_date):
            self.logger.info(f"Moneyflow industry {latest_trade_date} already synced, skipping")
            return {"count": 0, "message": f"Already synced {latest_trade_date}", "skipped": True}
        
        # 确定同步日期范围（支持历史数据补充）
        sync_info = await self._determine_sync_range(latest_trade_date)
        
        if sync_info is None:
            return {"count": 0, "message": f"Already synced {latest_trade_date}", "skipped": True}
        
        start_date, end_date = sync_info
        
        self.logger.info(
            f"Syncing moneyflow industry data: {start_date} -> {end_date}"
        )
        
        total_count = 0
        buffer: List[Dict[str, Any]] = []
        
        # 按日期逐日获取（API 可能有数据量限制）
        current_date = datetime.strptime(start_date, "%Y%m%d")
        end_dt = datetime.strptime(end_date, "%Y%m%d")
        
        while current_date <= end_dt:
            trade_date = current_date.strftime("%Y%m%d")
            
            try:
                t1 = time.time()
                records = await tushare_manager.get_moneyflow_ind_ths(
                    trade_date=trade_date
                )
                t2 = time.time()
                
                if records:
                    buffer.extend(records)
                    self.logger.info(
                        f"  {trade_date}: {len(records)} industries, API={t2-t1:.2f}s"
                    )
                    
            except Exception as e:
                self.logger.warning(f"Failed to fetch moneyflow for {trade_date}: {e}")
            
            # 缓冲区满时批量写入
            if len(buffer) >= self.WRITE_BATCH_SIZE:
                result = await mongo_manager.bulk_upsert(
                    collection="moneyflow_industry",
                    documents=buffer,
                    key_fields=["ts_code", "trade_date"],
                    batch_size=self.WRITE_BATCH_SIZE,
                )
                total_count += result["upserted"] + result["modified"]
                self.logger.info(
                    f"Bulk write: {len(buffer)} records, "
                    f"upserted={result['upserted']}, modified={result['modified']}"
                )
                buffer.clear()
            
            current_date += timedelta(days=1)
        
        # 写入剩余数据
        if buffer:
            result = await mongo_manager.bulk_upsert(
                collection="moneyflow_industry",
                documents=buffer,
                key_fields=["ts_code", "trade_date"],
                batch_size=self.WRITE_BATCH_SIZE,
            )
            total_count += result["upserted"] + result["modified"]
            self.logger.info(
                f"Final bulk write: {len(buffer)} records, "
                f"upserted={result['upserted']}, modified={result['modified']}"
            )
        
        # 记录同步完成
        await mongo_manager.record_sync(
            sync_type=self.name,
            sync_date=end_date,
            count=total_count,
        )
        
        return {
            "count": total_count,
            "start_date": start_date,
            "end_date": end_date,
            "message": f"Synced {total_count} moneyflow records from {start_date} to {end_date}",
        }
    
    async def _determine_sync_range(
        self,
        latest_trade_date: str,
    ) -> Optional[Tuple[str, str]]:
        """
        确定同步日期范围
        
        Args:
            latest_trade_date: 最新交易日
            
        Returns:
            (start_date, end_date) 元组
            如果不需要同步返回 None
        """
        # 获取最后同步日期
        last_sync_date = await mongo_manager.get_last_sync_date(self.name)
        
        if last_sync_date is None:
            # 从未同步过，从前30天开始
            start_dt = datetime.now() - timedelta(days=self.INITIAL_SYNC_DAYS)
            start_date = start_dt.strftime("%Y%m%d")
            self.logger.info(f"First sync, starting from {start_date} (last {self.INITIAL_SYNC_DAYS} days)")
            return (start_date, latest_trade_date)
        
        if last_sync_date >= latest_trade_date:
            # 已同步到最新，无需再同步
            return None
        
        # 增量同步：从上次同步日期的下一天开始
        last_sync_dt = datetime.strptime(last_sync_date, "%Y%m%d")
        next_day = (last_sync_dt + timedelta(days=1)).strftime("%Y%m%d")
        
        self.logger.info(
            f"Incremental sync: last_sync={last_sync_date}, "
            f"syncing {next_day} -> {latest_trade_date}"
        )
        
        return (next_day, latest_trade_date)
