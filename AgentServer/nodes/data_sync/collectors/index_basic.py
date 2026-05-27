"""
指数基础信息采集器

同步主要市场的指数基础信息：
- SSE: 上交所指数
- SZSE: 深交所指数
- SW: 申万指数
- CSI: 中证指数
"""

from typing import Dict, Any
from datetime import date

from core.base import BaseCollector
from core.settings import settings
from core.managers import tushare_manager, mongo_manager


class IndexBasicCollector(BaseCollector):
    """
    指数基础信息采集器
    
    采集所有主要市场指数的基础信息：
    - 指数代码
    - 名称
    - 市场
    - 发布商
    - 类型
    - 等
    
    优化策略:
    - 使用 BulkWrite 批量写入
    - 检查是否已同步避免重复
    
    调度时间:
    - 可通过 SYNC_INDEX_BASIC_SCHEDULE 环境变量配置
    - 默认: 每个交易日 9:00
    """
    
    name = "index_basic"
    description = "采集指数基础信息"
    default_schedule = "0 9 * * 1-5"  # 默认: 每个交易日 9:00
    
    @property
    def schedule(self) -> str:
        """从配置读取调度时间，未配置则使用默认值"""
        return settings.data_sync.index_basic_schedule or self.default_schedule
    
    async def collect(self) -> Dict[str, Any]:
        """执行采集"""
        today = date.today().strftime("%Y%m%d")
        
        # 检查是否已同步
        if await mongo_manager.is_synced("index_basic", today):
            self.logger.info(f"Index basic {today} already synced, skipping")
            return {"count": 0, "message": f"Already synced {today}", "skipped": True}
        
        # 获取所有主要市场的指数基础信息
        records = await tushare_manager.get_all_index_basic()
        
        if not records:
            return {"count": 0, "message": "No data"}
        
        self.logger.info(f"Fetched {len(records)} indices from all markets")
        
        # 批量 upsert 到 MongoDB (使用 BulkWrite)
        result = await mongo_manager.bulk_upsert(
            collection="index_basic",
            documents=records,
            key_fields=["ts_code"],
            batch_size=1000,
        )
        
        self.logger.info(
            f"Bulk write completed: total={result['total']}, "
            f"upserted={result['upserted']}, modified={result['modified']}"
        )
        
        # 记录同步完成
        await mongo_manager.record_sync(
            sync_type="index_basic",
            sync_date=today,
            count=result["total"],
        )
        
        return {
            "count": result["total"],
            "upserted": result["upserted"],
            "modified": result["modified"],
            "message": f"Synced {result['total']} indices",
        }
