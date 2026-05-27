"""
股票基础信息采集器

采集股票基础信息，并合并最新交易日的指标数据（PE/PB/市值/换手率等）。
使用 BulkWrite 批量写入提高效率。
"""

from typing import Dict, Any
from datetime import datetime, date

from core.base import BaseCollector
from core.settings import settings
from core.managers import tushare_manager, mongo_manager


def _add_financial_metrics(doc: Dict, daily_metrics: Dict) -> None:
    """
    将财务与交易指标写入 doc（就地修改）。
    - 市值：total_mv/circ_mv（从万元转换为亿元）
    - 估值：pe/pb/pe_ttm/ps/ps_ttm（过滤 NaN/None）
    - 交易：turnover_rate/volume_ratio（过滤 NaN/None）
    - 股本：total_share/float_share（万股，过滤 NaN/None）
    """
    # 市值（万元 -> 亿元）
    for field in ["total_mv", "circ_mv"]:
        if field in daily_metrics and daily_metrics[field] is not None:
            try:
                value = float(daily_metrics[field])
                if value == value:  # 非 NaN
                    doc[field] = value / 10000
            except (ValueError, TypeError):
                pass

    # 估值指标 (保留字段，即使为空也存储 None)
    for field in ["pe", "pb", "pe_ttm", "ps", "ps_ttm", "dv_ratio", "dv_ttm"]:
        if field in daily_metrics:
            val = daily_metrics[field]
            if val is None:
                doc[field] = None  # 亏损公司 PE 为空，也存储
            else:
                try:
                    value = float(val)
                    if value == value:  # 非 NaN
                        doc[field] = value
                    else:
                        doc[field] = None  # NaN 也存为 None
                except (ValueError, TypeError):
                    doc[field] = None

    # 交易指标
    for field in ["turnover_rate", "volume_ratio"]:
        if field in daily_metrics and daily_metrics[field] is not None:
            try:
                value = float(daily_metrics[field])
                if value == value:  # 过滤 NaN
                    doc[field] = value
            except (ValueError, TypeError):
                pass

    # 股本数据（万股）
    for field in ["total_share", "float_share"]:
        if field in daily_metrics and daily_metrics[field] is not None:
            try:
                value = float(daily_metrics[field])
                if value == value:  # 过滤 NaN
                    doc[field] = value
            except (ValueError, TypeError):
                pass


class StockBasicCollector(BaseCollector):
    """
    股票基础信息采集器
    
    采集所有上市股票的基础信息：
    - 股票代码、名称、行业、上市日期等
    - 最新交易日的估值指标（PE/PB/市值/换手率等）
    
    优化策略:
    - 使用 BulkWrite 批量写入
    - 检查是否已同步避免重复
    - 合并 daily_basic 指标数据
    
    调度时间:
    - 可通过 SYNC_STOCK_BASIC_SCHEDULE 环境变量配置
    - 默认: 每个交易日 9:00
    """
    
    name = "stock_basic"
    description = "采集股票基础信息和估值指标"
    default_schedule = "0 9 * * 1-5"  # 默认: 每个交易日 9:00
    
    @property
    def schedule(self) -> str:
        """从配置读取调度时间，未配置则使用默认值"""
        return settings.data_sync.stock_basic_schedule or self.default_schedule
    
    async def collect(self) -> Dict[str, Any]:
        """执行采集"""
        today = date.today().strftime("%Y%m%d")
        
        # 检查是否已同步
        if await mongo_manager.is_synced("stock_basic", today):
            self.logger.info(f"Stock basic {today} already synced, skipping")
            return {"count": 0, "message": f"Already synced {today}", "skipped": True}
        
        # Step 1: 获取股票基础信息
        records = await tushare_manager.get_stock_basic()
        
        if not records:
            return {"count": 0, "message": "No data"}
        
        self.logger.info(f"Fetched {len(records)} stocks")
        
        # Step 2: 获取最新交易日的 daily_basic 数据
        latest_trade_date = await tushare_manager.get_latest_trade_date()
        daily_basic_map: Dict[str, Dict] = {}
        
        if latest_trade_date:
            daily_basic = await tushare_manager.get_daily_basic(trade_date=latest_trade_date)
            if daily_basic:
                for item in daily_basic:
                    ts_code = item.get("ts_code")
                    if ts_code:
                        daily_basic_map[ts_code] = item
                self.logger.info(f"Fetched {len(daily_basic_map)} daily_basic records for {latest_trade_date}")
        
        # Step 3: 合并数据
        merged_count = 0
        for record in records:
            ts_code = record.get("ts_code")
            record["updated_at"] = datetime.utcnow()
            
            if ts_code and ts_code in daily_basic_map:
                _add_financial_metrics(record, daily_basic_map[ts_code])
                merged_count += 1
        
        self.logger.info(f"Merged {merged_count}/{len(records)} records with daily_basic")
        
        # Step 4: 批量 upsert 到 MongoDB
        result = await mongo_manager.bulk_upsert(
            collection="stock_basic",
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
            sync_type="stock_basic",
            sync_date=today,
            count=result["total"],
        )
        
        return {
            "count": result["total"],
            "upserted": result["upserted"],
            "modified": result["modified"],
            "merged": merged_count,
            "message": f"Synced {result['total']} stocks with {merged_count} metrics",
        }
