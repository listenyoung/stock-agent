"""
财务数据采集器

采集上市公司完整财务数据：
- 利润表 (income)
- 资产负债表 (balance_sheet)  
- 现金流量表 (cashflow)
- 财务指标 (fina_indicator)

初始化模式：同步5年历史数据
增量模式：同步最近8个季度数据
"""

from typing import Dict, Any, List
from datetime import datetime, date
import asyncio

from core.base import BaseCollector
from core.settings import settings
from core.managers import tushare_manager, mongo_manager


class FinaIndicatorCollector(BaseCollector):
    """
    财务数据采集器
    
    采集上市公司的完整财务数据：
    
    1. 利润表 (income): 营业收入、净利润、毛利率等
    2. 资产负债表 (balance_sheet): 总资产、负债、股东权益等
    3. 现金流量表 (cashflow): 经营/投资/筹资现金流
    4. 财务指标 (fina_indicator): ROE、ROA、EPS、成长性等
    
    同步策略:
    - 初始化: 同步所有股票近5年的财务数据
    - 增量: 同步每只股票最近8个季度数据
    
    调度时间:
    - 可通过 SYNC_FINA_INDICATOR_SCHEDULE 环境变量配置
    - 默认: 每月1号 09:00 (财报按季度披露，月度更新即可)
    """
    
    name = "fina_indicator"
    description = "采集财务数据 (三大报表 + 财务指标)"
    default_schedule = "0 9 1 * *"  # 默认: 每月1号 09:00
    
    # 财务数据集合名称
    COLLECTIONS = {
        "income_statement": "fina_income",
        "balance_sheet": "fina_balance",
        "cashflow_statement": "fina_cashflow",
        "financial_indicators": "fina_indicator",
    }
    
    # 批量处理参数
    BATCH_SIZE = 100  # 每批处理的股票数量
    SLEEP_INTERVAL = 0.05  # 请求间隔 (秒)
    
    @property
    def schedule(self) -> str:
        """从配置读取调度时间，未配置则使用默认值"""
        return getattr(settings.data_sync, 'fina_indicator_schedule', None) or self.default_schedule
    
    async def collect(self) -> Dict[str, Any]:
        """执行增量采集 (获取每只股票最近8个季度的财务数据)"""
        today = date.today().strftime("%Y%m%d")
        
        # 检查是否已同步 (按月检查，同一月份内不重复同步)
        if await mongo_manager.is_synced("fina_indicator", today, granularity="month"):
            self.logger.info(f"Fina indicator {today[:6]} already synced this month, skipping")
            return {"count": 0, "message": f"Already synced this month ({today[:6]})", "skipped": True}
        
        self.logger.info("Collecting fina_indicator (latest 8 quarters per stock)")
        
        # 增量模式：每只股票获取最近8个季度数据
        result = await self._sync_all_stocks(limit=8)
        
        # 记录同步完成
        await mongo_manager.record_sync(
            sync_type="fina_indicator",
            sync_date=today,
            count=result["count"],
        )
        
        return result
    
    async def init_sync(self, years: int = 5) -> Dict[str, Any]:
        """
        初始化同步：同步所有股票的历史财务数据
        
        Args:
            years: 同步年数，默认5年 (约20个季度)
        """
        # 计算需要获取的季度数 (每年4个季度)
        quarters = years * 4
        
        self.logger.info(f"Init sync fina_indicator: last {quarters} quarters ({years} years)")
        
        result = await self._sync_all_stocks(limit=quarters, include_delisted=True)
        
        # 创建索引
        await self._ensure_indexes()
        
        return result
    
    async def _sync_all_stocks(
        self, 
        limit: int = 8,
        include_delisted: bool = False,
    ) -> Dict[str, Any]:
        """
        同步所有股票的财务数据
        
        Args:
            limit: 每只股票获取的最大记录数 (按报告期降序)
            include_delisted: 是否包含退市股票
        """
        # 从数据库读取股票列表 (比调用 Tushare API 更快)
        query = {} if include_delisted else {"list_status": "L"}
        stocks = await mongo_manager.find_many(
            "stock_basic",
            query,
            projection={"ts_code": 1},
        )
        ts_codes = [s["ts_code"] for s in stocks]
        
        if not ts_codes:
            self.logger.warning("No stocks found in stock_basic, please sync stock_basic first")
            return {"count": 0, "success": 0, "error": 0, "message": "No stocks in database"}
        
        self.logger.info(f"Syncing {len(ts_codes)} stocks (limit={limit} per stock)...")
        
        return await self._batch_sync(ts_codes, limit=limit)
    
    async def _batch_sync(
        self,
        ts_codes: List[str],
        limit: int = 8,
    ) -> Dict[str, Any]:
        """
        批量同步财务数据 (三大报表 + 财务指标)
        
        Args:
            ts_codes: 股票代码列表
            limit: 每只股票获取的记录数限制
        """
        total = len(ts_codes)
        success_count = 0
        error_count = 0
        record_counts = {
            "income": 0,
            "balance": 0,
            "cashflow": 0,
            "indicator": 0,
        }
        
        for i in range(0, total, self.BATCH_SIZE):
            batch = ts_codes[i:i + self.BATCH_SIZE]
            batch_num = i // self.BATCH_SIZE + 1
            total_batches = (total + self.BATCH_SIZE - 1) // self.BATCH_SIZE
            
            self.logger.info(f"Batch {batch_num}/{total_batches}: {len(batch)} stocks")
            
            for ts_code in batch:
                try:
                    # 获取完整财务数据 (四类数据并行获取)
                    financial_data = await tushare_manager.get_financial_data(
                        ts_code=ts_code,
                        limit=limit,
                    )
                    
                    now = datetime.utcnow()
                    
                    # 存储利润表
                    income_records = financial_data.get("income_statement", [])
                    if income_records:
                        for r in income_records:
                            r["updated_at"] = now
                        await mongo_manager.bulk_upsert(
                            collection="fina_income",
                            documents=income_records,
                            key_fields=["ts_code", "end_date"],
                            batch_size=500,
                        )
                        record_counts["income"] += len(income_records)
                    
                    # 存储资产负债表
                    balance_records = financial_data.get("balance_sheet", [])
                    if balance_records:
                        for r in balance_records:
                            r["updated_at"] = now
                        await mongo_manager.bulk_upsert(
                            collection="fina_balance",
                            documents=balance_records,
                            key_fields=["ts_code", "end_date"],
                            batch_size=500,
                        )
                        record_counts["balance"] += len(balance_records)
                    
                    # 存储现金流量表
                    cashflow_records = financial_data.get("cashflow_statement", [])
                    if cashflow_records:
                        for r in cashflow_records:
                            r["updated_at"] = now
                        await mongo_manager.bulk_upsert(
                            collection="fina_cashflow",
                            documents=cashflow_records,
                            key_fields=["ts_code", "end_date"],
                            batch_size=500,
                        )
                        record_counts["cashflow"] += len(cashflow_records)
                    
                    # 存储财务指标
                    indicator_records = financial_data.get("financial_indicators", [])
                    if indicator_records:
                        for r in indicator_records:
                            r["updated_at"] = now
                        await mongo_manager.bulk_upsert(
                            collection="fina_indicator",
                            documents=indicator_records,
                            key_fields=["ts_code", "end_date"],
                            batch_size=500,
                        )
                        record_counts["indicator"] += len(indicator_records)
                    
                    success_count += 1
                    
                except Exception as e:
                    error_count += 1
                    self.logger.warning(f"Failed to sync {ts_code}: {e}")
                
                # 控制频率
                await asyncio.sleep(self.SLEEP_INTERVAL)
            
            # 批次进度日志
            total_records = sum(record_counts.values())
            self.logger.info(
                f"Progress: {min(i + self.BATCH_SIZE, total)}/{total}, "
                f"records: {total_records} (I:{record_counts['income']}, "
                f"B:{record_counts['balance']}, C:{record_counts['cashflow']}, "
                f"F:{record_counts['indicator']})"
            )
        
        total_records = sum(record_counts.values())
        self.logger.info(
            f"Sync completed: success={success_count}, error={error_count}, "
            f"total={total_records}"
        )
        self.logger.info(
            f"  Income: {record_counts['income']}, Balance: {record_counts['balance']}, "
            f"Cashflow: {record_counts['cashflow']}, Indicator: {record_counts['indicator']}"
        )
        
        return {
            "count": total_records,
            "success": success_count,
            "error": error_count,
            "details": record_counts,
            "message": f"Synced {total_records} records from {success_count} stocks",
        }
    
    async def _ensure_indexes(self) -> None:
        """确保所有财务数据表的索引存在"""
        collections = ["fina_income", "fina_balance", "fina_cashflow", "fina_indicator"]
        
        self.logger.info(f"Creating indexes for {len(collections)} collections...")
        
        for collection in collections:
            # 复合唯一索引
            await mongo_manager.create_index(
                collection,
                [("ts_code", 1), ("end_date", -1)],
                unique=True,
            )
            
            # 单字段索引
            await mongo_manager.create_index(
                collection,
                [("end_date", -1)],
            )
            await mongo_manager.create_index(
                collection,
                [("ts_code", 1)],
            )
            
            self.logger.info(f"  {collection} indexes created ✓")
        
        self.logger.info("All indexes created ✓")


# 全局实例
fina_indicator_collector = FinaIndicatorCollector()
