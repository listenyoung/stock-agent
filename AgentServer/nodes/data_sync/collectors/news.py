"""
新闻采集器 (V2.1 - AKShare + RAG 向量化版)

采集逻辑:
1. 从 limit_list 表获取今日涨跌停股票
2. 使用 AKShare 采集这些股票的新闻
3. 保存到 MongoDB news 表
4. 向量化存入 Milvus (支持 RAG 检索)

调度时间: 大盘统计完成后执行 (或手动触发)
"""

import asyncio
from typing import Dict, Any, List
from datetime import datetime

from core.base import BaseCollector
from core.settings import settings
from core.managers import mongo_manager
from core.managers.akshare_manager import akshare_manager
from core.managers.milvus_manager import milvus_manager


class NewsCollector(BaseCollector):
    """
    新闻采集器 (AKShare版)
    
    采集策略:
    - 从 limit_list 获取今日涨跌停股票
    - 对每只股票采集最新新闻
    - 限流处理避免被封禁
    
    调度时间:
    - 可通过 SYNC_NEWS_SCHEDULE 环境变量配置
    - 默认: 每日 18:30 (大盘统计完成后)
    """
    
    name = "news"
    description = "采集涨跌停股票新闻"
    default_schedule = "30 18 * * 1-5"  # 默认: 工作日 18:30
    
    @property
    def schedule(self) -> str:
        """从配置读取调度时间，未配置则使用默认值"""
        return settings.data_sync.news_schedule or self.default_schedule
    
    async def collect(self) -> Dict[str, Any]:
        """
        执行采集
        
        流程:
        0. 检查今日是否已同步，避免重复采集
        1. 初始化 AKShare (按需)
        2. 获取今日涨跌停股票列表
        3. 批量采集新闻
        4. 保存到数据库
        5. 记录同步日期
        """
        today = datetime.now().strftime("%Y%m%d")
        
        # ==================== 0. 检查是否已同步 ====================
        if await mongo_manager.is_synced(self.name, today):
            self.logger.info(f"News for {today} already synced, skipping")
            return {"count": 0, "message": f"Already synced {today}", "skipped": True}
        
        # ==================== 1. 确保 AKShare 已初始化 ====================
        if not akshare_manager.is_initialized:
            await akshare_manager.initialize()
        
        # ==================== 2. 获取涨跌停股票 ====================
        
        limit_stocks = await self._get_limit_stocks(today)
        
        if not limit_stocks:
            self.logger.info(f"No limit stocks found for {today}")
            return {"count": 0, "message": "No limit stocks"}
        
        self.logger.info(f"Found {len(limit_stocks)} limit stocks for {today}")
        
        # ==================== 3. 批量采集新闻 ====================
        
        batch_result = await akshare_manager.get_stock_news_batch(
            symbols=limit_stocks,
            limit_per_stock=10,  # 每只股票最多10条新闻
            batch_size=5,        # 每批5只股票
            delay=0.3,           # 每次请求间隔0.3秒
        )
        
        news_list = batch_result.get("news_list", [])
        
        if not news_list:
            # 即使没有新闻也记录同步，避免重复尝试
            await mongo_manager.record_sync(self.name, today, 0)
            return {
                "count": 0,
                "stocks_processed": batch_result.get("success_count", 0),
                "errors": batch_result.get("error_count", 0),
                "message": "No news collected",
            }
        
        # ==================== 4. 保存到数据库 ====================
        
        saved_count = await self._save_news(news_list)
        
        # ==================== 5. 记录同步日期 ====================
        await mongo_manager.record_sync(self.name, today, saved_count)
        self.logger.info(f"Recorded sync for {today}, count={saved_count}")
        
        return {
            "count": saved_count,
            "stocks_processed": batch_result.get("success_count", 0),
            "errors": batch_result.get("error_count", 0),
            "message": f"Synced {saved_count} news from {len(limit_stocks)} stocks",
        }
    
    async def _get_limit_stocks(self, trade_date: str) -> List[str]:
        """
        获取指定日期的涨跌停股票
        
        Args:
            trade_date: 交易日期 (YYYYMMDD)
            
        Returns:
            股票代码列表 (如 ["000001.SZ", "600000.SH"])
        """
        # 从 limit_list 表查询涨跌停股票
        limit_records = await mongo_manager.find_many(
            "limit_list",
            {"trade_date": trade_date},
            projection={"ts_code": 1, "limit": 1, "_id": 0},
        )
        
        if not limit_records:
            # 尝试获取最新日期的数据
            latest = await mongo_manager.find_one(
                "limit_list",
                {},
                sort=[("trade_date", -1)],
                projection={"trade_date": 1},
            )
            if latest:
                latest_date = latest.get("trade_date")
                self.logger.info(f"No data for {trade_date}, using latest: {latest_date}")
                limit_records = await mongo_manager.find_many(
                    "limit_list",
                    {"trade_date": latest_date},
                    projection={"ts_code": 1, "limit": 1, "_id": 0},
                )
        
        # 提取股票代码 (涨停U和跌停D都采集)
        stocks = [r.get("ts_code") for r in limit_records if r.get("ts_code")]
        
        # 去重
        return list(set(stocks))
    
    async def _save_news(self, news_list: List[Dict[str, Any]]) -> int:
        """
        保存新闻到数据库 + 向量化存入 Milvus
        
        Args:
            news_list: 新闻列表
            
        Returns:
            保存成功的数量
        """
        if not news_list:
            return 0
        
        # 添加唯一标识和时间戳
        for news in news_list:
            # 使用 ts_code + datetime + title前50字 作为唯一标识
            news["_key"] = f"{news.get('ts_code', '')}_{news.get('datetime', '')}_{news.get('title', '')[:50]}"
            news["created_at"] = datetime.utcnow()
        
        # ==================== 1. 保存到 MongoDB ====================
        result = await mongo_manager.bulk_upsert(
            collection="news",
            documents=news_list,
            key_fields=["_key"],
        )
        
        saved_count = result.get("upserted", 0) + result.get("modified", 0)
        self.logger.info(f"Saved {saved_count} news to MongoDB")
        
        # ==================== 2. 向量化存入 Milvus ====================
        await self._vectorize_news(news_list)
        
        return saved_count
    
    async def _vectorize_news(self, news_list: List[Dict[str, Any]]) -> None:
        """
        将新闻向量化并存入 Milvus
        
        Args:
            news_list: 新闻列表
        """
        if not news_list:
            return
        
        # 检查 Milvus 是否可用
        if milvus_manager.is_disabled():
            self.logger.debug("Milvus disabled, skipping vectorization")
            return
        
        # 确保 Milvus 已初始化
        if not milvus_manager.is_initialized:
            try:
                await milvus_manager.initialize()
            except Exception as e:
                self.logger.warning(f"Failed to initialize Milvus: {e}")
                return
        
        self.logger.info(f"Vectorizing {len(news_list)} news to Milvus...")
        
        # 批量插入
        result = await milvus_manager.insert_news_batch(
            news_list=news_list,
            batch_size=10,  # 每批10条，控制 embedding 并发
        )
        
        self.logger.info(
            f"Milvus vectorization: success={result.get('success', 0)}, "
            f"failed={result.get('failed', 0)}"
        )
    
    async def collect_for_stocks(self, stocks: List[str]) -> Dict[str, Any]:
        """
        为指定股票采集新闻 (手动调用)
        
        Args:
            stocks: 股票代码列表
            
        Returns:
            采集结果
        """
        if not stocks:
            return {"count": 0, "message": "No stocks provided"}
        
        # 确保 AKShare 已初始化
        if not akshare_manager.is_initialized:
            await akshare_manager.initialize()
        
        self.logger.info(f"Collecting news for {len(stocks)} stocks")
        
        batch_result = await akshare_manager.get_stock_news_batch(
            symbols=stocks,
            limit_per_stock=10,
            batch_size=5,
            delay=0.3,
        )
        
        news_list = batch_result.get("news_list", [])
        saved_count = await self._save_news(news_list) if news_list else 0
        
        return {
            "count": saved_count,
            "stocks_processed": batch_result.get("success_count", 0),
            "errors": batch_result.get("error_count", 0),
            "message": f"Synced {saved_count} news",
        }
