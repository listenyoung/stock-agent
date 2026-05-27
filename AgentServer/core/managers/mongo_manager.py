"""
MongoDB 管理器

负责:
- 业务数据 CRUD
- 索引管理 (ensure_indexes)
- 高性能批量写入 (BulkWrite)
- 聚合查询
"""

from typing import Optional, Any, List, Dict
from datetime import datetime

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import ASCENDING, DESCENDING, IndexModel, UpdateOne, InsertOne

from .base import BaseManager
from ..settings import settings


class MongoManager(BaseManager):
    """
    MongoDB 资源管理器
    
    特性:
    - 连接池管理 (默认 max_pool_size=100)
    - 索引自动创建
    - 高性能批量写入
    """
    
    def __init__(self):
        super().__init__()
        self._client: Optional[AsyncIOMotorClient] = None
        self._db: Optional[AsyncIOMotorDatabase] = None
        self._config = settings.mongo
    
    async def initialize(self) -> None:
        """初始化 MongoDB 连接"""
        if self._initialized:
            return
        
        self.logger.info(
            f"Connecting to MongoDB: {self._config.host}:{self._config.port} "
            f"(pool_size={self._config.max_pool_size})"
        )
        
        self._client = AsyncIOMotorClient(
            self._config.url,
            maxPoolSize=self._config.max_pool_size,
        )
        self._db = self._client[self._config.database]
        
        # 测试连接
        await self._client.admin.command("ping")
        
        # 创建索引
        await self._ensure_indexes()
        
        self._initialized = True
        self.logger.info(f"MongoDB connected, database: {self._config.database} ✓")
    
    async def shutdown(self) -> None:
        """关闭连接"""
        if self._client:
            self._client.close()
            self._client = None
            self._db = None
        
        self._initialized = False
        self.logger.info("MongoDB disconnected")
    
    async def health_check(self) -> bool:
        """健康检查"""
        try:
            if self._client:
                await self._client.admin.command("ping")
                return True
        except Exception:
            pass
        return False
    
    @property
    def db(self) -> AsyncIOMotorDatabase:
        """获取数据库实例"""
        self._ensure_initialized()
        return self._db
    
    async def _ensure_indexes(self) -> None:
        """
        确保索引存在
        
        在初始化时自动调用，为所有业务表创建必要的索引。
        """
        self.logger.info("Ensuring MongoDB indexes...")
        
        # 用户表
        await self._db.users.create_indexes([
            IndexModel([("username", ASCENDING)], unique=True),
            IndexModel([("email", ASCENDING)], unique=True),
        ])
        
        # 任务表
        await self._db.tasks.create_indexes([
            IndexModel([("task_id", ASCENDING)], unique=True),
            IndexModel([("user_id", ASCENDING)]),
            IndexModel([("status", ASCENDING)]),
            IndexModel([("created_at", DESCENDING)]),
            IndexModel([("trace_id", ASCENDING)]),
            IndexModel([("node_id", ASCENDING)]),  # 处理节点索引
        ])
        
        # 股票基础信息表
        await self._db.stock_basic.create_indexes([
            IndexModel([("ts_code", ASCENDING)], unique=True),
            IndexModel([("name", ASCENDING)]),
            IndexModel([("industry", ASCENDING)]),
            IndexModel([("list_status", ASCENDING)]),
            IndexModel([("total_mv", -1)]),
            IndexModel([("pe", 1)]),
            IndexModel([("pb", 1)]),
        ])
        
        # 日线数据表 (高频查询)
        await self._db.stock_daily.create_indexes([
            IndexModel(
                [("ts_code", ASCENDING), ("trade_date", DESCENDING)],
                unique=True,
            ),
            IndexModel([("trade_date", DESCENDING)]),
        ])
        
        # 指数基础信息表
        await self._db.index_basic.create_indexes([
            IndexModel([("ts_code", ASCENDING)], unique=True),
            IndexModel([("name", ASCENDING)]),
            IndexModel([("market", ASCENDING)]),
            IndexModel([("index_type", ASCENDING)]),
        ])
        
        # 指数日线数据表 (高频查询)
        await self._db.index_daily.create_indexes([
            IndexModel(
                [("ts_code", ASCENDING), ("trade_date", DESCENDING)],
                unique=True,
            ),
            IndexModel([("trade_date", DESCENDING)]),
        ])
        
        # 行业资金流向表
        await self._db.moneyflow_industry.create_indexes([
            IndexModel(
                [("ts_code", ASCENDING), ("trade_date", DESCENDING)],
                unique=True,
            ),
            IndexModel([("trade_date", DESCENDING)]),
            IndexModel([("name", ASCENDING)]),
            IndexModel([("net_amount", DESCENDING)]),  # 按净流入排序
        ])
        
        # 概念板块资金流向表
        await self._db.moneyflow_concept.create_indexes([
            IndexModel(
                [("ts_code", ASCENDING), ("trade_date", DESCENDING)],
                unique=True,
            ),
            IndexModel([("trade_date", DESCENDING)]),
            IndexModel([("name", ASCENDING)]),
            IndexModel([("net_amount", DESCENDING)]),  # 按净流入排序
        ])
        
        # 涨跌停数据表
        await self._db.limit_list.create_indexes([
            IndexModel(
                [("ts_code", ASCENDING), ("trade_date", DESCENDING)],
                unique=True,
            ),
            IndexModel([("trade_date", DESCENDING)]),
            IndexModel([("limit", ASCENDING)]),  # U-涨停 D-跌停
            IndexModel([("industry", ASCENDING)]),
            IndexModel([("limit_times", DESCENDING)]),  # 连续涨停次数
        ])
        
        # 板块/行业排名表
        await self._db.sector_ranking.create_indexes([
            IndexModel(
                [("trade_date", DESCENDING), ("ranking_type", ASCENDING), ("rank", ASCENDING)],
                unique=True,
            ),
            IndexModel([("trade_date", DESCENDING)]),
            IndexModel([("ranking_type", ASCENDING)]),
        ])
        
        # 每日统计表
        await self._db.daily_stats.create_indexes([
            IndexModel([("trade_date", DESCENDING)], unique=True),
        ])
        
        # 每日指标表 (PE/PB/换手率/市值等)
        await self._db.daily_basic.create_indexes([
            IndexModel(
                [("ts_code", ASCENDING), ("trade_date", DESCENDING)],
                unique=True,
            ),
            IndexModel([("trade_date", DESCENDING)]),
            IndexModel([("pe_ttm", ASCENDING)]),  # 常用排序字段
            IndexModel([("pb", ASCENDING)]),
            IndexModel([("total_mv", DESCENDING)]),  # 市值排序
        ])
        
        # 市场分析表 (情绪周期分析结果)
        await self._db.market_analysis.create_indexes([
            IndexModel([("trade_date", DESCENDING)], unique=True),
            IndexModel([("cycle", ASCENDING)]),
        ])
        
        # 新闻表
        await self._db.news.create_indexes([
            IndexModel([("_key", ASCENDING)], unique=True),
            IndexModel([("datetime", DESCENDING)]),
            IndexModel([("title", "text"), ("content", "text")]),  # 全文索引
        ])
        
        # 策略表
        await self._db.strategies.create_indexes([
            IndexModel([("strategy_id", ASCENDING)], unique=True),
            IndexModel([("user_id", ASCENDING)]),
        ])
        
        # 数据同步记录表 (每个 sync_type 只保留一条记录)
        await self._db.sync_records.create_indexes([
            IndexModel([("sync_type", ASCENDING)], unique=True),
        ])

        # Agent 运行轨迹、消息、记忆
        await self._db.agent_runs.create_indexes([
            IndexModel([("run_id", ASCENDING)], unique=True),
            IndexModel([("thread_id", ASCENDING), ("started_at", DESCENDING)]),
            IndexModel([("user_id", ASCENDING), ("started_at", DESCENDING)]),
            IndexModel([("status", ASCENDING)]),
        ])

        await self._db.agent_events.create_indexes([
            IndexModel([("run_id", ASCENDING), ("sequence", ASCENDING)]),
            IndexModel([("thread_id", ASCENDING), ("created_at", DESCENDING)]),
            IndexModel([("event", ASCENDING)]),
        ])

        await self._db.agent_jobs.create_indexes([
            IndexModel([("job_id", ASCENDING)], unique=True),
            IndexModel([("run_id", ASCENDING)]),
            IndexModel([("user_id", ASCENDING), ("status", ASCENDING), ("updated_at", DESCENDING)]),
            IndexModel([("current_node", ASCENDING)]),
            IndexModel([("heartbeat_at", DESCENDING)]),
        ])

        await self._db.agent_tool_calls.create_indexes([
            IndexModel([("idempotency_key", ASCENDING)], unique=True),
            IndexModel([("run_id", ASCENDING), ("created_at", DESCENDING)]),
            IndexModel([("user_id", ASCENDING), ("tool_name", ASCENDING)]),
            IndexModel([("status", ASCENDING)]),
            IndexModel([("external_task_id", ASCENDING)]),
        ])

        await self._db.agent_messages.create_indexes([
            IndexModel([("thread_id", ASCENDING), ("created_at", ASCENDING)]),
            IndexModel([("user_id", ASCENDING), ("created_at", DESCENDING)]),
            IndexModel([("run_id", ASCENDING)]),
        ])

        await self._db.agent_memories.create_indexes([
            IndexModel([("memory_id", ASCENDING)], unique=True),
            IndexModel([("user_id", ASCENDING), ("importance", DESCENDING)]),
            IndexModel([("user_id", ASCENDING), ("hit_count", DESCENDING)]),
            IndexModel([("user_id", ASCENDING), ("status", ASCENDING)]),
            IndexModel([("user_id", ASCENDING), ("pinned", DESCENDING)]),
            IndexModel([("expires_at", ASCENDING)]),
            IndexModel([("source_run_id", ASCENDING)]),
            IndexModel([("content", "text")]),
        ])

        await self._db.agent_checkpoints.create_indexes([
            IndexModel([("run_id", ASCENDING), ("sequence", ASCENDING)]),
            IndexModel([("thread_id", ASCENDING), ("created_at", DESCENDING)]),
            IndexModel([("stage", ASCENDING)]),
        ])

        await self._db.agent_feedback.create_indexes([
            IndexModel([("feedback_id", ASCENDING)], unique=True),
            IndexModel([("run_id", ASCENDING)]),
            IndexModel([("user_id", ASCENDING), ("created_at", DESCENDING)]),
            IndexModel([("rating", ASCENDING)]),
        ])

        await self._db.agent_eval_cases.create_indexes([
            IndexModel([("case_id", ASCENDING)], unique=True),
            IndexModel([("user_id", ASCENDING), ("created_at", DESCENDING)]),
            IndexModel([("scope", ASCENDING)]),
            IndexModel([("tags", ASCENDING)]),
        ])

        await self._db.agent_eval_runs.create_indexes([
            IndexModel([("eval_id", ASCENDING)], unique=True),
            IndexModel([("user_id", ASCENDING), ("created_at", DESCENDING)]),
            IndexModel([("status", ASCENDING)]),
        ])

        await self._db.agent_eval_results.create_indexes([
            IndexModel([("result_id", ASCENDING)], unique=True),
            IndexModel([("eval_id", ASCENDING), ("overall", ASCENDING)]),
            IndexModel([("run_id", ASCENDING)]),
            IndexModel([("user_id", ASCENDING), ("created_at", DESCENDING)]),
            IndexModel([("sample_type", ASCENDING)]),
        ])

        await self._db.agent_training_exports.create_indexes([
            IndexModel([("export_id", ASCENDING)], unique=True),
            IndexModel([("user_id", ASCENDING), ("created_at", DESCENDING)]),
        ])

        await self._db.agent_tool_approvals.create_indexes([
            IndexModel([("approval_id", ASCENDING)], unique=True),
            IndexModel([("run_id", ASCENDING)]),
            IndexModel([("user_id", ASCENDING), ("status", ASCENDING)]),
        ])

        await self._db.agent_user_profiles.create_indexes([
            IndexModel([("user_id", ASCENDING)], unique=True),
            IndexModel([("updated_at", DESCENDING)]),
        ])
        
        self.logger.info("MongoDB indexes ensured ✓")
    
    # ==================== 通用 CRUD ====================
    
    async def insert_one(self, collection: str, document: dict) -> str:
        """插入单条文档"""
        self._ensure_initialized()
        document["created_at"] = datetime.utcnow()
        result = await self._db[collection].insert_one(document)
        return str(result.inserted_id)
    
    async def insert_many(self, collection: str, documents: List[dict]) -> List[str]:
        """批量插入"""
        self._ensure_initialized()
        now = datetime.utcnow()
        for doc in documents:
            doc["created_at"] = now
        result = await self._db[collection].insert_many(documents)
        return [str(id) for id in result.inserted_ids]
    
    async def find_many(
        self,
        collection: str,
        filter: dict,
        projection: Optional[dict] = None,
        sort: Optional[List[tuple]] = None,
        limit: int = 0,
        skip: int = 0,
    ) -> List[dict]:
        """查询多条文档"""
        self._ensure_initialized()
        
        cursor = self._db[collection].find(filter, projection)
        
        if sort:
            cursor = cursor.sort(sort)
        if skip:
            cursor = cursor.skip(skip)
        if limit:
            cursor = cursor.limit(limit)
        
        return await cursor.to_list(length=limit or None)
    
    async def update_one(
        self,
        collection: str,
        filter: dict,
        update: dict,
        upsert: bool = False,
    ) -> int:
        """更新单条文档"""
        self._ensure_initialized()
        
        # 检查是否包含任何 MongoDB 更新操作符
        has_operator = any(key.startswith("$") for key in update.keys())
        
        if not has_operator:
            # 如果没有操作符，自动包装为 $set
            update = {"$set": update}
        
        # 添加 updated_at 时间戳
        if "$set" in update:
            update["$set"]["updated_at"] = datetime.utcnow()
        else:
            update["$set"] = {"updated_at": datetime.utcnow()}
        
        result = await self._db[collection].update_one(filter, update, upsert=upsert)
        return result.modified_count
    
    async def update_many(
        self,
        collection: str,
        filter: dict,
        update: dict,
    ) -> int:
        """更新多条文档"""
        self._ensure_initialized()
        
        # 检查是否包含任何 MongoDB 更新操作符
        has_operator = any(key.startswith("$") for key in update.keys())
        
        if not has_operator:
            # 如果没有操作符，自动包装为 $set
            update = {"$set": update}
        
        # 添加 updated_at 时间戳
        if "$set" in update:
            update["$set"]["updated_at"] = datetime.utcnow()
        else:
            update["$set"] = {"updated_at": datetime.utcnow()}
        
        result = await self._db[collection].update_many(filter, update)
        return result.modified_count
    
    async def delete_one(self, collection: str, filter: dict) -> int:
        """删除单条文档"""
        self._ensure_initialized()
        result = await self._db[collection].delete_one(filter)
        return result.deleted_count
    
    async def delete_many(self, collection: str, filter: dict) -> int:
        """删除多条文档"""
        self._ensure_initialized()
        result = await self._db[collection].delete_many(filter)
        return result.deleted_count
    
    async def count(self, collection: str, filter: dict) -> int:
        """统计数量"""
        self._ensure_initialized()
        return await self._db[collection].count_documents(filter)
    
    async def aggregate(
        self,
        collection: str,
        pipeline: List[dict],
    ) -> List[dict]:
        """聚合查询"""
        self._ensure_initialized()
        cursor = self._db[collection].aggregate(pipeline)
        return await cursor.to_list(length=None)
    
    # ==================== 高性能批量写入 ====================
    
    async def bulk_upsert(
        self,
        collection: str,
        documents: List[dict],
        key_fields: List[str],
        batch_size: int = 1000,
    ) -> dict:
        """
        高性能批量 upsert
        
        使用 MongoDB BulkWrite 实现高效批量写入。
        
        Args:
            collection: 集合名
            documents: 文档列表
            key_fields: 用于匹配的字段名列表 (支持复合键)
            batch_size: 每批次大小 (默认 1000)
            
        Returns:
            {
                "matched": int,     # 匹配数
                "modified": int,    # 修改数
                "upserted": int,    # 新插入数
                "total": int,       # 总处理数
            }
        """
        self._ensure_initialized()
        
        if not documents:
            return {"matched": 0, "modified": 0, "upserted": 0, "total": 0}
        
        total_matched = 0
        total_modified = 0
        total_upserted = 0
        
        now = datetime.utcnow()
        
        # 分批处理
        for i in range(0, len(documents), batch_size):
            batch = documents[i:i + batch_size]
            
            operations = []
            for doc in batch:
                doc["updated_at"] = now
                # 构建复合键查询条件
                filter_query = {k: doc[k] for k in key_fields}
                operations.append(
                    UpdateOne(
                        filter_query,
                        {"$set": doc},
                        upsert=True,
                    )
                )
            
            result = await self._db[collection].bulk_write(
                operations,
                ordered=False,  # 无序执行，提高性能
            )
            
            total_matched += result.matched_count
            total_modified += result.modified_count
            total_upserted += result.upserted_count
        
        return {
            "matched": total_matched,
            "modified": total_modified,
            "upserted": total_upserted,
            "total": len(documents),
        }
    
    async def bulk_insert(
        self,
        collection: str,
        documents: List[dict],
        batch_size: int = 1000,
        ordered: bool = False,
    ) -> int:
        """
        高性能批量插入
        
        Args:
            collection: 集合名
            documents: 文档列表
            batch_size: 每批次大小
            ordered: 是否有序插入
            
        Returns:
            插入的文档数
        """
        self._ensure_initialized()
        
        if not documents:
            return 0
        
        now = datetime.utcnow()
        total_inserted = 0
        
        for i in range(0, len(documents), batch_size):
            batch = documents[i:i + batch_size]
            
            for doc in batch:
                doc["created_at"] = now
            
            try:
                result = await self._db[collection].insert_many(
                    batch,
                    ordered=ordered,
                )
                total_inserted += len(result.inserted_ids)
            except Exception as e:
                # 如果是 unordered 模式，可能部分成功
                self.logger.warning(f"Bulk insert partial failure: {e}")
        
        return total_inserted
    
    # ==================== 数据同步辅助 ====================
    
    async def record_sync(
        self,
        sync_type: str,
        sync_date: str,
        count: int = 0,
    ) -> None:
        """
        记录数据同步 (更新最后同步日期)
        
        每个 sync_type 只保留一条记录，更新 sync_date 字段。
        
        Args:
            sync_type: 同步类型 (stock_basic, stock_daily, news, etc.)
            sync_date: 最后同步日期 (YYYYMMDD)
            count: 本次同步数量 (可选，用于记录)
        """
        await self.update_one(
            "sync_records",
            {"sync_type": sync_type},
            {
                "sync_type": sync_type,
                "sync_date": sync_date,
                "last_count": count,
                "updated_at": datetime.utcnow(),
            },
            upsert=True,
        )
    
    async def is_synced(self, sync_type: str, sync_date: str, granularity: str = "day") -> bool:
        """
        检查指定日期是否已同步
        
        Args:
            sync_type: 同步类型
            sync_date: 同步日期 (YYYYMMDD 格式)
            granularity: 粒度 - "day" 按天检查, "month" 按月检查
        
        Returns:
            如果 sync_date <= 记录的 sync_date（按指定粒度），则认为已同步
        """
        record = await self.find_one(
            "sync_records",
            {"sync_type": sync_type},
        )
        if not record:
            return False
        last_sync = record.get("sync_date", "")
        
        if granularity == "month":
            # 按月比较: 只比较 YYYYMM
            return sync_date[:6] <= last_sync[:6]
        else:
            # 按天比较
            return sync_date <= last_sync
    
    async def get_last_sync_date(self, sync_type: str) -> Optional[str]:
        """
        获取最后同步日期
        
        Args:
            sync_type: 同步类型
            
        Returns:
            最后同步日期 (YYYYMMDD 格式)，从未同步过返回 None
        """
        record = await self.find_one(
            "sync_records",
            {"sync_type": sync_type},
        )
        return record.get("sync_date") if record else None
    
    async def find_one(
        self,
        collection: str,
        filter: dict,
        projection: Optional[dict] = None,
        sort: Optional[list] = None,
    ) -> Optional[dict]:
        """
        查询单条文档 (支持排序)
        
        Args:
            collection: 集合名
            filter: 过滤条件
            projection: 投影
            sort: 排序规则
        """
        self._ensure_initialized()
        
        if sort:
            cursor = self._db[collection].find(filter, projection).sort(sort).limit(1)
            results = await cursor.to_list(length=1)
            return results[0] if results else None
        
        return await self._db[collection].find_one(filter, projection)


# ==================== 全局单例 ====================
mongo_manager = MongoManager()
