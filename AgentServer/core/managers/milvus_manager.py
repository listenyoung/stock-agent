"""
Milvus 管理器 (V2.0 - RAG 增强版)

负责:
- 向量数据库连接
- Collection 管理
- 向量检索
- 研报/新闻向量化存储

支持三种模式:
1. 远程 Milvus Server (http://host:port)
2. Milvus Lite (本地文件，仅 Linux/macOS)
3. Disabled (Windows 无远程服务时，禁用向量功能)
"""

from typing import Optional, List, Dict, Any
from pathlib import Path
from datetime import datetime
import platform
import hashlib
import uuid

from .base import BaseManager
from ..settings import settings


class MilvusManager(BaseManager):
    """Milvus 向量数据库管理器"""
    
    def __init__(self):
        super().__init__()
        self._client = None
        self._config = settings.milvus
        self._is_lite_mode = False
        self._is_disabled = False  # Windows 无远程服务时禁用
        self._lite_db_path: Optional[Path] = None
    
    async def initialize(self) -> None:
        """
        初始化 Milvus 连接
        
        自动检测模式:
        1. 如果 URI 是本地路径 (如 ./milvus.db) -> Milvus Lite
        2. 如果远程连接失败 -> 自动降级为 Milvus Lite
        """
        if self._initialized:
            return
        
        from pymilvus import MilvusClient
        
        # 判断是否为本地路径模式
        uri = self._build_uri()
        
        if self._is_local_path(uri):
            # 本地文件模式 -> 直接使用 Milvus Lite
            await self._init_lite_mode(MilvusClient, uri)
        else:
            # 远程模式 -> 尝试连接，失败则降级
            await self._init_remote_or_fallback(MilvusClient, uri)
        
        # 确保 Collection 存在（禁用模式跳过）
        if not self._is_disabled:
            await self._ensure_collections()
        
        self._initialized = True
        
        if self._is_disabled:
            self.logger.info("Milvus initialized ✓ (mode=Disabled)")
        else:
            mode_str = "Lite" if self._is_lite_mode else "Remote"
            self.logger.info(f"Milvus connected ✓ (mode={mode_str})")
    
    def _build_uri(self) -> str:
        """构建连接 URI"""
        # 优先检查是否有 MILVUS_URI 环境变量或配置
        if hasattr(self._config, 'uri') and self._config.uri:
            return self._config.uri
        
        # 否则使用 host:port 构建
        return f"http://{self._config.host}:{self._config.port}"
    
    def _is_local_path(self, uri: str) -> bool:
        """判断是否为本地文件路径"""
        # 本地路径特征: 以 ./ 或 / 开头，或以 .db 结尾
        if uri.startswith("./") or uri.startswith("/"):
            return True
        if uri.endswith(".db"):
            return True
        # Windows 绝对路径
        if len(uri) > 2 and uri[1] == ":" and uri[2] in ["/", "\\"]:
            return True
        return False
    
    async def _init_lite_mode(self, MilvusClient, uri: str) -> None:
        """初始化 Milvus Lite 模式"""
        self._is_lite_mode = True
        
        # 确保目录存在
        self._lite_db_path = Path(uri)
        self._lite_db_path.parent.mkdir(parents=True, exist_ok=True)
        
        self.logger.info(f"Initializing Milvus Lite: {self._lite_db_path}")
        
        self._client = MilvusClient(uri=str(self._lite_db_path))
        self.logger.info(f"Milvus Lite initialized at {self._lite_db_path}")
    
    async def _init_remote_or_fallback(self, MilvusClient, uri: str) -> None:
        """尝试远程连接，失败则降级为 Lite 模式（非 Windows）或禁用"""
        self.logger.info(f"Connecting to Milvus: {uri}")
        
        try:
            self._client = MilvusClient(uri=uri)
            # 测试连接
            self._client.list_collections()
            self._is_lite_mode = False
            self.logger.info(f"Milvus remote connected: {uri}")
        except Exception as e:
            self.logger.warning(f"Failed to connect to remote Milvus: {e}")
            
            # 检查操作系统
            is_windows = platform.system() == "Windows"
            
            if is_windows:
                # Windows 不支持 Milvus Lite，禁用向量功能
                self._is_disabled = True
                self._client = None
                self.logger.warning(
                    "Milvus Lite not supported on Windows. "
                    "Vector search features are DISABLED. "
                    "To enable, run a remote Milvus server."
                )
            else:
                # Linux/macOS 降级为 Lite 模式
                self.logger.info("Falling back to Milvus Lite mode...")
                lite_path = "./data/milvus_lite.db"
                await self._init_lite_mode(MilvusClient, lite_path)
    
    async def shutdown(self) -> None:
        """关闭连接"""
        if self._client:
            try:
                self._client.close()
            except Exception as e:
                self.logger.warning(f"Error closing Milvus client: {e}")
            self._client = None
        
        self._initialized = False
        mode_str = "Lite" if self._is_lite_mode else "Remote"
        self.logger.info(f"Milvus disconnected (mode={mode_str})")
    
    async def health_check(self) -> bool:
        """健康检查"""
        if not self._initialized:
            return False
        
        # 禁用模式也算健康（只是功能不可用）
        if self._is_disabled:
            return True
        
        if self._client is None:
            return False
        
        try:
            self._client.list_collections()
            return True
        except Exception:
            return False
    
    def is_lite_mode(self) -> bool:
        """是否为 Lite 模式"""
        return self._is_lite_mode
    
    def is_disabled(self) -> bool:
        """是否为禁用模式（Windows 无远程服务）"""
        return self._is_disabled
    
    async def _ensure_collections(self) -> None:
        """确保 Collection 存在"""
        # 研报 Collection
        if not self._client.has_collection(self._config.research_reports_collection):
            self._client.create_collection(
                collection_name=self._config.research_reports_collection,
                dimension=self._config.embedding_dim,
                metric_type="COSINE",
            )
            self.logger.info(f"Created collection: {self._config.research_reports_collection}")
        
        # 市场片段 Collection
        if not self._client.has_collection(self._config.market_snippets_collection):
            self._client.create_collection(
                collection_name=self._config.market_snippets_collection,
                dimension=self._config.embedding_dim,
                metric_type="COSINE",
            )
            self.logger.info(f"Created collection: {self._config.market_snippets_collection}")
    
    # ==================== 向量操作 ====================
    
    async def insert(
        self,
        collection: str,
        vectors: List[List[float]],
        metadata: List[Dict[str, Any]],
    ) -> List[str]:
        """
        插入向量
        
        Args:
            collection: Collection 名称
            vectors: 向量列表
            metadata: 元数据列表
            
        Returns:
            插入的 ID 列表
        """
        self._ensure_initialized()
        
        # 禁用模式返回空
        if self._is_disabled:
            self.logger.debug("Milvus disabled, skipping insert")
            return []
        
        data = []
        for i, (vector, meta) in enumerate(zip(vectors, metadata)):
            # 生成唯一 ID（int64 类型，如果没有提供）
            item_id = meta.pop("id", None)
            if item_id is None:
                # 使用 UUID 的 int 版本截取为 int64 范围
                item_id = uuid.uuid4().int % (2**63 - 1)
            item = {
                "id": int(item_id),
                "vector": vector,
                **meta,
            }
            data.append(item)
        
        result = self._client.insert(
            collection_name=collection,
            data=data,
        )
        
        return [str(id) for id in result["ids"]]
    
    async def search(
        self,
        collection: str,
        query_vector: List[float],
        top_k: int = 10,
        filter_expr: Optional[str] = None,
        output_fields: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        向量检索
        
        Args:
            collection: Collection 名称
            query_vector: 查询向量
            top_k: 返回数量
            filter_expr: 过滤表达式
            output_fields: 返回字段
            
        Returns:
            检索结果列表
        """
        self._ensure_initialized()
        
        # 禁用模式返回空
        if self._is_disabled:
            self.logger.debug("Milvus disabled, returning empty search results")
            return []
        
        results = self._client.search(
            collection_name=collection,
            data=[query_vector],
            limit=top_k,
            filter=filter_expr,
            output_fields=output_fields or ["*"],
        )
        
        if not results or not results[0]:
            return []
        
        return [
            {
                "id": str(hit["id"]),
                "distance": hit["distance"],
                **hit["entity"],
            }
            for hit in results[0]
        ]
    
    async def delete(
        self,
        collection: str,
        ids: List[str],
    ) -> int:
        """
        删除向量
        
        Returns:
            删除数量
        """
        self._ensure_initialized()
        
        # 禁用模式返回 0
        if self._is_disabled:
            return 0
        
        result = self._client.delete(
            collection_name=collection,
            ids=ids,
        )
        
        return len(ids)
    
    # ==================== RAG 专用方法 ====================
    
    async def search_reports(
        self,
        query_vector: List[float],
        top_k: int = 5,
        ts_code: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        搜索研报
        
        Args:
            query_vector: 查询向量
            top_k: 返回数量
            ts_code: 股票代码过滤
        """
        filter_expr = f'ts_code == "{ts_code}"' if ts_code else None
        
        return await self.search(
            collection=self._config.research_reports_collection,
            query_vector=query_vector,
            top_k=top_k,
            filter_expr=filter_expr,
            output_fields=["ts_code", "title", "content", "publish_date"],
        )
    
    async def search_market_snippets(
        self,
        query_vector: List[float],
        top_k: int = 5,
    ) -> List[Dict[str, Any]]:
        """搜索市场片段"""
        return await self.search(
            collection=self._config.market_snippets_collection,
            query_vector=query_vector,
            top_k=top_k,
            output_fields=["content", "analysis_date", "market_sentiment"],
        )
    
    async def add_report(
        self,
        vector: List[float],
        ts_code: str,
        title: str,
        content: str,
        publish_date: str,
    ) -> str:
        """添加研报"""
        ids = await self.insert(
            collection=self._config.research_reports_collection,
            vectors=[vector],
            metadata=[{
                "ts_code": ts_code,
                "title": title,
                "content": content,
                "publish_date": publish_date,
            }],
        )
        return ids[0]
    
    async def add_market_snippet(
        self,
        vector: List[float],
        content: str,
        analysis_date: str,
        market_sentiment: str,
    ) -> str:
        """添加市场片段"""
        ids = await self.insert(
            collection=self._config.market_snippets_collection,
            vectors=[vector],
            metadata=[{
                "content": content,
                "analysis_date": analysis_date,
                "market_sentiment": market_sentiment,
            }],
        )
        return ids[0]
    
    # ==================== V2.0 增强方法 ====================
    
    async def insert_report(
        self,
        ts_code: str,
        title: str,
        content: str,
        trade_date: str,
        source: str = "unknown",
        chunk_index: int = 0,
    ) -> Optional[str]:
        """
        插入研报（自动生成向量）
        
        Args:
            ts_code: 股票代码
            title: 研报标题
            content: 研报内容（单个分段）
            trade_date: 交易日期 (YYYYMMDD)
            source: 数据来源
            chunk_index: 分段索引
            
        Returns:
            插入的向量 ID，失败返回 None
        """
        if self._is_disabled:
            self.logger.debug("Milvus disabled, skipping insert_report")
            return None
        
        # 延迟导入避免循环依赖
        from .llm_manager import llm_manager
        
        # 构建用于向量化的文本
        embed_text = f"{title}\n{content}"
        
        try:
            # 生成向量
            embeddings = await llm_manager.embedding([embed_text])
            vector = embeddings[0]
            
            # 生成唯一 key
            content_hash = hashlib.md5(content.encode()).hexdigest()[:8]
            doc_key = f"report_{ts_code}_{trade_date}_{chunk_index}_{content_hash}"
            
            # 插入向量
            ids = await self.insert(
                collection=self._config.research_reports_collection,
                vectors=[vector],
                metadata=[{
                    "ts_code": ts_code,
                    "title": title,
                    "content": content[:2000],  # 限制内容长度
                    "publish_date": trade_date,
                    "source": source,
                    "chunk_index": chunk_index,
                    "doc_key": doc_key,
                    "created_at": datetime.now().isoformat(),
                }],
            )
            
            return ids[0] if ids else None
            
        except Exception as e:
            self.logger.error(f"Failed to insert report: {e}")
            return None
    
    async def insert_news(
        self,
        ts_code: str,
        title: str,
        content: str,
        trade_date: str,
        news_datetime: Optional[str] = None,
        source: str = "unknown",
    ) -> Optional[str]:
        """
        插入新闻（自动生成向量）
        
        Args:
            ts_code: 股票代码
            title: 新闻标题
            content: 新闻内容
            trade_date: 交易日期 (YYYYMMDD)
            news_datetime: 新闻发布时间
            source: 数据来源
            
        Returns:
            插入的向量 ID，失败返回 None
        """
        if self._is_disabled:
            self.logger.debug("Milvus disabled, skipping insert_news")
            return None
        
        # 延迟导入避免循环依赖
        from .llm_manager import llm_manager
        
        # 构建用于向量化的文本
        embed_text = f"{title}\n{content[:500]}"  # 新闻使用标题+部分内容
        
        try:
            # 生成向量
            embeddings = await llm_manager.embedding([embed_text])
            vector = embeddings[0]
            
            # 生成唯一 key
            content_hash = hashlib.md5(f"{title}{content}".encode()).hexdigest()[:8]
            doc_key = f"news_{ts_code}_{trade_date}_{content_hash}"
            
            # 插入向量
            ids = await self.insert(
                collection=self._config.market_snippets_collection,  # 新闻存入 snippets
                vectors=[vector],
                metadata=[{
                    "ts_code": ts_code,
                    "content": f"[{ts_code}] {title}\n{content[:1000]}",
                    "analysis_date": trade_date,
                    "market_sentiment": "neutral",  # 默认中性
                    "source": source,
                    "news_datetime": news_datetime or "",
                    "doc_key": doc_key,
                    "created_at": datetime.now().isoformat(),
                }],
            )
            
            return ids[0] if ids else None
            
        except Exception as e:
            self.logger.error(f"Failed to insert news: {e}")
            return None
    
    async def insert_news_batch(
        self,
        news_list: List[Dict[str, Any]],
        batch_size: int = 10,
    ) -> Dict[str, int]:
        """
        批量插入新闻（自动生成向量）
        
        Args:
            news_list: 新闻列表，每条需包含 ts_code, title, content, datetime
            batch_size: 每批次处理数量（控制 embedding 并发）
            
        Returns:
            {"success": N, "failed": M}
        """
        if self._is_disabled:
            self.logger.debug("Milvus disabled, skipping insert_news_batch")
            return {"success": 0, "failed": 0, "skipped": len(news_list)}
        
        success = 0
        failed = 0
        
        # 分批处理
        for i in range(0, len(news_list), batch_size):
            batch = news_list[i:i + batch_size]
            
            for news in batch:
                ts_code = news.get("ts_code", "")
                title = news.get("title", "")
                content = news.get("content", "")
                dt = news.get("datetime")
                source = news.get("source", "akshare")
                
                # 提取交易日期
                if isinstance(dt, datetime):
                    trade_date = dt.strftime("%Y%m%d")
                    news_datetime = dt.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    trade_date = datetime.now().strftime("%Y%m%d")
                    news_datetime = str(dt) if dt else ""
                
                result = await self.insert_news(
                    ts_code=ts_code,
                    title=title,
                    content=content,
                    trade_date=trade_date,
                    news_datetime=news_datetime,
                    source=source,
                )
                
                if result:
                    success += 1
                else:
                    failed += 1
        
        self.logger.info(f"Batch insert news: success={success}, failed={failed}")
        return {"success": success, "failed": failed}
    
    async def search_reports_filtered(
        self,
        query_vector: List[float],
        top_k: int = 5,
        ts_code: Optional[str] = None,
        ts_codes: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        增强版研报搜索（支持多条件过滤）
        
        Args:
            query_vector: 查询向量
            top_k: 返回数量
            ts_code: 单个股票代码过滤
            ts_codes: 多个股票代码过滤（OR 关系）
            start_date: 起始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            
        Returns:
            检索结果列表
        """
        # 构建过滤表达式
        filters = []
        
        if ts_code:
            filters.append(f'ts_code == "{ts_code}"')
        elif ts_codes:
            codes_str = ", ".join([f'"{c}"' for c in ts_codes])
            filters.append(f'ts_code in [{codes_str}]')
        
        if start_date:
            filters.append(f'publish_date >= "{start_date}"')
        if end_date:
            filters.append(f'publish_date <= "{end_date}"')
        
        filter_expr = " and ".join(filters) if filters else None
        
        return await self.search(
            collection=self._config.research_reports_collection,
            query_vector=query_vector,
            top_k=top_k,
            filter_expr=filter_expr,
            output_fields=["ts_code", "title", "content", "publish_date", "source"],
        )
    
    async def search_news_filtered(
        self,
        query_vector: List[float],
        top_k: int = 5,
        ts_code: Optional[str] = None,
        ts_codes: Optional[List[str]] = None,
        start_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        增强版新闻搜索（支持股票代码过滤）
        
        Args:
            query_vector: 查询向量
            top_k: 返回数量
            ts_code: 单个股票代码过滤
            ts_codes: 多个股票代码过滤
            start_date: 起始日期 (YYYYMMDD)
            
        Returns:
            检索结果列表
        """
        filters = []
        
        if ts_code:
            filters.append(f'ts_code == "{ts_code}"')
        elif ts_codes:
            codes_str = ", ".join([f'"{c}"' for c in ts_codes])
            filters.append(f'ts_code in [{codes_str}]')
        
        if start_date:
            filters.append(f'analysis_date >= "{start_date}"')
        
        filter_expr = " and ".join(filters) if filters else None
        
        return await self.search(
            collection=self._config.market_snippets_collection,
            query_vector=query_vector,
            top_k=top_k,
            filter_expr=filter_expr,
            output_fields=["ts_code", "content", "analysis_date", "source", "news_datetime"],
        )


# ==================== 全局单例 ====================
milvus_manager = MilvusManager()
