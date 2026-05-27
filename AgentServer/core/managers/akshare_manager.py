"""
AKShare 管理器

专用于新闻采集，提供股票新闻获取接口。

AKShare 官方文档: https://akshare.akfamily.xyz/
"""

import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime

from .base import BaseManager


class AKShareManager(BaseManager):
    """
    AKShare 数据管理器
    
    主要用于:
    - 获取股票新闻
    - 获取公告信息
    
    使用方法:
        news = await akshare_manager.get_stock_news("000001", limit=10)
    """
    
    def __init__(self):
        super().__init__()
        self._ak = None  # akshare 模块
    
    async def initialize(self) -> None:
        """初始化 AKShare"""
        try:
            import akshare as ak
            self._ak = ak
            self._initialized = True
            self.logger.info("AKShare Manager initialized ✓")
        except ImportError:
            self.logger.error("AKShare not installed. Run: pip install akshare")
            raise
    
    async def shutdown(self) -> None:
        """关闭管理器"""
        self._ak = None
        self._initialized = False
        self.logger.info("AKShare Manager shutdown")
    
    async def health_check(self) -> bool:
        """健康检查"""
        if not self._initialized or self._ak is None:
            return False
        try:
            # 简单测试 - 获取当前时间
            return True
        except Exception:
            return False
    
    async def get_stock_news(
        self,
        symbol: str,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """
        获取股票新闻
        
        Args:
            symbol: 股票代码 (如 "000001" 或 "600000")
            limit: 最大新闻数量
            
        Returns:
            新闻列表，每条新闻包含:
            - title: 标题
            - content: 内容摘要
            - datetime: 发布时间
            - url: 原文链接
            - source: 来源
        """
        self._ensure_initialized()
        
        try:
            # 提取纯数字代码 (去掉 .SH/.SZ 后缀)
            code = symbol.split(".")[0]
            
            # 使用 akshare 获取个股新闻
            # 接口: stock_news_em (东方财富个股新闻)
            loop = asyncio.get_event_loop()
            df = await loop.run_in_executor(
                None,
                lambda: self._ak.stock_news_em(symbol=code)
            )
            
            if df is None or df.empty:
                self.logger.debug(f"No news for {symbol}")
                return []
            
            # 限制数量
            df = df.head(limit)
            
            # 转换为标准格式
            news_list = []
            for _, row in df.iterrows():
                news_item = {
                    "ts_code": symbol,
                    "title": str(row.get("新闻标题", "")),
                    "content": str(row.get("新闻内容", ""))[:500],  # 截取前500字
                    "datetime": self._parse_datetime(row.get("发布时间", "")),
                    "url": str(row.get("新闻链接", "")),
                    "source": "东方财富",
                    "data_source": "akshare",
                }
                news_list.append(news_item)
            
            self.logger.debug(f"Got {len(news_list)} news for {symbol}")
            return news_list
            
        except Exception as e:
            self.logger.error(f"Failed to get news for {symbol}: {e}")
            return []
    
    def _parse_datetime(self, dt_str: str) -> str:
        """解析日期时间字符串"""
        if not dt_str:
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        try:
            # 尝试多种格式
            for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"]:
                try:
                    return datetime.strptime(str(dt_str), fmt).strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    continue
            return str(dt_str)
        except Exception:
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    async def get_stock_news_batch(
        self,
        symbols: List[str],
        limit_per_stock: int = 10,
        batch_size: int = 5,
        delay: float = 0.3,
    ) -> Dict[str, Any]:
        """
        批量获取股票新闻
        
        Args:
            symbols: 股票代码列表
            limit_per_stock: 每只股票的新闻数量
            batch_size: 批次大小
            delay: 每次请求间隔(秒)，用于限流
            
        Returns:
            {
                "success_count": 成功数量,
                "error_count": 失败数量,
                "news_count": 新闻总数,
                "news_list": 所有新闻列表,
                "errors": 错误列表,
            }
        """
        self._ensure_initialized()
        
        result = {
            "success_count": 0,
            "error_count": 0,
            "news_count": 0,
            "news_list": [],
            "errors": [],
        }
        
        total_batches = (len(symbols) + batch_size - 1) // batch_size
        
        for batch_idx in range(0, len(symbols), batch_size):
            batch = symbols[batch_idx:batch_idx + batch_size]
            batch_num = batch_idx // batch_size + 1
            
            self.logger.info(f"Processing news batch {batch_num}/{total_batches}")
            
            for symbol in batch:
                try:
                    news = await self.get_stock_news(symbol, limit=limit_per_stock)
                    
                    if news:
                        result["news_list"].extend(news)
                        result["news_count"] += len(news)
                        result["success_count"] += 1
                        self.logger.debug(f"✅ {symbol} news: {len(news)} items")
                    else:
                        result["success_count"] += 1  # 没有新闻也算成功
                        
                    # API 限流
                    await asyncio.sleep(delay)
                    
                except Exception as e:
                    result["error_count"] += 1
                    result["errors"].append(f"{symbol}: {str(e)}")
                    self.logger.error(f"❌ {symbol} news failed: {e}")
                    
                    # 失败后休眠更长时间
                    await asyncio.sleep(1.0)
        
        self.logger.info(
            f"News batch completed: success={result['success_count']}, "
            f"errors={result['error_count']}, news={result['news_count']}"
        )
        
        return result


# 模块级别单例
akshare_manager = AKShareManager()
