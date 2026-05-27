"""
数据同步节点

职责:
- 定时同步股票基础数据
- 定时同步日线数据
- 定时同步新闻资讯
- 数据清洗与标准化
"""

from .node import DataSyncNode

__all__ = ["DataSyncNode"]
