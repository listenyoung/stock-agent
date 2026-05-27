#!/usr/bin/env python
"""
财务指标数据同步脚本

功能：
- 初始化同步：同步所有上市股票近5年的财务数据
- 增量同步：同步最近一个季度的新公告数据

使用方法：
    # 初始化同步 (5年数据)
    python scripts/sync_fina_indicator.py --init
    
    # 增量同步 (最近90天公告)
    python scripts/sync_fina_indicator.py
    
    # 指定同步年数 (初始化模式)
    python scripts/sync_fina_indicator.py --init --years 3
"""

import asyncio
import argparse
import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.managers import mongo_manager, tushare_manager
from nodes.data_sync.collectors.fina_indicator import FinaIndicatorCollector


async def main(args):
    """主函数"""
    print("=" * 60)
    print("财务指标数据同步脚本")
    print("=" * 60)
    
    # 初始化 managers
    await mongo_manager.initialize()
    await tushare_manager.initialize()
    
    # 创建采集器实例
    collector = FinaIndicatorCollector()
    
    if args.init:
        # 初始化模式：同步N年数据
        years = args.years or 5
        print(f"初始化模式：同步近 {years} 年的财务数据 (约 {years * 4} 个季度/股票)")
        result = await collector.init_sync(years=years)
    else:
        # 增量模式
        print("增量模式：同步每只股票最近 8 个季度的财务数据")
        result = await collector.collect()
    
    print("\n" + "=" * 60)
    print("同步完成!")
    print("=" * 60)
    print(f"总记录数: {result.get('count', 0)}")
    print(f"成功股票: {result.get('success', 0)}")
    print(f"失败股票: {result.get('error', 0)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="财务指标数据同步脚本")
    parser.add_argument("--init", action="store_true", help="初始化模式，同步历史数据")
    parser.add_argument("--years", type=int, default=5, help="初始化同步年数 (默认5年)")
    
    args = parser.parse_args()
    asyncio.run(main(args))
