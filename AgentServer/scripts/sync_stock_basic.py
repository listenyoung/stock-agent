#!/usr/bin/env python
"""
股票基础信息同步脚本

功能：
- 同步所有上市股票的基础信息 (代码、名称、行业、上市日期等)
- 同时获取最新交易日的指标数据 (PE/PB/市值/换手率等) 并合并
- 支持强制刷新模式，跳过今日已同步检查

使用方法：
    # 普通同步 (跳过今日已同步)
    python scripts/sync_stock_basic.py
    
    # 强制刷新 (忽略同步记录)
    python scripts/sync_stock_basic.py --force
"""

import asyncio
import argparse
import sys
import os
from typing import Dict, Any

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import date, datetime
from core.managers import mongo_manager, tushare_manager


def add_financial_metrics(doc: Dict, daily_metrics: Dict) -> None:
    """
    将财务与交易指标写入 doc（就地修改）。
    - 市值：total_mv/circ_mv（从万元转换为亿元）
    - 估值：pe/pb/pe_ttm/ps/ps_ttm（过滤 NaN/None）
    - 交易：turnover_rate/volume_ratio（过滤 NaN/None）
    - 股本：total_share/float_share（万股，过滤 NaN/None）
    """
    # 市值（万元 -> 亿元）
    if "total_mv" in daily_metrics and daily_metrics["total_mv"] is not None:
        try:
            value = float(daily_metrics["total_mv"])
            if value == value:  # 非 NaN
                doc["total_mv"] = value / 10000
        except (ValueError, TypeError):
            pass
    
    if "circ_mv" in daily_metrics and daily_metrics["circ_mv"] is not None:
        try:
            value = float(daily_metrics["circ_mv"])
            if value == value:  # 非 NaN
                doc["circ_mv"] = value / 10000
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


async def sync_stock_basic(force: bool = False):
    """同步股票基础信息（含最新指标数据）"""
    print("=" * 60)
    print("股票基础信息同步脚本 (含财务指标)")
    print("=" * 60)
    
    # 初始化 managers
    await mongo_manager.initialize()
    await tushare_manager.initialize()
    
    today = date.today().strftime("%Y%m%d")
    
    # 检查是否已同步
    if not force and await mongo_manager.is_synced("stock_basic", today):
        print(f"今日 ({today}) 已同步，跳过。使用 --force 强制刷新。")
        return {"count": 0, "skipped": True}
    
    # Step 1: 获取所有上市股票基础信息
    print(f"\n[Step 1] 获取股票基础信息...")
    records = await tushare_manager.get_stock_basic()
    
    if not records:
        print("未获取到任何数据")
        return {"count": 0}
    
    print(f"  获取到 {len(records)} 只股票")
    
    # 打印示例数据的字段
    if records:
        sample = records[0]
        print(f"  基础字段: {list(sample.keys())}")
    
    # Step 2: 获取最新交易日
    print(f"\n[Step 2] 获取最新交易日...")
    latest_trade_date = await tushare_manager.get_latest_trade_date()
    print(f"  最新交易日: {latest_trade_date}")
    
    # Step 3: 获取最新交易日的 daily_basic 数据
    print(f"\n[Step 3] 获取每日指标数据 (PE/PB/市值等)...")
    daily_basic_map: Dict[str, Dict] = {}
    
    if latest_trade_date:
        daily_basic = await tushare_manager.get_daily_basic(trade_date=latest_trade_date)
        if daily_basic:
            for item in daily_basic:
                ts_code = item.get("ts_code")
                if ts_code:
                    daily_basic_map[ts_code] = item
            print(f"  获取到 {len(daily_basic_map)} 条指标数据")
            
            # 打印示例
            if daily_basic:
                sample = daily_basic[0]
                print(f"  指标字段: {list(sample.keys())}")
        else:
            print("  未获取到指标数据")
    
    # Step 4: 合并数据
    print(f"\n[Step 4] 合并基础信息与指标数据...")
    merged_count = 0
    
    for record in records:
        ts_code = record.get("ts_code")
        
        # 添加更新时间
        record["updated_at"] = datetime.utcnow()
        
        # 合并 daily_basic 指标
        if ts_code and ts_code in daily_basic_map:
            add_financial_metrics(record, daily_basic_map[ts_code])
            merged_count += 1
    
    print(f"  成功合并 {merged_count}/{len(records)} 条记录的指标数据")
    
    # Step 5: 写入数据库
    print(f"\n[Step 5] 写入数据库...")
    result = await mongo_manager.bulk_upsert(
        collection="stock_basic",
        documents=records,
        key_fields=["ts_code"],
        batch_size=1000,
    )
    
    print(f"  写入完成:")
    print(f"    - 总数: {result['total']}")
    print(f"    - 新增: {result['upserted']}")
    print(f"    - 更新: {result['modified']}")
    
    # 记录同步完成
    await mongo_manager.record_sync(
        sync_type="stock_basic",
        sync_date=today,
        count=result["total"],
    )
    
    # 打印合并后的示例数据
    if records:
        sample = records[0]
        print(f"\n合并后示例 ({sample.get('ts_code')}):")
        for key in ["name", "industry", "pe", "pb", "total_mv", "turnover_rate"]:
            if key in sample:
                print(f"    {key}: {sample[key]}")
    
    print("\n" + "=" * 60)
    print("同步完成!")
    print("=" * 60)
    
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="股票基础信息同步脚本")
    parser.add_argument("--force", action="store_true", help="强制刷新，忽略今日已同步检查")
    
    args = parser.parse_args()
    asyncio.run(sync_stock_basic(force=args.force))
