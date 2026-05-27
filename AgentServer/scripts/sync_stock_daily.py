"""
手动同步指定日期范围的 stock_daily 数据

用法:
    cd AgentServer
    
    # 同步单个日期
    python scripts/sync_stock_daily.py --date 20260106
    
    # 同步时间段
    python scripts/sync_stock_daily.py --start 20260106 --end 20260109
"""

import asyncio
import argparse
import sys
import os
from datetime import datetime

# 添加项目根目录到 path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.managers import mongo_manager, tushare_manager


async def get_trade_dates_in_range(start_date: str, end_date: str) -> list:
    """获取指定范围内的交易日"""
    df = await tushare_manager._call_api(
        "trade_cal",
        exchange="SSE",
        start_date=start_date,
        end_date=end_date,
        is_open="1",
    )
    
    if df.empty:
        return []
    
    dates = df["cal_date"].tolist()
    dates.sort()
    return dates


async def sync_stock_daily_for_date(trade_date: str, batch_size: int = 500) -> dict:
    """
    同步指定日期的 stock_daily 数据
    
    Args:
        trade_date: 交易日期 (YYYYMMDD)
        batch_size: 每批处理的股票数量
        
    Returns:
        同步结果统计
    """
    print(f"\n{'='*60}")
    print(f"Syncing stock_daily for {trade_date}")
    print(f"{'='*60}")
    
    # 获取所有股票列表
    stock_list = await mongo_manager.find_many(
        "stock_basic",
        {"list_status": "L"},
        projection={"ts_code": 1},
    )
    
    if not stock_list:
        # 如果没有 stock_basic，直接用 daily 接口按日期获取
        print("No stock_basic found, fetching all stocks for the date...")
        df = await tushare_manager._call_api(
            "daily",
            trade_date=trade_date,
        )
        
        if df.empty:
            print(f"  No data returned for {trade_date}")
            return {"date": trade_date, "count": 0, "status": "no_data"}
        
        records = df.to_dict("records")
        
        # 批量写入
        result = await mongo_manager.bulk_upsert(
            collection="stock_daily",
            documents=records,
            key_fields=["ts_code", "trade_date"],
        )
        
        count = result.get("upserted", 0) + result.get("modified", 0)
        print(f"  Synced {count} records (upserted={result.get('upserted', 0)}, modified={result.get('modified', 0)})")
        return {"date": trade_date, "count": count, "status": "ok"}
    
    # 有 stock_basic，按批次获取
    ts_codes = [s["ts_code"] for s in stock_list]
    total_count = len(ts_codes)
    total_synced = 0
    
    print(f"Found {total_count} stocks, processing in batches of {batch_size}...")
    
    for i in range(0, total_count, batch_size):
        batch_codes = ts_codes[i:i + batch_size]
        batch_end = min(i + batch_size, total_count)
        
        print(f"  Processing {i+1}-{batch_end}/{total_count}...", end=" ")
        
        # 调用 API
        df = await tushare_manager._call_api(
            "daily",
            ts_code=",".join(batch_codes),
            start_date=trade_date,
            end_date=trade_date,
        )
        
        if df.empty:
            print("no data")
            continue
        
        records = df.to_dict("records")
        
        # 批量写入
        result = await mongo_manager.bulk_upsert(
            collection="stock_daily",
            documents=records,
            key_fields=["ts_code", "trade_date"],
        )
        
        count = result.get("upserted", 0) + result.get("modified", 0)
        total_synced += count
        print(f"{len(records)} rows, synced {count}")
    
    print(f"\nTotal synced for {trade_date}: {total_synced} records")
    return {"date": trade_date, "count": total_synced, "status": "ok"}


async def sync_index_daily_for_date(trade_date: str) -> dict:
    """同步指定日期的 index_daily 数据"""
    print(f"\nSyncing index_daily for {trade_date}...")
    
    # 主要指数列表
    index_codes = [
        "000001.SH",  # 上证指数
        "399001.SZ",  # 深证成指
        "399006.SZ",  # 创业板指
        "000016.SH",  # 上证50
        "000300.SH",  # 沪深300
        "000905.SH",  # 中证500
        "000688.SH",  # 科创50
    ]
    
    all_records = []
    
    # 逐个获取每个指数的数据（有些API不支持批量查询）
    for ts_code in index_codes:
        try:
            df = await tushare_manager._call_api(
                "index_daily",
                ts_code=ts_code,
                start_date=trade_date,
                end_date=trade_date,
            )
            
            if not df.empty:
                all_records.extend(df.to_dict("records"))
                print(f"    {ts_code}: OK")
            else:
                print(f"    {ts_code}: no data")
        except Exception as e:
            print(f"    {ts_code}: error - {e}")
    
    if not all_records:
        print(f"  No index data for {trade_date}")
        return {"date": trade_date, "count": 0}
    
    result = await mongo_manager.bulk_upsert(
        collection="index_daily",
        documents=all_records,
        key_fields=["ts_code", "trade_date"],
    )
    
    count = result.get("upserted", 0) + result.get("modified", 0)
    print(f"  Synced {count} index records")
    return {"date": trade_date, "count": count}


async def sync_limit_list_for_date(trade_date: str) -> dict:
    """同步指定日期的涨跌停数据"""
    print(f"\nSyncing limit_list for {trade_date}...")
    
    df = await tushare_manager._call_api(
        "limit_list_d",
        trade_date=trade_date,
    )
    
    if df.empty:
        print(f"  No limit_list data for {trade_date}")
        return {"date": trade_date, "count": 0}
    
    records = df.to_dict("records")
    
    result = await mongo_manager.bulk_upsert(
        collection="limit_list",
        documents=records,
        key_fields=["ts_code", "trade_date"],
    )
    
    count = result.get("upserted", 0) + result.get("modified", 0)
    print(f"  Synced {count} limit records")
    return {"date": trade_date, "count": count}


async def main(args):
    print("=" * 60)
    print("Stock Daily Data Sync Script")
    print("=" * 60)
    
    # 初始化
    await mongo_manager.initialize()
    await tushare_manager.initialize()
    
    # 确定日期范围
    if args.date:
        trade_dates = [args.date]
    elif args.start and args.end:
        trade_dates = await get_trade_dates_in_range(args.start, args.end)
    else:
        print("Error: Please specify --date or --start/--end")
        return
    
    if not trade_dates:
        print("No trading dates found in the specified range")
        return
    
    print(f"\nWill sync {len(trade_dates)} trading days: {trade_dates[0]} ~ {trade_dates[-1]}")
    
    # 同步每个日期
    results = []
    for trade_date in trade_dates:
        # 同步 stock_daily
        result = await sync_stock_daily_for_date(trade_date)
        results.append(result)
        
        # 同步 index_daily
        await sync_index_daily_for_date(trade_date)
        
        # 同步 limit_list
        await sync_limit_list_for_date(trade_date)
    
    # 汇总
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    for r in results:
        status = "✓" if r["status"] == "ok" else "✗"
        print(f"  {status} {r['date']}: {r['count']} records")
    
    total = sum(r["count"] for r in results)
    print(f"\nTotal: {total} records synced")
    
    # 关闭连接
    await mongo_manager.shutdown()
    await tushare_manager.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync stock_daily for specified dates")
    parser.add_argument("--date", type=str, help="Single date to sync (e.g., 20260106)")
    parser.add_argument("--start", type=str, help="Start date of range (e.g., 20260106)")
    parser.add_argument("--end", type=str, help="End date of range (e.g., 20260109)")
    
    args = parser.parse_args()
    
    if not args.date and not (args.start and args.end):
        parser.print_help()
        print("\nExamples:")
        print("  python scripts/sync_stock_daily.py --date 20260106")
        print("  python scripts/sync_stock_daily.py --start 20260106 --end 20260109")
        sys.exit(1)
    
    asyncio.run(main(args))
