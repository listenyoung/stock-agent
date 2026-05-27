"""
手动同步涨跌停数据

用法:
    cd AgentServer
    
    # 同步单个日期
    python scripts/sync_limit_list.py --date 20260128
    
    # 同步时间段
    python scripts/sync_limit_list.py --start 20260106 --end 20260128
    
    # 同步最近N天
    python scripts/sync_limit_list.py --days 30
"""

import asyncio
import argparse
from datetime import datetime, timedelta
import sys
import os

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


async def sync_limit_list(trade_dates: list):
    """同步涨跌停数据"""
    print(f"=== Syncing limit_list for {len(trade_dates)} trading days ===")
    print(f"Date range: {trade_dates[0]} ~ {trade_dates[-1]}\n")
    
    total_count = 0
    success_days = 0
    failed_days = 0
    
    for i, trade_date in enumerate(trade_dates):
        try:
            # 获取涨跌停数据
            records = await tushare_manager.get_limit_list_d(trade_date=trade_date)
            
            if records:
                # 批量写入
                result = await mongo_manager.bulk_upsert(
                    collection="limit_list",
                    documents=records,
                    key_fields=["ts_code", "trade_date"],
                    batch_size=1000,
                )
                
                count = result["upserted"] + result["modified"]
                total_count += count
                
                # 统计涨停/跌停数量
                limit_up = sum(1 for r in records if r.get("limit") == "U")
                limit_down = sum(1 for r in records if r.get("limit") == "D")
                
                print(
                    f"[{i+1}/{len(trade_dates)}] {trade_date} - "
                    f"{len(records)} records (涨停={limit_up}, 跌停={limit_down}), "
                    f"upserted={result['upserted']}, modified={result['modified']}"
                )
                success_days += 1
            else:
                print(f"[{i+1}/{len(trade_dates)}] {trade_date} - no data (非交易日或数据未更新)")
                
        except Exception as e:
            print(f"[{i+1}/{len(trade_dates)}] {trade_date} - ERROR: {e}")
            failed_days += 1
        
        # 避免 API 限流
        await asyncio.sleep(1)
    
    # 更新同步记录
    if trade_dates and success_days > 0:
        await mongo_manager.record_sync(
            sync_type="limit_list",
            sync_date=trade_dates[-1],
            count=total_count,
        )
    
    print(f"\n=== Done! ===")
    print(f"  Total records: {total_count}")
    print(f"  Success days:  {success_days}")
    print(f"  Failed days:   {failed_days}")


async def main(args):
    # 初始化
    await mongo_manager.initialize()
    await tushare_manager.initialize()
    
    # 确定要处理的日期范围
    if args.date:
        trade_dates = [args.date]
    elif args.start and args.end:
        trade_dates = await get_trade_dates_in_range(args.start, args.end)
    elif args.days:
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=args.days)).strftime("%Y%m%d")
        trade_dates = await get_trade_dates_in_range(start_date, end_date)
    else:
        print("Error: Please specify --date, --start/--end, or --days")
        return
    
    if not trade_dates:
        print("No trading dates found in the specified range")
        return
    
    # 同步数据
    await sync_limit_list(trade_dates)
    
    # 关闭连接
    await mongo_manager.shutdown()
    await tushare_manager.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync limit_list (涨跌停) data from Tushare")
    parser.add_argument("--date", type=str, help="Single date to sync (e.g., 20260128)")
    parser.add_argument("--start", type=str, help="Start date of range (e.g., 20260106)")
    parser.add_argument("--end", type=str, help="End date of range (e.g., 20260128)")
    parser.add_argument("--days", type=int, help="Number of recent days to sync")
    
    args = parser.parse_args()
    
    if not args.date and not (args.start and args.end) and not args.days:
        parser.print_help()
        print("\nExamples:")
        print("  python scripts/sync_limit_list.py --date 20260128")
        print("  python scripts/sync_limit_list.py --start 20260106 --end 20260128")
        print("  python scripts/sync_limit_list.py --days 30")
        sys.exit(1)
    
    asyncio.run(main(args))
