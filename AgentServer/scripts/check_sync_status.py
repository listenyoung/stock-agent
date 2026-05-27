"""检查同步状态"""
import asyncio
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.managers import mongo_manager, tushare_manager

async def main():
    await mongo_manager.initialize()
    await tushare_manager.initialize()
    
    # 获取 Tushare 认为的最新交易日
    latest_trade_date = await tushare_manager.get_latest_trade_date()
    print(f"=== Tushare latest trade date: {latest_trade_date} ===\n")
    
    print("=== sync_records (控制增量同步的起点) ===")
    records = await mongo_manager.find_many("sync_records", {})
    if records:
        for doc in records:
            sync_type = doc.get('sync_type', 'unknown')
            sync_date = doc.get('sync_date', 'N/A')
            need_sync = sync_date < latest_trade_date if sync_date != 'N/A' else True
            status = "需要同步" if need_sync else "已是最新"
            print(f"  {sync_type}: {sync_date} ({status})")
    else:
        print("  (空)")
    
    print()
    print("=== Latest dates per collection (实际数据) ===")
    for col in ["stock_daily", "index_daily", "limit_list", "daily_stats", "market_analysis"]:
        doc = await mongo_manager.find_one(col, {}, sort=[("trade_date", -1)])
        if doc:
            actual_date = doc.get('trade_date', 'N/A')
            match = "✓" if actual_date >= latest_trade_date else f"⚠ 缺失 {actual_date} -> {latest_trade_date}"
            print(f"  {col}: {actual_date} {match}")
        else:
            print(f"  {col}: no data")
    
    await tushare_manager.shutdown()
    await mongo_manager.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
