"""
修改 sync_records 表的同步日期

用于手动调整同步记录的日期，触发重新同步

Usage:
    # 修改单个类型的同步日期
    python scripts/update_sync_date.py --types stock_daily --date 20260101
    
    # 修改多个类型的同步日期
    python scripts/update_sync_date.py --types stock_daily,limit_list,daily_stats --date 20260201
    
    # 修改所有类型的同步日期
    python scripts/update_sync_date.py --all --date 20260101
    
    # 查看当前所有同步记录
    python scripts/update_sync_date.py --list
"""

import asyncio
import argparse
from datetime import datetime
from typing import List, Optional

import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.managers import mongo_manager


# 默认同步类型（常用）
DEFAULT_SYNC_TYPES = [
    "stock_daily",
    "daily_basic",
    "limit_list",
    "moneyflow_concept",
    "index_daily",
    "moneyflow_industry",
    "daily_stats",
]

# 所有可用的同步类型
ALL_SYNC_TYPES = [
    "stock_basic",
    "stock_daily",
    "daily_basic",
    "index_basic",
    "index_daily",
    "limit_list",
    "moneyflow_concept",
    "moneyflow_industry",
    "fina_indicator",
    "daily_stats",
    "news",
    "hot_news",
]


async def list_sync_records() -> None:
    """列出所有同步记录"""
    records = await mongo_manager.find_many(
        "sync_records",
        {},
        sort=[("sync_type", 1)],
    )
    
    if not records:
        print("没有找到同步记录")
        return
    
    print("\n" + "=" * 80)
    print(f"{'类型':<25} {'同步日期':<12} {'数量':<10} {'更新时间'}")
    print("=" * 80)
    
    for record in records:
        sync_type = record.get("sync_type", "unknown")
        sync_date = record.get("sync_date", "N/A")
        last_count = record.get("last_count", 0)
        updated_at = record.get("updated_at", "")
        
        if isinstance(updated_at, datetime):
            updated_at = updated_at.strftime("%Y-%m-%d %H:%M:%S")
        
        print(f"{sync_type:<25} {sync_date:<12} {last_count:<10} {updated_at}")
    
    print("=" * 80 + "\n")


async def update_sync_date(
    sync_types: List[str],
    new_date: str,
    dry_run: bool = False,
) -> None:
    """
    更新同步记录的日期
    
    Args:
        sync_types: 要更新的同步类型列表
        new_date: 新的同步日期 (YYYYMMDD 格式)
        dry_run: 是否只是预览，不实际执行
    """
    # 验证日期格式
    try:
        datetime.strptime(new_date, "%Y%m%d")
    except ValueError:
        print(f"❌ 日期格式错误: {new_date}，请使用 YYYYMMDD 格式 (如 20260101)")
        return
    
    print(f"\n{'[预览模式] ' if dry_run else ''}准备更新以下同步类型的日期为 {new_date}:\n")
    
    updated_count = 0
    not_found = []
    
    for sync_type in sync_types:
        # 查找现有记录
        record = await mongo_manager.find_one(
            "sync_records",
            {"sync_type": sync_type},
        )
        
        if record:
            old_date = record.get("sync_date", "N/A")
            print(f"  ✓ {sync_type}: {old_date} → {new_date}")
            
            if not dry_run:
                await mongo_manager.update_one(
                    "sync_records",
                    {"sync_type": sync_type},
                    {
                        "$set": {
                            "sync_date": new_date,
                            "updated_at": datetime.utcnow(),
                        }
                    },
                )
            updated_count += 1
        else:
            not_found.append(sync_type)
            print(f"  ✗ {sync_type}: 记录不存在")
    
    print()
    
    if dry_run:
        print(f"[预览模式] 将会更新 {updated_count} 条记录")
        print("添加 --execute 参数执行实际更新")
    else:
        print(f"✅ 已更新 {updated_count} 条记录")
    
    if not_found:
        print(f"\n⚠️  以下类型未找到记录: {', '.join(not_found)}")
        print("可用的类型:", ", ".join(ALL_SYNC_TYPES))


async def main():
    parser = argparse.ArgumentParser(
        description="修改 sync_records 表的同步日期",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 查看当前所有同步记录
  python scripts/update_sync_date.py --list
  
  # 使用默认类型，预览修改
  python scripts/update_sync_date.py --date 20260101
  
  # 使用默认类型，执行修改
  python scripts/update_sync_date.py --date 20260101 --execute
  
  # 指定类型
  python scripts/update_sync_date.py --types stock_daily,limit_list --date 20260101 --execute
  
  # 修改所有类型
  python scripts/update_sync_date.py --all --date 20260101 --execute
        """,
    )
    
    parser.add_argument(
        "--list", "-l",
        action="store_true",
        help="列出所有同步记录",
    )
    
    parser.add_argument(
        "--types", "-t",
        type=str,
        help="要修改的同步类型，多个用逗号分隔 (如: stock_daily,limit_list)",
    )
    
    parser.add_argument(
        "--all", "-a",
        action="store_true",
        help="修改所有同步类型",
    )
    
    parser.add_argument(
        "--date", "-d",
        type=str,
        help="新的同步日期 (YYYYMMDD 格式，如: 20260101)",
    )
    
    parser.add_argument(
        "--execute", "-e",
        action="store_true",
        help="执行实际更新（默认只预览）",
    )
    
    args = parser.parse_args()
    
    # 初始化 MongoDB
    await mongo_manager.initialize()
    
    try:
        if args.list:
            await list_sync_records()
            return
        
        if not args.date:
            print("❌ 请指定日期参数 --date")
            parser.print_help()
            return
        
        if args.all:
            sync_types = ALL_SYNC_TYPES
        elif args.types:
            sync_types = [t.strip() for t in args.types.split(",")]
        else:
            # 默认使用常用类型
            sync_types = DEFAULT_SYNC_TYPES
            print(f"ℹ️  未指定类型，使用默认类型: {', '.join(sync_types)}\n")
        
        await update_sync_date(
            sync_types=sync_types,
            new_date=args.date,
            dry_run=not args.execute,
        )
        
    finally:
        await mongo_manager.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
