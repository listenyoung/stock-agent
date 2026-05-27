"""
每日指标数据同步脚本

用于手动同步 daily_basic 数据（PE/PB/换手率/市值等）。
首次同步历史数据可能需要较长时间。

数据清洗逻辑参考 stock_basic.py：
- 市值从万元转换为亿元
- 过滤 NaN 值
- 亏损公司 PE 保留 None

Usage:
    # 同步所有历史数据（从 2018-01-01 开始）
    python scripts/sync_daily_basic.py
    
    # 同步指定日期范围
    python scripts/sync_daily_basic.py --start 20230101 --end 20231231
    
    # 只同步最近 N 天
    python scripts/sync_daily_basic.py --days 30
"""

import asyncio
import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict

# 添加项目根目录到 Python 路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.managers import tushare_manager, mongo_manager


def _clean_daily_basic_record(record: Dict) -> Dict:
    """
    清洗 daily_basic 记录
    
    参考 stock_basic.py 的 _add_financial_metrics 逻辑:
    - 市值：从万元转换为亿元
    - 估值：过滤 NaN/None，亏损公司保留 None
    - 交易指标：过滤 NaN
    """
    cleaned = {
        "ts_code": record.get("ts_code"),
        "trade_date": record.get("trade_date"),
    }
    
    # 收盘价
    if "close" in record and record["close"] is not None:
        try:
            value = float(record["close"])
            if value == value:  # 非 NaN
                cleaned["close"] = value
        except (ValueError, TypeError):
            pass
    
    # 市值（万元 -> 亿元）
    for field in ["total_mv", "circ_mv"]:
        if field in record and record[field] is not None:
            try:
                value = float(record[field])
                if value == value:  # 非 NaN
                    cleaned[field] = value / 10000  # 转换为亿元
            except (ValueError, TypeError):
                pass
    
    # 估值指标 (保留字段，即使为空也存储 None，因为亏损公司 PE 就是空)
    for field in ["pe", "pb", "pe_ttm", "ps", "ps_ttm", "dv_ratio", "dv_ttm"]:
        if field in record:
            val = record[field]
            if val is None:
                cleaned[field] = None
            else:
                try:
                    value = float(val)
                    if value == value:  # 非 NaN
                        cleaned[field] = value
                    else:
                        cleaned[field] = None
                except (ValueError, TypeError):
                    cleaned[field] = None
    
    # 交易指标
    for field in ["turnover_rate", "turnover_rate_f", "volume_ratio"]:
        if field in record and record[field] is not None:
            try:
                value = float(record[field])
                if value == value:  # 非 NaN
                    cleaned[field] = value
            except (ValueError, TypeError):
                pass
    
    # 股本数据（万股，保持原单位）
    for field in ["total_share", "float_share", "free_share"]:
        if field in record and record[field] is not None:
            try:
                value = float(record[field])
                if value == value:  # 非 NaN
                    cleaned[field] = value
            except (ValueError, TypeError):
                pass
    
    return cleaned


async def sync_daily_basic(
    start_date: str,
    end_date: str,
) -> dict:
    """
    同步 daily_basic 数据
    
    Args:
        start_date: 开始日期 (YYYYMMDD)
        end_date: 结束日期 (YYYYMMDD)
        
    Returns:
        同步结果
    """
    print(f"\n📊 开始同步 daily_basic 数据")
    print(f"   日期范围: {start_date} ~ {end_date}")
    print(f"   数据清洗: 市值(万元→亿元)、过滤NaN、保留PE为None")
    
    # 初始化管理器
    await tushare_manager.initialize()
    await mongo_manager.initialize()
    
    # 获取交易日列表
    print("\n📅 获取交易日历...")
    trade_dates = await tushare_manager.get_trade_cal(start_date, end_date)
    trade_dates = sorted(trade_dates)
    
    if not trade_dates:
        print("❌ 没有找到交易日")
        return {"count": 0, "message": "No trade dates"}
    
    print(f"   找到 {len(trade_dates)} 个交易日")
    
    # 检查上次同步日期
    last_sync_date = await mongo_manager.get_last_sync_date("daily_basic")
    
    if last_sync_date:
        print(f"   上次同步日期: {last_sync_date}")
        # 过滤出需要同步的日期（大于上次同步日期的）
        need_sync_dates = [d for d in trade_dates if d > last_sync_date]
    else:
        print(f"   首次同步，从 {trade_dates[0]} 开始")
        need_sync_dates = trade_dates
    
    if not need_sync_dates:
        print("\n✅ 所有日期已同步，无需操作")
        return {"count": 0, "message": "Already synced"}
    
    print(f"   需要同步 {len(need_sync_dates)} 个交易日")
    
    # 开始同步
    import time
    total_count = 0
    failed_dates = []
    
    print("\n📥 开始同步数据...")
    start_time = time.time()
    
    for i, trade_date in enumerate(need_sync_dates):
        try:
            t1 = time.time()
            
            # Step 1: 获取当日全市场数据
            records = await tushare_manager.get_daily_basic(trade_date=trade_date)
            t2 = time.time()
            
            if records:
                # Step 2: 清洗数据
                cleaned_records = []
                for record in records:
                    cleaned = _clean_daily_basic_record(record)
                    cleaned["updated_at"] = datetime.utcnow()
                    cleaned_records.append(cleaned)
                
                # Step 3: 批量写入
                result = await mongo_manager.bulk_upsert(
                    collection="daily_basic",
                    documents=cleaned_records,
                    key_fields=["ts_code", "trade_date"],
                    batch_size=5000,
                )
                
                count = result["upserted"] + result["modified"]
                total_count += count
                
                # 进度显示
                progress = (i + 1) / len(need_sync_dates) * 100
                elapsed = time.time() - start_time
                eta = elapsed / (i + 1) * (len(need_sync_dates) - i - 1) if i > 0 else 0
                
                if (i + 1) % 10 == 0 or i == len(need_sync_dates) - 1:
                    print(f"   [{progress:5.1f}%] {trade_date}: {len(records)} records, "
                          f"API={t2-t1:.2f}s, ETA={eta/60:.1f}min")
            else:
                print(f"   ⚠️ {trade_date}: 无数据")
                
        except Exception as e:
            failed_dates.append(trade_date)
            print(f"   ❌ {trade_date}: {e}")
    
    elapsed = time.time() - start_time
    
    # 记录同步完成
    if need_sync_dates:
        await mongo_manager.record_sync(
            sync_type="daily_basic",
            sync_date=need_sync_dates[-1],
            count=total_count,
        )
    
    # 汇总
    print(f"\n{'='*50}")
    print(f"✅ 同步完成!")
    print(f"   总记录数: {total_count:,}")
    print(f"   总耗时: {elapsed/60:.1f} 分钟")
    print(f"   失败日期: {len(failed_dates)}")
    
    if failed_dates:
        print(f"   失败列表: {', '.join(failed_dates[:10])}{'...' if len(failed_dates) > 10 else ''}")
    
    return {
        "count": total_count,
        "failed_dates": failed_dates,
        "elapsed_seconds": elapsed,
    }


async def main():
    parser = argparse.ArgumentParser(description="同步 daily_basic 数据")
    
    parser.add_argument(
        "--start", "-s",
        type=str,
        default="20180101",
        help="开始日期 (YYYYMMDD)，默认: 20180101",
    )
    
    parser.add_argument(
        "--end", "-e",
        type=str,
        default=None,
        help="结束日期 (YYYYMMDD)，默认: 今天",
    )
    
    parser.add_argument(
        "--days", "-d",
        type=int,
        default=None,
        help="只同步最近 N 天（覆盖 --start）",
    )
    
    args = parser.parse_args()
    
    # 确定日期范围
    if args.end:
        end_date = args.end
    else:
        end_date = datetime.now().strftime("%Y%m%d")
    
    if args.days:
        start_dt = datetime.now() - timedelta(days=args.days)
        start_date = start_dt.strftime("%Y%m%d")
    else:
        start_date = args.start
    
    # 执行同步
    await sync_daily_basic(start_date, end_date)
    
    # 关闭连接
    await mongo_manager.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
