"""
重新计算指定时间段的 market_analysis 数据 (仅分析，不重算 daily_stats)

用法:
    cd AgentServer
    
    # 重算单个日期
    python scripts/recalc_market_analysis.py --date 20260128
    
    # 重算时间段
    python scripts/recalc_market_analysis.py --start 20260106 --end 20260128
    
    # 重算最近N个交易日
    python scripts/recalc_market_analysis.py --days 20
"""

import asyncio
import argparse
from datetime import datetime, timedelta
import sys
import os

# 添加项目根目录到 path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.managers import mongo_manager, tushare_manager, analysis_manager


async def get_trade_dates_in_range(start_date: str, end_date: str) -> list:
    """获取指定范围内的交易日"""
    await tushare_manager.initialize()
    
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
    dates.sort()  # 从早到晚排序
    return dates


async def get_recent_trade_dates(days: int) -> list:
    """获取最近N个交易日"""
    await tushare_manager.initialize()
    
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=days * 2)).strftime("%Y%m%d")
    
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
    dates.sort(reverse=True)
    return dates[:days][::-1]  # 取最近N天，然后反转为从早到晚


async def recalc_analysis_for_dates(trade_dates: list):
    """
    重新计算指定日期列表的 market_analysis
    
    算法特性:
    1. 方向修正强度算法: 放量杀跌时强度分显著下降
    2. EMA 平滑情绪分: Final = Today * 0.8 + Yesterday * 0.2
    3. 动态 MA30 基准: 使用实际可用数据天数
    """
    print(f"=== Recalculating market_analysis for {len(trade_dates)} trading days ===")
    print(f"Date range: {trade_dates[0]} ~ {trade_dates[-1]}")
    print(f"Algorithm: Direction-corrected strength + EMA smoothed sentiment\n")
    
    # 清除 analysis_manager 的缓存
    analysis_manager._ma30_cache.clear()
    
    success_count = 0
    skip_count = 0
    error_count = 0
    
    for i, trade_date in enumerate(trade_dates):
        try:
            # 从 daily_stats 获取当日数据
            stats = await mongo_manager.find_one(
                "daily_stats",
                {"trade_date": trade_date},
            )
            
            if not stats:
                print(f"[{i+1}/{len(trade_dates)}] {trade_date} - SKIP (no daily_stats)")
                skip_count += 1
                continue
            
            # 获取前一天数据
            prev_stats = await mongo_manager.find_one(
                "daily_stats",
                {"trade_date": {"$lt": trade_date}},
                sort=[("trade_date", -1)],
            )
            
            # 重新计算分析 (使用新算法)
            analysis_result = await analysis_manager.analyze_and_store(
                stats=stats,
                prev_stats=prev_stats,
                mongo_manager=mongo_manager,
            )
            
            # 显示结果 (包含新字段)
            v_ratio = analysis_result.get("v_ratio", 0)
            baseline_count = analysis_result.get("baseline_data_count", 0)
            sentiment_raw = analysis_result.get("sentiment_score", 0)
            sentiment_ema = analysis_result.get("sentiment_score_ema", sentiment_raw)
            strength_diff = analysis_result.get("strength_diff", 0)
            up_ratio = stats.get("up_ratio", 0)
            
            # 方向修正标记
            direction_flag = "↓" if up_ratio < 40 else "→" if up_ratio < 60 else "↑"
            
            print(
                f"[{i+1}/{len(trade_dates)}] {trade_date} {direction_flag} "
                f"S:{analysis_result['strength_score']:.0f} "
                f"E:{sentiment_raw:.0f}→{sentiment_ema:.0f} "
                f"Diff:{strength_diff:+.0f} "
                f"v:{v_ratio:.2f} "
                f"{analysis_result['cycle']}"
            )
            success_count += 1
            
        except Exception as e:
            print(f"[{i+1}/{len(trade_dates)}] {trade_date} - ERROR: {e}")
            import traceback
            traceback.print_exc()
            error_count += 1
    
    print(f"\n=== Done! ===")
    print(f"  Success: {success_count}")
    print(f"  Skipped: {skip_count}")
    print(f"  Errors:  {error_count}")
    print(f"\nLegend: S=Strength, E=Sentiment(raw→ema), Diff=Strength-Sentiment")
    print(f"Direction: ↓=放量杀跌(up<40%), →=中性, ↑=上涨行情")


async def main(args):
    # 初始化
    await mongo_manager.initialize()
    await tushare_manager.initialize()
    await analysis_manager.initialize()
    
    # 确定要处理的日期范围
    if args.date:
        trade_dates = [args.date]
    elif args.start and args.end:
        trade_dates = await get_trade_dates_in_range(args.start, args.end)
    elif args.days:
        trade_dates = await get_recent_trade_dates(args.days)
    else:
        print("Error: Please specify --date, --start/--end, or --days")
        return
    
    if not trade_dates:
        print("No trading dates found in the specified range")
        return
    
    # 重新计算
    await recalc_analysis_for_dates(trade_dates)
    
    # 关闭连接
    await mongo_manager.shutdown()
    await tushare_manager.shutdown()
    await analysis_manager.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Recalculate market_analysis for specified date range")
    parser.add_argument("--date", type=str, help="Single date to recalculate (e.g., 20260128)")
    parser.add_argument("--start", type=str, help="Start date of range (e.g., 20260106)")
    parser.add_argument("--end", type=str, help="End date of range (e.g., 20260128)")
    parser.add_argument("--days", type=int, help="Number of recent trading days to recalculate")
    
    args = parser.parse_args()
    
    if not args.date and not (args.start and args.end) and not args.days:
        parser.print_help()
        print("\nExamples:")
        print("  python scripts/recalc_market_analysis.py --date 20260128")
        print("  python scripts/recalc_market_analysis.py --start 20260106 --end 20260128")
        print("  python scripts/recalc_market_analysis.py --days 20")
        sys.exit(1)
    
    asyncio.run(main(args))
