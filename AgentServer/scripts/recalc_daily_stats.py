"""
重新计算指定时间段的 daily_stats 数据

用法:
    cd AgentServer
    
    # 重算单个日期
    python scripts/recalc_daily_stats.py --date 20260128
    
    # 重算时间段
    python scripts/recalc_daily_stats.py --start 20260120 --end 20260128
    
    # 重算最近N个交易日
    python scripts/recalc_daily_stats.py --days 10
"""

import asyncio
import argparse
from datetime import datetime, timedelta
import sys
import os

# 添加项目根目录到 path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.managers import mongo_manager, tushare_manager, analysis_manager
from core.settings import settings


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


async def compute_daily_stats_for_date(trade_date: str) -> dict:
    """计算指定日期的统计数据"""
    stats = {
        "trade_date": trade_date,
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
        
        # 连板统计
        "limit_1": 0,
        "limit_2": 0,
        "limit_3": 0,
        "limit_4": 0,
        "limit_5": 0,
        "limit_6_plus": 0,
        
        # 涨跌统计
        "up_count": 0,
        "down_count": 0,
        "flat_count": 0,
        
        # 涨跌停统计
        "limit_up_count": 0,
        "limit_down_count": 0,
        "broken_limit_count": 0,
        "max_limit_height": 0,
        
        # 沪深港通资金流向 (百万元)
        "hgt": None,
        "sgt": None,
        "north_money": None,
        "ggt_ss": None,
        "ggt_sz": None,
        "south_money": None,
        
        # 市场成交额 (千元)
        "sh_amount": None,
        "sz_amount": None,
        "total_amount": None,
    }
    
    # 1. 从 limit_list 获取涨跌停数据
    limit_data = await mongo_manager.find_many(
        "limit_list",
        {"trade_date": trade_date},
        projection={"ts_code": 1, "limit": 1, "limit_times": 1, "open_times": 1, "_id": 0},
    )
    
    if limit_data:
        for item in limit_data:
            limit_type = item.get("limit")
            limit_times = item.get("limit_times", 1) or 1
            open_times = item.get("open_times", 0) or 0
            
            if limit_type == "U":
                stats["limit_up_count"] += 1
                
                if limit_times > stats["max_limit_height"]:
                    stats["max_limit_height"] = limit_times
                
                if limit_times == 1:
                    stats["limit_1"] += 1
                elif limit_times == 2:
                    stats["limit_2"] += 1
                elif limit_times == 3:
                    stats["limit_3"] += 1
                elif limit_times == 4:
                    stats["limit_4"] += 1
                elif limit_times == 5:
                    stats["limit_5"] += 1
                else:
                    stats["limit_6_plus"] += 1
                
                if open_times > 0:
                    stats["broken_limit_count"] += 1
                    
            elif limit_type == "D":
                stats["limit_down_count"] += 1
    
    # 2. 从 stock_daily 获取涨跌统计
    daily_data = await mongo_manager.find_many(
        "stock_daily",
        {"trade_date": trade_date},
        projection={"ts_code": 1, "pct_chg": 1, "_id": 0},
    )
    
    if daily_data:
        for item in daily_data:
            pct_chg = item.get("pct_chg", 0) or 0
            try:
                pct_chg = float(pct_chg)
            except:
                pct_chg = 0
                
            if pct_chg > 0:
                stats["up_count"] += 1
            elif pct_chg < 0:
                stats["down_count"] += 1
            else:
                stats["flat_count"] += 1
    
    # 3. 获取沪深港通资金流向
    try:
        hsgt_data = await tushare_manager.get_moneyflow_hsgt(trade_date=trade_date)
        if hsgt_data:
            hsgt = hsgt_data[0]
            stats["hgt"] = hsgt.get("hgt")
            stats["sgt"] = hsgt.get("sgt")
            stats["north_money"] = hsgt.get("north_money")
            stats["ggt_ss"] = hsgt.get("ggt_ss")
            stats["ggt_sz"] = hsgt.get("ggt_sz")
            stats["south_money"] = hsgt.get("south_money")
    except Exception as e:
        print(f"  Warning: Failed to get HSGT data: {e}")
    
    # 4. 获取两市成交额
    try:
        sh_index = await mongo_manager.find_one(
            "index_daily",
            {"ts_code": "000001.SH", "trade_date": trade_date},
            projection={"amount": 1, "_id": 0},
        )
        if sh_index:
            stats["sh_amount"] = sh_index.get("amount")
        
        sz_index = await mongo_manager.find_one(
            "index_daily",
            {"ts_code": "399001.SZ", "trade_date": trade_date},
            projection={"amount": 1, "_id": 0},
        )
        if sz_index:
            stats["sz_amount"] = sz_index.get("amount")
        
        if stats["sh_amount"] is not None and stats["sz_amount"] is not None:
            print(f"------------------------ 深证成交 sz_amount: {stats['sz_amount']}")
            print(f"------------------------ 上证成交 sh_amount: {stats['sh_amount']}")
            try:
                stats["total_amount"] = float(stats["sh_amount"]) + float(stats["sz_amount"])
            except:
                pass
    except Exception as e:
        print(f"  Warning: Failed to get market turnover: {e}")
    
    # 计算衍生指标
    total_stocks = stats["up_count"] + stats["down_count"] + stats["flat_count"]
    stats["total_stocks"] = total_stocks
    stats["up_ratio"] = round(stats["up_count"] / total_stocks * 100, 2) if total_stocks > 0 else 0
    stats["down_ratio"] = round(stats["down_count"] / total_stocks * 100, 2) if total_stocks > 0 else 0
    
    stats["total_limit_up"] = (
        stats["limit_1"] + stats["limit_2"] + stats["limit_3"] + 
        stats["limit_4"] + stats["limit_5"] + stats["limit_6_plus"]
    )
    
    return stats


async def recalc_for_dates(trade_dates: list, force: bool = True):
    """重新计算指定日期列表的统计数据"""
    print(f"=== Recalculating daily_stats for {len(trade_dates)} trading days ===")
    print(f"Date range: {trade_dates[0]} ~ {trade_dates[-1]}")
    print(f"Force overwrite: {force}\n")
    
    success_count = 0
    skip_count = 0
    error_count = 0
    
    for i, trade_date in enumerate(trade_dates):
        try:
            # 检查是否有基础数据
            stock_count = await mongo_manager._db.stock_daily.count_documents({"trade_date": trade_date})
            
            if stock_count == 0:
                print(f"[{i+1}/{len(trade_dates)}] {trade_date} - SKIP (no stock_daily data)")
                skip_count += 1
                continue
            
            # 计算统计
            stats = await compute_daily_stats_for_date(trade_date)
            
            if stats["total_stocks"] == 0:
                limit_count = await mongo_manager._db.limit_list.count_documents({"trade_date": trade_date})
                index_count = await mongo_manager._db.index_daily.count_documents({"trade_date": trade_date})
                print(f"[{i+1}/{len(trade_dates)}] {trade_date} - SKIP (no stats data: stock={stock_count}, limit={limit_count}, index={index_count})")
                skip_count += 1
                continue
            
            # 保存到 daily_stats (upsert 会覆盖已有数据)
            await mongo_manager.update_one(
                "daily_stats",
                {"trade_date": trade_date},
                {"$set": stats},
                upsert=True,
            )
            
            # 删除旧的 sector_ranking
            await mongo_manager.delete_many(
                "sector_ranking",
                {"trade_date": trade_date},
            )
            
            # 重新计算 sector_ranking
            ranking_records = []
            
            # 行业排名
            industry_data = await mongo_manager.find_many(
                "moneyflow_industry",
                {"trade_date": trade_date},
                projection={"ts_code": 1, "name": 1, "pct_change": 1, "net_amount": 1, "_id": 0},
            )
            if industry_data:
                sorted_industry = sorted(industry_data, key=lambda x: x.get("pct_change", 0) or 0, reverse=True)
                for j, item in enumerate(sorted_industry[:20]):
                    ranking_records.append({
                        "trade_date": trade_date,
                        "ranking_type": "industry_top",
                        "rank": j + 1,
                        "ts_code": item.get("ts_code"),
                        "name": item.get("name"),
                        "pct_change": item.get("pct_change"),
                        "net_amount": item.get("net_amount"),
                    })
                for j, item in enumerate(sorted_industry[-20:][::-1]):
                    ranking_records.append({
                        "trade_date": trade_date,
                        "ranking_type": "industry_bottom",
                        "rank": j + 1,
                        "ts_code": item.get("ts_code"),
                        "name": item.get("name"),
                        "pct_change": item.get("pct_change"),
                        "net_amount": item.get("net_amount"),
                    })
            
            # 概念排名
            concept_data = await mongo_manager.find_many(
                "moneyflow_concept",
                {"trade_date": trade_date},
                projection={"ts_code": 1, "name": 1, "pct_change": 1, "net_amount": 1, "_id": 0},
            )
            if concept_data:
                sorted_concept = sorted(concept_data, key=lambda x: x.get("pct_change", 0) or 0, reverse=True)
                for j, item in enumerate(sorted_concept[:20]):
                    ranking_records.append({
                        "trade_date": trade_date,
                        "ranking_type": "concept_top",
                        "rank": j + 1,
                        "ts_code": item.get("ts_code"),
                        "name": item.get("name"),
                        "pct_change": item.get("pct_change"),
                        "net_amount": item.get("net_amount"),
                    })
                for j, item in enumerate(sorted_concept[-20:][::-1]):
                    ranking_records.append({
                        "trade_date": trade_date,
                        "ranking_type": "concept_bottom",
                        "rank": j + 1,
                        "ts_code": item.get("ts_code"),
                        "name": item.get("name"),
                        "pct_change": item.get("pct_change"),
                        "net_amount": item.get("net_amount"),
                    })
            
            if ranking_records:
                await mongo_manager.insert_many("sector_ranking", ranking_records)
            
            # 获取前一天数据用于周期分析
            prev_stats = await mongo_manager.find_one(
                "daily_stats",
                {"trade_date": {"$lt": trade_date}},
                sort=[("trade_date", -1)],
            )
            
            # 计算情绪分析
            analysis_result = await analysis_manager.analyze_and_store(
                stats=stats,
                prev_stats=prev_stats,
                mongo_manager=mongo_manager,
            )
            
            print(
                f"[{i+1}/{len(trade_dates)}] {trade_date} - OK "
                f"(up={stats['up_count']}, down={stats['down_count']}, "
                f"limit_up={stats['limit_up_count']}, height={stats['max_limit_height']}, "
                f"sentiment={analysis_result['sentiment_score']:.0f}, cycle={analysis_result['cycle']})"
            )
            success_count += 1
            
            # 避免 API 限流
            await asyncio.sleep(0.3)
            
        except Exception as e:
            print(f"[{i+1}/{len(trade_dates)}] {trade_date} - ERROR: {e}")
            error_count += 1
    
    print(f"\n=== Done! ===")
    print(f"  Success: {success_count}")
    print(f"  Skipped: {skip_count}")
    print(f"  Errors:  {error_count}")


async def main(args):
    # 初始化
    await mongo_manager.initialize()
    await tushare_manager.initialize()
    
    # 确定要处理的日期范围
    if args.date:
        # 单个日期
        trade_dates = [args.date]
    elif args.start and args.end:
        # 日期范围
        trade_dates = await get_trade_dates_in_range(args.start, args.end)
    elif args.days:
        # 最近N天
        trade_dates = await get_recent_trade_dates(args.days)
    else:
        print("Error: Please specify --date, --start/--end, or --days")
        return
    
    if not trade_dates:
        print("No trading dates found in the specified range")
        return
    
    # 重新计算
    await recalc_for_dates(trade_dates, force=True)
    
    # 关闭连接
    await mongo_manager.shutdown()
    await tushare_manager.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Recalculate daily_stats for specified date range")
    parser.add_argument("--date", type=str, help="Single date to recalculate (e.g., 20260128)")
    parser.add_argument("--start", type=str, help="Start date of range (e.g., 20260120)")
    parser.add_argument("--end", type=str, help="End date of range (e.g., 20260128)")
    parser.add_argument("--days", type=int, help="Number of recent trading days to recalculate")
    
    args = parser.parse_args()
    
    # 验证参数
    if not args.date and not (args.start and args.end) and not args.days:
        parser.print_help()
        print("\nExamples:")
        print("  python scripts/recalc_daily_stats.py --date 20260128")
        print("  python scripts/recalc_daily_stats.py --start 20260120 --end 20260128")
        print("  python scripts/recalc_daily_stats.py --days 10")
        sys.exit(1)
    
    asyncio.run(main(args))
