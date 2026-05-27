"""
重新计算 sector_ranking 表的历史数据

用法:
    python scripts/recalc_sector_ranking.py --days 30
    python scripts/recalc_sector_ranking.py --start 20251201 --end 20260130
"""

import asyncio
import argparse
from datetime import datetime, timedelta
import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.managers import tushare_manager, mongo_manager


async def get_trade_dates(start_date: str, end_date: str) -> list:
    """获取日期范围内的交易日"""
    # 从 moneyflow_industry 获取有数据的日期
    all_dates = await mongo_manager.find_many(
        "moneyflow_industry",
        {"trade_date": {"$gte": start_date, "$lte": end_date}},
        projection={"trade_date": 1, "_id": 0},
    )
    unique_dates = sorted(list(set(d.get("trade_date") for d in all_dates if d.get("trade_date"))), reverse=True)
    return unique_dates


async def compute_ranking_for_date(trade_date: str, debug: bool = False) -> dict:
    """计算单日的排名数据"""
    ranking_records = []
    
    # 1. 行业排名
    industry_data = await mongo_manager.find_many(
        "moneyflow_industry",
        {"trade_date": trade_date},
    )
    
    if debug and industry_data:
        print(f"  [DEBUG] 行业数据示例: {industry_data[0]}")
    
    if industry_data:
        sorted_industry = sorted(industry_data, key=lambda x: float(x.get("pct_change") or 0), reverse=True)
        
        # 涨幅前20
        for i, item in enumerate(sorted_industry[:20]):
            name = item.get("industry") or item.get("name") or ""
            ranking_records.append({
                "trade_date": trade_date,
                "ranking_type": "industry_top",
                "rank": i + 1,
                "ts_code": item.get("ts_code"),
                "name": name,
                "pct_change": float(item.get("pct_change") or 0),
                "net_amount": float(item.get("net_amount") or 0),
                "lead_stock": item.get("lead_stock", ""),
            })
        
        # 跌幅前20
        for i, item in enumerate(sorted_industry[-20:][::-1]):
            name = item.get("industry") or item.get("name") or ""
            ranking_records.append({
                "trade_date": trade_date,
                "ranking_type": "industry_bottom",
                "rank": i + 1,
                "ts_code": item.get("ts_code"),
                "name": name,
                "pct_change": float(item.get("pct_change") or 0),
                "net_amount": float(item.get("net_amount") or 0),
                "lead_stock": item.get("lead_stock", ""),
            })
    
    # 2. 概念板块排名
    concept_data = await mongo_manager.find_many(
        "moneyflow_concept",
        {"trade_date": trade_date},
    )
    
    if debug and concept_data:
        print(f"  [DEBUG] 概念数据示例: {concept_data[0]}")
    
    if concept_data:
        sorted_concept = sorted(concept_data, key=lambda x: float(x.get("pct_change") or 0), reverse=True)
        
        # 涨幅前20
        for i, item in enumerate(sorted_concept[:20]):
            name = item.get("name") or item.get("concept") or ""
            ranking_records.append({
                "trade_date": trade_date,
                "ranking_type": "concept_top",
                "rank": i + 1,
                "ts_code": item.get("ts_code"),
                "name": name,
                "pct_change": float(item.get("pct_change") or 0),
                "net_amount": float(item.get("net_amount") or 0),
                "lead_stock": item.get("lead_stock", ""),
            })
        
        # 跌幅前20
        for i, item in enumerate(sorted_concept[-20:][::-1]):
            name = item.get("name") or item.get("concept") or ""
            ranking_records.append({
                "trade_date": trade_date,
                "ranking_type": "concept_bottom",
                "rank": i + 1,
                "ts_code": item.get("ts_code"),
                "name": name,
                "pct_change": float(item.get("pct_change") or 0),
                "net_amount": float(item.get("net_amount") or 0),
                "lead_stock": item.get("lead_stock", ""),
            })
    
    return ranking_records


async def main(args):
    """主函数"""
    print("=" * 60)
    print("重新计算 sector_ranking 历史数据")
    print("=" * 60)
    
    # 初始化连接
    await mongo_manager.initialize()
    
    # 确定日期范围
    if args.start and args.end:
        start_date = args.start
        end_date = args.end
    else:
        days = args.days or 30
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days + 10)).strftime("%Y%m%d")
    
    print(f"\n日期范围: {start_date} -> {end_date}")
    
    # 获取有数据的交易日列表
    trade_dates = await get_trade_dates(start_date, end_date)
    print(f"找到 {len(trade_dates)} 个有数据的交易日")
    
    if not trade_dates:
        print("没有找到数据，退出")
        return
    
    total_records = 0
    success_count = 0
    
    for i, trade_date in enumerate(trade_dates, 1):
        try:
            # 计算排名 (第一天打印调试信息)
            records = await compute_ranking_for_date(trade_date, debug=(i == 1))
            
            if records:
                # 删除当天旧数据
                await mongo_manager.delete_many(
                    "sector_ranking",
                    {"trade_date": trade_date},
                )
                
                # 插入新数据
                await mongo_manager.insert_many(
                    "sector_ranking",
                    records,
                )
                
                total_records += len(records)
                success_count += 1
                print(f"[{i}/{len(trade_dates)}] {trade_date} - 计算 {len(records)} 条排名记录")
            else:
                print(f"[{i}/{len(trade_dates)}] {trade_date} - 无数据")
                
        except Exception as e:
            print(f"[{i}/{len(trade_dates)}] {trade_date} - 失败: {e}")
    
    # 打印汇总
    print("\n" + "=" * 60)
    print("计算完成!")
    print("=" * 60)
    print(f"成功: {success_count} 天")
    print(f"总记录: {total_records} 条")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="重新计算 sector_ranking 历史数据")
    parser.add_argument("--days", type=int, default=30, help="计算最近N天 (默认30)")
    parser.add_argument("--start", type=str, help="开始日期 (YYYYMMDD)")
    parser.add_argument("--end", type=str, help="结束日期 (YYYYMMDD)")
    
    args = parser.parse_args()
    
    asyncio.run(main(args))
