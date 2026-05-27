"""
补全行业/板块资金流向历史数据

用法:
    python scripts/sync_moneyflow.py --days 30
    python scripts/sync_moneyflow.py --start 20251201 --end 20260130
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
    # 从 trade_cal 获取交易日历
    cal_data = await mongo_manager.find_many(
        "trade_cal",
        {
            "cal_date": {"$gte": start_date, "$lte": end_date},
            "is_open": 1
        },
        sort=[("cal_date", -1)]
    )
    
    if cal_data:
        return [d["cal_date"] for d in cal_data]
    
    # 如果没有交易日历，尝试从 Tushare 获取
    print("从 Tushare 获取交易日历...")
    try:
        df = await asyncio.to_thread(
            lambda: tushare_manager.pro.trade_cal(
                start_date=start_date,
                end_date=end_date,
                is_open=1
            )
        )
        if df is not None and not df.empty:
            return sorted(df["cal_date"].tolist(), reverse=True)
    except Exception as e:
        print(f"获取交易日历失败: {e}")
    
    # 最后备选：生成日期列表（排除周末）
    dates = []
    current = datetime.strptime(end_date, "%Y%m%d")
    end_dt = datetime.strptime(start_date, "%Y%m%d")
    while current >= end_dt:
        if current.weekday() < 5:  # 排除周末
            dates.append(current.strftime("%Y%m%d"))
        current -= timedelta(days=1)
    return dates


async def sync_moneyflow_industry(trade_dates: list) -> dict:
    """同步行业资金流向数据"""
    print(f"\n=== 同步行业资金流向 ({len(trade_dates)} 天) ===")
    
    total_count = 0
    success_dates = []
    failed_dates = []
    
    for i, trade_date in enumerate(trade_dates, 1):
        try:
            # 检查是否已有数据
            existing = await mongo_manager.find_one(
                "moneyflow_industry",
                {"trade_date": trade_date}
            )
            if existing:
                print(f"[{i}/{len(trade_dates)}] {trade_date} - 已存在，跳过")
                success_dates.append(trade_date)
                continue
            
            # 从 Tushare 获取数据
            records = await tushare_manager.get_moneyflow_ind_ths(trade_date=trade_date)
            
            if records:
                # 写入数据库
                result = await mongo_manager.bulk_upsert(
                    collection="moneyflow_industry",
                    documents=records,
                    key_fields=["ts_code", "trade_date"],
                    batch_size=500,
                )
                count = result["upserted"] + result["modified"]
                total_count += count
                success_dates.append(trade_date)
                print(f"[{i}/{len(trade_dates)}] {trade_date} - 同步 {count} 条行业数据")
            else:
                print(f"[{i}/{len(trade_dates)}] {trade_date} - 无数据（可能非交易日）")
                
            # 避免请求过快
            await asyncio.sleep(0.3)
            
        except Exception as e:
            print(f"[{i}/{len(trade_dates)}] {trade_date} - 失败: {e}")
            failed_dates.append(trade_date)
    
    return {
        "total_count": total_count,
        "success_dates": len(success_dates),
        "failed_dates": failed_dates,
    }


async def sync_moneyflow_concept(trade_dates: list) -> dict:
    """同步板块资金流向数据"""
    print(f"\n=== 同步板块资金流向 ({len(trade_dates)} 天) ===")
    
    total_count = 0
    success_dates = []
    failed_dates = []
    
    for i, trade_date in enumerate(trade_dates, 1):
        try:
            # 检查是否已有数据
            existing = await mongo_manager.find_one(
                "moneyflow_concept",
                {"trade_date": trade_date}
            )
            if existing:
                print(f"[{i}/{len(trade_dates)}] {trade_date} - 已存在，跳过")
                success_dates.append(trade_date)
                continue
            
            # 从 Tushare 获取数据
            records = await tushare_manager.get_moneyflow_cnt_ths(trade_date=trade_date)
            
            if records:
                # 写入数据库
                result = await mongo_manager.bulk_upsert(
                    collection="moneyflow_concept",
                    documents=records,
                    key_fields=["ts_code", "trade_date"],
                    batch_size=500,
                )
                count = result["upserted"] + result["modified"]
                total_count += count
                success_dates.append(trade_date)
                print(f"[{i}/{len(trade_dates)}] {trade_date} - 同步 {count} 条板块数据")
            else:
                print(f"[{i}/{len(trade_dates)}] {trade_date} - 无数据（可能非交易日）")
                
            # 避免请求过快
            await asyncio.sleep(0.3)
            
        except Exception as e:
            print(f"[{i}/{len(trade_dates)}] {trade_date} - 失败: {e}")
            failed_dates.append(trade_date)
    
    return {
        "total_count": total_count,
        "success_dates": len(success_dates),
        "failed_dates": failed_dates,
    }


async def main(args):
    """主函数"""
    print("=" * 60)
    print("行业/板块资金流向数据补全脚本")
    print("=" * 60)
    
    # 初始化连接
    await mongo_manager.initialize()
    await tushare_manager.initialize()
    
    # 确定日期范围
    if args.start and args.end:
        start_date = args.start
        end_date = args.end
    else:
        days = args.days or 30
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
    
    print(f"\n日期范围: {start_date} -> {end_date}")
    
    # 获取交易日列表
    trade_dates = await get_trade_dates(start_date, end_date)
    print(f"找到 {len(trade_dates)} 个交易日")
    
    if not trade_dates:
        print("没有找到交易日，退出")
        return
    
    # 同步行业数据
    industry_result = await sync_moneyflow_industry(trade_dates)
    
    # 同步板块数据
    concept_result = await sync_moneyflow_concept(trade_dates)
    
    # 打印汇总
    print("\n" + "=" * 60)
    print("同步完成!")
    print("=" * 60)
    print(f"\n行业资金流向:")
    print(f"  - 成功: {industry_result['success_dates']} 天")
    print(f"  - 记录: {industry_result['total_count']} 条")
    if industry_result['failed_dates']:
        print(f"  - 失败: {industry_result['failed_dates']}")
    
    print(f"\n板块资金流向:")
    print(f"  - 成功: {concept_result['success_dates']} 天")
    print(f"  - 记录: {concept_result['total_count']} 条")
    if concept_result['failed_dates']:
        print(f"  - 失败: {concept_result['failed_dates']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="补全行业/板块资金流向历史数据")
    parser.add_argument("--days", type=int, default=30, help="同步最近N天 (默认30)")
    parser.add_argument("--start", type=str, help="开始日期 (YYYYMMDD)")
    parser.add_argument("--end", type=str, help="结束日期 (YYYYMMDD)")
    
    args = parser.parse_args()
    
    asyncio.run(main(args))
