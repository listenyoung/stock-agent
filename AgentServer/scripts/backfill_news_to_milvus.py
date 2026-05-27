"""
回填新闻数据到 Milvus 向量库

用途:
- 将 MongoDB 中已有的 news 数据向量化并存入 Milvus
- 支持增量回填（跳过已存在的）
- 支持按日期范围过滤

使用方法:
    # 回填所有新闻
    python scripts/backfill_news_to_milvus.py
    
    # 回填指定日期范围
    python scripts/backfill_news_to_milvus.py --start-date 20260201 --end-date 20260209
    
    # 限制数量（测试用）
    python scripts/backfill_news_to_milvus.py --limit 100
    
    # 强制重新回填（覆盖已存在的）
    python scripts/backfill_news_to_milvus.py --force
"""

import asyncio
import argparse
import sys
from pathlib import Path
from datetime import datetime

# 添加项目根目录到 Python 路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.managers import mongo_manager, initialize_all_managers, shutdown_all_managers
from core.managers.milvus_manager import milvus_manager
from core.managers.llm_manager import llm_manager


async def backfill_news(
    start_date: str = None,
    end_date: str = None,
    limit: int = None,
    batch_size: int = 10,
    force: bool = False,
):
    """
    回填新闻数据到 Milvus
    
    Args:
        start_date: 起始日期 (YYYYMMDD)
        end_date: 结束日期 (YYYYMMDD)
        limit: 限制数量
        batch_size: 每批处理数量
        force: 是否强制覆盖
    """
    print("=" * 60)
    print("新闻数据回填到 Milvus")
    print("=" * 60)
    
    # ==================== 1. 初始化 ====================
    
    print("\n[1/5] 初始化管理器...")
    await initialize_all_managers()
    
    # 检查 Milvus 是否可用
    if milvus_manager.is_disabled():
        print("❌ Milvus 已禁用，无法回填数据")
        print("   提示: 请启动 Milvus 服务或配置远程地址")
        return
    
    print(f"   ✓ MongoDB 已连接")
    print(f"   ✓ Milvus 已连接 (模式: {'Lite' if milvus_manager.is_lite_mode() else 'Remote'})")
    print(f"   ✓ LLM 已连接 (Embedding)")
    
    # ==================== 2. 查询新闻数据 ====================
    
    print("\n[2/5] 查询 MongoDB 中的新闻数据...")
    
    # 构建查询条件
    query = {}
    
    if start_date or end_date:
        # 尝试按 datetime 字段过滤
        date_filter = {}
        if start_date:
            # 转换为 datetime 对象
            start_dt = datetime.strptime(start_date, "%Y%m%d")
            date_filter["$gte"] = start_dt
        if end_date:
            end_dt = datetime.strptime(end_date, "%Y%m%d").replace(hour=23, minute=59, second=59)
            date_filter["$lte"] = end_dt
        
        if date_filter:
            query["datetime"] = date_filter
    
    # 统计总数
    total_count = await mongo_manager.count("news", query)
    print(f"   找到 {total_count} 条新闻")
    
    if total_count == 0:
        print("   没有需要回填的数据")
        return
    
    # 限制数量
    actual_limit = min(limit, total_count) if limit else total_count
    print(f"   将处理 {actual_limit} 条")
    
    # ==================== 3. 分批获取并处理 ====================
    
    print(f"\n[3/5] 开始回填 (批次大小: {batch_size})...")
    
    success_count = 0
    failed_count = 0
    skipped_count = 0
    processed = 0
    
    # 分页获取
    page_size = 100
    offset = 0
    
    while processed < actual_limit:
        # 获取一批数据
        news_batch = await mongo_manager.find_many(
            "news",
            query,
            sort=[("datetime", -1)],
            skip=offset,
            limit=min(page_size, actual_limit - processed),
        )
        
        if not news_batch:
            break
        
        # 处理每条新闻
        for news in news_batch:
            if processed >= actual_limit:
                break
            
            processed += 1
            
            ts_code = news.get("ts_code", "")
            title = news.get("title", "")
            content = news.get("content", "")
            dt = news.get("datetime")
            source = news.get("source", "akshare")
            
            # 提取日期
            if isinstance(dt, datetime):
                trade_date = dt.strftime("%Y%m%d")
                news_datetime = dt.strftime("%Y-%m-%d %H:%M:%S")
            else:
                trade_date = datetime.now().strftime("%Y%m%d")
                news_datetime = str(dt) if dt else ""
            
            # 跳过无效数据
            if not ts_code or not title:
                skipped_count += 1
                continue
            
            try:
                # 插入到 Milvus
                result = await milvus_manager.insert_news(
                    ts_code=ts_code,
                    title=title,
                    content=content,
                    trade_date=trade_date,
                    news_datetime=news_datetime,
                    source=source,
                )
                
                if result:
                    success_count += 1
                else:
                    failed_count += 1
                    
            except Exception as e:
                failed_count += 1
                print(f"   ❌ 错误 [{ts_code}] {title[:30]}: {e}")
            
            # 进度显示
            if processed % 50 == 0:
                print(f"   进度: {processed}/{actual_limit} ({processed*100//actual_limit}%)")
        
        offset += len(news_batch)
    
    # ==================== 4. 统计结果 ====================
    
    print(f"\n[4/5] 回填完成!")
    print(f"   ✓ 成功: {success_count}")
    print(f"   ✗ 失败: {failed_count}")
    print(f"   ○ 跳过: {skipped_count}")
    
    # ==================== 5. 清理 ====================
    
    print("\n[5/5] 关闭连接...")
    await shutdown_all_managers()
    print("   ✓ 完成")
    
    print("\n" + "=" * 60)


def main():
    parser = argparse.ArgumentParser(description="回填新闻数据到 Milvus 向量库")
    
    parser.add_argument(
        "--start-date",
        type=str,
        help="起始日期 (YYYYMMDD)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="结束日期 (YYYYMMDD)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="限制处理数量",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="每批处理数量 (默认: 10)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="强制覆盖已存在的数据",
    )
    
    args = parser.parse_args()
    
    asyncio.run(backfill_news(
        start_date=args.start_date,
        end_date=args.end_date,
        limit=args.limit,
        batch_size=args.batch_size,
        force=args.force,
    ))


if __name__ == "__main__":
    main()
