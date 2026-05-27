"""
查看 Milvus 向量数据库内容

用法:
    # 查看所有 collection 统计
    python scripts/view_milvus_data.py
    
    # 查看指定 collection 的数据
    python scripts/view_milvus_data.py --collection Market_Snippets --limit 5
    
    # 搜索包含关键词的数据
    python scripts/view_milvus_data.py --search "人工智能"
"""

import asyncio
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.managers import initialize_all_managers, shutdown_all_managers
from core.managers.milvus_manager import milvus_manager
from core.settings import settings


async def view_collections():
    """查看所有 collection 信息"""
    print("\n" + "=" * 60)
    print("Milvus Collections 统计")
    print("=" * 60)
    
    if milvus_manager.is_disabled():
        print("❌ Milvus 已禁用")
        return
    
    client = milvus_manager._client
    collections = client.list_collections()
    
    print(f"\n找到 {len(collections)} 个 Collection:\n")
    
    for coll_name in collections:
        try:
            # 获取 collection 信息
            stats = client.get_collection_stats(coll_name)
            row_count = stats.get("row_count", 0)
            
            print(f"📁 {coll_name}")
            print(f"   数据量: {row_count} 条")
            print()
        except Exception as e:
            print(f"📁 {coll_name}")
            print(f"   ⚠️ 无法获取统计: {e}")
            print()


async def view_data(collection: str, limit: int = 10):
    """查看指定 collection 的数据"""
    print(f"\n" + "=" * 60)
    print(f"Collection: {collection} (前 {limit} 条)")
    print("=" * 60)
    
    if milvus_manager.is_disabled():
        print("❌ Milvus 已禁用")
        return
    
    client = milvus_manager._client
    
    # 检查 collection 是否存在
    if not client.has_collection(collection):
        print(f"❌ Collection '{collection}' 不存在")
        print(f"\n可用的 Collections:")
        for c in client.list_collections():
            print(f"  - {c}")
        return
    
    try:
        # 查询数据
        results = client.query(
            collection_name=collection,
            filter="",
            output_fields=["*"],
            limit=limit,
        )
        
        if not results:
            print("\n📭 Collection 为空，没有数据")
            return
        
        print(f"\n找到 {len(results)} 条数据:\n")
        
        for i, item in enumerate(results, 1):
            print(f"--- [{i}] ---")
            for key, value in item.items():
                if key == "vector":
                    print(f"  vector: [{len(value)} dims]")
                elif isinstance(value, str) and len(value) > 100:
                    print(f"  {key}: {value[:100]}...")
                else:
                    print(f"  {key}: {value}")
            print()
            
    except Exception as e:
        print(f"❌ 查询失败: {e}")


async def search_data(query: str, limit: int = 5):
    """语义搜索"""
    print(f"\n" + "=" * 60)
    print(f"语义搜索: '{query}'")
    print("=" * 60)
    
    if milvus_manager.is_disabled():
        print("❌ Milvus 已禁用")
        return
    
    from core.managers.llm_manager import llm_manager
    
    # 生成查询向量
    print("\n生成查询向量...")
    embeddings = await llm_manager.embedding([query])
    query_vector = embeddings[0]
    
    # 搜索研报
    print(f"\n📚 研报搜索结果:")
    reports = await milvus_manager.search_reports(query_vector, top_k=limit)
    if reports:
        for i, r in enumerate(reports, 1):
            print(f"  [{i}] 相似度: {1 - r.get('distance', 0):.3f}")
            content = r.get("content", "")[:150]
            print(f"      {content}...")
    else:
        print("  (无结果)")
    
    # 搜索新闻
    print(f"\n📰 新闻搜索结果:")
    snippets = await milvus_manager.search_market_snippets(query_vector, top_k=limit)
    if snippets:
        for i, s in enumerate(snippets, 1):
            print(f"  [{i}] 相似度: {1 - s.get('distance', 0):.3f}")
            content = s.get("content", "")[:150]
            print(f"      {content}...")
    else:
        print("  (无结果)")


async def main():
    parser = argparse.ArgumentParser(description="查看 Milvus 数据")
    parser.add_argument("--collection", "-c", type=str, help="指定 Collection 名称")
    parser.add_argument("--limit", "-l", type=int, default=10, help="显示数量 (默认: 10)")
    parser.add_argument("--search", "-s", type=str, help="语义搜索关键词")
    
    args = parser.parse_args()
    
    # 初始化
    print("初始化管理器...")
    await initialize_all_managers()
    
    try:
        if args.search:
            # 语义搜索
            await search_data(args.search, args.limit)
        elif args.collection:
            # 查看指定 collection
            await view_data(args.collection, args.limit)
        else:
            # 查看所有 collection 统计
            await view_collections()
    finally:
        await shutdown_all_managers()


if __name__ == "__main__":
    asyncio.run(main())
