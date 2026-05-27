"""
初始化策略订阅

简化架构：
- 每种策略类型只有一条策略数据
- 使用默认参数初始化
- 幂等操作：已存在则跳过
"""

import asyncio
import uuid
from datetime import datetime
import sys
import os

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.managers import mongo_manager
from core.protocols import StrategyType


# 策略配置（与 subscription.py 中的 STRATEGY_META 保持一致）
STRATEGIES = [
    {
        "strategy_type": StrategyType.MA5_BUY.value,
        "strategy_name": "5日线低吸",
        "params": {
            "touch_range": 2,
            "max_break_pct": 3,
            "require_stabilize": False,
        },
    },
    {
        "strategy_type": StrategyType.LIMIT_OPEN.value,
        "strategy_name": "涨跌停打开",
        "params": {
            "open_threshold": 2,
        },
    },
    {
        "strategy_type": StrategyType.PRICE_CHANGE.value,
        "strategy_name": "涨跌幅阈值",
        "params": {
            "change_threshold": 5,
        },
    },
]


async def init_strategies():
    """
    初始化所有策略
    
    每种策略类型只创建一条记录
    """
    print("=" * 60)
    print("初始化策略订阅")
    print("=" * 60)
    
    # 初始化 MongoDB
    await mongo_manager.initialize()
    print("MongoDB 连接成功")
    
    created = []
    existing = []
    
    for strategy in STRATEGIES:
        strategy_type = strategy["strategy_type"]
        strategy_name = strategy["strategy_name"]
        
        # 检查是否已存在
        record = await mongo_manager.find_one(
            "strategy_subscriptions",
            {"strategy_type": strategy_type},
        )
        
        if record:
            print(f"  [跳过] {strategy_name} ({strategy_type}) - 已存在")
            existing.append(strategy_type)
            continue
        
        # 创建新策略
        now = datetime.utcnow()
        doc = {
            "subscription_id": uuid.uuid4().hex,
            "strategy_id": uuid.uuid4().hex,
            "strategy_name": strategy_name,
            "strategy_type": strategy_type,
            "watch_list": [],  # 初始为空
            "params": strategy["params"],
            "is_active": True,
            "created_at": now,
            "updated_at": now,
        }
        
        await mongo_manager.insert_one("strategy_subscriptions", doc)
        print(f"  [创建] {strategy_name} ({strategy_type})")
        created.append(strategy_type)
    
    print()
    print("-" * 60)
    print(f"完成: 创建 {len(created)} 个, 跳过 {len(existing)} 个")
    print("-" * 60)


async def cleanup_duplicates():
    """
    清理重复的策略（每种类型只保留一个）
    """
    print()
    print("=" * 60)
    print("清理重复策略")
    print("=" * 60)
    
    for strategy_type in [s["strategy_type"] for s in STRATEGIES]:
        records = await mongo_manager.find_many(
            "strategy_subscriptions",
            {"strategy_type": strategy_type},
            sort=[("created_at", 1)],  # 按创建时间排序，保留最早的
        )
        
        if len(records) <= 1:
            print(f"  [正常] {strategy_type}: {len(records)} 条记录")
            continue
        
        # 保留第一条，删除其他
        to_delete = records[1:]
        for record in to_delete:
            await mongo_manager.delete_one(
                "strategy_subscriptions",
                {"_id": record["_id"]},
            )
        
        print(f"  [清理] {strategy_type}: 删除 {len(to_delete)} 条重复记录")


async def show_current_state():
    """
    显示当前策略状态
    """
    print()
    print("=" * 60)
    print("当前策略状态")
    print("=" * 60)
    
    records = await mongo_manager.find_many(
        "strategy_subscriptions",
        {},
        sort=[("strategy_type", 1)],
    )
    
    if not records:
        print("  (无策略)")
        return
    
    for record in records:
        strategy_type = record.get("strategy_type", "unknown")
        strategy_name = record.get("strategy_name", "未命名")
        is_active = record.get("is_active", True)
        watch_list = record.get("watch_list", [])
        params = record.get("params", {})
        
        status = "✓ 激活" if is_active else "✗ 停用"
        stocks = f"{len(watch_list)} 只股票"
        
        print(f"  [{strategy_type}] {strategy_name}")
        print(f"      状态: {status}")
        print(f"      监听: {stocks}")
        print(f"      参数: {params}")
        print()


async def main():
    """主函数"""
    try:
        await cleanup_duplicates()
        await init_strategies()
        await show_current_state()
    finally:
        await mongo_manager.close()


if __name__ == "__main__":
    asyncio.run(main())
