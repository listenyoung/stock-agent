"""
Listener 节点测试脚本

验证:
1. 策略订阅配置存储到 MongoDB
2. "涨幅超过 3%" 策略能正确识别个股
3. 企业微信消息推送 (需配置 NOTIFY_WECOM_WEBHOOK)

使用方式:
    cd AgentServer
    python scripts/test_listener.py
"""

import asyncio
import sys
import os

# 添加项目根目录到 path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from core.managers import mongo_manager, tushare_manager
from core.managers.notification_manager import notification_manager
from core.protocols import (
    StrategySubscription,
    StrategyAlert,
    MarketSnapshot,
    StrategyType,
)
from nodes.listener.strategies import PriceChangeStrategy, MA5BuyStrategy


async def test_create_subscription():
    """测试创建策略订阅配置"""
    print("\n=== 1. 测试创建策略订阅 ===")
    
    await mongo_manager.initialize()
    
    # 创建一个 "涨幅超过 3%" 的策略订阅
    # 注意: watch_list 必须明确指定股票代码，空列表不会触发任何监听
    # 如需全市场监听，必须包含 "ALL" 标识
    subscription = StrategySubscription(
        strategy_id="test_price_change_3pct",
        strategy_name="涨幅超过3%预警",
        strategy_type=StrategyType.PRICE_CHANGE,
        watch_list=["000001.SZ", "600000.SH", "000815.SZ"],  # 指定个股
        params={
            "threshold": 3.0,
            "direction": "both",
            "once_per_day": True,
        },
        is_active=True,
        user_id="test_user",
    )
    
    # 存储到 MongoDB
    await mongo_manager.update_one(
        "strategy_subscriptions",
        {"strategy_id": subscription.strategy_id},
        {"$set": subscription.model_dump(mode="json")},
        upsert=True,
    )
    
    print(f"✓ 创建订阅: {subscription.strategy_name}")
    print(f"  策略ID: {subscription.strategy_id}")
    print(f"  监听股票: {subscription.watch_list}")
    print(f"  阈值: {subscription.params.get('threshold')}%")
    
    # 验证存储
    record = await mongo_manager.find_one(
        "strategy_subscriptions",
        {"strategy_id": subscription.strategy_id},
    )
    print(f"✓ 存储验证: {record is not None}")
    
    return subscription


async def test_price_change_strategy(subscription: StrategySubscription):
    """测试涨幅策略能否正确识别个股"""
    print("\n=== 2. 测试涨幅策略识别 ===")
    
    strategy = PriceChangeStrategy()
    
    # 模拟市场快照数据
    snapshot = MarketSnapshot()
    snapshot.quotes = {
        "000001.SZ": {
            "ts_code": "000001.SZ",
            "name": "平安银行",
            "price": 10.50,
            "pct_chg": 2.5,  # 涨 2.5%，不触发
        },
        "600000.SH": {
            "ts_code": "600000.SH",
            "name": "浦发银行",
            "price": 8.20,
            "pct_chg": 3.5,  # 涨 3.5%，触发!
        },
        "000815.SZ": {
            "ts_code": "000815.SZ",
            "name": "美利云",
            "price": 12.00,
            "pct_chg": -4.0,  # 跌 4%，触发!
        },
    }
    snapshot.total_stocks = len(snapshot.quotes)
    
    # 执行策略评估
    alerts = await strategy.evaluate(
        subscription=subscription,
        snapshot=snapshot,
        previous_snapshot=None,
    )
    
    print(f"✓ 评估完成，触发 {len(alerts)} 个预警")
    
    for alert in alerts:
        print(f"  - {alert.stock_name} ({alert.ts_code}): {alert.trigger_reason}")
    
    # 验证结果
    assert len(alerts) == 2, f"预期触发 2 个预警，实际 {len(alerts)}"
    triggered_codes = {alert.ts_code for alert in alerts}
    assert "600000.SH" in triggered_codes, "600000.SH 应该触发"
    assert "000815.SZ" in triggered_codes, "000815.SZ 应该触发"
    assert "000001.SZ" not in triggered_codes, "000001.SZ 不应该触发"
    
    print("✓ 策略识别验证通过!")
    
    return alerts


async def test_notification(alerts: list):
    """测试企业微信通知推送"""
    print("\n=== 3. 测试企业微信通知 ===")
    
    await notification_manager.initialize()
    
    if not notification_manager._config.is_configured:
        print("⚠ 未配置 NOTIFY_WECOM_WEBHOOK，跳过通知测试")
        print("  请在 .env 中设置: NOTIFY_WECOM_WEBHOOK=https://qyapi.weixin.qq.com/...")
        return
    
    # 发送第一个预警
    if alerts:
        alert = alerts[0]
        success = await notification_manager.send_alert(alert)
        print(f"✓ 发送预警: {alert.stock_name} -> {'成功' if success else '失败'}")


async def test_ma5_buy_strategy():
    """测试5日线低吸策略"""
    print("\n=== 4. 测试5日线低吸策略 ===")
    
    # 创建5日线低吸策略订阅
    subscription = StrategySubscription(
        strategy_id="test_ma5_buy",
        strategy_name="5日线低吸",
        strategy_type=StrategyType.MA5_BUY,
        watch_list=["000001.SZ", "600000.SH"],
        params={
            "ma_period": 5,         # 均线周期
            "touch_range": 0.02,    # 触及范围 ±2%
            "max_break_pct": 0.03,  # 最大跌破 3%
            "require_stabilize": True,  # 需要企稳信号
        },
        is_active=True,
        user_id="test_user",
    )
    
    # 存储到 MongoDB
    await mongo_manager.update_one(
        "strategy_subscriptions",
        {"strategy_id": subscription.strategy_id},
        {"$set": subscription.model_dump(mode="json")},
        upsert=True,
    )
    
    print(f"✓ 创建5日线低吸订阅: {subscription.strategy_name}")
    print(f"  监听股票: {subscription.watch_list}")
    print(f"  参数: {subscription.params}")
    
    # 测试策略逻辑
    strategy = MA5BuyStrategy()
    
    # 模拟数据: 股价从 10.5 回落到 10.2 (MA5=10.0)
    snapshot = MarketSnapshot()
    snapshot.quotes = {
        "000001.SZ": {
            "ts_code": "000001.SZ",
            "name": "平安银行",
            "price": 10.15,  # 当前价接近 MA5
            "low": 10.05,    # 最低价略低于当前价
            "pct_chg": -1.5,  # 小幅下跌
        },
    }
    
    # 模拟缓存数据 (正常情况从 MongoDB 获取)
    strategy._stock_cache["000001.SZ"] = {
        "ma5": 10.0,        # 5日均线
        "prev_close": 10.3,  # 昨收在 MA5 上方
    }
    
    alerts = await strategy.evaluate(subscription, snapshot)
    
    if alerts:
        print(f"✓ 策略触发 {len(alerts)} 个信号:")
        for alert in alerts:
            print(f"  - {alert.stock_name}: {alert.trigger_reason}")
            print(f"    MA5={alert.extra_data.get('ma5')}, 距离={alert.extra_data.get('distance_to_ma5')}%")
    else:
        print("  未触发信号 (可能不满足条件)")
    
    return subscription


async def test_tushare_apis():
    """测试 Tushare 实时行情接口"""
    print("\n=== 5. 测试 Tushare 实时接口 ===")
    
    await tushare_manager.initialize()
    
    if not tushare_manager.is_initialized:
        print("⚠ Tushare 未初始化，跳过接口测试")
        return
    
    # 测试 stk_limit
    print("测试 stk_limit 接口...")
    try:
        limit_data = await tushare_manager.get_stk_limit()
        print(f"✓ 获取涨跌停价格: {len(limit_data)} 条记录")
        if limit_data:
            sample = limit_data[0]
            print(f"  示例: {sample.get('ts_code')} 涨停价={sample.get('up_limit')} 跌停价={sample.get('down_limit')}")
    except Exception as e:
        print(f"✗ stk_limit 接口异常: {e}")
    
    # 测试 realtime_quote (需要较高权限)
    print("\n测试 realtime_quote 接口...")
    try:
        quotes = await tushare_manager.get_realtime_quote(["000001.SZ", "600000.SH"])
        print(f"✓ 获取实时行情: {len(quotes)} 条记录")
        for q in quotes[:2]:
            print(f"  {q.get('ts_code')} {q.get('name')}: 价格={q.get('price')} 涨跌={q.get('pct_chg')}%")
    except Exception as e:
        print(f"✗ realtime_quote 接口异常: {e}")
        print("  注意: 此接口需要 Tushare 较高积分权限")


async def main():
    """主测试流程"""
    print("=" * 50)
    print("Listener 节点测试")
    print("=" * 50)
    
    try:
        # 1. 测试创建订阅
        subscription = await test_create_subscription()
        
        # 2. 测试策略识别
        alerts = await test_price_change_strategy(subscription)
        
        # 3. 测试通知推送
        await test_notification(alerts)
        
        # 4. 测试5日线低吸策略
        await test_ma5_buy_strategy()
        
        # 5. 测试 Tushare 接口
        await test_tushare_apis()
        
        print("\n" + "=" * 50)
        print("✓ 所有测试完成!")
        print("=" * 50)
        
        print("\n启动 Listener 节点:")
        print("  cd AgentServer")
        print("  $env:NODE_TYPE='listener'; python main.py")
        
    except Exception as e:
        print(f"\n✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # 清理
        await mongo_manager.shutdown()
        await notification_manager.shutdown()
        await tushare_manager.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
