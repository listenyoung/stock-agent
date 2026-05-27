"""
测试 gRPC RPC 通信

验证 Web 节点能够通过 RPC 通知 Listener 节点刷新策略。

使用方法:
1. 先启动一个模拟的 Listener 节点: python scripts/test_rpc.py server
2. 再启动客户端测试: python scripts/test_rpc.py client
"""

import asyncio
import sys
import os
import logging
from datetime import datetime
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("test_rpc")


async def run_test_server():
    """运行测试 RPC 服务器（模拟 Listener 节点）"""
    from core.rpc import RPCServer
    from core.managers import redis_manager
    
    # 初始化 Redis（用于注册节点）
    await redis_manager.initialize()
    
    # 创建 RPC 服务器
    server = RPCServer(
        node_id="listener-test-001",
        node_type="listener",
        port=50053,
    )
    
    # 注册 refresh_strategies 方法
    async def handle_refresh(params):
        trace_id = params.get("_trace_id", "-")
        strategy_type = params.get("strategy_type")
        source = params.get("_source_node", "unknown")
        
        logger.info(f"[{trace_id}] 收到刷新请求! strategy_type={strategy_type}, from={source}")
        
        # 模拟刷新逻辑
        await asyncio.sleep(0.1)
        
        return {
            "status": "ok",
            "subscriptions_count": 3,
            "message": f"已刷新策略配置 (模拟)",
            "timestamp": datetime.utcnow().isoformat(),
        }
    
    server.register_method("refresh_strategies", handle_refresh)
    
    # 注册节点到 Redis（心跳）
    node_info = {
        "node_id": "listener-test-001",
        "node_type": "listener",
        "host": "localhost",
        "port": 8001,
        "status": "online",
        "rpc_address": server.address,
    }
    await redis_manager.register_node("listener-test-001", node_info, ttl=300)
    
    # 启动服务器
    await server.start()
    
    logger.info("=" * 60)
    logger.info("测试 RPC 服务器已启动")
    logger.info(f"RPC 地址: {server.address}")
    logger.info("等待 RPC 调用...")
    logger.info("按 Ctrl+C 停止")
    logger.info("=" * 60)
    
    try:
        # 保持运行
        while True:
            # 续期心跳
            await redis_manager.register_node("listener-test-001", node_info, ttl=300)
            await asyncio.sleep(10)
    except KeyboardInterrupt:
        pass
    finally:
        await server.stop()
        await redis_manager.shutdown()


async def run_test_client():
    """运行测试 RPC 客户端（模拟 Web 节点调用）"""
    from core.rpc import rpc_manager
    from core.managers import redis_manager
    import uuid
    
    # 初始化 Redis
    await redis_manager.initialize()
    
    logger.info("=" * 60)
    logger.info("测试 RPC 客户端")
    logger.info("=" * 60)
    
    # 测试 1: 直接地址调用
    logger.info("\n[测试 1] 直接地址调用 localhost:50053")
    result = await rpc_manager.invoke(
        address="localhost:50053",
        method="refresh_strategies",
        params={"strategy_type": "ma5_buy"},
        trace_id=uuid.uuid4().hex[:8],
        source_node="web-test",
    )
    logger.info(f"结果: {result}")
    
    # 测试 2: 通过节点 ID 调用
    logger.info("\n[测试 2] 通过节点 ID 调用 listener-test-001")
    result = await rpc_manager.invoke_by_node_id(
        node_id="listener-test-001",
        method="refresh_strategies",
        params={"strategy_type": "limit_open"},
        trace_id=uuid.uuid4().hex[:8],
        source_node="web-test",
    )
    logger.info(f"结果: {result}")
    
    # 测试 3: 广播给所有 Listener 节点
    logger.info("\n[测试 3] 广播给所有 listener 类型节点")
    results = await rpc_manager.broadcast_by_type(
        node_type="listener",
        method="refresh_strategies",
        params={"strategy_type": None},  # 刷新全部
        trace_id=uuid.uuid4().hex[:8],
        source_node="web-test",
    )
    logger.info(f"结果: {results}")
    
    # 测试 4: 健康检查
    logger.info("\n[测试 4] 健康检查")
    is_healthy = await rpc_manager.health_check("localhost:50053")
    logger.info(f"健康状态: {is_healthy}")
    
    # 测试 5: 调用不存在的方法
    logger.info("\n[测试 5] 调用不存在的方法")
    result = await rpc_manager.invoke(
        address="localhost:50053",
        method="non_existent_method",
        params={},
        trace_id=uuid.uuid4().hex[:8],
        source_node="web-test",
    )
    logger.info(f"结果: {result}")
    
    logger.info("\n" + "=" * 60)
    logger.info("测试完成!")
    logger.info("=" * 60)
    
    # 清理
    await rpc_manager.close()
    await redis_manager.shutdown()


async def run_ping_test():
    """快速 ping 测试"""
    from core.rpc import rpc_manager
    import uuid
    
    logger.info("Ping 测试: localhost:50053")
    
    result = await rpc_manager.invoke(
        address="localhost:50053",
        method="ping",
        params={},
        trace_id=uuid.uuid4().hex[:8],
        source_node="ping-test",
        timeout=3.0,
        retries=0,
    )
    
    if result.get("success"):
        logger.info(f"Pong! 节点: {result['result'].get('node_id')}")
    else:
        logger.error(f"Ping 失败: {result.get('error')}")
    
    await rpc_manager.close()


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        print("\n用法:")
        print("  python scripts/test_rpc.py server  - 启动测试服务器")
        print("  python scripts/test_rpc.py client  - 运行客户端测试")
        print("  python scripts/test_rpc.py ping    - 快速 ping 测试")
        sys.exit(1)
    
    mode = sys.argv[1].lower()
    
    if mode == "server":
        asyncio.run(run_test_server())
    elif mode == "client":
        asyncio.run(run_test_client())
    elif mode == "ping":
        asyncio.run(run_ping_test())
    else:
        print(f"未知模式: {mode}")
        sys.exit(1)


if __name__ == "__main__":
    main()
