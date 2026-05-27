"""
端到端 RPC 测试

在同一个进程中启动服务器和客户端，验证 RPC 通信。
"""

import asyncio
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.rpc import RPCServer, rpc_manager
from core.managers import redis_manager


async def handle_refresh(params):
    """处理 refresh_strategies RPC 请求"""
    trace_id = params.get("_trace_id", "-")
    strategy_type = params.get("strategy_type")
    source = params.get("_source_node", "unknown")
    
    print(f"[Server] Received refresh request!")
    print(f"         trace_id={trace_id}")
    print(f"         strategy_type={strategy_type}")
    print(f"         from={source}")
    
    return {
        "status": "ok",
        "message": "Strategies refreshed!",
        "subscriptions_count": 3,
    }


async def test_e2e():
    print("=" * 60)
    print("RPC 端到端测试")
    print("=" * 60)
    
    # 初始化 Redis
    print("\n[Setup] 初始化 Redis...")
    await redis_manager.initialize()
    
    # 创建并启动服务器
    print("[Setup] 启动 RPC 服务器...")
    server = RPCServer("listener-test", "listener", port=50053)
    server.register_method("refresh_strategies", handle_refresh)
    await server.start()
    
    rpc_address = server.address
    print(f"[Setup] 服务器已启动: {rpc_address}")
    
    # 注册节点到 Redis
    node_info = {
        "node_id": "listener-test",
        "node_type": "listener",
        "rpc_address": rpc_address,
    }
    await redis_manager.register_node("listener-test", node_info, ttl=60)
    print("[Setup] 节点已注册到 Redis")
    
    # 等待服务器就绪
    await asyncio.sleep(0.5)
    
    # 测试 1: 直接地址调用
    print("\n" + "-" * 40)
    print("[Test 1] 直接地址调用")
    print("-" * 40)
    
    result = await rpc_manager.invoke(
        address=rpc_address,
        method="refresh_strategies",
        params={"strategy_type": "ma5_buy"},
        trace_id="test-001",
        source_node="web-test",
    )
    
    print(f"\n[Client] 调用结果:")
    print(f"         success={result.get('success')}")
    print(f"         result={result.get('result')}")
    print(f"         elapsed_ms={result.get('elapsed_ms')}")
    
    if result.get("success"):
        print("         [OK] Test passed!")
    else:
        print(f"         [FAIL] Test failed: {result.get('error')}")
    
    # 测试 2: 广播调用
    print("\n" + "-" * 40)
    print("[Test 2] 广播给所有 listener 类型节点")
    print("-" * 40)
    
    results = await rpc_manager.broadcast_by_type(
        node_type="listener",
        method="refresh_strategies",
        params={"strategy_type": "limit_open"},
        trace_id="test-002",
        source_node="web-test",
    )
    
    print(f"\n[Client] 广播结果:")
    for r in results:
        node_id = r.get("node_id", "unknown")
        success = r.get("success")
        print(f"         {node_id}: success={success}")
    
    if results and all(r.get("success") for r in results):
        print("         [OK] Test passed!")
    else:
        print("         [FAIL] Test failed!")
    
    # 测试 3: 健康检查
    print("\n" + "-" * 40)
    print("[Test 3] 健康检查")
    print("-" * 40)
    
    is_healthy = await rpc_manager.health_check(rpc_address)
    print(f"\n[Client] 健康状态: {is_healthy}")
    
    if is_healthy:
        print("         [OK] Test passed!")
    else:
        print("         [FAIL] Test failed!")
    
    # 清理
    print("\n" + "-" * 40)
    print("[Cleanup] 清理资源...")
    print("-" * 40)
    
    await server.stop()
    await rpc_manager.close()
    await redis_manager.shutdown()
    
    print("\n" + "=" * 60)
    print("测试完成!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(test_e2e())
