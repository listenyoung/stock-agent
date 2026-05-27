"""
StockAgent 统一入口

根据环境变量 NODE_TYPE 启动对应节点:
- web: Web 网关节点
- data_sync: 数据同步节点
- mcp: MCP 服务节点
- inference: 分析智能体节点
- listener: 实时监听节点
- backtest: 量化回测节点

使用方式:
    # 启动 Web 节点
    NODE_TYPE=web python main.py
    
    # 启动数据同步节点
    NODE_TYPE=data_sync python main.py
    
    # 启动推理节点 (可启动多个)
    NODE_TYPE=inference MAX_CONCURRENT_TASKS=10 python main.py
    
    # 启动监听节点
    NODE_TYPE=listener python main.py
    
    # 启动回测节点
    NODE_TYPE=backtest python main.py
"""

import asyncio
import os
import sys

from core.settings import settings
from core.protocols import NodeType


def main():
    """主入口"""
    # 从环境变量或配置获取节点类型
    node_type_str = os.environ.get("NODE_TYPE", settings.node.node_type)
    
    try:
        node_type = NodeType(node_type_str)
    except ValueError:
        print(f"Invalid NODE_TYPE: {node_type_str}")
        print(f"Valid types: {[t.value for t in NodeType]}")
        sys.exit(1)
    
    print(f"Starting {node_type.value} node...")
    
    # 根据节点类型创建并启动节点
    if node_type == NodeType.WEB:
        from nodes.web.node import WebNode
        node = WebNode()
        
    elif node_type == NodeType.DATA_SYNC:
        from nodes.data_sync.node import DataSyncNode
        node = DataSyncNode()
        
    elif node_type == NodeType.MCP:
        from nodes.mcp.node import MCPNode
        node = MCPNode()
        
    elif node_type == NodeType.INFERENCE:
        from nodes.inference.node import InferenceNode
        max_tasks = int(os.environ.get("MAX_CONCURRENT_TASKS", 5))
        node = InferenceNode(max_concurrent_tasks=max_tasks)
    
    elif node_type == NodeType.LISTENER:
        from nodes.listener.node import ListenerNode
        node = ListenerNode()
    
    elif node_type == NodeType.BACKTEST:
        from nodes.backtest_engine.node import BacktestNode
        node = BacktestNode()
        
    else:
        print(f"Unknown node type: {node_type}")
        sys.exit(1)
    
    # 运行节点
    try:
        asyncio.run(node.main())
    except KeyboardInterrupt:
        print("\nShutdown by user")
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
