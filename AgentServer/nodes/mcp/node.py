"""
MCP 服务节点实现
"""

import asyncio
from typing import Optional, Dict, Type
import json

from nodes.base import BaseNode
from core.protocols import NodeType, ToolRequest, ToolResponse
from core.managers import (
    redis_manager,
    mongo_manager,
    tushare_manager,
    milvus_manager,
)
from core.base import BaseTool

from .tools import (
    GetStockBasicTool,
    GetStockDailyTool,
    GetFinancialIndicatorTool,
    GetNewsSentimentTool,
    SearchSimilarReportsTool,
)


class MCPNode(BaseNode):
    """
    MCP 服务节点
    
    职责:
    - 作为数据中间层，为 Inference Agent 提供数据访问工具
    - 封装 Tushare 频率限制逻辑
    - 提供 RAG 检索工具
    """
    
    node_type = NodeType.MCP
    
    def __init__(self, node_id: Optional[str] = None, port: int = 9000):
        super().__init__(node_id)
        self.port = port
        
        # 工具注册表
        self._tools: Dict[str, BaseTool] = {}
    
    async def start(self) -> None:
        """启动 MCP 服务"""
        # 按依赖顺序初始化管理器
        self.logger.info("Initializing managers...")
        await redis_manager.initialize()      # 消息队列
        await mongo_manager.initialize()      # 数据查询
        await tushare_manager.initialize()    # 股票数据
        await milvus_manager.initialize()     # 向量检索
        
        # 注册工具
        self._register_tools()
        
        self.logger.info(f"MCP node started with {len(self._tools)} tools")
    
    async def stop(self) -> None:
        """停止服务"""
        pass
    
    async def run(self) -> None:
        """
        节点主循环
        
        监听 Redis 中的工具调用请求。
        """
        self.logger.info("MCP node listening for tool requests...")
        
        while self._running:
            try:
                # 从队列获取请求
                request_json = await redis_manager.dequeue_task(timeout=5)
                
                if request_json:
                    request_data = json.loads(request_json)
                    
                    # 判断是否是工具调用请求
                    if request_data.get("tool_name"):
                        # 设置 trace_id
                        trace_id = request_data.get("trace_id", "-")
                        self.set_trace_id(trace_id)
                        
                        try:
                            request = ToolRequest(**request_data)
                            response = await self._handle_tool_request(request)
                            
                            # 发布响应
                            await redis_manager.publish_result(
                                request.message_id,
                                response,
                            )
                        finally:
                            self.clear_trace_id()
                        
            except Exception as e:
                self.logger.error(f"Error processing request: {e}")
                await asyncio.sleep(1)
    
    def _register_tools(self) -> None:
        """注册所有工具"""
        tool_classes: list[Type[BaseTool]] = [
            GetStockBasicTool,
            GetStockDailyTool,
            GetFinancialIndicatorTool,
            GetNewsSentimentTool,
            SearchSimilarReportsTool,
        ]
        
        for cls in tool_classes:
            tool = cls()
            self._tools[tool.name] = tool
            self.logger.info(f"Registered tool: {tool.name}")
    
    async def _handle_tool_request(self, request: ToolRequest) -> ToolResponse:
        """处理工具调用请求"""
        tool_name = request.tool_name
        
        if tool_name not in self._tools:
            return ToolResponse(
                request_id=request.message_id,
                success=False,
                error=f"Tool not found: {tool_name}",
            )
        
        tool = self._tools[tool_name]
        
        try:
            # 构建输入
            input_data = tool.input_model(**request.arguments)
            
            # 执行工具
            result = await tool(input_data)
            
            return ToolResponse(
                request_id=request.message_id,
                success=result.success,
                result=result.model_dump() if result.success else None,
                error=result.error_message if not result.success else None,
                execution_time_ms=result.execution_time_ms,
            )
            
        except Exception as e:
            return ToolResponse(
                request_id=request.message_id,
                success=False,
                error=str(e),
            )
    
    async def call_tool(self, tool_name: str, **kwargs) -> dict:
        """直接调用工具 (本地调用)"""
        if tool_name not in self._tools:
            raise ValueError(f"Tool not found: {tool_name}")
        
        tool = self._tools[tool_name]
        input_data = tool.input_model(**kwargs)
        result = await tool(input_data)
        
        return result.model_dump()
    
    def get_tool_schemas(self) -> list[dict]:
        """获取所有工具的 Schema"""
        return [tool.get_schema() for tool in self._tools.values()]


def main():
    """入口函数"""
    import os
    
    port = int(os.environ.get("MCP_PORT", 9000))
    node = MCPNode(port=port)
    asyncio.run(node.main())


if __name__ == "__main__":
    main()
