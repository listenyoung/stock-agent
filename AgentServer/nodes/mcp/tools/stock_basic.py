"""
股票基础信息工具
"""

from typing import Optional, List, Dict, Any

from pydantic import BaseModel, Field

from core.base import BaseTool, ToolResult
from core.managers import tushare_manager, mongo_manager


class GetStockBasicInput(BaseModel):
    """输入"""
    ts_code: Optional[str] = Field(None, description="股票代码，如 000001.SZ")
    name: Optional[str] = Field(None, description="股票名称，支持模糊搜索")


class GetStockBasicOutput(ToolResult):
    """输出"""
    data: List[Dict[str, Any]] = Field(default_factory=list)


class GetStockBasicTool(BaseTool[GetStockBasicInput, GetStockBasicOutput]):
    """获取股票基础信息"""
    
    name = "get_stock_basic"
    description = "获取股票基础信息，包括代码、名称、行业、上市日期等"
    input_model = GetStockBasicInput
    output_model = GetStockBasicOutput
    
    async def execute(self, input_data: GetStockBasicInput) -> GetStockBasicOutput:
        """执行"""
        if input_data.ts_code:
            # 精确查询
            stock = await mongo_manager.find_one(
                "stock_basic",
                {"ts_code": input_data.ts_code.upper()},
            )
            data = [stock] if stock else []
        elif input_data.name:
            # 模糊搜索
            data = await mongo_manager.find_many(
                "stock_basic",
                {"name": {"$regex": input_data.name}},
                limit=10,
            )
        else:
            # 从 Tushare 获取
            data = await tushare_manager.get_stock_basic()
        
        return GetStockBasicOutput(data=data)
