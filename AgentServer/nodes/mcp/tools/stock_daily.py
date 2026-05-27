"""
日线数据工具
"""

from typing import Optional, List, Dict, Any

from pydantic import BaseModel, Field

from core.base import BaseTool, ToolResult
from core.managers import tushare_manager, mongo_manager


class GetStockDailyInput(BaseModel):
    """输入"""
    ts_code: str = Field(..., description="股票代码")
    start_date: Optional[str] = Field(None, description="开始日期 YYYYMMDD")
    end_date: Optional[str] = Field(None, description="结束日期 YYYYMMDD")
    limit: int = Field(default=30, description="返回条数")


class GetStockDailyOutput(ToolResult):
    """输出"""
    data: List[Dict[str, Any]] = Field(default_factory=list)


class GetStockDailyTool(BaseTool[GetStockDailyInput, GetStockDailyOutput]):
    """获取股票日线数据"""
    
    name = "get_stock_daily"
    description = "获取股票日线行情数据，包括开高低收、成交量等"
    input_model = GetStockDailyInput
    output_model = GetStockDailyOutput
    
    async def execute(self, input_data: GetStockDailyInput) -> GetStockDailyOutput:
        """执行"""
        # 先从 MongoDB 查询
        filter_query = {"ts_code": input_data.ts_code.upper()}
        
        if input_data.start_date:
            filter_query["trade_date"] = {"$gte": input_data.start_date}
        if input_data.end_date:
            filter_query.setdefault("trade_date", {})["$lte"] = input_data.end_date
        
        data = await mongo_manager.find_many(
            "stock_daily",
            filter_query,
            sort=[("trade_date", -1)],
            limit=input_data.limit,
        )
        
        # 如果没有数据，从 Tushare 获取
        if not data:
            data = await tushare_manager.get_daily(
                ts_code=input_data.ts_code,
                start_date=input_data.start_date,
                end_date=input_data.end_date,
            )
        
        return GetStockDailyOutput(data=data)
