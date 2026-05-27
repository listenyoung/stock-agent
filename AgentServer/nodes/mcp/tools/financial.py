"""
财务指标工具
"""

from typing import Optional, List, Dict, Any

from pydantic import BaseModel, Field

from core.base import BaseTool, ToolResult
from core.managers import tushare_manager


class GetFinancialIndicatorInput(BaseModel):
    """输入"""
    ts_code: str = Field(..., description="股票代码")
    period: Optional[str] = Field(None, description="报告期 YYYYMMDD")


class GetFinancialIndicatorOutput(ToolResult):
    """输出"""
    data: List[Dict[str, Any]] = Field(default_factory=list)


class GetFinancialIndicatorTool(BaseTool[GetFinancialIndicatorInput, GetFinancialIndicatorOutput]):
    """获取财务指标"""
    
    name = "get_financial_indicator"
    description = "获取股票财务指标，包括 EPS、ROE、毛利率等"
    input_model = GetFinancialIndicatorInput
    output_model = GetFinancialIndicatorOutput
    
    async def execute(self, input_data: GetFinancialIndicatorInput) -> GetFinancialIndicatorOutput:
        """执行"""
        data = await tushare_manager.get_financial_indicator(
            ts_code=input_data.ts_code,
            period=input_data.period,
        )
        
        return GetFinancialIndicatorOutput(data=data)
