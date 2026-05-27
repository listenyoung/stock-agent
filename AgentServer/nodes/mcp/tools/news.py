"""
新闻舆情工具
"""

from typing import Optional, List, Dict, Any

from pydantic import BaseModel, Field

from core.base import BaseTool, ToolResult
from core.managers import mongo_manager


class GetNewsSentimentInput(BaseModel):
    """输入"""
    ts_code: Optional[str] = Field(None, description="股票代码")
    keyword: Optional[str] = Field(None, description="关键词")
    limit: int = Field(default=10, description="返回条数")


class GetNewsSentimentOutput(ToolResult):
    """输出"""
    data: List[Dict[str, Any]] = Field(default_factory=list)


class GetNewsSentimentTool(BaseTool[GetNewsSentimentInput, GetNewsSentimentOutput]):
    """获取新闻舆情"""
    
    name = "get_news_sentiment"
    description = "获取财经新闻和舆情分析"
    input_model = GetNewsSentimentInput
    output_model = GetNewsSentimentOutput
    
    async def execute(self, input_data: GetNewsSentimentInput) -> GetNewsSentimentOutput:
        """执行"""
        filter_query = {}
        
        if input_data.keyword:
            filter_query["$or"] = [
                {"title": {"$regex": input_data.keyword}},
                {"content": {"$regex": input_data.keyword}},
            ]
        
        data = await mongo_manager.find_many(
            "news",
            filter_query,
            sort=[("datetime", -1)],
            limit=input_data.limit,
        )
        
        return GetNewsSentimentOutput(data=data)
