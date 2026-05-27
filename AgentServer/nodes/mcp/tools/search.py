"""
RAG 检索工具
"""

from typing import Optional, List, Dict, Any

from pydantic import BaseModel, Field

from core.base import BaseTool, ToolResult
from core.managers import milvus_manager, llm_manager


class SearchSimilarReportsInput(BaseModel):
    """输入"""
    query: str = Field(..., description="查询内容")
    ts_code: Optional[str] = Field(None, description="股票代码过滤")
    top_k: int = Field(default=5, description="返回数量")


class SearchSimilarReportsOutput(ToolResult):
    """输出"""
    data: List[Dict[str, Any]] = Field(default_factory=list)


class SearchSimilarReportsTool(BaseTool[SearchSimilarReportsInput, SearchSimilarReportsOutput]):
    """搜索相似研报"""
    
    name = "search_similar_reports"
    description = "基于语义搜索相似的研报和分析文档"
    input_model = SearchSimilarReportsInput
    output_model = SearchSimilarReportsOutput
    
    async def execute(self, input_data: SearchSimilarReportsInput) -> SearchSimilarReportsOutput:
        """执行"""
        # 生成查询向量
        embeddings = await llm_manager.embedding([input_data.query])
        query_vector = embeddings[0]
        
        # 搜索相似研报
        results = await milvus_manager.search_reports(
            query_vector=query_vector,
            top_k=input_data.top_k,
            ts_code=input_data.ts_code,
        )
        
        return SearchSimilarReportsOutput(data=results)
