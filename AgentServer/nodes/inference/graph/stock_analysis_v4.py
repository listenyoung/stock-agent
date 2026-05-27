"""
股票分析 LangGraph 工作流 (V4.0 - LangGraph StateGraph 版)

使用 LangGraph 的 StateGraph 替代手动状态编排。
保留 V3.2 所有节点函数，只改变执行引擎。

图拓扑 (V4.0):
    data_collect
         ↓
    ┌────┼────┐
    ↓    ↓    ↓
  fund tech  sent  (并行)
    └────┼────┘
         ↓
    supervisor
         ↓
    [条件边缘] ── round2 ──→ output
         ↓ round1
    check_result
         ↓
    [条件边缘] ──→ output (confidence >= 60)
         ↓ (confidence < 60)
    query_refinement
         ↓
    mcp_search
         ↓
    ┌────┼────┐
    ↓    ↓    ↓
  fund tech  sent  (增量分析 Round 2)
    └────┼────┘
         ↓
    supervisor (重评)
         ↓
    [条件边缘] ──→ output
"""

import logging
import uuid
from typing import Dict, Any, Optional, Callable

from langgraph.graph import StateGraph, END

from core.managers import prompt_manager
from core.protocols import StockAnalysisState

from .stock_analysis import (
    data_collect_node,
    fundamental_node,
    technical_node,
    sentiment_node,
    supervisor_node,
    check_result_node,
    query_refinement_node,
    mcp_search_node,
)

logger = logging.getLogger("graph.stock_analysis_v4")


# ==================== 路由函数 ====================


def route_after_supervisor(state: StockAnalysisState) -> str:
    """
    Supervisor 后的条件路由

    - Round 1 (retry_count=0): 走 check_result 做置信度检查
    - Round 2 (retry_count>0): 直接输出，不再检查
    """
    if state.retry_count > 0:
        return "output"
    return "check"


def route_after_check(state: StockAnalysisState) -> str:
    """
    Check Result 后的条件路由

    - confidence >= 60: 直接输出
    - confidence < 60 且还有重试次数: 走增量分析
    """
    if state.needs_refinement:
        return "refinement"
    return "output"


# ==================== 图构建 ====================


def build_analysis_graph() -> StateGraph:
    """构建 LangGraph StateGraph"""

    workflow = StateGraph(StockAnalysisState)

    # ---- 添加节点 ----
    workflow.add_node("data_collect", data_collect_node)
    workflow.add_node("fundamental", fundamental_node)
    workflow.add_node("technical", technical_node)
    workflow.add_node("sentiment", sentiment_node)
    workflow.add_node("supervisor", supervisor_node)
    workflow.add_node("check_result", check_result_node)
    workflow.add_node("query_refinement", query_refinement_node)
    workflow.add_node("mcp_search", mcp_search_node)

    # ---- 入口 ----
    workflow.set_entry_point("data_collect")

    # ---- Round 1: data_collect → 三方分析（并行） ----
    workflow.add_edge("data_collect", "fundamental")
    workflow.add_edge("data_collect", "technical")
    workflow.add_edge("data_collect", "sentiment")

    # 三方分析 → supervisor（LangGraph 自动等待所有并行节点完成）
    workflow.add_edge("fundamental", "supervisor")
    workflow.add_edge("technical", "supervisor")
    workflow.add_edge("sentiment", "supervisor")

    # ---- Supervisor 后条件路由 ----
    workflow.add_conditional_edges(
        "supervisor",
        route_after_supervisor,
        {
            "output": END,
            "check": "check_result",
        },
    )

    # ---- Check Result 后条件路由 ----
    workflow.add_conditional_edges(
        "check_result",
        route_after_check,
        {
            "output": END,
            "refinement": "query_refinement",
        },
    )

    # ---- Round 2: 增量分析 ----
    workflow.add_edge("query_refinement", "mcp_search")
    workflow.add_edge("mcp_search", "fundamental")
    workflow.add_edge("mcp_search", "technical")
    workflow.add_edge("mcp_search", "sentiment")

    # 注意: fundamental/tech/sent → supervisor 和 supervisor → 条件路由的边已定义
    # Round 2 的 supervisor 走 route_after_supervisor → output → END

    return workflow


# ==================== 主类 ====================


class StockAnalysisGraph:
    """
    股票分析图 (V4.0 - LangGraph 版)

    使用 LangGraph StateGraph 替代手动的 asyncio.gather + state.copy 编排。
    所有节点函数保持原样不变，只改变执行引擎。

    特性：
    - 原生并行执行（基本面/技术面/舆情自动并发）
    - 原生条件分支（置信度路由）
    - 自动状态管理（无需手动 state.copy）
    - 完整的节点级追踪
    - 与 V3.2 完全相同的分析逻辑
    """

    def __init__(self):
        self.logger = logging.getLogger("graph.stock_analysis_v4")
        self._initialized = False
        self._app = None

    async def initialize(self) -> None:
        """初始化：编译 LangGraph"""
        await prompt_manager.initialize()

        workflow = build_analysis_graph()
        self._app = workflow.compile()

        self._initialized = True
        self.logger.info("Stock analysis graph initialized (V4.0 LangGraph) ✓")

    async def analyze_stock(
        self,
        ts_code: str,
        task_id: str,
        progress_callback: Optional[Callable] = None,
    ) -> Dict[str, Any]:
        """个股分析"""
        trace_id = uuid.uuid4().hex

        self.logger.info(f"[analyze_stock] trace_id={trace_id} | Starting analysis for {ts_code}")

        initial_state = StockAnalysisState(
            ts_code=ts_code,
            task_id=task_id,
            trace_id=trace_id,
        )

        if progress_callback:
            await progress_callback(5, "启动 LangGraph 分析引擎...")

        try:
            # 执行图
            final_state = await self._app.ainvoke(initial_state)

            # 从最终状态构建结果
            result = self._build_result(final_state, trace_id)

            if progress_callback:
                await progress_callback(100, "分析完成")

            self.logger.info(
                f"[analyze_stock] trace_id={trace_id} | "
                f"Signal={result['signal']}, Confidence={result['confidence']:.2f}, "
                f"Rounds={len(final_state.reasoning_steps)}"
            )

            return result

        except Exception as e:
            self.logger.error(f"[analyze_stock] trace_id={trace_id} | Error: {e}")
            import traceback
            traceback.print_exc()
            raise

    def _build_result(self, state: StockAnalysisState, trace_id: str) -> Dict[str, Any]:
        """从最终状态构建输出结果"""
        return {
            "trace_id": trace_id,

            "signal": state.signal or "hold",
            "confidence": state.confidence or 0.5,
            "summary": state.summary or "",
            "scores": state.scores or {},

            "fundamental_analysis": state.fundamental_res or "",
            "technical_analysis": state.technical_res or "",
            "sentiment_analysis": state.sentiment_res or "",

            "structured_summary": state.structured_summary.model_dump() if state.structured_summary else {},

            "analysis_conflicts": [
                c.model_dump() for c in state.analysis_conflicts
            ] if state.analysis_conflicts else [],

            "confidence_score": state.confidence_score.model_dump() if state.confidence_score else {},

            "final_decision": state.final_decision or "",
            "decision_reason": state.decision_reason or "",
            "risks": state.risks or [],

            "mcp_tool_calls": [
                t.model_dump() for t in state.mcp_tool_calls
            ] if state.mcp_tool_calls else [],
            "mcp_evidence": state.mcp_evidence or [],
            "supplementary_data": [
                s.model_dump() for s in state.supplementary_data
            ] if state.supplementary_data else [],

            "reasoning_chain": [
                {
                    "step": rs.step_id,
                    "node": rs.node_name,
                    "action": rs.action,
                    "result": rs.result_summary,
                }
                for rs in state.reasoning_chain
            ] if state.reasoning_chain else [],

            "refinement_queries": state.refinement_queries or [],
            "retry_count": state.retry_count or 0,

            "round_history": [
                {
                    "round_id": rs.round_id,
                    "confidence": rs.confidence_score,
                    "decision": rs.decision,
                    "conflicts_count": len(rs.conflicts_found),
                    "unresolved_count": len(rs.unresolved_issues),
                }
                for rs in state.reasoning_steps
            ] if state.reasoning_steps else [],
        }

    async def analyze_market(
        self,
        task_id: str,
        progress_callback: Optional[Callable] = None,
    ) -> Dict[str, Any]:
        """大盘分析 - 复用 V3.2 原有实现"""
        from .stock_analysis import StockAnalysisGraph as V3Graph
        old = V3Graph()
        await old.initialize()
        return await old.analyze_market(task_id, progress_callback)

    async def custom_query(
        self,
        query: str,
        task_id: str,
        progress_callback: Optional[Callable] = None,
    ) -> Dict[str, Any]:
        """自定义查询 - 复用 V3.2 原有实现"""
        from .stock_analysis import StockAnalysisGraph as V3Graph
        old = V3Graph()
        await old.initialize()
        return await old.custom_query(query, task_id, progress_callback)
