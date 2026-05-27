"""
股票分析 LangGraph 工作流 (V3.2)

定义分析流程 (Supervisor + Review + MCP Search + 增量分析架构):
1. 数据采集节点 (data_collect_node)
2. 基本面分析节点 (fundamental_node) - 支持增量分析
3. 技术面分析节点 (technical_node) - 支持增量分析
4. 舆情分析节点 (sentiment_node) - 支持增量分析
5. Supervisor 节点 (supervisor_node) - 冲突调和与跨轮次记忆
6. Check Result 节点 (check_result_node) - 置信度检查
7. Query Refinement 节点 (query_refinement_node) - 生成补充查询
8. MCP Search 节点 (mcp_search_node) - 执行 MCP 工具调用

图拓扑 (V3.2):
    data_collect
         ↓
    ┌────┼────┐
    ↓    ↓    ↓
  fund tech  sent  (并行) ← 首轮分析
    └────┼────┘
         ↓
    supervisor ← 评估 + 保存 RoundSummary
         ↓
    check_result
         ↓
    [条件边缘] ─────────────────────────────────┐
         ↓                                     ↓
    confidence >= 60                   confidence < 60
         ↓                                     ↓
       output                         query_refinement
                                               ↓
                                          mcp_search
                                               ↓
                                      ┌────┼────┐
                                      ↓    ↓    ↓
                                    fund tech  sent (增量分析)
                                      └────┼────┘
                                               ↓
                                         supervisor (重评 + 对比上轮)
"""

from typing import Optional, Callable, Dict, Any, List, Literal
from datetime import datetime
import logging
import json
import re
import asyncio

from core.managers import llm_manager, mongo_manager, milvus_manager, prompt_manager
from core.protocols import (
    SignalType,
    StockAnalysisState,
    AnalysisConflict,
    ConfidenceScore,
    StructuredSummary,
    MCPToolCall,
    ReasoningStep,
    RoundSummary,
    SupplementaryData,
)

# 常量配置
CONFIDENCE_THRESHOLD = 60
MAX_RETRY_COUNT = 2


# ==================== 工具函数 ====================

def _parse_json_response(response: str) -> Dict[str, Any]:
    """解析 JSON 响应"""
    try:
        json_match = re.search(r'\{[\s\S]*\}', response)
        if json_match:
            return json.loads(json_match.group())
    except Exception:
        pass
    return {}


def _extract_core_conclusion(analysis: str, max_len: int = 50) -> str:
    """从分析文本中提取核心结论"""
    if not analysis:
        return ""
    
    # 尝试提取关键句子
    patterns = [
        r"核心结论[：:]\s*(.+?)(?:\n|。|$)",
        r"综合来看[，,]\s*(.+?)(?:\n|。|$)",
        r"建议[：:]\s*(.+?)(?:\n|。|$)",
        r"总结[：:]\s*(.+?)(?:\n|。|$)",
        r"结论修正[：:]\s*(.+?)(?:\n|。|$)",
    ]
    
    for pattern in patterns:
        match = re.search(pattern, analysis)
        if match:
            conclusion = match.group(1).strip()
            return conclusion[:max_len] if len(conclusion) > max_len else conclusion
    
    # 回退：取第一段
    first_para = analysis.split('\n')[0]
    return first_para[:max_len] if len(first_para) > max_len else first_para


def _log_with_trace(logger: logging.Logger, trace_id: str, node: str, message: str, level: str = "info"):
    """带 trace_id 的结构化日志"""
    log_msg = f"[{node}] trace_id={trace_id} | {message}"
    getattr(logger, level)(log_msg)


def _get_supplementary_data_for_node(state: StockAnalysisState, node_type: str) -> List[Dict[str, Any]]:
    """获取与特定节点相关的补充数据"""
    relevant_data = []
    for data in state.supplementary_data:
        # 根据数据来源匹配节点
        if node_type == "fundamental" and data.source in ["get_financial_indicator", "search_similar_reports"]:
            relevant_data.append({"source": data.source, "content": data.content})
        elif node_type == "technical" and data.source in ["get_stock_daily"]:
            relevant_data.append({"source": data.source, "content": data.content})
        elif node_type == "sentiment" and data.source in ["get_news_sentiment", "search_similar_reports"]:
            relevant_data.append({"source": data.source, "content": data.content})
        elif node_type == "all":
            relevant_data.append({"source": data.source, "content": data.content})
    return relevant_data


# ==================== 节点函数 ====================

def _normalize_ts_code(code: str) -> str:
    """
    标准化股票代码为 Tushare 格式
    
    Examples:
        "002131" -> "002131.SZ"
        "600519" -> "600519.SH"
        "002131.SZ" -> "002131.SZ" (不变)
    """
    if not code:
        return code
    
    # 已经是完整格式
    if "." in code:
        return code.upper()
    
    # 纯数字代码，根据规则补全后缀
    code = code.strip()
    if code.startswith(("6", "5", "9")):
        return f"{code}.SH"  # 上海
    elif code.startswith(("0", "3", "2")):
        return f"{code}.SZ"  # 深圳
    elif code.startswith(("4", "8")):
        return f"{code}.BJ"  # 北京
    else:
        return f"{code}.SZ"  # 默认深圳


async def data_collect_node(state: StockAnalysisState) -> Dict[str, Any]:
    """
    数据采集节点
    
    获取股票基本信息和日线数据
    """
    logger = logging.getLogger("node.data_collect")
    ts_code = _normalize_ts_code(state.ts_code)  # 标准化代码
    trace_id = state.trace_id
    
    _log_with_trace(logger, trace_id, "data_collect", f"Fetching data for {ts_code}")
    
    # 获取股票基本信息
    stock = await mongo_manager.find_one("stock_basic", {"ts_code": ts_code})
    stock_name = stock.get("name", ts_code) if stock else ts_code
    
    # 获取日线数据
    daily_data = await mongo_manager.find_many(
        "stock_daily",
        {"ts_code": ts_code},
        sort=[("trade_date", -1)],
        limit=60,
    )
    
    _log_with_trace(logger, trace_id, "data_collect", f"Got {len(daily_data)} daily records for {stock_name}")
    
    # 获取财务指标数据 (最近8个季度，约2年)
    fina_data = await mongo_manager.find_many(
        "fina_indicator",
        {"ts_code": ts_code},
        sort=[("end_date", -1)],
        limit=8,
    )
    
    _log_with_trace(logger, trace_id, "data_collect", f"Got {len(fina_data)} financial records")
    
    # DEBUG: 完整数据打印
    _log_with_trace(logger, trace_id, "data_collect", "=" * 60)
    _log_with_trace(logger, trace_id, "data_collect", "【数据采集 - 完整数据】")
    _log_with_trace(logger, trace_id, "data_collect", f"股票代码: {ts_code}")
    _log_with_trace(logger, trace_id, "data_collect", f"股票名称: {stock_name}")
    _log_with_trace(logger, trace_id, "data_collect", "-" * 40)
    _log_with_trace(logger, trace_id, "data_collect", "【stock_basic 数据】")
    if stock:
        _log_with_trace(logger, trace_id, "data_collect", f"  所有字段: {list(stock.keys())}")
        _log_with_trace(logger, trace_id, "data_collect", f"  industry: {stock.get('industry')}")
        _log_with_trace(logger, trace_id, "data_collect", f"  pe: {stock.get('pe')}")
        _log_with_trace(logger, trace_id, "data_collect", f"  pb: {stock.get('pb')}")
        _log_with_trace(logger, trace_id, "data_collect", f"  total_mv: {stock.get('total_mv')}")
        _log_with_trace(logger, trace_id, "data_collect", f"  turnover_rate: {stock.get('turnover_rate')}")
    else:
        _log_with_trace(logger, trace_id, "data_collect", "  (无数据)")
    _log_with_trace(logger, trace_id, "data_collect", "-" * 40)
    _log_with_trace(logger, trace_id, "data_collect", "【stock_daily 数据】")
    _log_with_trace(logger, trace_id, "data_collect", f"  记录数: {len(daily_data)}")
    if daily_data:
        latest = daily_data[0]
        _log_with_trace(logger, trace_id, "data_collect", f"  最新日期: {latest.get('trade_date')}")
        _log_with_trace(logger, trace_id, "data_collect", f"  所有字段: {list(latest.keys())}")
        _log_with_trace(logger, trace_id, "data_collect", f"  close: {latest.get('close')}")
        _log_with_trace(logger, trace_id, "data_collect", f"  pct_chg: {latest.get('pct_chg')}")
        _log_with_trace(logger, trace_id, "data_collect", f"  vol: {latest.get('vol')}")
        _log_with_trace(logger, trace_id, "data_collect", f"  pe: {latest.get('pe')}")
        _log_with_trace(logger, trace_id, "data_collect", f"  pb: {latest.get('pb')}")
        _log_with_trace(logger, trace_id, "data_collect", f"  total_mv: {latest.get('total_mv')}")
        _log_with_trace(logger, trace_id, "data_collect", f"  turnover_rate: {latest.get('turnover_rate')}")
    _log_with_trace(logger, trace_id, "data_collect", "-" * 40)
    _log_with_trace(logger, trace_id, "data_collect", "【fina_indicator 数据】")
    _log_with_trace(logger, trace_id, "data_collect", f"  记录数: {len(fina_data)}")
    if fina_data:
        latest_fina = fina_data[0]
        _log_with_trace(logger, trace_id, "data_collect", f"  最新报告期: {latest_fina.get('end_date')}")
        _log_with_trace(logger, trace_id, "data_collect", f"  所有字段: {list(latest_fina.keys())}")
        _log_with_trace(logger, trace_id, "data_collect", f"  eps: {latest_fina.get('eps')}")
        _log_with_trace(logger, trace_id, "data_collect", f"  roe: {latest_fina.get('roe')}")
        _log_with_trace(logger, trace_id, "data_collect", f"  netprofit_yoy: {latest_fina.get('netprofit_yoy')}")
    _log_with_trace(logger, trace_id, "data_collect", "=" * 60)
    
    # 记录决策链
    reasoning_step = ReasoningStep(
        step_id=1,
        node_name="data_collect",
        action="数据采集",
        reasoning=f"获取 {ts_code} 的基本信息、近60日行情和近8季度财务数据",
        result_summary=f"日线 {len(daily_data)} 条, 财务 {len(fina_data)} 条",
    )
    
    return {
        "ts_code": ts_code,  # 返回标准化后的代码
        "stock": stock,
        "stock_name": stock_name,
        "daily_data": daily_data,
        "fina_data": fina_data,
        "reasoning_chain": [reasoning_step],
    }


async def fundamental_node(state: StockAnalysisState) -> Dict[str, Any]:
    """
    基本面分析节点 (V3.2 增强)
    
    支持增量分析：
    - 首轮：正常分析
    - 补充轮：结合上轮结论和补充数据，显式说明修正
    """
    logger = logging.getLogger("node.fundamental")
    ts_code = state.ts_code
    trace_id = state.trace_id
    stock = state.stock
    
    industry = stock.get("industry", "未知") if stock else "未知"
    is_refinement = state.is_refinement_round()
    
    _log_with_trace(logger, trace_id, "fundamental", 
                    f"Analyzing {ts_code}, industry={industry}, refinement={is_refinement}")
    
    # 提取关键财务数据 (从 daily_data 或 stock 中获取)
    daily_data = state.daily_data or []
    latest = daily_data[0] if daily_data else {}
    stock = state.stock or {}
    
    # 计算简单的财务/价格指标
    close = latest.get("close", 0)
    pct_chg = latest.get("pct_chg", 0)
    pe = latest.get("pe", stock.get("pe", 0))
    pb = latest.get("pb", stock.get("pb", 0))
    # total_mv: stock_basic 已经是亿元，stock_daily 是万元
    total_mv_raw = latest.get("total_mv") or stock.get("total_mv") or 0
    # 判断来源：stock_basic 的 total_mv 已经转换为亿元 (值较小)，stock_daily 是万元 (值较大)
    # stock_basic 存的是亿元 (sync_stock_basic.py 中已转换)
    total_mv_billion = total_mv_raw if total_mv_raw < 100000 else total_mv_raw / 10000
    turnover_rate = latest.get("turnover_rate") or stock.get("turnover_rate") or 0
    
    # 计算区间涨跌幅 (30天)
    if len(daily_data) >= 30:
        close_30d_ago = daily_data[29].get("close", close)
        pct_30d = ((close - close_30d_ago) / close_30d_ago * 100) if close_30d_ago else 0
    else:
        pct_30d = 0
    
    # 提取财务指标数据
    fina_data = state.fina_data or []
    latest_fina = fina_data[0] if fina_data else {}
    
    # 核心财务指标
    eps = latest_fina.get("eps", 0)
    roe = latest_fina.get("roe", 0)
    roa = latest_fina.get("roa", 0)
    gross_margin = latest_fina.get("grossprofit_margin", 0)
    net_margin = latest_fina.get("netprofit_margin", 0)
    debt_ratio = latest_fina.get("debt_to_assets", 0)
    current_ratio = latest_fina.get("current_ratio", 0)
    
    # 成长性指标 (同比)
    revenue_yoy = latest_fina.get("tr_yoy", 0)
    profit_yoy = latest_fina.get("netprofit_yoy", 0)
    
    # 格式化财务摘要
    fina_summary = []
    if fina_data:
        for f in fina_data[:4]:  # 最近4个季度
            period = f.get("end_date", "")
            fina_summary.append(
                f"{period}: EPS={f.get('eps', 0):.2f}, ROE={f.get('roe', 0):.2f}%, "
                f"营收同比={f.get('tr_yoy', 0):.1f}%, 净利同比={f.get('netprofit_yoy', 0):.1f}%"
            )
    fina_summary_text = "\n  ".join(fina_summary) if fina_summary else "暂无财务数据"
    
    # 准备提示词参数
    prompt_params = {
        "ts_code": ts_code,
        "industry": industry,
        "stock_name": state.stock_name or ts_code,
        "close": close,
        "pct_chg": f"{pct_chg:.2f}",
        "pct_30d": f"{pct_30d:.2f}",
        "pe": f"{pe:.2f}" if pe else ("亏损" if eps and eps < 0 else "N/A"),
        "pb": f"{pb:.2f}" if pb else "N/A",
        "total_mv": f"{total_mv_billion:.2f}亿" if total_mv_billion else "N/A",
        "turnover_rate": f"{turnover_rate:.2f}%" if turnover_rate else "N/A",
        # 财务指标
        "eps": f"{eps:.2f}" if eps else "N/A",
        "roe": f"{roe:.2f}" if roe else "N/A",
        "roa": f"{roa:.2f}" if roa else "N/A",
        "gross_margin": f"{gross_margin:.2f}" if gross_margin else "N/A",
        "net_margin": f"{net_margin:.2f}" if net_margin else "N/A",
        "debt_ratio": f"{debt_ratio:.2f}" if debt_ratio else "N/A",
        "current_ratio": f"{current_ratio:.2f}" if current_ratio else "N/A",
        "revenue_yoy": f"{revenue_yoy:.2f}" if revenue_yoy else "N/A",
        "profit_yoy": f"{profit_yoy:.2f}" if profit_yoy else "N/A",
        "fina_summary": fina_summary_text,
        "has_fina_data": bool(fina_data),
        "is_refinement": is_refinement,
        "retry_count": state.retry_count,
    }
    
    # DEBUG: 完整数据打印
    _log_with_trace(logger, trace_id, "fundamental", "=" * 60)
    _log_with_trace(logger, trace_id, "fundamental", "【基本面分析 - 完整数据】")
    _log_with_trace(logger, trace_id, "fundamental", f"股票: {ts_code} ({state.stock_name})")
    _log_with_trace(logger, trace_id, "fundamental", f"行业: {industry}")
    _log_with_trace(logger, trace_id, "fundamental", "-" * 40)
    _log_with_trace(logger, trace_id, "fundamental", "【原始 stock 数据】")
    _log_with_trace(logger, trace_id, "fundamental", f"  stock字段: {list(stock.keys()) if stock else 'None'}")
    _log_with_trace(logger, trace_id, "fundamental", f"  stock.pe: {stock.get('pe')}")
    _log_with_trace(logger, trace_id, "fundamental", f"  stock.pb: {stock.get('pb')}")
    _log_with_trace(logger, trace_id, "fundamental", f"  stock.total_mv: {stock.get('total_mv')}")
    _log_with_trace(logger, trace_id, "fundamental", "-" * 40)
    _log_with_trace(logger, trace_id, "fundamental", "【原始 latest daily 数据】")
    _log_with_trace(logger, trace_id, "fundamental", f"  daily字段: {list(latest.keys()) if latest else 'None'}")
    _log_with_trace(logger, trace_id, "fundamental", f"  latest.pe: {latest.get('pe')}")
    _log_with_trace(logger, trace_id, "fundamental", f"  latest.pb: {latest.get('pb')}")
    _log_with_trace(logger, trace_id, "fundamental", f"  latest.total_mv: {latest.get('total_mv')}")
    _log_with_trace(logger, trace_id, "fundamental", f"  latest.turnover_rate: {latest.get('turnover_rate')}")
    _log_with_trace(logger, trace_id, "fundamental", "-" * 40)
    _log_with_trace(logger, trace_id, "fundamental", "【财务指标数据】")
    _log_with_trace(logger, trace_id, "fundamental", f"  fina_data条数: {len(fina_data)}")
    if latest_fina:
        _log_with_trace(logger, trace_id, "fundamental", f"  latest_fina字段: {list(latest_fina.keys())}")
        _log_with_trace(logger, trace_id, "fundamental", f"  end_date: {latest_fina.get('end_date')}")
        _log_with_trace(logger, trace_id, "fundamental", f"  eps: {latest_fina.get('eps')}")
        _log_with_trace(logger, trace_id, "fundamental", f"  roe: {latest_fina.get('roe')}")
        _log_with_trace(logger, trace_id, "fundamental", f"  netprofit_yoy: {latest_fina.get('netprofit_yoy')}")
    _log_with_trace(logger, trace_id, "fundamental", "-" * 40)
    _log_with_trace(logger, trace_id, "fundamental", "【传入LLM的参数】")
    for k, v in prompt_params.items():
        if k not in ["fina_summary"]:  # 跳过长文本
            _log_with_trace(logger, trace_id, "fundamental", f"  {k}: {v}")
    _log_with_trace(logger, trace_id, "fundamental", "=" * 60)
    
    # 增量分析模式
    if is_refinement:
        prompt_params["previous_conclusion"] = state.initial_fundamental_res or state.structured_summary.fundamental_core
        prompt_params["previous_issues"] = state.get_previous_issues()
        prompt_params["supplementary_data"] = _get_supplementary_data_for_node(state, "fundamental")
    
    # 从配置获取提示词
    system_prompt = prompt_manager.get_system_prompt("stock_analysis/fundamental", **prompt_params)
    user_prompt = prompt_manager.get_prompt("stock_analysis/fundamental", **prompt_params)
    
    # DEBUG: 打印用户提示词前200字符
    _log_with_trace(logger, trace_id, "fundamental", 
                    f"User prompt preview: {user_prompt[:200]}...")
    
    analysis = await llm_manager.chat([
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ])
    
    _log_with_trace(logger, trace_id, "fundamental", 
                    f"Analysis complete, length={len(analysis)}, refinement={is_refinement}")
    # DEBUG: 打印分析结果前200字符
    _log_with_trace(logger, trace_id, "fundamental", 
                    f"Analysis preview: {analysis[:200]}...")
    
    # 首轮保存初始分析结果
    result = {"fundamental_res": analysis}
    if not is_refinement:
        result["initial_fundamental_res"] = analysis
    
    return result


async def technical_node(state: StockAnalysisState) -> Dict[str, Any]:
    """
    技术面分析节点 (V3.2 增强)
    
    支持增量分析：
    - 首轮：正常分析
    - 补充轮：结合上轮结论和补充数据，显式说明修正
    """
    logger = logging.getLogger("node.technical")
    ts_code = state.ts_code
    trace_id = state.trace_id
    daily_data = state.daily_data or []
    is_refinement = state.is_refinement_round()
    
    if not daily_data:
        _log_with_trace(logger, trace_id, "technical", f"No daily data for {ts_code}", "warning")
        return {"technical_res": "无足够的行情数据进行技术分析。"}
    
    # 提取关键数据
    latest = daily_data[0] if daily_data else {}
    close = latest.get("close", 0)
    pct_chg = latest.get("pct_chg", 0)
    vol = latest.get("vol", 0)
    
    # 计算简单均线
    closes = [d.get("close", 0) for d in daily_data[:20]]
    ma5 = sum(closes[:5]) / 5 if len(closes) >= 5 else 0
    ma20 = sum(closes[:20]) / 20 if len(closes) >= 20 else 0
    
    # 计算量能变化
    vols = [d.get("vol", 0) for d in daily_data[:10]]
    vol_avg = sum(vols) / len(vols) if vols else 1
    vol_ratio = vol / vol_avg if vol_avg > 0 else 1
    
    # 计算更多技术指标
    # 趋势判断
    trend = "上升" if close > ma20 and ma5 > ma20 else ("下降" if close < ma20 and ma5 < ma20 else "横盘")
    
    # 近 5 日 K 线摘要
    recent_klines = []
    for d in daily_data[:5]:
        date = d.get("trade_date", "")
        c = d.get("close", 0)
        chg = d.get("pct_chg", 0)
        v = d.get("vol", 0) / 10000 if d.get("vol") else 0  # 转换为万手
        recent_klines.append(f"{date}: 收{c:.2f} 涨跌{chg:.2f}% 量{v:.0f}万手")
    
    # 区间极值
    highs = [d.get("high", 0) for d in daily_data[:20]]
    lows = [d.get("low", 0) for d in daily_data[:20]]
    period_high = max(highs) if highs else 0
    period_low = min(lows) if lows else 0
    
    _log_with_trace(logger, trace_id, "technical", 
                    f"Analyzing {ts_code}, close={close}, pct={pct_chg}%, refinement={is_refinement}")
    
    # 准备提示词参数
    prompt_params = {
        "ts_code": ts_code,
        "stock_name": state.stock_name or ts_code,
        "close": close,
        "pct_chg": f"{pct_chg:.2f}",
        "ma5": f"{ma5:.2f}",
        "ma20": f"{ma20:.2f}",
        "vol_ratio": f"{vol_ratio:.2f}",
        "trend": trend,
        "period_high": f"{period_high:.2f}",
        "period_low": f"{period_low:.2f}",
        "recent_klines": "\n  ".join(recent_klines),
        "is_refinement": is_refinement,
        "retry_count": state.retry_count,
    }
    
    # DEBUG: 完整数据打印
    _log_with_trace(logger, trace_id, "technical", "=" * 60)
    _log_with_trace(logger, trace_id, "technical", "【技术面分析 - 完整数据】")
    _log_with_trace(logger, trace_id, "technical", f"股票: {ts_code} ({state.stock_name})")
    _log_with_trace(logger, trace_id, "technical", "-" * 40)
    _log_with_trace(logger, trace_id, "technical", "【原始 latest daily 数据】")
    _log_with_trace(logger, trace_id, "technical", f"  daily_data条数: {len(daily_data)}")
    _log_with_trace(logger, trace_id, "technical", f"  latest字段: {list(latest.keys()) if latest else 'None'}")
    _log_with_trace(logger, trace_id, "technical", f"  trade_date: {latest.get('trade_date')}")
    _log_with_trace(logger, trace_id, "technical", f"  open: {latest.get('open')}")
    _log_with_trace(logger, trace_id, "technical", f"  high: {latest.get('high')}")
    _log_with_trace(logger, trace_id, "technical", f"  low: {latest.get('low')}")
    _log_with_trace(logger, trace_id, "technical", f"  close: {latest.get('close')}")
    _log_with_trace(logger, trace_id, "technical", f"  vol: {latest.get('vol')}")
    _log_with_trace(logger, trace_id, "technical", f"  amount: {latest.get('amount')}")
    _log_with_trace(logger, trace_id, "technical", f"  pct_chg: {latest.get('pct_chg')}")
    _log_with_trace(logger, trace_id, "technical", "-" * 40)
    _log_with_trace(logger, trace_id, "technical", "【计算指标】")
    _log_with_trace(logger, trace_id, "technical", f"  ma5: {ma5:.2f}")
    _log_with_trace(logger, trace_id, "technical", f"  ma20: {ma20:.2f}")
    _log_with_trace(logger, trace_id, "technical", f"  vol_ratio: {vol_ratio:.2f}")
    _log_with_trace(logger, trace_id, "technical", f"  trend: {trend}")
    _log_with_trace(logger, trace_id, "technical", f"  period_high: {period_high:.2f}")
    _log_with_trace(logger, trace_id, "technical", f"  period_low: {period_low:.2f}")
    _log_with_trace(logger, trace_id, "technical", "-" * 40)
    _log_with_trace(logger, trace_id, "technical", "【近5日K线】")
    for kline in recent_klines:
        _log_with_trace(logger, trace_id, "technical", f"  {kline}")
    _log_with_trace(logger, trace_id, "technical", "=" * 60)
    
    # 增量分析模式
    if is_refinement:
        prompt_params["previous_conclusion"] = state.initial_technical_res or state.structured_summary.technical_core
        prompt_params["previous_issues"] = state.get_previous_issues()
        prompt_params["supplementary_data"] = _get_supplementary_data_for_node(state, "technical")
    
    # 从配置获取提示词
    system_prompt = prompt_manager.get_system_prompt("stock_analysis/technical", **prompt_params)
    user_prompt = prompt_manager.get_prompt("stock_analysis/technical", **prompt_params)
    
    # DEBUG: 打印用户提示词前200字符
    _log_with_trace(logger, trace_id, "technical", 
                    f"User prompt preview: {user_prompt[:200]}...")
    
    analysis = await llm_manager.chat([
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ])
    
    _log_with_trace(logger, trace_id, "technical", 
                    f"Analysis complete, length={len(analysis)}, refinement={is_refinement}")
    # DEBUG: 打印分析结果前200字符
    _log_with_trace(logger, trace_id, "technical", 
                    f"Analysis preview: {analysis[:200]}...")
    
    # 首轮保存初始分析结果
    result = {"technical_res": analysis}
    if not is_refinement:
        result["initial_technical_res"] = analysis
    
    return result


async def sentiment_node(state: StockAnalysisState) -> Dict[str, Any]:
    """
    舆情分析节点 (V3.2 增强)
    
    支持增量分析：
    - 首轮：正常分析
    - 补充轮：结合上轮结论和补充数据，显式说明修正
    """
    logger = logging.getLogger("node.sentiment")
    ts_code = state.ts_code
    trace_id = state.trace_id
    stock_name = state.stock_name or ts_code
    is_refinement = state.is_refinement_round()
    
    _log_with_trace(logger, trace_id, "sentiment", f"Analyzing {stock_name}, refinement={is_refinement}")
    
    # 获取相关新闻
    news = await mongo_manager.find_many(
        "news",
        {"$or": [
            {"title": {"$regex": stock_name}},
            {"content": {"$regex": stock_name}},
        ]},
        sort=[("datetime", -1)],
        limit=5,
    )
    
    news_summary = "\n".join([f"- {n.get('title', '')}" for n in news]) if news else "暂无直接相关新闻"
    
    # 即使没有新闻，也从市场数据中提取情绪信号
    daily_data = state.daily_data or []
    latest = daily_data[0] if daily_data else {}
    stock = state.stock or {}
    
    # 计算市场情绪指标
    pct_chg = latest.get("pct_chg", 0)
    vol = latest.get("vol", 0)
    # 换手率：优先从 daily 取，取不到从 stock_basic 取
    turnover = latest.get("turnover_rate") or stock.get("turnover_rate") or 0
    
    # 判断涨跌停状态
    if pct_chg >= 9.9:
        price_signal = "涨停"
    elif pct_chg >= 5:
        price_signal = "大涨"
    elif pct_chg >= 0:
        price_signal = "上涨"
    elif pct_chg >= -5:
        price_signal = "下跌"
    elif pct_chg >= -9.9:
        price_signal = "大跌"
    else:
        price_signal = "跌停"
    
    # 计算量能状态
    vols = [d.get("vol", 0) for d in daily_data[:10]]
    vol_avg = sum(vols) / len(vols) if vols else 1
    vol_status = "放量" if vol > vol_avg * 1.5 else ("缩量" if vol < vol_avg * 0.7 else "正常")
    
    # 准备提示词参数
    prompt_params = {
        "ts_code": ts_code,
        "stock_name": stock_name,
        "news_summary": news_summary,
        "price_signal": price_signal,
        "vol_status": vol_status,
        "pct_chg": f"{pct_chg:.2f}",
        "turnover": f"{turnover:.2f}" if turnover else "N/A",
        "has_news": bool(news),
        "is_refinement": is_refinement,
        "retry_count": state.retry_count,
    }
    
    # DEBUG: 完整数据打印
    _log_with_trace(logger, trace_id, "sentiment", "=" * 60)
    _log_with_trace(logger, trace_id, "sentiment", "【舆情分析 - 完整数据】")
    _log_with_trace(logger, trace_id, "sentiment", f"股票: {ts_code} ({stock_name})")
    _log_with_trace(logger, trace_id, "sentiment", "-" * 40)
    _log_with_trace(logger, trace_id, "sentiment", "【市场情绪指标】")
    _log_with_trace(logger, trace_id, "sentiment", f"  pct_chg: {pct_chg:.2f}%")
    _log_with_trace(logger, trace_id, "sentiment", f"  price_signal: {price_signal}")
    _log_with_trace(logger, trace_id, "sentiment", f"  vol: {vol}")
    _log_with_trace(logger, trace_id, "sentiment", f"  vol_avg: {vol_avg:.2f}")
    _log_with_trace(logger, trace_id, "sentiment", f"  vol_status: {vol_status}")
    _log_with_trace(logger, trace_id, "sentiment", f"  turnover_rate: {turnover}")
    _log_with_trace(logger, trace_id, "sentiment", "-" * 40)
    _log_with_trace(logger, trace_id, "sentiment", "【新闻数据】")
    _log_with_trace(logger, trace_id, "sentiment", f"  news条数: {len(news)}")
    if news:
        for n in news[:3]:
            _log_with_trace(logger, trace_id, "sentiment", f"  - {n.get('title', '')[:50]}")
    _log_with_trace(logger, trace_id, "sentiment", "=" * 60)
    
    # 增量分析模式
    if is_refinement:
        prompt_params["previous_conclusion"] = state.initial_sentiment_res or state.structured_summary.sentiment_core
        prompt_params["previous_issues"] = state.get_previous_issues()
        prompt_params["supplementary_data"] = _get_supplementary_data_for_node(state, "sentiment")
    
    # 从配置获取提示词
    system_prompt = prompt_manager.get_system_prompt("stock_analysis/sentiment", **prompt_params)
    user_prompt = prompt_manager.get_prompt("stock_analysis/sentiment", **prompt_params)
    
    # DEBUG: 打印用户提示词前200字符
    _log_with_trace(logger, trace_id, "sentiment", 
                    f"User prompt preview: {user_prompt[:200]}...")
    
    analysis = await llm_manager.chat([
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ])
    
    _log_with_trace(logger, trace_id, "sentiment", 
                    f"Analysis complete, news_count={len(news)}, refinement={is_refinement}")
    # DEBUG: 打印分析结果前200字符
    _log_with_trace(logger, trace_id, "sentiment", 
                    f"Analysis preview: {analysis[:200]}...")
    
    # 首轮保存初始分析结果
    result = {"sentiment_res": analysis}
    if not is_refinement:
        result["initial_sentiment_res"] = analysis
    
    return result


async def supervisor_node(state: StockAnalysisState) -> Dict[str, Any]:
    """
    Supervisor 节点 (V3.2 增强版)
    
    职责：
    1. 结构化精简三方意见
    2. 对比三方分析意见
    3. 识别逻辑冲突
    4. 冲突调和
    5. 评估置信度
    6. 判断上轮问题是否解决
    7. 给出最终决策
    8. 保存 RoundSummary 用于跨轮次记忆
    """
    logger = logging.getLogger("node.supervisor")
    ts_code = state.ts_code
    trace_id = state.trace_id
    stock_name = state.stock_name or ts_code
    
    fundamental = state.fundamental_res or ""
    technical = state.technical_res or ""
    sentiment = state.sentiment_res or ""
    
    is_refinement = state.is_refinement_round()
    round_id = len(state.reasoning_steps) + 1
    
    _log_with_trace(logger, trace_id, "supervisor", 
                    f"Synthesizing analysis for {stock_name}, round={round_id}")
    _log_with_trace(logger, trace_id, "supervisor", 
                    f"Input lengths: F={len(fundamental)}, T={len(technical)}, S={len(sentiment)}")
    
    # DEBUG: 打印三方分析的完整内容
    _log_with_trace(logger, trace_id, "supervisor", "=" * 30 + " DEBUG: INPUT DATA " + "=" * 30)
    _log_with_trace(logger, trace_id, "supervisor", f"[FUNDAMENTAL] {fundamental[:200]}..." if fundamental else "[FUNDAMENTAL] EMPTY!")
    _log_with_trace(logger, trace_id, "supervisor", f"[TECHNICAL] {technical[:200]}..." if technical else "[TECHNICAL] EMPTY!")
    _log_with_trace(logger, trace_id, "supervisor", f"[SENTIMENT] {sentiment[:200]}..." if sentiment else "[SENTIMENT] EMPTY!")
    _log_with_trace(logger, trace_id, "supervisor", "=" * 80)
    
    # 预处理：提取核心结论 (结构化精简)
    fundamental_core = _extract_core_conclusion(fundamental)
    technical_core = _extract_core_conclusion(technical)
    sentiment_core = _extract_core_conclusion(sentiment)
    
    _log_with_trace(logger, trace_id, "supervisor", 
                    f"Core conclusions extracted: F='{fundamental_core[:50]}...'")
    
    # 准备 MCP 补充证据
    mcp_evidence = state.mcp_evidence or []
    
    # 准备 reasoning_steps 用于跨轮次记忆
    reasoning_steps_data = [
        {
            "round_id": rs.round_id,
            "fundamental_conclusion": rs.fundamental_conclusion,
            "technical_conclusion": rs.technical_conclusion,
            "sentiment_conclusion": rs.sentiment_conclusion,
            "conflicts_found": rs.conflicts_found,
            "confidence_score": rs.confidence_score,
            "decision": rs.decision,
        }
        for rs in state.reasoning_steps
    ]
    
    # 准备上轮未解决的问题
    previous_issues = state.get_previous_issues() if is_refinement else []
    
    # 从配置获取提示词
    system_prompt = prompt_manager.get_system_prompt(
        "stock_analysis/supervisor",
        is_refinement=is_refinement,
        round_id=round_id,
    )
    user_prompt = prompt_manager.get_prompt(
        "stock_analysis/supervisor",
        ts_code=ts_code,
        stock_name=stock_name,
        fundamental=fundamental,
        technical=technical,
        sentiment=sentiment,
        is_refinement=is_refinement,
        round_id=round_id,
        reasoning_steps=reasoning_steps_data,
        previous_issues=previous_issues,
        mcp_evidence=mcp_evidence if mcp_evidence else None,
        current_time=datetime.now().strftime("%Y-%m-%d %H:%M"),
    )
    
    # DEBUG: 打印发送给 LLM 的 Prompt 长度
    _log_with_trace(logger, trace_id, "supervisor", 
                    f"Prompt lengths: system={len(system_prompt)}, user={len(user_prompt)}")
    
    response = await llm_manager.chat([
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ])
    
    # DEBUG: 打印 LLM 原始响应
    _log_with_trace(logger, trace_id, "supervisor", 
                    f"LLM raw response length: {len(response)}")
    _log_with_trace(logger, trace_id, "supervisor", 
                    f"LLM response preview: {response[:300]}...")
    
    # 解析 JSON 响应
    result = _parse_json_response(response)
    
    # DEBUG: 打印解析后的结构化摘要
    _log_with_trace(logger, trace_id, "supervisor", 
                    f"Parsed result keys: {list(result.keys())}")
    
    # 提取结构化摘要
    structured_summary_data = result.get("结构化摘要", {})
    structured_summary = StructuredSummary(
        fundamental_core=structured_summary_data.get("基本面", fundamental_core),
        technical_core=structured_summary_data.get("技术面", technical_core),
        sentiment_core=structured_summary_data.get("舆情面", sentiment_core),
    )
    
    # 提取冲突信息 (V3.2: 包含状态)
    conflicts = []
    for conflict in result.get("逻辑冲突", []):
        conflicts.append(AnalysisConflict(
            conflict_type=conflict.get("冲突类型", ""),
            description=conflict.get("冲突描述", ""),
            resolution=conflict.get("调和结论", "") + f" [{conflict.get('状态', '未知')}]",
        ))
    
    # 提取置信度
    confidence_data = result.get("置信度评估", {})
    confidence_score = ConfidenceScore(
        data_completeness=confidence_data.get("数据完整性", 50),
        opinion_consistency=confidence_data.get("意见一致性", 50),
        overall=confidence_data.get("综合置信度", 50),
    )
    
    # 记录 Supervisor 评价日志
    _log_with_trace(logger, trace_id, "supervisor", "="*50)
    _log_with_trace(logger, trace_id, "supervisor", f"SUPERVISOR EVALUATION RESULT (Round {round_id})")
    _log_with_trace(logger, trace_id, "supervisor", f"Signal: {result.get('投资信号', 'N/A')}")
    _log_with_trace(logger, trace_id, "supervisor", f"Confidence: {result.get('信心程度', 0)}")
    _log_with_trace(logger, trace_id, "supervisor", f"Conflicts found: {len(conflicts)}")
    
    # 如果是增量分析，记录问题解决情况
    if is_refinement:
        resolution_status = result.get("上轮问题解决情况", {})
        resolved = resolution_status.get("resolved", [])
        unresolved = resolution_status.get("unresolved", [])
        _log_with_trace(logger, trace_id, "supervisor", f"Resolved issues: {len(resolved)}")
        _log_with_trace(logger, trace_id, "supervisor", f"Unresolved issues: {len(unresolved)}")
    
    for i, c in enumerate(conflicts):
        _log_with_trace(logger, trace_id, "supervisor", 
                        f"  Conflict {i+1}: [{c.conflict_type}] {c.description[:50]}...")
    _log_with_trace(logger, trace_id, "supervisor", f"Overall confidence: {confidence_score.overall}")
    _log_with_trace(logger, trace_id, "supervisor", "="*50)
    
    # 构建信号
    signal_map = {
        "强烈买入": SignalType.STRONG_BUY,
        "买入": SignalType.BUY,
        "持有": SignalType.HOLD,
        "卖出": SignalType.SELL,
        "强烈卖出": SignalType.STRONG_SELL,
    }
    signal_text = result.get("投资信号", "持有")
    signal = signal_map.get(signal_text, SignalType.HOLD)
    
    # 添加决策链步骤
    reasoning_step = ReasoningStep(
        step_id=len(state.reasoning_chain) + 1,
        node_name="supervisor",
        action=f"逻辑对冲与置信度评估 (Round {round_id})",
        reasoning=f"发现 {len(conflicts)} 个逻辑冲突点，综合置信度 {confidence_score.overall}%",
        result_summary=f"投资信号: {signal_text}",
    )
    
    # 保存本轮决策摘要 (V3.2 新增)
    # 注意：这里不直接调用 state.save_round_summary()，因为 state 是只读的
    # 我们返回新的 reasoning_steps
    new_round_summary = RoundSummary(
        round_id=round_id,
        fundamental_conclusion=structured_summary.fundamental_core,
        technical_conclusion=structured_summary.technical_core,
        sentiment_conclusion=structured_summary.sentiment_core,
        conflicts_found=[c.description for c in conflicts],
        confidence_score=confidence_score.overall,
        decision=signal_text,
        unresolved_issues=[
            c.description for c in conflicts 
            if "未解决" in c.resolution or "需进一步" in c.resolution
        ],
    )
    
    return {
        "structured_summary": structured_summary,
        "analysis_conflicts": conflicts,
        "confidence_score": confidence_score,
        "final_decision": signal_text,
        "decision_reason": result.get("最终决策理由", ""),
        "signal": signal.value,
        "confidence": result.get("信心程度", 50) / 100,
        "summary": result.get("综合摘要", response[:200]),
        "scores": {
            "fundamental": result.get("各维度评分", {}).get("基本面", 60),
            "technical": result.get("各维度评分", {}).get("技术面", 60),
            "sentiment": result.get("各维度评分", {}).get("舆情", 60),
        },
        "risks": result.get("风险提示", []),
        "reasoning_chain": state.reasoning_chain + [reasoning_step],
        "reasoning_steps": state.reasoning_steps + [new_round_summary],
    }


async def check_result_node(state: StockAnalysisState) -> Dict[str, Any]:
    """
    Check Result 节点
    
    检查置信度是否满足阈值，决定是否需要补充数据
    """
    logger = logging.getLogger("node.check_result")
    trace_id = state.trace_id
    
    confidence_score = state.confidence_score
    overall_confidence = confidence_score.overall if confidence_score else 50
    retry_count = state.retry_count or 0
    
    # 获取阈值
    try:
        config = prompt_manager.get_config("stock_analysis/supervisor")
        threshold = config.get("confidence_threshold", CONFIDENCE_THRESHOLD)
    except Exception:
        threshold = CONFIDENCE_THRESHOLD
    
    needs_refinement = overall_confidence < threshold and retry_count < MAX_RETRY_COUNT
    
    _log_with_trace(logger, trace_id, "check_result", 
                    f"Confidence={overall_confidence}, threshold={threshold}, retry={retry_count}")
    _log_with_trace(logger, trace_id, "check_result", f"Needs refinement: {needs_refinement}")
    
    # 记录决策链
    reasoning_step = ReasoningStep(
        step_id=len(state.reasoning_chain) + 1,
        node_name="check_result",
        action="置信度检查",
        reasoning=f"置信度 {overall_confidence}% {'<' if needs_refinement else '>='} 阈值 {threshold}%",
        result_summary="需要补充数据" if needs_refinement else "置信度满足要求",
    )
    
    return {
        "needs_refinement": needs_refinement,
        "reasoning_chain": state.reasoning_chain + [reasoning_step],
    }


async def query_refinement_node(state: StockAnalysisState) -> Dict[str, Any]:
    """
    Query Refinement 节点 (V3.2 增强版)
    
    当置信度不足时，生成需要补充的 MCP 工具调用指令
    """
    logger = logging.getLogger("node.query_refinement")
    trace_id = state.trace_id
    ts_code = state.ts_code
    stock_name = state.stock_name or ts_code
    
    confidence = state.confidence_score.overall if state.confidence_score else 50
    data_completeness = state.confidence_score.data_completeness if state.confidence_score else 50
    opinion_consistency = state.confidence_score.opinion_consistency if state.confidence_score else 50
    conflicts = state.analysis_conflicts or []
    structured_summary = state.structured_summary
    
    _log_with_trace(logger, trace_id, "query_refinement", 
                    f"Generating refinement queries, confidence={confidence}")
    
    # 从配置获取提示词
    try:
        system_prompt = prompt_manager.get_system_prompt("stock_analysis/refinement")
        user_prompt = prompt_manager.get_prompt(
            "stock_analysis/refinement",
            ts_code=ts_code,
            stock_name=stock_name,
            confidence=confidence,
            data_completeness=data_completeness,
            opinion_consistency=opinion_consistency,
            conflicts=[{
                "conflict_type": c.conflict_type, 
                "description": c.description,
                "resolution": c.resolution,
            } for c in conflicts],
            fundamental_core=structured_summary.fundamental_core,
            technical_core=structured_summary.technical_core,
            sentiment_core=structured_summary.sentiment_core,
        )
        
        response = await llm_manager.chat([
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ])
        
        result = _parse_json_response(response)
        refinement_queries = result.get("refinement_queries", [])
        reasoning = result.get("reasoning", "")
        
    except Exception as e:
        _log_with_trace(logger, trace_id, "query_refinement", 
                        f"Failed to generate queries: {e}", "warning")
        # 默认查询
        refinement_queries = [
            {"tool": "get_stock_daily", "query": "获取最近10日成交量变化", 
             "target_conflict": "量价关系", "expected_evidence": "验证量能配合"},
            {"tool": "get_news_sentiment", "query": "搜索资金流向相关新闻", 
             "target_conflict": "资金动态", "expected_evidence": "机构动态信息"},
        ]
        reasoning = "默认补充量价和资金数据"
    
    _log_with_trace(logger, trace_id, "query_refinement", 
                    f"Generated {len(refinement_queries)} queries")
    for q in refinement_queries:
        query_desc = q.get("query", q) if isinstance(q, dict) else q
        _log_with_trace(logger, trace_id, "query_refinement", f"  - {query_desc}")
    
    # 记录决策链
    reasoning_step = ReasoningStep(
        step_id=len(state.reasoning_chain) + 1,
        node_name="query_refinement",
        action="生成补充查询",
        reasoning=reasoning,
        result_summary=f"生成 {len(refinement_queries)} 条 MCP 工具调用",
    )
    
    return {
        "refinement_queries": refinement_queries,
        "retry_count": (state.retry_count or 0) + 1,
        "reasoning_chain": state.reasoning_chain + [reasoning_step],
    }


async def mcp_search_node(state: StockAnalysisState) -> Dict[str, Any]:
    """
    MCP Search 节点 (V3.2 增强)
    
    执行 MCP 工具调用，获取补充数据
    生成 SupplementaryData 供增量分析使用
    """
    logger = logging.getLogger("node.mcp_search")
    trace_id = state.trace_id
    ts_code = state.ts_code
    
    refinement_queries = state.refinement_queries or []
    
    _log_with_trace(logger, trace_id, "mcp_search", 
                    f"Executing {len(refinement_queries)} MCP tool calls")
    
    mcp_tool_calls = []
    mcp_evidence = []
    supplementary_data = []  # V3.2 新增
    
    for query_item in refinement_queries:
        if isinstance(query_item, dict):
            tool_name = query_item.get("tool", "get_stock_daily")
            query_desc = query_item.get("query", "")
            target_conflict = query_item.get("target_conflict", "")
            expected_evidence = query_item.get("expected_evidence", "")
        else:
            tool_name = "get_news_sentiment"
            query_desc = str(query_item)
            target_conflict = ""
            expected_evidence = ""
        
        _log_with_trace(logger, trace_id, "mcp_search", 
                        f"Calling tool: {tool_name}, query: {query_desc}")
        
        # 执行 MCP 工具调用
        result = None
        success = False
        content_summary = ""
        
        try:
            if tool_name == "get_stock_daily":
                # 获取日线数据
                daily_data = await mongo_manager.find_many(
                    "stock_daily",
                    {"ts_code": ts_code},
                    sort=[("trade_date", -1)],
                    limit=10,
                )
                if daily_data:
                    # 计算量能变化
                    vols = [d.get("vol", 0) for d in daily_data]
                    vol_changes = []
                    for i in range(1, len(vols)):
                        if vols[i] > 0:
                            change = (vols[i-1] - vols[i]) / vols[i] * 100
                            vol_changes.append(f"{change:+.1f}%")
                    
                    content_summary = f"近10日成交量变化: {', '.join(vol_changes[:5])}"
                    result = {
                        "dates": [d.get("trade_date") for d in daily_data[:5]],
                        "vol_changes": vol_changes[:5],
                        "summary": content_summary,
                    }
                    success = True
                    
                    mcp_evidence.append({
                        "source": "日线数据",
                        "content": content_summary,
                    })
            
            elif tool_name == "get_financial_indicator":
                # 获取财务数据 (简化实现)
                content_summary = "暂无最新财务数据"
                result = {"summary": content_summary}
                success = True
            
            elif tool_name == "get_news_sentiment":
                # 获取新闻
                stock_name = state.stock_name or ts_code
                news = await mongo_manager.find_many(
                    "news",
                    {"$or": [
                        {"title": {"$regex": stock_name}},
                        {"content": {"$regex": ts_code}},
                    ]},
                    sort=[("datetime", -1)],
                    limit=5,
                )
                
                if news:
                    news_titles = [n.get("title", "") for n in news]
                    content_summary = f"相关新闻: {', '.join(news_titles[:3])}"
                    result = {
                        "count": len(news),
                        "titles": news_titles,
                        "summary": content_summary,
                    }
                    success = True
                    
                    mcp_evidence.append({
                        "source": "新闻舆情",
                        "content": content_summary,
                    })
                else:
                    content_summary = "未找到相关新闻"
                    result = {"summary": content_summary}
                    success = True
            
            elif tool_name == "search_similar_reports":
                # 搜索相似研报 (需要 Milvus)
                if not milvus_manager.is_disabled():
                    embeddings = await llm_manager.embedding([query_desc])
                    if embeddings:
                        reports = await milvus_manager.search_reports(embeddings[0], top_k=3)
                        if reports:
                            content_summary = f"找到 {len(reports)} 篇相关研报"
                            result = {
                                "count": len(reports),
                                "summary": content_summary,
                            }
                            success = True
                            
                            mcp_evidence.append({
                                "source": "研报检索",
                                "content": content_summary,
                            })
                
                if not success:
                    content_summary = "研报检索服务不可用"
                    result = {"summary": content_summary}
                    success = True
        
        except Exception as e:
            _log_with_trace(logger, trace_id, "mcp_search", 
                            f"Tool call failed: {e}", "warning")
            result = {"error": str(e)}
            content_summary = f"调用失败: {str(e)}"
            success = False
        
        mcp_tool_calls.append(MCPToolCall(
            tool=tool_name,
            query=query_desc,
            target_conflict=target_conflict,
            expected_evidence=expected_evidence,
            result=result,
            success=success,
        ))
        
        # V3.2: 生成 SupplementaryData
        if success and content_summary:
            supplementary_data.append(SupplementaryData(
                source=tool_name,
                target_conflict=target_conflict,
                content=content_summary,
                raw_data=result,
            ))
    
    _log_with_trace(logger, trace_id, "mcp_search", 
                    f"Completed {len(mcp_tool_calls)} tool calls, got {len(supplementary_data)} supplementary data")
    
    # 记录决策链
    reasoning_step = ReasoningStep(
        step_id=len(state.reasoning_chain) + 1,
        node_name="mcp_search",
        action="执行 MCP 工具调用",
        reasoning=f"调用 {len(mcp_tool_calls)} 个工具获取补充数据",
        result_summary=f"获得 {len(supplementary_data)} 条补充证据",
    )
    
    return {
        "mcp_tool_calls": mcp_tool_calls,
        "mcp_evidence": mcp_evidence,
        "supplementary_data": supplementary_data,  # V3.2 新增
        "reasoning_chain": state.reasoning_chain + [reasoning_step],
    }


def route_after_check(state: StockAnalysisState) -> Literal["refinement", "output"]:
    """
    条件边缘：根据 check_result 结果决定下一步
    """
    if state.needs_refinement:
        return "refinement"
    return "output"


def build_output(state: StockAnalysisState) -> Dict[str, Any]:
    """
    构建最终输出报告
    """
    logger = logging.getLogger("node.output")
    trace_id = state.trace_id
    
    # 构建冲突对冲说明
    conflicts = state.analysis_conflicts or []
    conflict_summary = ""
    if conflicts:
        conflict_lines = []
        for c in conflicts:
            conflict_lines.append(
                f"• [{c.conflict_type}] {c.description} → {c.resolution}"
            )
        conflict_summary = "\n".join(conflict_lines)
    else:
        conflict_summary = "三方意见一致，无逻辑矛盾。"
    
    confidence_score = state.confidence_score
    
    # 构建决策链摘要
    reasoning_chain = state.reasoning_chain or []
    reasoning_summary = []
    for step in reasoning_chain:
        reasoning_summary.append({
            "step": step.step_id,
            "node": step.node_name,
            "action": step.action,
            "result": step.result_summary,
        })
    
    # 构建跨轮次决策历史 (V3.2)
    round_history = []
    for rs in state.reasoning_steps:
        round_history.append({
            "round_id": rs.round_id,
            "confidence": rs.confidence_score,
            "decision": rs.decision,
            "conflicts_count": len(rs.conflicts_found),
            "unresolved_count": len(rs.unresolved_issues),
        })
    
    result = {
        # 追踪信息
        "trace_id": trace_id,
        
        # 核心结果
        "signal": state.signal or SignalType.HOLD.value,
        "confidence": state.confidence or 0.5,
        "summary": state.summary or "",
        "scores": state.scores or {},
        
        # 详细分析
        "fundamental_analysis": state.fundamental_res or "",
        "technical_analysis": state.technical_res or "",
        "sentiment_analysis": state.sentiment_res or "",
        
        # 结构化摘要
        "structured_summary": state.structured_summary.dict() if state.structured_summary else {},
        
        # 冲突与调和
        "analysis_conflicts": [c.dict() for c in conflicts] if conflicts else [],
        "conflict_summary": conflict_summary,
        
        # 置信度
        "confidence_score": confidence_score.dict() if confidence_score else {},
        
        # 最终决策
        "final_decision": state.final_decision or "",
        "decision_reason": state.decision_reason or "",
        
        # 风险提示
        "risks": state.risks or [],
        
        # MCP 补充信息
        "mcp_tool_calls": [t.dict() for t in state.mcp_tool_calls] if state.mcp_tool_calls else [],
        "mcp_evidence": state.mcp_evidence or [],
        "supplementary_data": [s.dict() for s in state.supplementary_data] if state.supplementary_data else [],
        
        # 决策链追踪
        "reasoning_chain": reasoning_summary,
        "refinement_queries": state.refinement_queries or [],
        "retry_count": state.retry_count or 0,
        
        # 跨轮次历史 (V3.2)
        "round_history": round_history,
    }
    
    _log_with_trace(logger, trace_id, "output", 
                    f"Final result: signal={result['signal']}, confidence={result['confidence']}")
    _log_with_trace(logger, trace_id, "output", 
                    f"Decision chain: {len(reasoning_summary)} steps, {state.retry_count or 0} retries, {len(round_history)} rounds")
    
    return result


# ==================== 主类 ====================

class StockAnalysisGraph:
    """
    股票分析图 (V3.2)
    
    特性：
    - Pydantic 强类型状态
    - Supervisor 冲突调和 + 结构化精简
    - Check Result 置信度检查
    - Query Refinement 数据补充
    - MCP Search 工具调用
    - 完整决策链追踪
    - 跨轮次记忆 (reasoning_steps)
    - 增量分析 (supplementary_data)
    """
    
    def __init__(self):
        self.logger = logging.getLogger("graph.stock_analysis")
        self._initialized = False
    
    async def initialize(self) -> None:
        """初始化"""
        # 确保 prompt_manager 已初始化
        await prompt_manager.initialize()
        
        self._initialized = True
        self.logger.info("Stock analysis graph initialized (V3.2 Incremental Analysis)")
    
    async def analyze_stock(
        self,
        ts_code: str,
        task_id: str,
        progress_callback: Optional[Callable] = None,
    ) -> Dict[str, Any]:
        """
        个股分析
        
        Args:
            ts_code: 股票代码
            task_id: 任务 ID
            progress_callback: 进度回调 (progress: float, message: str)
        """
        import uuid
        trace_id = uuid.uuid4().hex
        
        self.logger.info(f"[analyze_stock] trace_id={trace_id} | Starting analysis for {ts_code}")
        
        # 初始化状态 (使用 Pydantic 模型)
        state = StockAnalysisState(
            ts_code=ts_code,
            task_id=task_id,
            trace_id=trace_id,
        )
        
        try:
            # Phase 1: 数据采集
            if progress_callback:
                await progress_callback(10, "获取股票数据...")
            
            data_result = await data_collect_node(state)
            state = state.copy(update=data_result)
            
            # Phase 2: 并行多维度分析 (首轮)
            if progress_callback:
                await progress_callback(30, "多维度分析中 (Round 1)...")
            
            fundamental_result, technical_result, sentiment_result = await asyncio.gather(
                fundamental_node(state),
                technical_node(state),
                sentiment_node(state),
            )
            
            state = state.copy(update={
                **fundamental_result,
                **technical_result,
                **sentiment_result,
            })
            
            # Phase 3: Supervisor 综合研判 (Round 1)
            if progress_callback:
                await progress_callback(55, "Supervisor 综合研判 (Round 1)...")
            
            supervisor_result = await supervisor_node(state)
            state = state.copy(update=supervisor_result)
            
            # Phase 4: Check Result 置信度检查
            if progress_callback:
                await progress_callback(65, "置信度检查...")
            
            check_result = await check_result_node(state)
            state = state.copy(update=check_result)
            
            # Phase 5: 条件分支 - 是否需要补充数据
            if state.needs_refinement:
                self.logger.info(f"[analyze_stock] trace_id={trace_id} | Confidence too low, triggering refinement...")
                
                if progress_callback:
                    await progress_callback(70, "置信度不足，生成补充查询...")
                
                # Query Refinement: 生成查询
                refinement_result = await query_refinement_node(state)
                state = state.copy(update=refinement_result)
                
                # MCP Search: 执行工具调用
                if progress_callback:
                    await progress_callback(75, "执行 MCP 工具调用...")
                
                mcp_result = await mcp_search_node(state)
                state = state.copy(update=mcp_result)
                
                # 增量分析 (Round 2): 重新执行三方分析
                if progress_callback:
                    await progress_callback(80, "增量分析中 (Round 2)...")
                
                fundamental_result, technical_result, sentiment_result = await asyncio.gather(
                    fundamental_node(state),
                    technical_node(state),
                    sentiment_node(state),
                )
                
                state = state.copy(update={
                    **fundamental_result,
                    **technical_result,
                    **sentiment_result,
                })
                
                # 重新 Supervisor 评估 (带 MCP 证据和跨轮次记忆)
                if progress_callback:
                    await progress_callback(90, "Supervisor 重新评估 (Round 2)...")
                
                supervisor_result = await supervisor_node(state)
                state = state.copy(update=supervisor_result)
            
            # Phase 6: 构建输出
            if progress_callback:
                await progress_callback(95, "生成报告...")
            
            result = build_output(state)
            
            if progress_callback:
                await progress_callback(100, "分析完成")
            
            self.logger.info(f"[analyze_stock] trace_id={trace_id} | Analysis complete, rounds={len(state.reasoning_steps)}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"[analyze_stock] trace_id={trace_id} | Error: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    async def analyze_market(
        self,
        task_id: str,
        progress_callback: Optional[Callable] = None,
    ) -> Dict[str, Any]:
        """
        大盘分析 V2.3 - 10日趋势深度对冲版
        
        数据采集:
        - 最近 10 个交易日的 daily_stats（涨跌家数、涨停跌停数、连板高度、成交额）
        - 最近 10 个交易日的 market_analysis（情绪评分、市场评分）
        - 最新的 sector_ranking（板块共识度 TOP10）
        
        逻辑预处理:
        - 量能变化：今日成交额 vs 10日均量
        - 评分趋势：今日情绪分 vs 昨日情绪分
        - 背离检查：看涨背离 / 诱多背离
        
        输出:
        - 周期定位（退潮冰点期/分歧修复期/高潮亢奋期/混沌震荡期）
        - 风险等级（0-100）
        - 实战策略建议（含具体仓位百分比）
        """
        trace_id = task_id[:8]
        self.logger.info(f"[analyze_market] trace_id={trace_id} | 开始大盘分析 (V2.3)")
        
        if progress_callback:
            await progress_callback(10, "采集市场数据...")
        
        # ==================== 1. 数据采集 ====================
        
        # 1.1 获取最近 10 个交易日的 daily_stats
        daily_stats_list = await mongo_manager.find_many(
            "daily_stats",
            {},
            sort=[("trade_date", -1)],
            limit=10,
        )
        self.logger.info(f"[analyze_market] trace_id={trace_id} | daily_stats: {len(daily_stats_list)} 条")
        
        # 1.2 获取最近 10 个交易日的 market_analysis
        market_analysis_list = await mongo_manager.find_many(
            "market_analysis",
            {},
            sort=[("trade_date", -1)],
            limit=10,
        )
        self.logger.info(f"[analyze_market] trace_id={trace_id} | market_analysis: {len(market_analysis_list)} 条")
        
        # 1.3 获取最新的 sector_ranking（前 10 名）
        latest_date = daily_stats_list[0].get("trade_date") if daily_stats_list else None
        sector_ranking_list = []
        sector_ranking_3d_ago = {}  # 3日前的板块数据（用于趋势追踪）
        
        if latest_date:
            # 获取行业板块排名（按 rank 排序）
            sector_ranking_list = await mongo_manager.find_many(
                "sector_ranking",
                {"trade_date": latest_date, "ranking_type": "industry_top"},
                sort=[("rank", 1)],  # 按排名升序
                limit=10,
            )
        
        self.logger.info(f"[analyze_market] trace_id={trace_id} | sector_ranking: {len(sector_ranking_list)} 条")
        
        # 1.4 获取上证指数10日K线数据
        sh_index_list = await mongo_manager.find_many(
            "index_daily",
            {"ts_code": "000001.SH"},
            sort=[("trade_date", -1)],
            limit=10,
        )
        self.logger.info(f"[analyze_market] trace_id={trace_id} | 上证指数K线: {len(sh_index_list)} 条")
        
        if progress_callback:
            await progress_callback(30, "预处理数据...")
        
        # ==================== 2. 逻辑预处理 ====================
        
        # 2.1 构建时序数据（按时间正序，保留完整 10 日）
        stats_by_date = {s.get("trade_date"): s for s in daily_stats_list}
        analysis_by_date = {a.get("trade_date"): a for a in market_analysis_list}
        
        # 合并数据，按时间正序（从旧到新）
        dates = sorted(set(stats_by_date.keys()) | set(analysis_by_date.keys()))[-10:]
        
        # 2.1.0 获取 3 日前的 sector_ranking（用于趋势对比）
        if len(dates) >= 4:
            date_3d_ago = dates[-4]  # 倒数第4天 = 3日前
            ranking_3d_list = await mongo_manager.find_many(
                "sector_ranking",
                {"trade_date": date_3d_ago, "ranking_type": "industry_top"},
                sort=[("rank", 1)],
                limit=50,  # 获取更多以确保能匹配到今日 TOP10
            )
            # 使用 name 字段和 rank 字段
            sector_ranking_3d_ago = {
                s.get("name"): s.get("rank", 99) 
                for s in ranking_3d_list if s.get("name")
            }
            self.logger.info(f"[analyze_market] trace_id={trace_id} | 3日前({date_3d_ago})板块数据: {len(sector_ranking_3d_ago)} 条")
        
        # 先收集原始数据（用于计算平均值）
        raw_time_series = []
        for d in dates:
            stats = stats_by_date.get(d, {})
            analysis = analysis_by_date.get(d, {})
            raw_time_series.append({
                "date": d,
                "sentiment_score": analysis.get("sentiment_score_ema") or analysis.get("sentiment_score"),
                "market_score": analysis.get("market_score"),
                "strength_score": analysis.get("strength_score"),
                "up_count": stats.get("up_count", 0),
                "down_count": stats.get("down_count", 0),
                "limit_up_count": stats.get("limit_up_count", 0),
                "limit_down_count": stats.get("limit_down_count", 0),
                "max_limit_height": stats.get("max_limit_height", 0),
                "total_amount": stats.get("total_amount"),  # 可能为 None
                "cycle": analysis.get("cycle", "unknown"),
            })
        
        # 2.1.1 空值填充：计算有效值的平均值，用于填充缺失数据
        def calc_avg(field: str) -> float:
            """计算字段的有效平均值"""
            valid_values = [t.get(field) for t in raw_time_series if t.get(field) is not None and t.get(field) > 0]
            return sum(valid_values) / len(valid_values) if valid_values else 0
        
        avg_sentiment = calc_avg("sentiment_score")
        avg_strength = calc_avg("strength_score")
        avg_market = calc_avg("market_score")
        avg_volume = calc_avg("total_amount")
        
        # 使用平均值填充缺失数据
        time_series = []
        for t in raw_time_series:
            time_series.append({
                "date": t["date"],
                "sentiment_score": t["sentiment_score"] if t["sentiment_score"] is not None else avg_sentiment,
                "market_score": t["market_score"] if t["market_score"] is not None else avg_market,
                "strength_score": t["strength_score"] if t["strength_score"] is not None else avg_strength,
                "up_count": t["up_count"],
                "down_count": t["down_count"],
                "limit_up_count": t["limit_up_count"],
                "limit_down_count": t["limit_down_count"],
                "max_limit_height": t["max_limit_height"],
                "total_amount": t["total_amount"] if t["total_amount"] is not None else avg_volume,
                "cycle": t["cycle"],
            })
        
        self.logger.info(
            f"[analyze_market] trace_id={trace_id} | 空值填充完成: "
            f"avg_sentiment={avg_sentiment:.1f}, avg_volume={avg_volume/100000:.0f}亿"
        )
        
        # 2.2 量能变化：今日成交额 vs 5日均量（更敏感）
        volume_change_pct = 0.0
        volume_status = "持平"  # 语义标签
        if len(time_series) >= 5:
            today_amount = time_series[-1].get("total_amount", 0)
            # 使用近5日均量作为基准
            recent_5d = time_series[-5:]
            avg_5d_amount = sum(t.get("total_amount", 0) for t in recent_5d) / len(recent_5d)
            if avg_5d_amount > 0:
                volume_change_pct = (today_amount - avg_5d_amount) / avg_5d_amount * 100
        
        # 2.2.1 归一化描述：映射为语义标签
        if volume_change_pct > 30:
            volume_status = "大幅放量"
        elif volume_change_pct > 15:
            volume_status = "温和放量"
        elif volume_change_pct > 0:
            volume_status = "小幅放量"
        elif volume_change_pct > -15:
            volume_status = "小幅缩量"
        elif volume_change_pct > -30:
            volume_status = "温和缩量"
        else:
            volume_status = "显著缩量"
        
        self.logger.info(
            f"[analyze_market] trace_id={trace_id} | 量能: {volume_status} "
            f"({volume_change_pct:+.1f}% vs 5日均量)"
        )
        
        # 2.2.2 上证指数分析（连阳天数、涨跌幅）
        sh_index_snapshot = ""
        consecutive_up_days = 0
        sh_latest_pct = 0.0
        
        if sh_index_list:
            # 按时间正序排列（从旧到新）
            sh_sorted = sorted(sh_index_list, key=lambda x: x.get("trade_date", ""))
            
            # 计算连阳天数（从今日往前数）
            for i in range(len(sh_sorted) - 1, -1, -1):
                pct_chg = sh_sorted[i].get("pct_chg", 0) or 0
                if pct_chg > 0:
                    consecutive_up_days += 1
                else:
                    break
            
            # 今日涨跌幅
            sh_latest_pct = sh_sorted[-1].get("pct_chg", 0) or 0
            
            # 构建上证指数快照
            sh_lines = []
            for idx in sh_sorted:
                date = idx.get("trade_date", "")
                close = idx.get("close", 0)
                pct = idx.get("pct_chg", 0) or 0
                vol = idx.get("vol", 0) or 0  # 成交量（手）
                arrow = "↑" if pct > 0 else ("↓" if pct < 0 else "→")
                sh_lines.append(f"  {date}: 收盘={close:.2f}, 涨跌={pct:+.2f}%{arrow}, 成交={vol/10000:.0f}万手")
            
            sh_index_snapshot = "\n".join(sh_lines)
            
            self.logger.info(
                f"[analyze_market] trace_id={trace_id} | 上证指数: "
                f"连阳{consecutive_up_days}天, 今日{sh_latest_pct:+.2f}%"
            )
        
        # 2.3 评分趋势：今日情绪分 vs 昨日情绪分（精准对比）
        sentiment_trend = "持平"
        today_sentiment = 0
        yesterday_sentiment = 0
        if len(time_series) >= 2:
            today_sentiment = time_series[-1].get("sentiment_score", 0)
            yesterday_sentiment = time_series[-2].get("sentiment_score", 0)
            diff = today_sentiment - yesterday_sentiment
            
            if diff > 5:
                sentiment_trend = f"上升 +{diff:.0f}"
            elif diff < -5:
                sentiment_trend = f"下降 {diff:.0f}"
            else:
                sentiment_trend = f"微幅变动 {diff:+.0f}"
        
        self.logger.info(
            f"[analyze_market] trace_id={trace_id} | 情绪趋势: {sentiment_trend} "
            f"(今日={today_sentiment:.0f}, 昨日={yesterday_sentiment:.0f})"
        )
        
        # 2.4 背离检查（基于 10 日窗口的深度分析）
        divergence = "无"
        if len(time_series) >= 5:
            today = time_series[-1]
            
            # 提取 10 日内最高板变化趋势
            heights = [t.get("max_limit_height", 0) for t in time_series]
            sentiments = [t.get("sentiment_score", 0) for t in time_series]
            market_scores = [t.get("market_score", 0) for t in time_series]
            
            # 看涨背离：10日内情绪评分底部抬升，且连板高度开始突破
            early_sentiment_avg = sum(sentiments[:5]) / 5 if len(sentiments) >= 5 else 0
            late_sentiment_avg = sum(sentiments[-3:]) / 3 if len(sentiments) >= 3 else 0
            early_height_max = max(heights[:5]) if len(heights) >= 5 else 0
            late_height_max = max(heights[-3:]) if len(heights) >= 3 else 0
            
            if late_sentiment_avg > early_sentiment_avg and late_height_max > early_height_max:
                divergence = "看涨背离（情绪底部抬升+连板高度突破）"
            
            # 诱多背离：市场评分持续下滑，但涨跌家数显示表面繁荣
            if len(market_scores) >= 5:
                early_market_avg = sum(market_scores[:5]) / 5
                late_market_avg = sum(market_scores[-3:]) / 3
                today_up_ratio = today.get("up_count", 0) / max(today.get("up_count", 0) + today.get("down_count", 0), 1)
                
                if late_market_avg < early_market_avg * 0.85 and today_up_ratio > 0.5:
                    divergence = "诱多背离（市场评分萎缩但涨多跌少）"
        
        if divergence != "无":
            self.logger.info(f"[analyze_market] trace_id={trace_id} | 背离检测: {divergence}")
        
        # 2.5 最新数据
        latest = time_series[-1] if time_series else {}
        current_cycle = latest.get("cycle", "unknown")
        max_height = latest.get("max_limit_height", 0)
        sentiment_score = latest.get("sentiment_score", 0)
        strength_score = latest.get("strength_score", 0)
        
        if progress_callback:
            await progress_callback(50, "生成分析报告...")
        
        # ==================== 3. 构建 Prompt ====================
        
        # 3.1 10日趋势快照（完整 10 条，按时间由旧到新）
        # 注：total_amount 存储单位是千元，转换为亿：千元 / 100000 = 亿
        trend_snapshot = "\n".join([
            f"  {t['date']}: 情绪={t['sentiment_score']:.0f}, 强度={t['strength_score']:.0f}, "
            f"涨停={t['limit_up_count']}, 跌停={t['limit_down_count']}, 最高板={t['max_limit_height']}, "
            f"成交额={t['total_amount']/100000:.0f}亿"
            for t in time_series
        ])
        
        self.logger.info(f"[analyze_market] trace_id={trace_id} | 10日趋势快照:\n{trend_snapshot}")
        
        # 3.2 板块排名 TOP10（含趋势追踪）
        sector_lines = []
        for i, s in enumerate(sector_ranking_list[:10]):
            sector_name = s.get("name", "N/A")
            rank_today = s.get("rank", i + 1)
            pct_change = s.get("pct_change", 0) or 0  # 涨跌幅
            net_amount = s.get("net_amount", 0) or 0  # 净流入（万元）
            lead_stock = s.get("lead_stock", "")  # 领涨股
            
            # 计算与 3 日前的排名变化
            rank_3d_ago = sector_ranking_3d_ago.get(sector_name, 99)
            if rank_3d_ago < 99:
                rank_change = rank_3d_ago - rank_today  # 正数表示排名上升
                if rank_change > 3:
                    trend_label = f"↑{rank_change}位 强势上升"
                elif rank_change > 0:
                    trend_label = f"↑{rank_change}位"
                elif rank_change < -3:
                    trend_label = f"↓{-rank_change}位 明显退潮"
                elif rank_change < 0:
                    trend_label = f"↓{-rank_change}位"
                else:
                    trend_label = "持平"
            else:
                trend_label = "新进榜"
            
            # 净流入转换为亿（原单位万元）
            net_amount_yi = net_amount / 10000 if net_amount else 0
            
            sector_lines.append(
                f"  {rank_today}. {sector_name}: 涨幅={pct_change:+.2f}%, "
                f"净流入={net_amount_yi:.1f}亿 ({trend_label})"
                + (f", 领涨:{lead_stock}" if lead_stock else "")
            )
        
        sector_snapshot = "\n".join(sector_lines) if sector_lines else "暂无板块数据"
        
        # 3.3 构建模板参数
        prompt_params = {
            "sentiment_score": f"{sentiment_score:.0f}",
            "strength_score": f"{strength_score:.0f}",
            "max_height": max_height,
            "volume_change_pct": f"{volume_change_pct:+.1f}",
            "volume_status": volume_status,  # 语义标签：大幅放量/显著缩量等
            "sentiment_trend": sentiment_trend,
            "divergence": divergence,
            "trend_snapshot": trend_snapshot,
            "sector_snapshot": sector_snapshot,
            # 上证指数数据
            "sh_index_snapshot": sh_index_snapshot if sh_index_snapshot else "暂无上证指数数据",
            "consecutive_up_days": consecutive_up_days,
            "sh_latest_pct": f"{sh_latest_pct:+.2f}",
        }
        
        # 3.4 从配置获取提示词
        system_prompt = prompt_manager.get_system_prompt("stock_analysis/market", **prompt_params)
        user_prompt = prompt_manager.get_prompt("stock_analysis/market", **prompt_params)
        
        self.logger.info(f"[analyze_market] trace_id={trace_id} | 调用 LLM 分析...")
        
        # ==================== 4. 调用 LLM ====================
        
        response = await llm_manager.chat([
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ])
        
        if progress_callback:
            await progress_callback(80, "解析分析结果...")
        
        # ==================== 5. 解析结构化输出 ====================
        
        result = {
            "signal": SignalType.HOLD.value,
            "confidence": 0.6,
            "summary": response,
            "scores": {
                "sentiment": sentiment_score,
                "strength": strength_score,
            },
            "cycle": current_cycle,
            "cycle_name": "",
            "cycle_reason": "",
            "risk_score": 50,
            "risk_reason": "",
            "position_advice": "",
            "strategy": "",
            "focus_sectors": [],
            "index_analysis": "",  # V2.5新增：上证指数走势分析
            "tomorrow_outlook": {
                "direction": "震荡",
                "confidence": "中",
                "key_observation": "",
                "risk_point": "",
            },
            "analysis_conflicts": [],
            "conflict_summary": "",
            "confidence_score": {"overall": 60},
            # 额外数据
            "sh_consecutive_up_days": consecutive_up_days,
            "sh_latest_pct": sh_latest_pct,
        }
        
        # 尝试解析 JSON
        try:
            # 提取 JSON
            json_match = re.search(r'```json\s*(.*?)\s*```', response, re.DOTALL)
            if json_match:
                parsed = json.loads(json_match.group(1))
            else:
                # 尝试直接解析
                parsed = json.loads(response)
            
            # 映射字段（V2.3 新增"混沌震荡期"）
            cycle_map = {
                "退潮冰点期": "ice_point",
                "分歧修复期": "divergence",
                "高潮亢奋期": "climax",
                "混沌震荡期": "chaos",
            }
            signal_map = {
                "buy": SignalType.BUY.value,
                "sell": SignalType.SELL.value,
                "hold": SignalType.HOLD.value,
            }
            
            # 解析周期
            raw_cycle = parsed.get("cycle", "")
            result["cycle"] = cycle_map.get(raw_cycle, current_cycle)
            result["cycle_name"] = raw_cycle  # 保留中文名称
            result["cycle_reason"] = parsed.get("cycle_reason", "")
            
            # 解析信号
            result["signal"] = signal_map.get(parsed.get("signal", "hold"), SignalType.HOLD.value)
            
            # 解析风险
            result["risk_score"] = parsed.get("risk_score", 50)
            result["risk_reason"] = parsed.get("risk_reason", "")
            
            # 解析策略建议
            result["position_advice"] = parsed.get("position_advice", "")
            result["strategy"] = parsed.get("strategy", "")
            result["focus_sectors"] = parsed.get("focus_sectors", [])
            result["summary"] = parsed.get("summary", response)
            
            # 解析上证指数分析 (V2.5新增)
            result["index_analysis"] = parsed.get("index_analysis", "")
            
            # 解析明日预判 (V2.4新增)
            tomorrow = parsed.get("tomorrow_outlook", {})
            if isinstance(tomorrow, dict):
                result["tomorrow_outlook"] = {
                    "direction": tomorrow.get("direction", "震荡"),
                    "confidence": tomorrow.get("confidence", "中"),
                    "key_observation": tomorrow.get("key_observation", ""),
                    "risk_point": tomorrow.get("risk_point", ""),
                }
            elif isinstance(tomorrow, str):
                # 兼容旧格式（纯文本）
                result["tomorrow_outlook"]["key_observation"] = tomorrow
            
            # 根据风险评分计算置信度
            risk = result["risk_score"]
            result["confidence"] = max(0.3, min(0.9, 1 - risk / 100))
            result["confidence_score"]["overall"] = int(result["confidence"] * 100)
            
            self.logger.info(
                f"[analyze_market] trace_id={trace_id} | 解析成功: "
                f"cycle={result['cycle']}({raw_cycle}), signal={result['signal']}, "
                f"risk={result['risk_score']}, position={result['position_advice']}, "
                f"tomorrow={result['tomorrow_outlook'].get('direction', 'N/A')}"
            )
            
        except (json.JSONDecodeError, Exception) as e:
            self.logger.warning(f"[analyze_market] trace_id={trace_id} | JSON 解析失败: {e}")
            # 保留原始响应
            result["summary"] = response
        
        # ==================== 6. 逻辑对冲校验 ====================
        result = self._hedge_check_market_result(result, trace_id)
        
        if progress_callback:
            await progress_callback(100, "分析完成")
        
        self.logger.info(f"[analyze_market] trace_id={trace_id} | 大盘分析完成")
        
        return result
    
    def _hedge_check_market_result(self, result: Dict[str, Any], trace_id: str) -> Dict[str, Any]:
        """
        逻辑对冲校验：确保 AI 输出的信号与风险/周期逻辑自洽
        
        规则：
        1. 风险与信号对冲：risk_score > 80 且 signal == 'buy' → 强制改为 hold
        2. 仓位与周期对冲：cycle 为"退潮冰点期"但仓位建议超过 40% → 记录 warning
        """
        hedged = False
        warnings = []
        
        # 规则 1：风险与信号对冲
        risk_score = result.get("risk_score", 50)
        signal = result.get("signal", "hold")
        
        if risk_score > 80 and signal == SignalType.BUY.value:
            result["signal"] = SignalType.HOLD.value
            result["summary"] = (
                result.get("summary", "") + 
                "\n\n（注：系统检测到极端风险，自动对买入信号进行了防守性修正）"
            )
            hedged = True
            self.logger.warning(
                f"[analyze_market] trace_id={trace_id} | ⚠️ 对冲触发: "
                f"risk_score={risk_score} > 80 但 signal=buy，已修正为 hold"
            )
        
        # 规则 2：仓位与周期对冲
        cycle = result.get("cycle", "")
        cycle_name = result.get("cycle_name", "")
        position_advice = result.get("position_advice", "")
        
        if cycle == "ice_point" or cycle_name == "退潮冰点期":
            # 尝试从 position_advice 中提取数字
            import re
            numbers = re.findall(r'(\d+)', position_advice)
            if numbers:
                max_position = max(int(n) for n in numbers)
                if max_position > 40:
                    warnings.append(
                        f"周期对冲警告：当前处于【退潮冰点期】，"
                        f"但仓位建议为 {position_advice}（超过40%），请谨慎操作"
                    )
                    self.logger.warning(
                        f"[analyze_market] trace_id={trace_id} | ⚠️ 仓位对冲警告: "
                        f"cycle=退潮冰点期 但 position_advice={position_advice} 超过 40%"
                    )
        
        # 将警告添加到结果
        if warnings:
            result["hedge_warnings"] = warnings
            # 同时追加到 summary
            result["summary"] = result.get("summary", "") + "\n\n⚠️ 风险提示：\n" + "\n".join(warnings)
        
        if hedged or warnings:
            self.logger.info(
                f"[analyze_market] trace_id={trace_id} | 对冲校验完成: "
                f"hedged={hedged}, warnings={len(warnings)}"
            )
        
        return result
    
    async def custom_query(
        self,
        query: str,
        task_id: str,
        progress_callback: Optional[Callable] = None,
    ) -> Dict[str, Any]:
        """
        意图驱动型 RAG 查询 (V2.0)
        
        流程:
        1. 意图拆解与查询重写
        2. 多路数据并行检索 (Milvus + MongoDB)
        3. 动态上下文构建
        4. 逻辑对冲回答
        """
        trace_id = task_id[:8]
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        self.logger.info(f"[custom_query] trace_id={trace_id} | 开始处理查询: {query[:50]}...")
        
        # ==================== 1. 意图拆解 ====================
        
        if progress_callback:
            await progress_callback(10, "理解问题意图...")
        
        intent = await self._parse_query_intent(query, trace_id)
        
        self.logger.info(
            f"[custom_query] trace_id={trace_id} | 意图解析: "
            f"type={intent.get('intent_type')}, "
            f"ts_codes={intent.get('ts_codes')}, "
            f"need_market={intent.get('need_market_stats')}"
        )
        
        # ==================== 2. 多路并行检索 ====================
        
        if progress_callback:
            await progress_callback(30, "检索相关信息...")
        
        # 准备检索任务
        search_tasks = []
        
        # 2.1 文本路：Milvus 向量检索
        search_queries = intent.get("search_queries", [query])
        ts_codes = intent.get("ts_codes", [])
        
        if search_queries:
            # 生成查询向量
            embeddings = await llm_manager.embedding(search_queries[:2])
            for emb in embeddings:
                # 如果有股票代码，使用过滤检索
                search_tasks.append(self._search_milvus(emb, trace_id, ts_codes=ts_codes if ts_codes else None))
        
        # 2.2 统计路：大盘数据
        market_stats_task = None
        if intent.get("need_market_stats") or intent.get("intent_type") == "market":
            market_stats_task = self._get_market_stats(trace_id)
            search_tasks.append(market_stats_task)
        
        # 2.3 个股路：股票行情
        stock_snapshot_task = None
        ts_codes = intent.get("ts_codes", [])
        if ts_codes and (intent.get("need_stock_snapshot") or intent.get("intent_type") == "stock"):
            stock_snapshot_task = self._get_stock_snapshots(ts_codes, trace_id)
            search_tasks.append(stock_snapshot_task)
        
        # 并行执行所有检索任务
        results = await asyncio.gather(*search_tasks, return_exceptions=True)
        
        # ==================== 3. 解析检索结果 ====================
        
        if progress_callback:
            await progress_callback(60, "整理检索结果...")
        
        reports = []
        news = []
        market_stats = ""
        stock_snapshot = ""
        
        for result in results:
            if isinstance(result, Exception):
                self.logger.warning(f"[custom_query] trace_id={trace_id} | 检索异常: {result}")
                continue
            
            if isinstance(result, dict):
                if result.get("type") == "milvus":
                    reports.extend(result.get("reports", []))
                    news.extend(result.get("news", []))
                elif result.get("type") == "market_stats":
                    market_stats = result.get("content", "")
                elif result.get("type") == "stock_snapshot":
                    stock_snapshot = result.get("content", "")
        
        self.logger.info(
            f"[custom_query] trace_id={trace_id} | 检索完成: "
            f"reports={len(reports)}, news={len(news)}, "
            f"has_market={bool(market_stats)}, has_stock={bool(stock_snapshot)}"
        )
        
        # ==================== 4. 构建上下文并生成回答 ====================
        
        if progress_callback:
            await progress_callback(80, "生成回答...")
        
        # 4.1 构建上下文
        from jinja2 import Template
        
        config = prompt_manager.get_config("stock_analysis/query")
        context_template = config.get("context_template", "")
        
        context_params = {
            "reports": reports[:5],  # 最多5条研报
            "news": news[:5],        # 最多5条新闻
            "market_stats": market_stats,
            "stock_snapshot": stock_snapshot,
        }
        
        rendered_context = ""
        if context_template and any([reports, news, market_stats, stock_snapshot]):
            rendered_context = Template(context_template).render(**context_params)
        
        # 4.2 获取系统提示词
        system_prompt = prompt_manager.get_system_prompt(
            "stock_analysis/query",
            current_time=current_time,
        )
        
        # 4.3 构建消息
        messages = [{"role": "system", "content": system_prompt}]
        
        if rendered_context:
            messages.append({"role": "system", "content": rendered_context})
        
        messages.append({"role": "user", "content": query})
        
        # 4.4 调用 LLM
        response = await llm_manager.chat(messages)
        
        if progress_callback:
            await progress_callback(100, "完成")
        
        self.logger.info(f"[custom_query] trace_id={trace_id} | 查询完成")
        
        return {
            "signal": SignalType.HOLD.value,
            "confidence": 0.6,
            "summary": response,
            "query": query,
            "intent": intent,
            "context_sources": {
                "reports_count": len(reports),
                "news_count": len(news),
                "has_market_stats": bool(market_stats),
                "has_stock_snapshot": bool(stock_snapshot),
            },
            "analysis_conflicts": [],
            "conflict_summary": "",
            "confidence_score": {"overall": 60},
        }
    
    async def _parse_query_intent(self, query: str, trace_id: str) -> Dict[str, Any]:
        """
        意图拆解：解析用户查询意图
        
        Returns:
            {
                "search_queries": ["语义短句1", "语义短句2"],
                "ts_codes": ["000001.SZ"],
                "intent_type": "stock|market|news|general",
                "need_market_stats": True/False,
                "need_stock_snapshot": True/False,
            }
        """
        config = prompt_manager.get_config("stock_analysis/query")
        intent_prompt = config.get("intent_parse_prompt", "")
        
        if not intent_prompt:
            # 使用简单的规则解析
            return self._simple_intent_parse(query)
        
        from jinja2 import Template
        rendered_prompt = Template(intent_prompt).render(query=query)
        
        try:
            response = await llm_manager.chat([
                {"role": "user", "content": rendered_prompt}
            ])
            
            # 提取 JSON
            json_match = re.search(r'```json\s*(.*?)\s*```', response, re.DOTALL)
            if json_match:
                intent = json.loads(json_match.group(1))
            else:
                intent = json.loads(response)
            
            return intent
            
        except Exception as e:
            self.logger.warning(f"[custom_query] trace_id={trace_id} | 意图解析失败: {e}")
            return self._simple_intent_parse(query)
    
    def _simple_intent_parse(self, query: str) -> Dict[str, Any]:
        """简单规则意图解析（备用方案）"""
        intent = {
            "search_queries": [query],
            "ts_codes": [],
            "intent_type": "general",
            "need_market_stats": False,
            "need_stock_snapshot": False,
            "time_reference": "none",
        }
        
        # 检测大盘相关关键词
        market_keywords = ["大盘", "市场", "行情", "指数", "上证", "深证", "创业板", "情绪", "涨跌"]
        if any(kw in query for kw in market_keywords):
            intent["intent_type"] = "market"
            intent["need_market_stats"] = True
        
        # 检测股票代码
        import re
        code_pattern = r'(\d{6})(?:\.SH|\.SZ)?'
        codes = re.findall(code_pattern, query)
        if codes:
            intent["intent_type"] = "stock"
            intent["need_stock_snapshot"] = True
            # 简单判断交易所
            for code in codes:
                if code.startswith(("6", "9")):
                    intent["ts_codes"].append(f"{code}.SH")
                else:
                    intent["ts_codes"].append(f"{code}.SZ")
        
        # 检测时间关键词
        if any(kw in query for kw in ["今天", "今日", "现在", "当前"]):
            intent["time_reference"] = "today"
        elif any(kw in query for kw in ["最近", "近期", "这几天"]):
            intent["time_reference"] = "recent"
        
        return intent
    
    async def _search_milvus(
        self,
        query_vector: List[float],
        trace_id: str,
        ts_codes: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Milvus 向量检索（增强版，支持股票代码过滤）
        
        Args:
            query_vector: 查询向量
            trace_id: 追踪 ID
            ts_codes: 股票代码列表（可选过滤条件）
        """
        try:
            # 使用增强版搜索（支持过滤）
            if ts_codes:
                reports = await milvus_manager.search_reports_filtered(
                    query_vector, top_k=5, ts_codes=ts_codes
                )
                snippets = await milvus_manager.search_news_filtered(
                    query_vector, top_k=5, ts_codes=ts_codes
                )
            else:
                reports = await milvus_manager.search_reports(query_vector, top_k=5)
                snippets = await milvus_manager.search_market_snippets(query_vector, top_k=5)
            
            return {
                "type": "milvus",
                "reports": [r.get("content", "")[:400] for r in reports if r.get("content")],
                "news": [s.get("content", "")[:400] for s in snippets if s.get("content")],
            }
        except Exception as e:
            self.logger.error(f"[custom_query] trace_id={trace_id} | Milvus 检索失败: {e}")
            return {"type": "milvus", "reports": [], "news": []}
    
    async def _get_market_stats(self, trace_id: str) -> Dict[str, Any]:
        """获取最新大盘统计"""
        try:
            # 获取最新的 daily_stats
            stats = await mongo_manager.find_one(
                "daily_stats",
                {},
                sort=[("trade_date", -1)],
            )
            
            # 获取最新的 market_analysis
            analysis = await mongo_manager.find_one(
                "market_analysis",
                {},
                sort=[("trade_date", -1)],
            )
            
            if not stats:
                return {"type": "market_stats", "content": ""}
            
            trade_date = stats.get("trade_date", "")
            sentiment = analysis.get("sentiment_score_ema", analysis.get("sentiment_score", 0)) if analysis else 0
            cycle = analysis.get("cycle_name", analysis.get("cycle", "未知")) if analysis else "未知"
            
            content = (
                f"日期: {trade_date}\n"
                f"情绪评分: {sentiment:.0f}/100\n"
                f"周期定位: {cycle}\n"
                f"涨家数: {stats.get('up_count', 0)}, 跌家数: {stats.get('down_count', 0)}\n"
                f"涨停: {stats.get('limit_up_count', 0)}, 跌停: {stats.get('limit_down_count', 0)}\n"
                f"最高连板: {stats.get('max_limit_height', 0)}板"
            )
            
            return {"type": "market_stats", "content": content}
            
        except Exception as e:
            self.logger.error(f"[custom_query] trace_id={trace_id} | 获取大盘统计失败: {e}")
            return {"type": "market_stats", "content": ""}
    
    async def _get_stock_snapshots(self, ts_codes: List[str], trace_id: str) -> Dict[str, Any]:
        """获取个股行情快照（最近3日）"""
        try:
            snapshots = []
            
            for ts_code in ts_codes[:3]:  # 最多3只股票
                # 获取基本信息
                basic = await mongo_manager.find_one(
                    "stock_basic",
                    {"ts_code": ts_code},
                    projection={"name": 1, "industry": 1},
                )
                
                # 获取最近3日行情
                daily = await mongo_manager.find_many(
                    "stock_daily",
                    {"ts_code": ts_code},
                    sort=[("trade_date", -1)],
                    limit=3,
                )
                
                if daily:
                    name = basic.get("name", ts_code) if basic else ts_code
                    industry = basic.get("industry", "") if basic else ""
                    
                    snapshot_lines = [f"{name}({ts_code}) {industry}:"]
                    for d in daily:
                        snapshot_lines.append(
                            f"  {d.get('trade_date')}: "
                            f"收={d.get('close', 0):.2f}, "
                            f"涨跌={d.get('pct_chg', 0):+.2f}%, "
                            f"换手={d.get('turnover_rate', 0):.1f}%"
                        )
                    snapshots.append("\n".join(snapshot_lines))
            
            return {"type": "stock_snapshot", "content": "\n\n".join(snapshots)}
            
        except Exception as e:
            self.logger.error(f"[custom_query] trace_id={trace_id} | 获取个股快照失败: {e}")
            return {"type": "stock_snapshot", "content": ""}
