"""
市场分析 API

提供大盘行情分析数据接口

数据源策略:
- 18:00 前 (交易时段/盘后整理中): 使用 Redis 中的实时数据
- 18:00 后 (DataSync 完成后): 使用 MongoDB 中的历史数据
- 指数数据从 index_daily 表获取
- 涨跌统计从 daily_stats 表获取
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta, date

from core.managers import mongo_manager, theme_manager, redis_manager
from .auth import require_admin, CurrentUser

router = APIRouter(prefix="/market", tags=["Market Analysis"])

# 数据源切换时间点 (18:00)
DATA_SOURCE_SWITCH_HOUR = 18


def _safe_float(val, default: float = 0.0) -> float:
    """安全转换为浮点数"""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _should_use_realtime_data() -> bool:
    """
    判断是否应该使用实时数据 (Redis)
    
    18:00 前使用 Redis 实时数据
    18:00 后使用 MongoDB 历史数据
    """
    now = datetime.now()
    return now.hour < DATA_SOURCE_SWITCH_HOUR


async def _get_index_data_from_mongodb(trade_date: Optional[str] = None) -> Dict[str, Any]:
    """
    从 MongoDB 的 index_daily 表获取指数数据
    
    Args:
        trade_date: 交易日期，不传则取最新
        
    Returns:
        {"sh_index": ..., "sh_change": ..., "sz_index": ..., ...}
    """
    result = {
        "sh_index": 0, "sh_change": 0,
        "sz_index": 0, "sz_change": 0,
        "cyb_index": 0, "cyb_change": 0,
    }
    
    # 指数代码映射
    index_map = {
        "000001.SH": ("sh_index", "sh_change"),
        "399001.SZ": ("sz_index", "sz_change"),
        "399006.SZ": ("cyb_index", "cyb_change"),
    }
    
    for ts_code, (index_key, change_key) in index_map.items():
        query = {"ts_code": ts_code}
        if trade_date:
            query["trade_date"] = trade_date
        
        index_data = await mongo_manager.find_one(
            "index_daily",
            query,
            sort=[("trade_date", -1)],
        )
        
        if index_data:
            result[index_key] = _safe_float(index_data.get("close", 0))
            result[change_key] = _safe_float(index_data.get("pct_chg", 0))
    
    return result


@router.get("/overview")
async def get_market_overview(
    trade_date: Optional[str] = None,
) -> Dict[str, Any]:
    """
    获取大盘概览数据
    
    数据源策略:
    - 18:00 前: 使用 Redis 中的实时数据 (Listener 节点更新)
    - 18:00 后: 使用 MongoDB 中的历史数据
    - 指数数据: index_daily 表
    - 涨跌统计: daily_stats 表
    
    Args:
        trade_date: 交易日期，不传则取最新
    
    Returns:
        大盘指数、涨跌统计、资金流向等概览数据
    """
    # 如果指定了历史日期，直接从 MongoDB 获取
    today_str = date.today().strftime("%Y%m%d")
    use_realtime = _should_use_realtime_data() and (not trade_date or trade_date == today_str)
    
    # 尝试从 Redis 获取实时数据 (18:00 前)
    if use_realtime:
        realtime_data = await redis_manager.get_realtime_market_data()
        if realtime_data:
            # 获取热门板块（仍从 MongoDB 获取）
            hot_sectors = await _get_hot_sectors(today_str)
            
            return {
                "trade_date": today_str,
                "sh_index": _safe_float(realtime_data.get("sh_index", 0)),
                "sh_change": _safe_float(realtime_data.get("sh_change", 0)),
                "sz_index": _safe_float(realtime_data.get("sz_index", 0)),
                "sz_change": _safe_float(realtime_data.get("sz_change", 0)),
                "cyb_index": _safe_float(realtime_data.get("cyb_index", 0)),
                "cyb_change": _safe_float(realtime_data.get("cyb_change", 0)),
                "up_count": realtime_data.get("up_count", 0),
                "down_count": realtime_data.get("down_count", 0),
                "flat_count": realtime_data.get("flat_count", 0),
                "limit_up": realtime_data.get("limit_up", 0),
                "limit_down": realtime_data.get("limit_down", 0),
                "total_amount": 0,  # 实时数据暂无
                "north_money": 0,   # 实时数据暂无
                "hot_sectors": hot_sectors,
                "data_source": "realtime",
                "update_time": realtime_data.get("update_time", ""),
            }
    
    # 从 MongoDB 获取历史数据 (18:00 后或无实时数据)
    return await _get_market_overview_from_mongodb(trade_date)


async def _get_hot_sectors(trade_date: str) -> List[str]:
    """获取热门板块"""
    hot_sectors_data = await mongo_manager.find_many(
        "sector_ranking",
        {"trade_date": trade_date, "ranking_type": "industry_top"},
        sort=[("rank", 1)],
        limit=5,
        projection={"name": 1, "_id": 0},
    )
    return [s.get("name", "") for s in hot_sectors_data if s.get("name")]


async def _get_market_overview_from_mongodb(trade_date: Optional[str] = None) -> Dict[str, Any]:
    """
    从 MongoDB 获取大盘概览数据
    
    指数数据从 index_daily 表获取
    涨跌统计从 daily_stats 表获取
    """
    # 构建查询条件
    query = {}
    if trade_date:
        query["trade_date"] = trade_date
    
    # 获取 daily_stats (涨跌统计)
    stats = await mongo_manager.find_one(
        "daily_stats",
        query,
        sort=[("trade_date", -1)],
    )
    
    # 确定交易日期
    actual_trade_date = stats.get("trade_date", "") if stats else trade_date or ""
    
    # 获取指数数据 (从 index_daily 表)
    index_data = await _get_index_data_from_mongodb(actual_trade_date)
    
    if not stats:
        # 只有指数数据，没有统计数据
        return {
            "trade_date": actual_trade_date,
            **index_data,
            "up_count": 0,
            "down_count": 0,
            "flat_count": 0,
            "limit_up": 0,
            "limit_down": 0,
            "total_amount": 0,
            "north_money": 0,
            "hot_sectors": [],
            "data_source": "mongodb",
        }
    
    # 获取热门板块
    hot_sectors = await _get_hot_sectors(stats.get("trade_date", ""))
    
    # 构建响应
    return {
        "trade_date": stats.get("trade_date", ""),
        **index_data,  # 指数数据从 index_daily 表获取
        "up_count": stats.get("up_count", 0),
        "down_count": stats.get("down_count", 0),
        "flat_count": stats.get("flat_count", 0),
        "limit_up": stats.get("limit_up_count", 0),
        "limit_down": stats.get("limit_down_count", 0),
        "total_amount": _safe_float(stats.get("total_amount", 0)),
        "north_money": _safe_float(stats.get("north_money", 0)),
        "hot_sectors": hot_sectors,
        "data_source": "mongodb",
    }


@router.get("/latest")
async def get_latest_market_data() -> Dict[str, Any]:
    """
    获取最新市场分析数据
    
    Returns:
        包含评分、周期、统计数据的完整市场分析
    """
    # 获取最新的 daily_stats
    latest_stats = await mongo_manager.find_one(
        "daily_stats",
        {},
        sort=[("trade_date", -1)],
    )
    
    if not latest_stats:
        raise HTTPException(status_code=404, detail="No market data available")
    
    trade_date = latest_stats.get("trade_date", "")
    
    # 获取最新的 market_analysis
    latest_analysis = await mongo_manager.find_one(
        "market_analysis",
        {"trade_date": trade_date},
    )
    
    # 构建响应 - 使用 EMA 平滑后的情绪分数保持一致
    sentiment_ema = latest_analysis.get("sentiment_score_ema") if latest_analysis else None
    sentiment_raw = latest_analysis.get("sentiment_score", 0) if latest_analysis else 0
    sentiment = sentiment_ema if sentiment_ema is not None else sentiment_raw
    
    # total_amount 存储单位是千元
    total_amount = _safe_float(latest_stats.get("total_amount", 0))
    
    response = {
        "trade_date": trade_date,
        "scores": {
            "sentiment": sentiment,
            "strength": latest_analysis.get("strength_score", 0) if latest_analysis else 0,
        },
        "cycle": latest_analysis.get("cycle", "unknown") if latest_analysis else "unknown",
        "cycle_name": latest_analysis.get("cycle_name", "") if latest_analysis else "",
        "cycle_reason": latest_analysis.get("cycle_reason", "") if latest_analysis else "",
        "stats": {
            "up_count": latest_stats.get("up_count", 0),
            "down_count": latest_stats.get("down_count", 0),
            "flat_count": latest_stats.get("flat_count", 0),
            "total_stocks": latest_stats.get("total_stocks", 0),
            "up_ratio": latest_stats.get("up_ratio", 0),
            "down_ratio": latest_stats.get("down_ratio", 0),
            "limit_up_count": latest_stats.get("limit_up_count", 0),
            "limit_down_count": latest_stats.get("limit_down_count", 0),
            "broken_limit_count": latest_stats.get("broken_limit_count", 0),
            "max_limit_height": latest_stats.get("max_limit_height", 0),
            "limit_1": latest_stats.get("limit_1", 0),
            "limit_2": latest_stats.get("limit_2", 0),
            "limit_3": latest_stats.get("limit_3", 0),
            "limit_4": latest_stats.get("limit_4", 0),
            "limit_5": latest_stats.get("limit_5", 0),
            "limit_6_plus": latest_stats.get("limit_6_plus", 0),
            "total_limit_up": latest_stats.get("total_limit_up", 0),
            "total_amount": total_amount,  # 保持千元单位，前端处理显示
            "sh_amount": _safe_float(latest_stats.get("sh_amount", 0)),
            "sz_amount": _safe_float(latest_stats.get("sz_amount", 0)),
            "north_money": _safe_float(latest_stats.get("north_money", 0)),  # 保持原单位
            "hgt": latest_stats.get("hgt"),
            "sgt": latest_stats.get("sgt"),
        },
    }
    
    return response


@router.get("/history")
async def get_market_history(
    days: int = Query(default=30, ge=1, le=90, description="历史天数"),
) -> Dict[str, Any]:
    """
    获取市场历史数据 (用于趋势图)
    
    Args:
        days: 查询天数 (默认30天)
    
    Returns:
        历史统计和分析数据列表
    """
    # 获取历史 daily_stats
    stats_list = await mongo_manager.find_many(
        "daily_stats",
        {},
        sort=[("trade_date", -1)],
        limit=days,
    )
    
    if not stats_list:
        return {"history": []}
    
    # 获取对应的 market_analysis
    trade_dates = [s.get("trade_date") for s in stats_list]
    analysis_list = await mongo_manager.find_many(
        "market_analysis",
        {"trade_date": {"$in": trade_dates}},
    )
    
    # 构建分析数据映射
    analysis_map = {a.get("trade_date"): a for a in analysis_list}
    
    # 组装历史数据
    history = []
    for stats in stats_list:
        trade_date = stats.get("trade_date", "")
        analysis = analysis_map.get(trade_date, {})
        
        history.append({
            "trade_date": trade_date,
            "sentiment_score": analysis.get("sentiment_score", 0),
            "sentiment_score_ema": analysis.get("sentiment_score_ema", analysis.get("sentiment_score", 0)),
            "strength_score": analysis.get("strength_score", 0),
            "strength_diff": analysis.get("strength_diff", 0),
            "v_ratio": analysis.get("v_ratio", 1.0),
            "cycle": analysis.get("cycle", "unknown"),
            "cycle_name": analysis.get("cycle_name", ""),
            "up_count": stats.get("up_count", 0),
            "down_count": stats.get("down_count", 0),
            "up_ratio": stats.get("up_ratio", 0),
            "limit_up_count": stats.get("limit_up_count", 0),
            "limit_down_count": stats.get("limit_down_count", 0),
            "broken_limit_count": stats.get("broken_limit_count", 0),
            "max_limit_height": stats.get("max_limit_height", 0),
            "limit_1": stats.get("limit_1", 0),
            "limit_2": stats.get("limit_2", 0),
            "limit_3": stats.get("limit_3", 0),
            "limit_4": stats.get("limit_4", 0),
            "limit_5": stats.get("limit_5", 0),
            "limit_6_plus": stats.get("limit_6_plus", 0),
            "total_amount": _safe_float(stats.get("total_amount", 0)),  # 保持千元单位
            "north_money": _safe_float(stats.get("north_money", 0)),  # 保持百万元单位
        })
    
    # 按日期正序排列 (图表需要从早到晚)
    history.reverse()
    
    return {"history": history}


@router.get("/sector-ranking")
async def get_sector_ranking(
    trade_date: Optional[str] = None,
    ranking_type: str = Query(default="industry_top", description="排名类型: industry_top, concept_top"),
    days: int = Query(default=1, ge=1, le=30, description="获取天数"),
) -> Dict[str, Any]:
    """
    获取板块排名数据（从预计算的 sector_ranking 表直接读取）
    
    Args:
        trade_date: 交易日期 (不传则取最新)
        ranking_type: 排名类型 (industry_top, concept_top)
        days: 获取天数 (默认1天，最多30天)
    
    Returns:
        板块排名列表
    """
    # 如果没有指定日期，获取最新日期
    if not trade_date:
        latest = await mongo_manager.find_one(
            "sector_ranking",
            {"ranking_type": ranking_type},
            sort=[("trade_date", -1)],
            projection={"trade_date": 1},
        )
        if latest:
            trade_date = latest.get("trade_date")
        else:
            return {"rankings": [], "history": []}
    
    # 直接从 sector_ranking 表获取当日预排序数据
    day_data = await mongo_manager.find_many(
        "sector_ranking",
        {"trade_date": trade_date, "ranking_type": ranking_type},
        projection={"rank": 1, "ts_code": 1, "name": 1, "pct_change": 1, "net_amount": 1, "lead_stock": 1, "_id": 0},
        sort=[("rank", 1)],  # 按排名升序
    )
    
    # 直接返回预排序的数据
    result = []
    for item in day_data:
        result.append({
            "rank": item.get("rank"),
            "ts_code": item.get("ts_code"),
            "name": item.get("name") or item.get("ts_code", "未知"),
            "pct_change": _safe_float(item.get("pct_change")),
            "net_amount": _safe_float(item.get("net_amount")),
            "lead_stock": item.get("lead_stock", ""),
        })
    
    # 如果需要获取多天历史数据
    history = []
    if days > 1:
        # 获取最近 N 天的交易日（从 sector_ranking 表）
        all_dates = await mongo_manager.find_many(
            "sector_ranking",
            {"ranking_type": ranking_type},
            projection={"trade_date": 1, "_id": 0},
            sort=[("trade_date", -1)],
        )
        # 去重并排序
        unique_dates = sorted(list(set(d.get("trade_date") for d in all_dates if d.get("trade_date"))), reverse=True)[:days]
        
        for dt in unique_dates:
            dt_data = await mongo_manager.find_many(
                "sector_ranking",
                {"trade_date": dt, "ranking_type": ranking_type},
                projection={"rank": 1, "ts_code": 1, "name": 1, "pct_change": 1, "lead_stock": 1, "_id": 0},
                sort=[("rank", 1)],
            )
            history.append({
                "trade_date": dt,
                "rankings": [{
                    "rank": r.get("rank"),
                    "ts_code": r.get("ts_code"),
                    "name": r.get("name") or r.get("ts_code", "未知"),
                    "pct_change": _safe_float(r.get("pct_change")),
                    "lead_stock": r.get("lead_stock", ""),
                } for r in dt_data]
            })
    
    return {"trade_date": trade_date, "rankings": result, "history": history}


@router.get("/stats-table")
async def get_stats_table(
    days: int = Query(default=15, ge=1, le=30, description="天数"),
) -> Dict[str, Any]:
    """
    获取统计表格数据 (用于顶部表格展示)
    
    Args:
        days: 查询天数
    
    Returns:
        表格数据
    """
    stats_list = await mongo_manager.find_many(
        "daily_stats",
        {},
        sort=[("trade_date", -1)],
        limit=days,
    )
    
    # 获取对应的 market_analysis
    trade_dates = [s.get("trade_date") for s in stats_list]
    analysis_list = await mongo_manager.find_many(
        "market_analysis",
        {"trade_date": {"$in": trade_dates}},
    )
    analysis_map = {a.get("trade_date"): a for a in analysis_list}
    
    table_data = []
    for stats in stats_list:
        trade_date = stats.get("trade_date", "")
        analysis = analysis_map.get(trade_date, {})
        
        table_data.append({
            "trade_date": trade_date,
            "up_count": stats.get("up_count", 0),
            "down_count": stats.get("down_count", 0),
            "limit_up_count": stats.get("limit_up_count", 0),
            "limit_down_count": stats.get("limit_down_count", 0),
            "limit_1": stats.get("limit_1", 0),
            "limit_2": stats.get("limit_2", 0),
            "limit_3": stats.get("limit_3", 0),
            "limit_4": stats.get("limit_4", 0),
            "limit_5": stats.get("limit_5", 0),
            "limit_6_plus": stats.get("limit_6_plus", 0),
            "max_limit_height": stats.get("max_limit_height", 0),
            "strength_score": analysis.get("strength_score"),
            "sentiment_score": analysis.get("sentiment_score_ema", analysis.get("sentiment_score")),
            "cycle": analysis.get("cycle"),
        })
    
    return {"data": table_data}


@router.get("/theme-radar")
async def get_theme_radar(
    trade_date: Optional[str] = None,
) -> Dict[str, Any]:
    """
    获取主线雷达数据
    
    展示当前共识度最高的3个板块及其5日位次变化
    """
    # 如果没有指定日期，获取最新日期
    if not trade_date:
        latest = await mongo_manager.find_one(
            "sector_ranking",
            {},
            sort=[("trade_date", -1)],
            projection={"trade_date": 1},
        )
        if latest:
            trade_date = latest.get("trade_date")
        else:
            return {"radar": [], "rotation": []}
    
    # 初始化 theme_manager
    await theme_manager.initialize()
    
    # 获取雷达数据
    radar_data = await theme_manager.get_theme_radar(mongo_manager, trade_date)
    
    return radar_data


@router.get("/theme-analysis")
async def get_theme_analysis(
    trade_date: Optional[str] = None,
    days: int = Query(default=5, ge=1, le=10, description="回看天数"),
) -> Dict[str, Any]:
    """
    获取完整的板块主线分析
    
    包括：主线板块、强势关注、轮动分析
    """
    # 如果没有指定日期，获取最新日期
    if not trade_date:
        latest = await mongo_manager.find_one(
            "sector_ranking",
            {},
            sort=[("trade_date", -1)],
            projection={"trade_date": 1},
        )
        if latest:
            trade_date = latest.get("trade_date")
        else:
            return {"main_themes": [], "strong_focus": [], "rotation_analysis": []}
    
    # 初始化 theme_manager
    await theme_manager.initialize()
    
    # 获取分析数据
    analysis = await theme_manager.analyze_themes(mongo_manager, trade_date, days)
    
    return analysis


@router.get("/sector-timeline")
async def get_sector_timeline(
    ranking_type: str = Query(default="industry_top", description="排名类型"),
    days: int = Query(default=10, ge=1, le=30, description="天数"),
) -> Dict[str, Any]:
    """
    获取板块位次时间线数据
    
    用于展示板块在过去N天的位次迁移
    """
    # 获取最近N天的排名数据
    all_dates = await mongo_manager.find_many(
        "sector_ranking",
        {"ranking_type": ranking_type},
        projection={"trade_date": 1, "_id": 0},
    )
    unique_dates = sorted(
        list(set(d.get("trade_date") for d in all_dates if d.get("trade_date"))),
        reverse=True
    )[:days]
    
    if not unique_dates:
        return {"dates": [], "sectors": []}
    
    # 获取这些日期的排名数据
    rankings = await mongo_manager.find_many(
        "sector_ranking",
        {"trade_date": {"$in": unique_dates}, "ranking_type": ranking_type},
        sort=[("trade_date", -1), ("rank", 1)],
    )
    
    # 构建时间线数据
    # 按板块名称分组
    sector_data = {}
    for r in rankings:
        name = r.get("name", "")
        if not name:
            continue
        
        if name not in sector_data:
            sector_data[name] = {
                "name": name,
                "ts_code": r.get("ts_code"),
                "ranks": {},
            }
        sector_data[name]["ranks"][r.get("trade_date")] = {
            "rank": r.get("rank"),
            "pct_change": _safe_float(r.get("pct_change")),
            "lead_stock": r.get("lead_stock", ""),
        }
    
    # 计算每个板块的出现次数和平均排名
    for name, data in sector_data.items():
        appearances = len(data["ranks"])
        avg_rank = sum(r["rank"] for r in data["ranks"].values()) / max(1, appearances)
        data["appearances"] = appearances
        data["avg_rank"] = round(avg_rank, 1)
    
    # 按出现次数和平均排名排序
    sorted_sectors = sorted(
        sector_data.values(),
        key=lambda x: (-x["appearances"], x["avg_rank"])
    )
    
    return {
        "dates": sorted(unique_dates, reverse=True),
        "sectors": sorted_sectors[:20],  # 返回前20个板块
    }


@router.get("/sector-scores")
async def get_sector_scores(
    trade_date: Optional[str] = None,
    days: int = Query(default=20, ge=5, le=30, description="回看天数"),
) -> Dict[str, Any]:
    """
    获取板块长周期评分
    
    基于 MA20 计算板块综合评分，识别主线/异动/退潮
    """
    # 如果没有指定日期，获取最新日期
    if not trade_date:
        latest = await mongo_manager.find_one(
            "sector_ranking",
            {},
            sort=[("trade_date", -1)],
            projection={"trade_date": 1},
        )
        if latest:
            trade_date = latest.get("trade_date")
        else:
            return {"sectors": [], "main_themes": [], "anomalies": [], "fading": []}
    
    # 初始化 theme_manager
    await theme_manager.initialize()
    
    # 获取评分数据
    result = await theme_manager.calculate_sector_scores(mongo_manager, trade_date, days)
    
    return result


@router.get("/sector-scatter")
async def get_sector_scatter(
    trade_date: Optional[str] = None,
) -> Dict[str, Any]:
    """
    获取板块散点图数据
    
    横轴: 20日共识度
    纵轴: 短期强度
    """
    # 如果没有指定日期，获取最新日期
    if not trade_date:
        latest = await mongo_manager.find_one(
            "sector_ranking",
            {},
            sort=[("trade_date", -1)],
            projection={"trade_date": 1},
        )
        if latest:
            trade_date = latest.get("trade_date")
        else:
            return {"scatter": []}
    
    # 初始化 theme_manager
    await theme_manager.initialize()
    
    # 获取散点图数据
    result = await theme_manager.get_scatter_data(mongo_manager, trade_date)
    
    return result


# ==================== 热点新闻 ====================


@router.get("/hot_news")
async def get_hot_news(
    source: Optional[str] = Query(default=None, description="来源过滤 (如 baidu, weibo)"),
    limit: int = Query(default=50, ge=1, le=200, description="返回条数"),
) -> Dict[str, Any]:
    """
    获取热点新闻 (数据源: Redis)
    
    支持的来源:
    - cls: 财联社
    - xueqiu: 雪球
    - wallstreetcn: 华尔街见闻
    - gelonghui: 格隆汇
    - jin10: 金十数据
    - juejin: 稀土掘金
    - ithome: IT之家
    - 36kr: 36氪
    - github: Github
    - douyin: 抖音
    - bilibili: 哔哩哔哩
    - kaopu: 靠谱新闻
    - thepaper: 澎湃新闻
    
    Args:
        source: 可选的来源过滤
        limit: 返回条数
        
    Returns:
        热点新闻列表
    """
    # 确保 Redis 已初始化
    if not redis_manager.is_initialized:
        await redis_manager.initialize()
    
    if source:
        # 获取指定来源
        data = await redis_manager.get_hot_news(source)
        if not data:
            return {"news": [], "total": 0, "updated_at": ""}
        
        news_list = data.get("news", [])[:limit]
        return {
            "news": news_list,
            "total": len(news_list),
            "updated_at": data.get("updated_at", ""),
        }
    else:
        # 获取所有来源
        all_data = await redis_manager.get_all_hot_news()
        
        result = []
        for source_id, data in all_data.items():
            news = data.get("news", [])
            for item in news:
                item["updated_at"] = data.get("updated_at", "")
            result.extend(news)
        
        # 按热度排序
        result.sort(key=lambda x: x.get("hot", 0), reverse=True)
        result = result[:limit]
        
        return {"news": result, "total": len(result)}


@router.get("/hot_news/sources")
async def get_hot_news_sources() -> Dict[str, Any]:
    """
    获取可用的热点新闻来源列表
    """
    sources = [
        # 金融类
        {"id": "cls", "name": "财联社", "color": "#E53935", "column": "finance"},
        {"id": "xueqiu", "name": "雪球", "color": "#1E88E5", "column": "finance"},
        {"id": "wallstreetcn", "name": "华尔街见闻", "color": "#1976D2", "column": "finance"},
        {"id": "gelonghui", "name": "格隆汇", "color": "#1565C0", "column": "finance"},
        {"id": "jin10", "name": "金十数据", "color": "#0D47A1", "column": "finance"},
        # 科技类
        {"id": "juejin", "name": "稀土掘金", "color": "#1E80FF", "column": "tech"},
        {"id": "ithome", "name": "IT之家", "color": "#D32F2F", "column": "tech"},
        {"id": "36kr", "name": "36氪", "color": "#0080FF", "column": "tech"},
        {"id": "github", "name": "Github", "color": "#24292E", "column": "tech"},
        # 娱乐类
        {"id": "douyin", "name": "抖音", "color": "#212121", "column": "entertainment"},
        {"id": "bilibili", "name": "哔哩哔哩", "color": "#00A1D6", "column": "entertainment"},
        # 综合/世界
        {"id": "kaopu", "name": "靠谱新闻", "color": "#607D8B", "column": "world"},
        {"id": "thepaper", "name": "澎湃新闻", "color": "#455A64", "column": "world"},
    ]
    return {"sources": sources}


@router.post("/hot_news/refresh")
async def refresh_hot_news(
    source: Optional[str] = Query(default=None, description="指定来源刷新，不传则刷新全部"),
    admin: CurrentUser = Depends(require_admin),  # 需要管理员权限
) -> Dict[str, Any]:
    """
    手动触发热点新闻刷新（管理员专用）
    
    通过 RPC 调用 DataSync 节点的采集器抓取热点新闻数据。
    
    Args:
        source: 可选的来源过滤，不传则刷新全部
            - cls: 财联社
            - xueqiu: 雪球
            - wallstreetcn: 华尔街见闻
            - gelonghui: 格隆汇
            - jin10: 金十数据
            - juejin: 稀土掘金
            - ithome: IT之家
            - 36kr: 36氪
            - github: Github
            - douyin: 抖音
            - bilibili: 哔哩哔哩
            - kaopu: 靠谱新闻
            - thepaper: 澎湃新闻
        
    Returns:
        刷新结果:
        - success_count: 成功的来源数
        - fail_count: 失败的来源数
        - total_news: 采集的新闻总数
    """
    import logging
    import uuid
    from core.rpc import RPCClient
    
    logger = logging.getLogger("api.market.hot_news")
    trace_id = uuid.uuid4().hex[:16]
    
    logger.info(f"[{trace_id}] Admin {admin.username} triggered refresh_hot_news, source={source or 'ALL'}")
    
    try:
        # 通过 RPC 调用 DataSync 节点
        rpc_client = RPCClient()
        
        # 广播给所有 data_sync 节点（通常只有一个）
        results = await rpc_client.broadcast_by_type(
            node_type="data_sync",
            method="refresh_hot_news",
            params={"source": source} if source else {},
            trace_id=trace_id,
            source_node="web",
            timeout=60.0,  # 采集可能较慢，设置 60 秒超时
        )
        
        if not results:
            logger.warning(f"[{trace_id}] No data_sync nodes available")
            return {
                "success": False,
                "error": "No data_sync nodes available",
            }
        
        # 返回第一个节点的结果
        first_result = results[0]
        logger.info(f"[{trace_id}] RPC result: {first_result}")
        
        if first_result.get("success"):
            return first_result.get("result", {})
        else:
            return {
                "success": False,
                "error": first_result.get("error", "Unknown RPC error"),
            }
        
    except Exception as e:
        logger.exception(f"[{trace_id}] refresh_hot_news RPC failed: {e}")
        return {
            "success": False,
            "error": str(e),
        }


@router.get("/hot_news/stats")
async def get_hot_news_stats() -> Dict[str, Any]:
    """
    获取热点新闻统计 (数据源: Redis)
    """
    return await redis_manager.get_hot_news_stats()
