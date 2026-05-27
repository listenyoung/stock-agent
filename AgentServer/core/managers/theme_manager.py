"""
板块主线分析管理器

功能：
1. 连续性指数计算：板块连续N天进入Top10标记为"强势关注"
2. 主线识别：板块内有3板以上标的判定为"当前主线"
3. 轮动分析：跟踪板块位次变化趋势
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
from enum import Enum

from core.managers.base import BaseManager


class ThemeStatus(str, Enum):
    """板块状态"""
    MAIN_THEME = "main_theme"       # 当前主线
    STRONG_FOCUS = "strong_focus"   # 强势关注
    RISING = "rising"               # 上升中
    ROTATING = "rotating"           # 轮动中
    FADING = "fading"               # 衰退中
    NORMAL = "normal"               # 普通


class ThemeManager(BaseManager):
    """
    板块主线分析管理器
    
    核心功能：
    - 计算板块连续性指数
    - 识别当前市场主线
    - 分析板块轮动趋势
    """
    
    def __init__(self):
        super().__init__()
        self._initialized = False
    
    async def initialize(self) -> None:
        """初始化"""
        if self._initialized:
            return
        self.logger.info("ThemeManager initialized")
        self._initialized = True
    
    async def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        return {
            "status": "healthy" if self._initialized else "not_initialized",
            "initialized": self._initialized,
        }
    
    async def shutdown(self) -> None:
        """关闭"""
        self._initialized = False
        self.logger.info("ThemeManager shutdown")
    
    async def analyze_themes(
        self,
        mongo_manager,
        trade_date: str,
        lookback_days: int = 5,
    ) -> Dict[str, Any]:
        """
        分析板块主线
        
        Args:
            mongo_manager: MongoDB管理器
            trade_date: 交易日期
            lookback_days: 回看天数
        
        Returns:
            主线分析结果
        """
        # 1. 获取最近N天的排名数据
        recent_rankings = await self._get_recent_rankings(
            mongo_manager, trade_date, lookback_days
        )
        
        if not recent_rankings:
            return {"main_themes": [], "strong_focus": [], "rotation_analysis": []}
        
        # 2. 计算连续性指数
        continuity_stats = self._calculate_continuity(recent_rankings)
        
        # 3. 获取涨停板数据来判断主线
        limit_data = await self._get_limit_board_data(mongo_manager, trade_date)
        
        # 4. 识别主线
        main_themes = self._identify_main_themes(continuity_stats, limit_data)
        
        # 5. 识别强势关注板块
        strong_focus = self._identify_strong_focus(continuity_stats)
        
        # 6. 分析轮动趋势
        rotation_analysis = self._analyze_rotation(recent_rankings)
        
        return {
            "trade_date": trade_date,
            "main_themes": main_themes,
            "strong_focus": strong_focus,
            "rotation_analysis": rotation_analysis,
            "continuity_stats": continuity_stats,
        }
    
    async def _get_recent_rankings(
        self,
        mongo_manager,
        trade_date: str,
        days: int,
    ) -> List[Dict]:
        """获取最近N天的排名数据"""
        # 获取最近的交易日
        all_dates = await mongo_manager.find_many(
            "sector_ranking",
            {"ranking_type": "industry_top"},
            projection={"trade_date": 1, "_id": 0},
        )
        unique_dates = sorted(
            list(set(d.get("trade_date") for d in all_dates if d.get("trade_date") and d.get("trade_date") <= trade_date)),
            reverse=True
        )[:days]
        
        if not unique_dates:
            return []
        
        # 获取这些日期的排名数据
        rankings = await mongo_manager.find_many(
            "sector_ranking",
            {
                "trade_date": {"$in": unique_dates},
                "ranking_type": {"$in": ["industry_top", "concept_top"]},
            },
            sort=[("trade_date", -1), ("rank", 1)],
        )
        
        return rankings
    
    async def _get_limit_board_data(
        self,
        mongo_manager,
        trade_date: str,
    ) -> Dict[str, int]:
        """获取涨停板数据，按板块统计3板以上个数"""
        # 从 limit_list 获取当日涨停数据
        limit_data = await mongo_manager.find_many(
            "limit_list",
            {"trade_date": trade_date, "limit_times": {"$gte": 3}},
        )
        
        # 统计每个行业的高板个数
        industry_high_boards = {}
        for item in limit_data:
            industry = item.get("industry", "未知")
            if industry not in industry_high_boards:
                industry_high_boards[industry] = 0
            industry_high_boards[industry] += 1
        
        return industry_high_boards
    
    def _calculate_continuity(self, rankings: List[Dict]) -> Dict[str, Dict]:
        """
        计算板块连续性指数
        
        返回每个板块的：
        - 出现次数
        - 平均排名
        - 连续天数
        - 位次变化趋势
        """
        # 按日期分组
        date_rankings = {}
        for r in rankings:
            dt = r.get("trade_date")
            if dt not in date_rankings:
                date_rankings[dt] = []
            date_rankings[dt].append(r)
        
        # 统计每个板块的表现
        sector_stats = {}
        sorted_dates = sorted(date_rankings.keys(), reverse=True)
        
        for dt in sorted_dates:
            for r in date_rankings[dt]:
                name = r.get("name", "")
                if not name:
                    continue
                
                if name not in sector_stats:
                    sector_stats[name] = {
                        "name": name,
                        "ts_code": r.get("ts_code"),
                        "type": r.get("ranking_type"),
                        "appearances": 0,
                        "total_rank": 0,
                        "ranks_by_date": {},
                        "consecutive_days": 0,
                    }
                
                sector_stats[name]["appearances"] += 1
                sector_stats[name]["total_rank"] += r.get("rank", 20)
                sector_stats[name]["ranks_by_date"][dt] = r.get("rank", 20)
        
        # 计算连续天数和平均排名
        for name, stats in sector_stats.items():
            stats["avg_rank"] = stats["total_rank"] / max(1, stats["appearances"])
            
            # 计算连续天数（从最近日期开始）
            consecutive = 0
            for dt in sorted_dates:
                if dt in stats["ranks_by_date"]:
                    consecutive += 1
                else:
                    break
            stats["consecutive_days"] = consecutive
            
            # 计算位次变化（最近 vs 最早）
            if len(stats["ranks_by_date"]) >= 2:
                latest_dt = sorted_dates[0]
                earliest_dt = sorted_dates[-1]
                latest_rank = stats["ranks_by_date"].get(latest_dt, 20)
                earliest_rank = stats["ranks_by_date"].get(earliest_dt, 20)
                stats["rank_change"] = earliest_rank - latest_rank  # 正数表示上升
            else:
                stats["rank_change"] = 0
        
        return sector_stats
    
    def _identify_main_themes(
        self,
        continuity_stats: Dict[str, Dict],
        limit_data: Dict[str, int],
    ) -> List[Dict]:
        """
        识别当前主线
        
        条件：
        1. 连续3天进入Top10
        2. 板块内有3板以上标的
        """
        main_themes = []
        
        for name, stats in continuity_stats.items():
            # 检查是否有3板以上标的
            high_board_count = limit_data.get(name, 0)
            
            # 连续3天进入Top10 且 有高板标的
            if stats["consecutive_days"] >= 3 and stats["avg_rank"] <= 10:
                if high_board_count >= 1:  # 有3板以上标的
                    main_themes.append({
                        "name": name,
                        "ts_code": stats.get("ts_code"),
                        "status": ThemeStatus.MAIN_THEME.value,
                        "consecutive_days": stats["consecutive_days"],
                        "avg_rank": round(stats["avg_rank"], 1),
                        "high_board_count": high_board_count,
                        "rank_change": stats.get("rank_change", 0),
                        "reason": f"连续{stats['consecutive_days']}天Top10，{high_board_count}只3板+",
                    })
        
        # 按高板数量和连续天数排序
        main_themes.sort(key=lambda x: (-x["high_board_count"], -x["consecutive_days"]))
        
        return main_themes[:5]  # 返回前5个主线
    
    def _identify_strong_focus(self, continuity_stats: Dict[str, Dict]) -> List[Dict]:
        """
        识别强势关注板块
        
        条件：连续3天进入Top10
        """
        strong_focus = []
        
        for name, stats in continuity_stats.items():
            if stats["consecutive_days"] >= 3 and stats["avg_rank"] <= 10:
                strong_focus.append({
                    "name": name,
                    "ts_code": stats.get("ts_code"),
                    "status": ThemeStatus.STRONG_FOCUS.value,
                    "consecutive_days": stats["consecutive_days"],
                    "avg_rank": round(stats["avg_rank"], 1),
                    "rank_change": stats.get("rank_change", 0),
                })
        
        # 按连续天数和平均排名排序
        strong_focus.sort(key=lambda x: (-x["consecutive_days"], x["avg_rank"]))
        
        return strong_focus[:10]
    
    def _analyze_rotation(self, rankings: List[Dict]) -> List[Dict]:
        """
        分析板块轮动趋势
        
        返回位次变化最大的板块
        """
        # 按日期分组
        date_rankings = {}
        for r in rankings:
            dt = r.get("trade_date")
            if dt not in date_rankings:
                date_rankings[dt] = {}
            name = r.get("name", "")
            if name:
                date_rankings[dt][name] = r.get("rank", 20)
        
        sorted_dates = sorted(date_rankings.keys(), reverse=True)
        if len(sorted_dates) < 2:
            return []
        
        latest_dt = sorted_dates[0]
        prev_dt = sorted_dates[1]
        
        rotation_analysis = []
        
        # 计算位次变化
        all_sectors = set(date_rankings[latest_dt].keys()) | set(date_rankings[prev_dt].keys())
        
        for sector in all_sectors:
            latest_rank = date_rankings[latest_dt].get(sector, 25)  # 未上榜给25
            prev_rank = date_rankings[prev_dt].get(sector, 25)
            change = prev_rank - latest_rank  # 正数表示上升
            
            if abs(change) >= 3:  # 位次变化超过3位才记录
                rotation_analysis.append({
                    "name": sector,
                    "latest_rank": latest_rank if latest_rank <= 20 else None,
                    "prev_rank": prev_rank if prev_rank <= 20 else None,
                    "change": change,
                    "trend": "rising" if change > 0 else "falling",
                })
        
        # 按变化幅度排序
        rotation_analysis.sort(key=lambda x: -abs(x["change"]))
        
        return rotation_analysis[:10]
    
    async def get_theme_radar(
        self,
        mongo_manager,
        trade_date: str,
    ) -> Dict[str, Any]:
        """
        获取主线雷达数据
        
        展示当前共识度最高的3个板块及其5日位次变化
        """
        analysis = await self.analyze_themes(mongo_manager, trade_date, lookback_days=5)
        
        # 构建雷达数据
        radar_items = []
        
        # 优先取主线，不足则补充强势关注
        themes = analysis.get("main_themes", [])[:3]
        if len(themes) < 3:
            for sf in analysis.get("strong_focus", []):
                if len(themes) >= 3:
                    break
                if sf["name"] not in [t["name"] for t in themes]:
                    themes.append(sf)
        
        for theme in themes[:3]:
            radar_items.append({
                "name": theme["name"],
                "rank_change": theme.get("rank_change", 0),
                "trend": "up" if theme.get("rank_change", 0) > 0 else ("down" if theme.get("rank_change", 0) < 0 else "flat"),
                "consecutive_days": theme.get("consecutive_days", 0),
                "status": theme.get("status", ThemeStatus.NORMAL.value),
            })
        
        return {
            "trade_date": trade_date,
            "radar": radar_items,
            "rotation": analysis.get("rotation_analysis", [])[:5],
        }
    
    async def calculate_sector_scores(
        self,
        mongo_manager,
        trade_date: str,
        lookback_days: int = 20,
    ) -> Dict[str, Any]:
        """
        计算板块长周期评分 (MA20)
        
        Sector_Score = (20日频率 * 0.4) + (连板系数 * 0.3) + (资金占比 * 0.3)
        
        判定逻辑:
        - [主线]: 评分 > 75 且 20日频率 > 8次
        - [异动]: 评分 < 50 但 今日涨幅 > 4%
        - [退潮]: 评分曾高但近期连续3日排名下滑
        """
        # 1. 获取最近20天的排名数据
        all_dates = await mongo_manager.find_many(
            "sector_ranking",
            {"ranking_type": "industry_top"},
            projection={"trade_date": 1, "_id": 0},
        )
        unique_dates = sorted(
            list(set(d.get("trade_date") for d in all_dates if d.get("trade_date") and d.get("trade_date") <= trade_date)),
            reverse=True
        )[:lookback_days]
        
        if not unique_dates:
            return {"sectors": [], "main_themes": [], "anomalies": [], "fading": []}
        
        # 获取排名数据
        rankings = await mongo_manager.find_many(
            "sector_ranking",
            {
                "trade_date": {"$in": unique_dates},
                "ranking_type": "industry_top",
            },
        )
        
        # 2. 获取涨停板数据（用于连板系数）
        limit_data = await mongo_manager.find_many(
            "limit_list",
            {"trade_date": trade_date},
        )
        
        # 统计每个行业的连板数
        industry_limit_stats = {}
        for item in limit_data:
            industry = item.get("industry", "未知")
            limit_times = item.get("limit_times", 1)
            if industry not in industry_limit_stats:
                industry_limit_stats[industry] = {"total": 0, "high_board": 0, "max_board": 0}
            industry_limit_stats[industry]["total"] += 1
            if limit_times >= 3:
                industry_limit_stats[industry]["high_board"] += 1
            industry_limit_stats[industry]["max_board"] = max(
                industry_limit_stats[industry]["max_board"], limit_times
            )
        
        # 3. 获取资金流向数据
        moneyflow = await mongo_manager.find_many(
            "moneyflow_industry",
            {"trade_date": trade_date},
        )
        moneyflow_map = {m.get("industry"): m for m in moneyflow}
        
        # 4. 计算每个板块的评分
        sector_stats = {}
        
        for r in rankings:
            name = r.get("name", "")
            if not name:
                continue
            
            dt = r.get("trade_date")
            if name not in sector_stats:
                sector_stats[name] = {
                    "name": name,
                    "ts_code": r.get("ts_code"),
                    "appearances": 0,
                    "ranks_by_date": {},
                    "total_rank": 0,
                }
            
            sector_stats[name]["appearances"] += 1
            sector_stats[name]["ranks_by_date"][dt] = r.get("rank", 20)
            sector_stats[name]["total_rank"] += r.get("rank", 20)
        
        # 5. 计算综合评分
        total_days = len(unique_dates)
        sectors = []
        
        for name, stats in sector_stats.items():
            # 20日频率分 (出现次数 / 总天数 * 100)
            frequency = stats["appearances"] / max(1, total_days)
            frequency_score = min(100, frequency * 100)
            
            # 连板系数分
            limit_stats = industry_limit_stats.get(name, {"total": 0, "high_board": 0, "max_board": 0})
            # 高板数量 * 20 + 最高板 * 10，上限100
            board_score = min(100, limit_stats["high_board"] * 25 + limit_stats["max_board"] * 10)
            
            # 资金占比分 (净流入排名转换)
            mf = moneyflow_map.get(name, {})
            net_amount = float(mf.get("net_amount") or 0)
            # 简化处理：正流入给高分
            if net_amount > 0:
                money_score = min(100, 50 + net_amount / 10)
            else:
                money_score = max(0, 50 + net_amount / 10)
            
            # 综合评分
            sector_score = frequency_score * 0.4 + board_score * 0.3 + money_score * 0.3
            
            # 计算平均排名
            avg_rank = stats["total_rank"] / max(1, stats["appearances"])
            
            # 今日涨幅
            today_pct = float(mf.get("pct_change") or 0)
            
            # 连续排名变化（判断退潮）
            sorted_dates = sorted(stats["ranks_by_date"].keys(), reverse=True)
            rank_trend = []
            for i in range(min(3, len(sorted_dates))):
                rank_trend.append(stats["ranks_by_date"].get(sorted_dates[i], 20))
            
            # 判断是否退潮（连续3日排名下滑）
            is_fading = False
            if len(rank_trend) >= 3:
                if rank_trend[0] > rank_trend[1] > rank_trend[2]:
                    is_fading = True
            
            # 过滤冗余：屏蔽 20日共识度<3 且 当日涨幅<3% 的板块
            if stats["appearances"] < 3 and today_pct < 3:
                continue
            
            # 计算短期强度 (用于散点图Y轴)
            short_strength = today_pct * 10 + board_score * 0.3
            
            # 判定分类标记
            # CORE: 20日共识度 > 9 (穿越主线)
            # PULSE: 今日第一但20日共识<3 (脉冲轮动)
            if stats["appearances"] >= 9:
                category = "CORE"
                status = "core"
            elif stats["appearances"] < 3 and today_pct >= 3:
                category = "PULSE"
                status = "pulse"
            elif is_fading and stats["appearances"] >= 5:
                category = "FADE"
                status = "fading"
            elif stats["appearances"] >= 5:
                category = "ACTIVE"
                status = "active"
            else:
                category = "-"
                status = "normal"
            
            # 自动生成理由 (20字以内)
            reason = self._generate_reason(
                name, stats["appearances"], today_pct, 
                limit_stats["high_board"], limit_stats["max_board"],
                is_fading, category
            )
            
            # 领涨个股
            lead_stock = mf.get("lead_stock", "") if mf else ""
            
            sectors.append({
                "name": name,
                "ts_code": stats.get("ts_code"),
                "score": round(sector_score, 1),
                "frequency": stats["appearances"],
                "short_strength": round(short_strength, 1),
                "avg_rank": round(avg_rank, 1),
                "today_pct": round(today_pct, 2),
                "high_board_count": limit_stats["high_board"],
                "max_board": limit_stats["max_board"],
                "net_amount": round(net_amount, 2),
                "lead_stock": lead_stock,
                "category": category,
                "status": status,
                "reason": reason,
                "rank_trend": rank_trend,
            })
        
        # 排序
        sectors.sort(key=lambda x: -x["score"])
        
        # 分类
        core_themes = [s for s in sectors if s["category"] == "CORE"][:5]
        pulse_themes = [s for s in sectors if s["category"] == "PULSE"][:5]
        fading_themes = [s for s in sectors if s["category"] == "FADE"][:5]
        
        # 生成诊断文字
        diagnosis = self._generate_diagnosis(core_themes, pulse_themes, fading_themes)
        
        return {
            "trade_date": trade_date,
            "lookback_days": lookback_days,
            "total_sectors": len(sectors),
            "sectors": sectors,
            "core_themes": core_themes,
            "pulse_themes": pulse_themes,
            "fading_themes": fading_themes,
            "diagnosis": diagnosis,
        }
    
    def _generate_reason(
        self, name: str, frequency: int, today_pct: float,
        high_board: int, max_board: int, is_fading: bool, category: str
    ) -> str:
        """生成20字以内的判定理由"""
        if category == "CORE":
            if high_board >= 2:
                return f"高共识+{high_board}只高板，核心主线"
            else:
                return f"20日{frequency}次上榜，穿越主线"
        elif category == "PULSE":
            return f"今涨{today_pct:.1f}%，低位脉冲轮动"
        elif category == "FADE":
            return f"共识度下滑，主线退潮中"
        elif category == "ACTIVE":
            if max_board >= 3:
                return f"{max_board}板龙头领涨，活跃板块"
            else:
                return f"20日{frequency}次上榜，保持活跃"
        return ""
    
    def _generate_diagnosis(
        self, core: List[Dict], pulse: List[Dict], fading: List[Dict]
    ) -> str:
        """生成市场趋势诊断文字"""
        lines = []
        
        if core:
            names = "、".join([c["name"] for c in core[:3]])
            lines.append(f"【核心主线】{names} 共识度领先，延续性强")
        
        if pulse:
            names = "、".join([p["name"] for p in pulse[:2]])
            lines.append(f"【脉冲轮动】{names} 今日异动，关注持续性")
        
        if fading:
            names = "、".join([f["name"] for f in fading[:2]])
            lines.append(f"【退潮信号】{names} 热度下降，注意风险")
        
        if not lines:
            lines.append("当前市场主线不明确，建议观望")
        
        return "\n".join(lines)
    
    async def get_scatter_data(
        self,
        mongo_manager,
        trade_date: str,
    ) -> Dict[str, Any]:
        """
        获取散点图数据
        
        横轴: 20日共识度 (frequency)
        纵轴: 短期强度
        
        区域定义:
        - 主线区: x >= 9 (高共识)
        - 轮动区: x < 5, y >= 30 (低共识高强度)
        - 冰封区: x < 3, y < 10 (低共识低强度)
        """
        result = await self.calculate_sector_scores(mongo_manager, trade_date, 20)
        
        scatter_data = []
        for sector in result.get("sectors", []):
            scatter_data.append({
                "name": sector["name"],
                "x": sector["frequency"],
                "y": sector.get("short_strength", 0),
                "score": sector["score"],
                "category": sector["category"],
                "status": sector["status"],
                "reason": sector["reason"],
            })
        
        # 区域定义 (用于前端绘制背景)
        zones = [
            {"name": "主线区", "xMin": 9, "xMax": 20, "yMin": -50, "yMax": 100, "color": "rgba(0,191,165,0.08)"},
            {"name": "轮动区", "xMin": 0, "xMax": 5, "yMin": 30, "yMax": 100, "color": "rgba(255,109,0,0.08)"},
            {"name": "冰封区", "xMin": 0, "xMax": 3, "yMin": -50, "yMax": 10, "color": "rgba(100,100,100,0.08)"},
        ]
        
        return {
            "trade_date": trade_date,
            "scatter": scatter_data,
            "zones": zones,
        }


# 单例
theme_manager = ThemeManager()
