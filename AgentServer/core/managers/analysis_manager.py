"""
市场情绪周期量化分析引擎 (MA30 动态基准版)

提供:
- 情绪评分 (Sentiment Score): 基于 MA30 动态基准的短线博弈温度
- 市场强度评分 (Strength Score): 基于量能偏离比的资金承接能力
- 周期定位 (Cycle Identification): 结合历史趋势的市场周期判定
"""

import logging
import math
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime
from enum import Enum

from .base import BaseManager


class MarketCycle(str, Enum):
    """市场周期枚举"""
    ICE_POINT = "ice_point"        # 冰点期
    DECLINE = "decline"            # 退潮期
    CHAOS = "chaos"                # 混沌期 (无主线轮动)
    INCUBATION = "incubation"      # 萌芽期
    MAIN_UPWARD = "main_upward"    # 主升期
    ROTATION = "rotation"          # 分歧/轮动期
    UNKNOWN = "unknown"            # 未知


# 周期中文描述
CYCLE_DESCRIPTIONS = {
    MarketCycle.ICE_POINT: "冰点期 - 极度悲观，连板高度极低，跌停遍地",
    MarketCycle.DECLINE: "退潮期 - 情绪快速降温，跌停激增，高标补跌",
    MarketCycle.CHAOS: "混沌期 - 无明确主线，量能萎缩，个股轮动",
    MarketCycle.INCUBATION: "萌芽期 - 新周期萌芽，高度突破，量能放大",
    MarketCycle.MAIN_UPWARD: "主升期 - 赚钱效应爆棚，高标加速，封板率高",
    MarketCycle.ROTATION: "分歧期 - 高位分歧，炸板率上升，需警惕退潮",
    MarketCycle.UNKNOWN: "未知 - 数据不足，无法判断",
}


class AnalysisManager(BaseManager):
    """
    市场分析管理器 (MA30 动态基准版)
    
    核心功能:
    1. 加载 MA30 历史数据计算动态基准
    2. 基于动态基准计算情绪评分和市场强度评分
    3. 结合趋势判定市场周期
    4. 存储分析结果
    """
    
    # 默认基准值 (当历史数据不足时使用)
    DEFAULT_AVG_AMOUNT = 10000 * 1e8  # 1万亿（千元单位）
    DEFAULT_AVG_LIMIT_UP = 80         # 80家涨停
    
    def __init__(self):
        super().__init__()  # 调用基类初始化，获取 logger
        self._ma30_cache = {}  # 缓存 MA30 数据，key 为 trade_date
    
    @staticmethod
    def _safe_float(val, default: float = 0.0) -> float:
        """安全转换为浮点数"""
        if val is None:
            return default
        try:
            return float(val)
        except (ValueError, TypeError):
            return default
    
    async def initialize(self) -> None:
        """初始化"""
        self._initialized = True
        self.logger.info("AnalysisManager initialized (MA30 dynamic baseline mode) ✓")
    
    async def shutdown(self) -> None:
        """关闭"""
        self._initialized = False
        self._ma30_cache.clear()
        self.logger.info("AnalysisManager shutdown ✓")
    
    async def health_check(self) -> bool:
        """健康检查"""
        return self._initialized
    
    async def load_ma30_baseline(
        self,
        trade_date: str,
        mongo_manager,
    ) -> Dict[str, float]:
        """
        加载 MA30 动态基准值
        
        从 MongoDB 加载最近 30 个交易日的历史统计数据，计算均值
        
        Args:
            trade_date: 当前交易日 (不包含在计算中，使用之前30天)
            mongo_manager: MongoDB 管理器实例
        
        Returns:
            包含 MA30 基准值的字典:
            - avg_amount_30d: 30日平均成交额 (千元)
            - avg_limit_up_30d: 30日平均涨停家数
            - avg_limit_down_30d: 30日平均跌停家数
            - data_count: 实际使用的历史数据天数
        """
        # 检查缓存
        cache_key = trade_date
        if cache_key in self._ma30_cache:
            return self._ma30_cache[cache_key]
        
        # 从 MongoDB 加载最近 30 个交易日的数据 (不含当天)
        history_data = await mongo_manager.find_many(
            "daily_stats",
            {"trade_date": {"$lt": trade_date}},
            projection={
                "trade_date": 1,
                "total_amount": 1,
                "limit_up_count": 1,
                "limit_down_count": 1,
                "_id": 0,
            },
            sort=[("trade_date", -1)],
            limit=30,
        )
        
        # 计算均值 (使用实际可获取的数据天数作为分母)
        if history_data and len(history_data) > 0:
            # 提取有效数据
            amounts = [self._safe_float(d.get("total_amount")) for d in history_data if d.get("total_amount")]
            limit_ups = [self._safe_float(d.get("limit_up_count")) for d in history_data if d.get("limit_up_count") is not None]
            limit_downs = [self._safe_float(d.get("limit_down_count")) for d in history_data if d.get("limit_down_count") is not None]
            
            # 使用实际数据天数计算均值，不使用默认值
            avg_amount = sum(amounts) / len(amounts) if amounts else None
            avg_limit_up = sum(limit_ups) / len(limit_ups) if limit_ups else None
            avg_limit_down = sum(limit_downs) / len(limit_downs) if limit_downs else None
            data_count = len(history_data)
            
            log_level = "debug" if data_count >= 10 else "info"
            msg = (
                f"MA baseline loaded ({data_count} days): "
                f"avg_amount={avg_amount/1e8:.0f}亿, " if avg_amount else "avg_amount=N/A, "
                f"avg_limit_up={avg_limit_up:.1f}, " if avg_limit_up else "avg_limit_up=N/A, "
                f"avg_limit_down={avg_limit_down:.1f}" if avg_limit_down else "avg_limit_down=N/A"
            )
            if data_count >= 10:
                self.logger.debug(
                    f"MA baseline loaded ({data_count} days): "
                    f"avg_amount={avg_amount/1e8:.0f}亿, avg_limit_up={avg_limit_up:.1f}, avg_limit_down={avg_limit_down:.1f}"
                )
            else:
                self.logger.info(
                    f"MA baseline loaded (only {data_count} days, <30): "
                    f"avg_amount={avg_amount/1e8:.0f}亿, avg_limit_up={avg_limit_up:.1f}" if avg_amount and avg_limit_up
                    else f"MA baseline: insufficient data ({data_count} days)"
                )
        else:
            # 完全没有历史数据
            avg_amount = None
            avg_limit_up = None
            avg_limit_down = None
            data_count = 0
            self.logger.warning("No historical data available for MA baseline")
        
        baseline = {
            "avg_amount_30d": avg_amount,
            "avg_limit_up_30d": avg_limit_up,
            "avg_limit_down_30d": avg_limit_down,
            "data_count": data_count,
        }
        
        # 缓存结果
        self._ma30_cache[cache_key] = baseline
        return baseline
    
    async def load_recent_v_ratios(
        self,
        trade_date: str,
        mongo_manager,
        days: int = 3,
    ) -> List[float]:
        """
        加载最近 N 天的量能偏离比 (v_ratio)
        
        用于判断趋势（如连续下降）
        
        Returns:
            v_ratio 列表，按日期从新到旧排序
        """
        history = await mongo_manager.find_many(
            "market_analysis",
            {"trade_date": {"$lt": trade_date}},
            projection={"trade_date": 1, "v_ratio": 1, "_id": 0},
            sort=[("trade_date", -1)],
            limit=days,
        )
        
        return [self._safe_float(d.get("v_ratio", 1.0)) for d in history if d.get("v_ratio")]
    
    async def load_prev_sentiment(
        self,
        trade_date: str,
        mongo_manager,
    ) -> Optional[float]:
        """
        加载前一天的情绪评分（用于 EMA 平滑）
        
        Returns:
            前一天的 sentiment_score_ema，如果没有则返回 None
        """
        prev = await mongo_manager.find_one(
            "market_analysis",
            {"trade_date": {"$lt": trade_date}},
            projection={"sentiment_score_ema": 1, "sentiment_score": 1, "_id": 0},
            sort=[("trade_date", -1)],
        )
        
        if prev:
            # 优先使用 EMA 值，如果没有则用原始值
            return self._safe_float(
                prev.get("sentiment_score_ema") or prev.get("sentiment_score"),
                default=None
            )
        return None
    
    def apply_ema_smoothing(
        self,
        current_score: float,
        prev_ema: Optional[float],
        alpha: float = 0.8,
    ) -> float:
        """
        应用 EMA 平滑
        
        公式: EMA = current * alpha + prev_ema * (1 - alpha)
        
        Args:
            current_score: 当日原始评分
            prev_ema: 前一日的 EMA 值
            alpha: 平滑系数 (默认 0.8，即今天权重 80%)
        
        Returns:
            平滑后的评分
        """
        if prev_ema is None:
            return current_score
        
        return round(current_score * alpha + prev_ema * (1 - alpha), 2)
    
    def calculate_scores_with_baseline(
        self, 
        stats: Dict[str, Any],
        baseline: Dict[str, float],
    ) -> Tuple[float, float, float]:
        """
        基于 MA30 动态基准计算情绪评分和市场强度评分
        
        Args:
            stats: 每日统计数据字典
            baseline: MA30 基准值字典
        
        Returns:
            (sentiment_score, strength_score, v_ratio) 元组
        """
        # 提取数据
        up_ratio = self._safe_float(stats.get("up_ratio"))
        max_height = self._safe_float(stats.get("max_limit_height"))
        limit_up = self._safe_float(stats.get("limit_up_count"))
        broken = self._safe_float(stats.get("broken_limit_count"))
        limit_down = self._safe_float(stats.get("limit_down_count"))
        total_limit_up = self._safe_float(stats.get("total_limit_up"))
        limit_1 = self._safe_float(stats.get("limit_1"))
        total_amount = self._safe_float(stats.get("total_amount"))  # 千元
        north_money = self._safe_float(stats.get("north_money"))    # 百万元
        
        # 提取 MA30 基准 (None 表示无历史数据，使用当日数据作为基准)
        avg_amount = baseline.get("avg_amount_30d")
        avg_limit_up = baseline.get("avg_limit_up_30d")
        avg_limit_down = baseline.get("avg_limit_down_30d")
        
        # 如果没有历史基准，使用当日数据作为基准 (ratio = 1.0)
        if avg_amount is None or avg_amount <= 0:
            avg_amount = total_amount if total_amount > 0 else 1.0
        if avg_limit_up is None or avg_limit_up <= 0:
            avg_limit_up = limit_up if limit_up > 0 else 1.0
        if avg_limit_down is None or avg_limit_down <= 0:
            avg_limit_down = limit_down if limit_down > 0 else 1.0
        
        # ==================== 市场强度评分 (Strength Score) ====================
        # 引入"量能效率"概念: Strength = Volume_Score * DirectionFactor
        # 放量杀跌时强度分应回落至 50-60 分区间
        
        v_ratio = total_amount / avg_amount if avg_amount > 0 else 1.0
        
        # 1. 量能得分 (Volume_Score): 基于 v_ratio 的对数映射，防止极端值
        # v_ratio=0.5 -> 35分, v_ratio=1.0 -> 65分, v_ratio=1.5 -> 80分, v_ratio=2.0 -> 95分
        if v_ratio > 0:
            volume_score = 65 + 30 * math.log2(v_ratio)  # 对数映射
        else:
            volume_score = 30
        volume_score = max(20, min(95, volume_score))  # 限制在 20-95 范围
        
        # 2. 方向因子 (DirectionFactor): 当 up_ratio < 45% 时线性减小
        # up_ratio=45% -> factor=1.0 (无惩罚)
        # up_ratio=30% -> factor=0.8 (打8折)
        # up_ratio=15% -> factor=0.6 (打6折)
        # up_ratio=0%  -> factor=0.5 (打5折，最低)
        if up_ratio < 45:
            # 线性减小: factor = 0.5 + (up_ratio / 45) * 0.5
            direction_factor = 0.5 + (up_ratio / 45) * 0.5
        elif up_ratio > 60:
            # 上涨行情轻微加成: 最高 1.15
            direction_factor = min(1.15, 1 + (up_ratio - 60) * 0.004)
        else:
            direction_factor = 1.0
        
        # 3. 计算最终强度分 = Volume_Score * DirectionFactor
        strength_score = volume_score * direction_factor
        
        # 4. 北向资金加成/惩罚 (±8分)
        north_in_yi = north_money / 100  # 百万元 -> 亿元
        if north_in_yi > 50:
            strength_score += min(8, north_in_yi / 50 * 4)
        elif north_in_yi < -50:
            strength_score -= min(8, abs(north_in_yi) / 50 * 4)
        
        # 限制范围
        strength_score = max(0, min(100, strength_score))
        
        self.logger.debug(
            f"Strength calc: v_ratio={v_ratio:.2f}, vol_score={volume_score:.1f}, "
            f"up_ratio={up_ratio:.1f}%, dir_factor={direction_factor:.2f}, final={strength_score:.1f}"
        )
        
        # ==================== 情绪评分 (Sentiment Score) ====================
        # 基于 MA30 动态对比
        
        # 1. 普涨率因子 (0-20分)
        r_up_score = min(20, up_ratio * 0.4)  # 50% 普涨率 = 20分
        
        # 2. 高度分 (0-25分，8板满分)
        h_score = min(25, max_height * 3.125)
        
        # 3. 封板率 (0-20分)
        if limit_up + broken > 0:
            success_rate = limit_up / (limit_up + broken)
        else:
            success_rate = 0
        success_score = success_rate * 20
        
        # 4. 涨停家数对比 MA30 (0-25分)
        # limit_up_ratio = current_limit_up / avg_limit_up_30d
        if avg_limit_up > 0:
            limit_up_ratio = limit_up / avg_limit_up
        else:
            limit_up_ratio = 1.0
        # 1.0 倍 = 12.5分，2.0 倍 = 25分，0.5 倍 = 6.25分
        limit_up_score = min(25, limit_up_ratio * 12.5)
        
        # 5. 跌停惩罚 (动态基准)
        # 跌停数超过平均涨停数的 20% 时开始重罚
        down_threshold = avg_limit_up * 0.2
        if down_threshold > 0 and limit_down > down_threshold:
            down_penalty = (limit_down - down_threshold) / down_threshold * 15
            down_penalty = min(30, down_penalty)  # 最多扣 30 分
        else:
            down_penalty = 0
        
        # 6. 接力率加分 (0-10分)
        if total_limit_up > 0:
            promo_rate = (total_limit_up - limit_1) / total_limit_up
        else:
            promo_rate = 0
        promo_score = promo_rate * 10
        
        # 计算情绪分
        sentiment_score = r_up_score + h_score + success_score + limit_up_score + promo_score - down_penalty
        sentiment_score = max(0, min(100, sentiment_score))
        
        self.logger.debug(
            f"Scores (MA30): sentiment={sentiment_score:.1f}, strength={strength_score:.1f}, v_ratio={v_ratio:.2f} | "
            f"r_up={r_up_score:.1f}, h={h_score:.1f}, success={success_score:.1f}, "
            f"limit_up_score={limit_up_score:.1f}, promo={promo_score:.1f}, down_penalty={down_penalty:.1f}"
        )
        
        return round(sentiment_score, 2), round(strength_score, 2), round(v_ratio, 3)
    
    async def identify_cycle_with_baseline(
        self,
        stats: Dict[str, Any],
        prev_stats: Optional[Dict[str, Any]],
        baseline: Dict[str, float],
        recent_v_ratios: List[float],
        v_ratio: float,
    ) -> Tuple[MarketCycle, str]:
        """
        基于 MA30 动态基准判定市场周期
        
        Args:
            stats: 当日统计数据
            prev_stats: 昨日统计数据
            baseline: MA30 基准值
            recent_v_ratios: 最近几天的 v_ratio 列表
            v_ratio: 当日量能偏离比
        
        Returns:
            (cycle_enum, reason) 元组
        """
        # 提取当日数据
        max_height = self._safe_float(stats.get("max_limit_height"))
        limit_down = self._safe_float(stats.get("limit_down_count"))
        broken = self._safe_float(stats.get("broken_limit_count"))
        limit_up = self._safe_float(stats.get("limit_up_count"))
        total_amount = self._safe_float(stats.get("total_amount"))
        
        # 提取 MA 基准 (None 表示无历史数据，使用当日数据作为基准)
        avg_limit_up = baseline.get("avg_limit_up_30d")
        avg_limit_down = baseline.get("avg_limit_down_30d")
        
        # 如果没有历史基准，使用当日数据作为基准
        if avg_limit_up is None or avg_limit_up <= 0:
            avg_limit_up = limit_up if limit_up > 0 else 1.0
        if avg_limit_down is None or avg_limit_down <= 0:
            avg_limit_down = limit_down if limit_down > 0 else 1.0
        
        # 计算评分
        sentiment, strength, _ = self.calculate_scores_with_baseline(stats, baseline)
        
        # 封板率
        if limit_up + broken > 0:
            success_rate = limit_up / (limit_up + broken) * 100
        else:
            success_rate = 0
        
        # 炸板率
        if limit_up > 0:
            broken_rate = broken / limit_up * 100
        else:
            broken_rate = 0
        
        # 成交额 (亿元)
        amount_yi = total_amount / 1e8
        
        # 昨日数据
        prev_height = 0.0
        prev_amount = 0.0
        if prev_stats:
            prev_height = self._safe_float(prev_stats.get("max_limit_height"))
            prev_amount = self._safe_float(prev_stats.get("total_amount"))
        
        # 判断 v_ratio 趋势
        v_ratio_declining = False
        if len(recent_v_ratios) >= 2:
            # 检查是否连续 2 日下降
            if recent_v_ratios[0] < recent_v_ratios[1] and v_ratio < recent_v_ratios[0]:
                v_ratio_declining = True
        
        # v_ratio 低位回升判断
        v_ratio_rebounding = False
        if len(recent_v_ratios) >= 1:
            prev_v = recent_v_ratios[0]
            if prev_v < 0.8 and v_ratio > prev_v:
                v_ratio_rebounding = True
        
        # ==================== 周期判定逻辑 (按优先级) ====================
        
        # 1. 冰点期: 高度 <= 2 且 情绪分 < 30 且 跌停超过均值 2 倍
        if max_height <= 2 and sentiment < 30 and limit_down > avg_limit_down * 2:
            return MarketCycle.ICE_POINT, f"高度仅{max_height}板，情绪{sentiment:.0f}分，跌停{limit_down}家(>{avg_limit_down*2:.0f})"
        
        # 2. 退潮期: v_ratio 连续 2 日下降且跌停超过均值 2 倍
        if v_ratio_declining and limit_down > avg_limit_down * 2:
            return MarketCycle.DECLINE, f"量能连续萎缩(v_ratio={v_ratio:.2f})，跌停{limit_down}家激增"
        
        # 2b. 退潮期: 高度大幅回落
        if prev_height >= 4 and max_height < prev_height - 2:
            return MarketCycle.DECLINE, f"高度从{prev_height}板回落至{max_height}板，高标补跌"
        
        # 2c. 退潮期: 跌停激增
        if limit_down > avg_limit_down * 3:
            return MarketCycle.DECLINE, f"跌停{limit_down}家(均值{avg_limit_down:.0f}的3倍)，市场情绪快速降温"
        
        # 3. 主升期: 高度 >= 5 且 v_ratio > 1.2 且 封板率 > 80%
        if max_height >= 5 and v_ratio > 1.2 and success_rate > 80:
            return MarketCycle.MAIN_UPWARD, f"高度{max_height}板，量能{v_ratio:.2f}倍均值，封板率{success_rate:.0f}%"
        
        # 4. 分歧/轮动期: 高度高但炸板率 > 25% 或 封板率 < 60%
        if max_height >= 4 and (broken_rate > 25 or success_rate < 60):
            return MarketCycle.ROTATION, f"高度{max_height}板但炸板率{broken_rate:.0f}%，封板率{success_rate:.0f}%"
        
        # 5. 萌芽期: 量能从低位回升 且 高度突破
        if v_ratio_rebounding and prev_amount > 0 and total_amount > prev_amount:
            if max_height >= 3:
                return MarketCycle.INCUBATION, f"量能回升(v_ratio={v_ratio:.2f})，高度{max_height}板，新周期萌芽"
        
        # 5b. 萌芽期: 高度突破且量能放大
        if prev_height > 0 and max_height > prev_height and max_height >= 4:
            if v_ratio > 1.0:
                return MarketCycle.INCUBATION, f"高度从{prev_height}突破至{max_height}板，量能{v_ratio:.2f}倍均值"
        
        # 6. 混沌期: 量能低于均值 且 高度 <= 3
        if v_ratio < 0.9 and max_height <= 3:
            return MarketCycle.CHAOS, f"量能{v_ratio:.2f}倍均值偏低，高度{max_height}板，无明确主线"
        
        # 6b. 混沌期: 情绪震荡
        if max_height <= 3 and 30 <= sentiment <= 50:
            return MarketCycle.CHAOS, f"高度{max_height}板，情绪{sentiment:.0f}分震荡，轮动行情"
        
        # 7. 冰点期 (兜底)
        if max_height <= 2 and sentiment < 30:
            return MarketCycle.ICE_POINT, f"高度仅{max_height}板，情绪{sentiment:.0f}分"
        
        # 默认情况
        return MarketCycle.UNKNOWN, f"数据不足或处于过渡状态(v_ratio={v_ratio:.2f})"
    
    # ==================== 兼容旧接口 ====================
    
    def calculate_scores(self, stats: Dict[str, Any]) -> Tuple[float, float]:
        """
        计算情绪评分和市场强度评分 (旧接口，使用默认基准)
        
        注意: 此方法使用固定默认基准，建议使用 calculate_scores_with_baseline
        """
        baseline = {
            "avg_amount_30d": self.DEFAULT_AVG_AMOUNT,
            "avg_limit_up_30d": self.DEFAULT_AVG_LIMIT_UP,
            "avg_limit_down_30d": 5,
        }
        sentiment, strength, _ = self.calculate_scores_with_baseline(stats, baseline)
        return sentiment, strength
    
    def identify_cycle(
        self, 
        stats: Dict[str, Any], 
        prev_stats: Optional[Dict[str, Any]] = None
    ) -> Tuple[MarketCycle, str]:
        """
        判定市场周期 (旧接口，使用默认基准)
        
        注意: 此方法使用固定默认基准，建议使用 identify_cycle_with_baseline
        """
        # 使用默认基准进行同步判定
        max_height = self._safe_float(stats.get("max_limit_height"))
        limit_down = self._safe_float(stats.get("limit_down_count"))
        broken = self._safe_float(stats.get("broken_limit_count"))
        limit_up = self._safe_float(stats.get("limit_up_count"))
        total_amount = self._safe_float(stats.get("total_amount"))
        
        sentiment, strength = self.calculate_scores(stats)
        
        if limit_up + broken > 0:
            success_rate = limit_up / (limit_up + broken) * 100
        else:
            success_rate = 0
        
        if limit_up > 0:
            broken_rate = broken / limit_up * 100
        else:
            broken_rate = 0
        
        amount_yi = total_amount / 1e8
        
        prev_height = 0.0
        if prev_stats:
            prev_height = self._safe_float(prev_stats.get("max_limit_height"))
        
        # 简化的周期判定
        if max_height <= 2 and sentiment < 30 and limit_down > 15:
            return MarketCycle.ICE_POINT, f"高度仅{max_height}板，情绪{sentiment:.0f}分，跌停{limit_down}家"
        
        if limit_down > 15:
            return MarketCycle.DECLINE, f"跌停{limit_down}家，市场情绪快速降温"
        
        if max_height >= 5 and amount_yi > 8000 and success_rate > 80:
            return MarketCycle.MAIN_UPWARD, f"高度{max_height}板，成交{amount_yi:.0f}亿，封板率{success_rate:.0f}%"
        
        if max_height >= 4 and (broken_rate > 25 or success_rate < 60):
            return MarketCycle.ROTATION, f"高度{max_height}板但炸板率{broken_rate:.0f}%"
        
        if max_height <= 3 and 30 <= sentiment <= 50:
            return MarketCycle.CHAOS, f"高度{max_height}板，情绪{sentiment:.0f}分震荡"
        
        return MarketCycle.UNKNOWN, "数据不足或处于过渡状态"
    
    async def analyze_and_store(
        self,
        stats: Dict[str, Any],
        prev_stats: Optional[Dict[str, Any]] = None,
        mongo_manager = None,
    ) -> Dict[str, Any]:
        """
        完整分析流程: 加载 MA30 基准 + 计算评分 + 判定周期 + 存储结果
        
        Args:
            stats: 当日统计数据
            prev_stats: 昨日统计数据
            mongo_manager: MongoDB 管理器实例
        
        Returns:
            分析结果字典
        """
        trade_date = stats.get("trade_date", "")
        
        # 加载 MA30 动态基准
        prev_sentiment_ema = None
        if mongo_manager:
            baseline = await self.load_ma30_baseline(trade_date, mongo_manager)
            recent_v_ratios = await self.load_recent_v_ratios(trade_date, mongo_manager, days=3)
            prev_sentiment_ema = await self.load_prev_sentiment(trade_date, mongo_manager)
        else:
            baseline = {
                "avg_amount_30d": self.DEFAULT_AVG_AMOUNT,
                "avg_limit_up_30d": self.DEFAULT_AVG_LIMIT_UP,
                "avg_limit_down_30d": 5,
                "data_count": 0,
            }
            recent_v_ratios = []
        
        # 计算原始评分
        sentiment_raw, strength_score, v_ratio = self.calculate_scores_with_baseline(stats, baseline)
        
        # 应用 EMA 平滑情绪分: Final = Today * 0.8 + Yesterday * 0.2
        sentiment_ema = self.apply_ema_smoothing(sentiment_raw, prev_sentiment_ema, alpha=0.8)
        
        # 判定周期 (使用平滑后的情绪分)
        cycle, reason = await self.identify_cycle_with_baseline(
            stats, prev_stats, baseline, recent_v_ratios, v_ratio
        )
        
        # 计算强弱差 (用于前端柱图)
        strength_diff = round(strength_score - sentiment_ema, 2)
        
        # 构建分析结果
        analysis = {
            "trade_date": trade_date,
            "created_at": datetime.utcnow(),
            
            # 评分 (原始 + 平滑)
            "sentiment_score": sentiment_raw,       # 原始情绪分
            "sentiment_score_ema": sentiment_ema,   # EMA 平滑后的情绪分
            "strength_score": strength_score,       # 强度分 (带方向修正)
            "strength_diff": strength_diff,         # 强弱差 = 强度 - 情绪EMA
            
            # 动态基准相关
            "v_ratio": v_ratio,
            "avg_amount_30d": baseline.get("avg_amount_30d"),
            "avg_limit_up_30d": baseline.get("avg_limit_up_30d"),
            "baseline_data_count": baseline.get("data_count", 0),
            
            # 周期
            "cycle": cycle.value,
            "cycle_name": CYCLE_DESCRIPTIONS.get(cycle, ""),
            "cycle_reason": reason,
            
            # 关键指标快照
            "max_limit_height": stats.get("max_limit_height", 0),
            "limit_up_count": stats.get("limit_up_count", 0),
            "limit_down_count": stats.get("limit_down_count", 0),
            "broken_limit_count": stats.get("broken_limit_count", 0),
            "up_ratio": stats.get("up_ratio", 0),
            "total_amount": stats.get("total_amount", 0),  # 保持原始千元单位
            "north_money": stats.get("north_money", 0),    # 保持原始百万元单位
        }
        
        self.logger.info(
            f"[{trade_date}] 情绪={sentiment_raw:.0f}(EMA:{sentiment_ema:.0f}), 强度={strength_score:.0f}, "
            f"v_ratio={v_ratio:.2f}, diff={strength_diff:.0f}, 周期={cycle.value}"
        )
        
        # 存储到 MongoDB
        if mongo_manager:
            await mongo_manager.update_one(
                "market_analysis",
                {"trade_date": trade_date},
                {"$set": analysis},
                upsert=True,
            )
            self.logger.debug(f"Analysis result saved to market_analysis collection")
        
        return analysis


# 全局单例
analysis_manager = AnalysisManager()
