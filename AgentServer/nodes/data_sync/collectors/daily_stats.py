"""
每日统计数据采集器

统计功能：
1. 板块/行业涨跌幅排名 (前20名)
2. 今日连板统计 (一板~六板及以上)
3. 今日涨跌个股数量
4. 今日涨停/跌停/炸板个股数量

数据来源: 基于已同步的 moneyflow_industry, moneyflow_concept, limit_list, stock_daily 进行统计
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, date
import time

from core.base import BaseCollector
from core.settings import settings
from core.managers import tushare_manager, mongo_manager, analysis_manager


class DailyStatsCollector(BaseCollector):
    """
    每日统计数据采集器
    
    统计内容:
    - 板块/行业涨跌幅排名 (前20名) -> sector_ranking 表
    - 连板统计 + 涨跌统计 + 涨跌停统计 -> daily_stats 表
    
    调度时间:
    - 可通过 SYNC_DAILY_STATS_SCHEDULE 环境变量配置
    - 默认: 每个交易日 16:30 (确保其他数据已同步完成)
    """
    
    name = "daily_stats"
    description = "计算每日统计数据"
    default_schedule = "30 16 * * 1-5"  # 默认: 每个交易日 16:30
    
    @property
    def schedule(self) -> str:
        """从配置读取调度时间，未配置则使用默认值"""
        return settings.data_sync.daily_stats_schedule or self.default_schedule
    
    async def collect(self) -> Dict[str, Any]:
        """执行统计"""
        # 获取最新交易日
        latest_trade_date = await tushare_manager.get_latest_trade_date()
        
        # 检查是否已统计
        if await mongo_manager.is_synced(self.name, latest_trade_date):
            self.logger.info(f"Daily stats {latest_trade_date} already computed, skipping")
            return {"count": 0, "message": f"Already computed {latest_trade_date}", "skipped": True}
        
        self.logger.info(f"Computing daily stats for {latest_trade_date}")
        
        results = {}
        
        # 1. 统计板块/行业涨跌幅排名
        t1 = time.time()
        ranking_result = await self._compute_sector_ranking(latest_trade_date)
        results["sector_ranking"] = ranking_result
        self.logger.info(f"Sector ranking computed: {ranking_result['count']} records, {time.time()-t1:.2f}s")
        
        # 2-4. 统计每日综合数据
        t2 = time.time()
        stats_result = await self._compute_daily_stats(latest_trade_date)
        results["daily_stats"] = stats_result
        self.logger.info(f"Daily stats computed: {time.time()-t2:.2f}s")
        
        # 5. 情绪周期分析
        t3 = time.time()
        # 获取昨日数据用于趋势判断
        prev_stats = await mongo_manager.find_one(
            "daily_stats",
            {"trade_date": {"$lt": latest_trade_date}},
            sort=[("trade_date", -1)],
        )
        analysis_result = await analysis_manager.analyze_and_store(
            stats=stats_result,
            prev_stats=prev_stats,
            mongo_manager=mongo_manager,
        )
        results["market_analysis"] = analysis_result
        self.logger.info(
            f"Market analysis: 情绪={analysis_result['sentiment_score']:.0f}, "
            f"强度={analysis_result['strength_score']:.0f}, "
            f"周期={analysis_result['cycle']}, {time.time()-t3:.2f}s"
        )
        
        # 记录同步完成
        await mongo_manager.record_sync(
            sync_type=self.name,
            sync_date=latest_trade_date,
            count=1,
        )
        
        return {
            "trade_date": latest_trade_date,
            "sector_ranking": ranking_result,
            "daily_stats": stats_result,
            "market_analysis": analysis_result,
            "message": f"Computed daily stats for {latest_trade_date}",
        }
    
    async def _compute_sector_ranking(self, trade_date: str) -> Dict[str, Any]:
        """
        统计板块/行业涨跌幅排名 (前20名)
        
        从 moneyflow_industry 和 moneyflow_concept 表获取数据
        预排序后存入 sector_ranking 表，供 API 直接查询
        """
        ranking_records = []
        
        # 1. 行业排名 (前20)
        # 注意: moneyflow_industry 表的名称字段是 "industry"
        industry_data = await mongo_manager.find_many(
            "moneyflow_industry",
            {"trade_date": trade_date},
        )
        
        if industry_data:
            # 按涨跌幅排序
            sorted_industry = sorted(industry_data, key=lambda x: float(x.get("pct_change") or 0), reverse=True)
            
            # 涨幅前20
            for i, item in enumerate(sorted_industry[:20]):
                name = item.get("industry") or item.get("name") or ""
                ranking_records.append({
                    "trade_date": trade_date,
                    "ranking_type": "industry_top",
                    "rank": i + 1,
                    "ts_code": item.get("ts_code"),
                    "name": name,
                    "pct_change": float(item.get("pct_change") or 0),
                    "net_amount": float(item.get("net_amount") or 0),
                    "lead_stock": item.get("lead_stock", ""),
                })
            
            # 跌幅前20 (涨幅倒数20)
            for i, item in enumerate(sorted_industry[-20:][::-1]):
                name = item.get("industry") or item.get("name") or ""
                ranking_records.append({
                    "trade_date": trade_date,
                    "ranking_type": "industry_bottom",
                    "rank": i + 1,
                    "ts_code": item.get("ts_code"),
                    "name": name,
                    "pct_change": float(item.get("pct_change") or 0),
                    "net_amount": float(item.get("net_amount") or 0),
                    "lead_stock": item.get("lead_stock", ""),
                })
        
        # 2. 概念板块排名 (前20)
        concept_data = await mongo_manager.find_many(
            "moneyflow_concept",
            {"trade_date": trade_date},
        )
        
        if concept_data:
            # 按涨跌幅排序
            sorted_concept = sorted(concept_data, key=lambda x: float(x.get("pct_change") or 0), reverse=True)
            
            # 涨幅前20
            for i, item in enumerate(sorted_concept[:20]):
                name = item.get("name") or item.get("concept") or ""
                ranking_records.append({
                    "trade_date": trade_date,
                    "ranking_type": "concept_top",
                    "rank": i + 1,
                    "ts_code": item.get("ts_code"),
                    "name": name,
                    "pct_change": float(item.get("pct_change") or 0),
                    "net_amount": float(item.get("net_amount") or 0),
                    "lead_stock": item.get("lead_stock", ""),
                })
            
            # 跌幅前20
            for i, item in enumerate(sorted_concept[-20:][::-1]):
                name = item.get("name") or item.get("concept") or ""
                ranking_records.append({
                    "trade_date": trade_date,
                    "ranking_type": "concept_bottom",
                    "rank": i + 1,
                    "ts_code": item.get("ts_code"),
                    "name": name,
                    "pct_change": float(item.get("pct_change") or 0),
                    "net_amount": float(item.get("net_amount") or 0),
                    "lead_stock": item.get("lead_stock", ""),
                })
        
        # 写入数据库
        if ranking_records:
            # 先删除当天的旧数据
            await mongo_manager.delete_many(
                "sector_ranking",
                {"trade_date": trade_date},
            )
            
            # 批量插入
            result = await mongo_manager.insert_many(
                "sector_ranking",
                ranking_records,
            )
            
            return {"count": len(ranking_records), "inserted": result}
        
        return {"count": 0}
    
    async def _compute_daily_stats(self, trade_date: str) -> Dict[str, Any]:
        """
        统计每日综合数据
        
        包含:
        - 连板统计 (一板~六板及以上)
        - 涨跌个股数量
        - 涨停/跌停/炸板数量
        - 沪深港通资金流向
        """
        stats = {
            "trade_date": trade_date,
            "created_at": datetime.utcnow(),
            
            # 连板统计
            "limit_1": 0,   # 一板 (首板)
            "limit_2": 0,   # 二板
            "limit_3": 0,   # 三板
            "limit_4": 0,   # 四板
            "limit_5": 0,   # 五板
            "limit_6_plus": 0,  # 六板及以上
            
            # 涨跌统计
            "up_count": 0,      # 上涨个股数
            "down_count": 0,    # 下跌个股数
            "flat_count": 0,    # 平盘个股数
            
            # 涨跌停统计
            "limit_up_count": 0,    # 涨停个股数
            "limit_down_count": 0,  # 跌停个股数
            "broken_limit_count": 0,  # 炸板个股数 (曾涨停但未封住)
            "max_limit_height": 0,  # 今日涨停高度 (最高连板数)
            
            # 沪深港通资金流向 (百万元)
            "hgt": None,         # 沪股通
            "sgt": None,         # 深股通
            "north_money": None, # 北向资金 (沪股通+深股通)
            "ggt_ss": None,      # 港股通(上海)
            "ggt_sz": None,      # 港股通(深圳)
            "south_money": None, # 南向资金 (港股通)
            
            # 市场成交额 (千元)
            "sh_amount": None,    # 上证成交额 (000001.SH)
            "sz_amount": None,    # 深证成交额 (399001.SZ)
            "total_amount": None, # 两市总成交额
        }
        
        # 1. 从 limit_list 获取涨跌停数据
        limit_data = await mongo_manager.find_many(
            "limit_list",
            {"trade_date": trade_date},
            projection={"ts_code": 1, "limit": 1, "limit_times": 1, "open_times": 1, "_id": 0},
        )
        
        if limit_data:
            for item in limit_data:
                limit_type = item.get("limit")
                limit_times = item.get("limit_times", 1) or 1
                open_times = item.get("open_times", 0) or 0
                
                if limit_type == "U":  # 涨停
                    stats["limit_up_count"] += 1
                    
                    # 更新涨停高度 (最高连板数)
                    if limit_times > stats["max_limit_height"]:
                        stats["max_limit_height"] = limit_times
                    
                    # 连板统计
                    if limit_times == 1:
                        stats["limit_1"] += 1
                    elif limit_times == 2:
                        stats["limit_2"] += 1
                    elif limit_times == 3:
                        stats["limit_3"] += 1
                    elif limit_times == 4:
                        stats["limit_4"] += 1
                    elif limit_times == 5:
                        stats["limit_5"] += 1
                    else:
                        stats["limit_6_plus"] += 1
                    
                    # 炸板统计 (涨停但有打开次数)
                    if open_times > 0:
                        stats["broken_limit_count"] += 1
                        
                elif limit_type == "D":  # 跌停
                    stats["limit_down_count"] += 1
        
        # 2. 从 stock_daily 获取涨跌统计
        daily_data = await mongo_manager.find_many(
            "stock_daily",
            {"trade_date": trade_date},
            projection={"ts_code": 1, "pct_chg": 1, "_id": 0},
        )
        
        if daily_data:
            for item in daily_data:
                pct_chg = item.get("pct_chg", 0) or 0
                
                if pct_chg > 0:
                    stats["up_count"] += 1
                elif pct_chg < 0:
                    stats["down_count"] += 1
                else:
                    stats["flat_count"] += 1
        
        # 3. 获取沪深港通资金流向
        try:
            hsgt_data = await tushare_manager.get_moneyflow_hsgt(trade_date=trade_date)
            if hsgt_data:
                hsgt = hsgt_data[0]
                stats["hgt"] = hsgt.get("hgt")           # 沪股通
                stats["sgt"] = hsgt.get("sgt")           # 深股通
                stats["north_money"] = hsgt.get("north_money")  # 北向资金
                stats["ggt_ss"] = hsgt.get("ggt_ss")     # 港股通(上海)
                stats["ggt_sz"] = hsgt.get("ggt_sz")     # 港股通(深圳)
                stats["south_money"] = hsgt.get("south_money")  # 南向资金
                self.logger.info(
                    f"HSGT data: north_money={stats['north_money']}M, "
                    f"south_money={stats['south_money']}M"
                )
        except Exception as e:
            self.logger.warning(f"Failed to get HSGT data: {e}")
        
        # 4. 获取两市成交额 (从 index_daily 表)
        try:
            # 上证指数 000001.SH
            sh_index = await mongo_manager.find_one(
                "index_daily",
                {"ts_code": "000001.SH", "trade_date": trade_date},
                projection={"amount": 1, "_id": 0},
            )
            if sh_index:
                stats["sh_amount"] = sh_index.get("amount")  # 千元
            
            # 深证成指 399001.SZ
            sz_index = await mongo_manager.find_one(
                "index_daily",
                {"ts_code": "399001.SZ", "trade_date": trade_date},
                projection={"amount": 1, "_id": 0},
            )
            if sz_index:
                stats["sz_amount"] = sz_index.get("amount")  # 千元
            
            # 计算两市总成交额 (保持千元单位，客户端显示时再换算)
            if stats["sh_amount"] is not None and stats["sz_amount"] is not None:
                stats["total_amount"] = stats["sh_amount"] + stats["sz_amount"]
                self.logger.info(
                    f"Market turnover: SH={stats['sh_amount']}, SZ={stats['sz_amount']}, Total={stats['total_amount']} (千元)"
                )
        except Exception as e:
            self.logger.warning(f"Failed to get market turnover: {e}")
        
        # 计算衍生指标
        total_stocks = stats["up_count"] + stats["down_count"] + stats["flat_count"]
        stats["total_stocks"] = total_stocks
        stats["up_ratio"] = round(stats["up_count"] / total_stocks * 100, 2) if total_stocks > 0 else 0
        stats["down_ratio"] = round(stats["down_count"] / total_stocks * 100, 2) if total_stocks > 0 else 0
        
        # 总涨停数
        stats["total_limit_up"] = (
            stats["limit_1"] + stats["limit_2"] + stats["limit_3"] + 
            stats["limit_4"] + stats["limit_5"] + stats["limit_6_plus"]
        )
        
        # 写入数据库 (upsert)
        await mongo_manager.update_one(
            "daily_stats",
            {"trade_date": trade_date},
            {"$set": stats},
            upsert=True,
        )
        
        return stats
