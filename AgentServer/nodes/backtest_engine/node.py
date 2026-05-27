"""
回测引擎节点

作为独立的计算节点运行，通过 RPC 接收回测任务请求。
支持水平扩展，多节点并行执行回测任务。
"""

import asyncio
from typing import Optional, Dict, Any
from datetime import datetime
import traceback

import pandas as pd

from nodes.base import BaseNode
from common.utils import convert_numpy_types
from core.protocols import NodeType
from core.managers import redis_manager, mongo_manager, tushare_manager

from .factors import FactorData
from .backtester import VectorizedBacktester, BacktestConfig
from .performance import PerformanceAnalyzer
from .factor_selection import PortfolioBacktester


class BacktestNode(BaseNode):
    """
    回测引擎节点
    
    职责:
    - 接收回测任务请求 (通过 RPC)
    - 执行向量化回测计算
    - 返回绩效分析结果
    
    特性:
    - 独立计算节点，可水平扩展
    - 支持异步任务执行
    - 任务结果持久化到 MongoDB
    """
    
    node_type = NodeType.BACKTEST
    DEFAULT_RPC_PORT = 50056  # BacktestNode 默认 RPC 端口
    
    def __init__(self, node_id: Optional[str] = None, rpc_port: int = 0):
        from core.settings import settings
        # 从 settings 获取端口，如果没有则使用默认值
        port = rpc_port or getattr(settings.rpc, 'backtest_port', self.DEFAULT_RPC_PORT)
        super().__init__(node_id, port)
        
        # 任务队列
        self._task_queue: asyncio.Queue = asyncio.Queue()
        self._running_tasks: Dict[str, asyncio.Task] = {}
        
        # 工作协程数
        self._worker_count = 2
    
    async def start(self) -> None:
        """启动回测节点"""
        self.logger.info("Starting Backtest Node...")
        
        # 初始化管理器
        self.logger.info("Initializing managers...")
        await redis_manager.initialize()
        await mongo_manager.initialize()
        await tushare_manager.initialize()
        
        # 启动 RPC 服务器
        await self._start_rpc_server()
        
        # 启动工作协程
        self._start_workers()
        
        self.logger.info(f"Backtest Node started: {self.node_id}")
        self.logger.info(f"RPC listening on port {self._rpc_port}")
    
    async def stop(self) -> None:
        """停止回测节点"""
        self.logger.info("Stopping Backtest Node...")
        
        # 取消所有运行中的任务
        for task_id, task in self._running_tasks.items():
            if not task.done():
                task.cancel()
                self.logger.info(f"Cancelled task: {task_id}")
        
        await super().stop()
    
    async def run(self) -> None:
        """节点主循环 - 保持节点运行"""
        self.logger.info("Backtest Node is running, waiting for tasks...")
        
        # 回测节点主要通过 RPC 接收任务，这里只需保持运行
        while self._running:
            await asyncio.sleep(1)
    
    def _register_rpc_methods(self) -> None:
        """注册 RPC 方法"""
        super()._register_rpc_methods()
        
        # 注册回测方法
        self.register_rpc_method("run_backtest", self._handle_run_backtest)
        self.register_rpc_method("run_factor_selection", self._handle_run_factor_selection)
        self.register_rpc_method("get_task_status", self._handle_get_task_status)
        self.register_rpc_method("cancel_task", self._handle_cancel_task)
    
    def _start_workers(self) -> None:
        """启动工作协程"""
        for i in range(self._worker_count):
            asyncio.create_task(self._worker_loop(i))
            self.logger.info(f"Started worker {i}")
    
    async def _worker_loop(self, worker_id: int) -> None:
        """工作协程循环"""
        while True:
            try:
                # 从队列获取任务
                task_info = await self._task_queue.get()
                task_id = task_info["task_id"]
                task_type = task_info.get("task_type", "single_stock")
                
                self.logger.info(f"[Worker-{worker_id}] Processing {task_type} task: {task_id}")
                
                try:
                    # 根据任务类型执行不同的回测
                    if task_type == "factor_selection":
                        result = await self._execute_factor_selection(task_info)
                    else:
                        result = await self._execute_backtest(task_info)
                    
                    # 更新任务状态
                    await self._update_task_result(task_id, "completed", result)
                    
                except Exception as e:
                    self.logger.error(f"[Worker-{worker_id}] Task {task_id} failed: {e}")
                    traceback.print_exc()
                    await self._update_task_result(task_id, "failed", error=str(e))
                
                finally:
                    self._task_queue.task_done()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.exception(f"[Worker-{worker_id}] Unexpected error: {e}")
    
    async def _handle_run_backtest(self, params: dict) -> dict:
        """
        处理回测 RPC 请求
        
        任务投递到队列后立即返回，不阻塞等待执行结果。
        客户端需要通过 get_task_status 查询任务状态和结果。
        
        Args:
            params: 回测参数
                - task_id: 任务ID
                - ts_code: 股票代码
                - start_date: 开始日期
                - end_date: 结束日期
                - initial_cash: 初始资金
                - entry_threshold: 买入阈值
                - exit_threshold: 卖出阈值
                - factor_weights: 因子权重
        
        Returns:
            任务投递状态
        """
        task_id = params.get("task_id", f"bt_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}")
        
        self.logger.info(f"Received backtest request: {task_id}")
        
        # 记录任务
        await mongo_manager.update_one(
            "backtest_tasks",
            {"task_id": task_id},
            {
                "$set": {
                    "task_id": task_id,
                    "status": "queued",
                    "params": params,
                    "node_id": self.node_id,
                    "created_at": datetime.utcnow(),
                }
            },
            upsert=True,
        )
        
        # 异步执行，加入队列后立即返回
        await self._task_queue.put({"task_id": task_id, **params})
        
        return {
            "success": True,
            "task_id": task_id,
            "status": "queued",
            "queue_size": self._task_queue.qsize(),
        }
    
    async def _handle_get_task_status(self, params: dict) -> dict:
        """查询任务状态"""
        task_id = params.get("task_id")
        if not task_id:
            return {"success": False, "error": "task_id is required"}
        
        record = await mongo_manager.find_one(
            "backtest_tasks",
            {"task_id": task_id},
        )
        
        if not record:
            return {"success": False, "error": "Task not found"}
        
        return {
            "success": True,
            "task_id": task_id,
            "status": record.get("status"),
            "created_at": record.get("created_at", "").isoformat() if record.get("created_at") else None,
            "completed_at": record.get("completed_at", "").isoformat() if record.get("completed_at") else None,
            "error": record.get("error"),
        }
    
    async def _handle_cancel_task(self, params: dict) -> dict:
        """取消任务"""
        task_id = params.get("task_id")
        if not task_id:
            return {"success": False, "error": "task_id is required"}
        
        # 更新数据库状态
        result = await mongo_manager.update_one(
            "backtest_tasks",
            {"task_id": task_id, "status": {"$in": ["pending", "queued"]}},
            {"$set": {"status": "cancelled", "cancelled_at": datetime.utcnow()}},
        )
        
        if result.modified_count > 0:
            return {"success": True, "task_id": task_id, "status": "cancelled"}
        else:
            return {"success": False, "error": "Task cannot be cancelled (already running or completed)"}
    
    async def _handle_run_factor_selection(self, params: dict) -> dict:
        """
        处理因子选股回测 RPC 请求
        
        Args:
            params: 回测参数
                - task_id: 任务ID
                - universe: 股票池类型 ("all_a")
                - start_date: 开始日期
                - end_date: 结束日期
                - initial_cash: 初始资金
                - rebalance_freq: 调仓频率 ("monthly" | "weekly" | "daily")
                - top_n: 选股数量
                - weight_method: 权重方法 ("equal" | "factor_weighted")
                - factors: 因子配置列表
                - exclude: 排除规则列表
                - benchmark: 基准指数代码
        
        Returns:
            任务投递状态
        """
        task_id = params.get("task_id", f"fs_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}")
        
        self.logger.info(f"Received factor selection backtest request: {task_id}")
        
        # 记录任务
        await mongo_manager.update_one(
            "backtest_tasks",
            {"task_id": task_id},
            {
                "$set": {
                    "task_id": task_id,
                    "task_type": "factor_selection",
                    "status": "queued",
                    "params": params,
                    "node_id": self.node_id,
                    "created_at": datetime.utcnow(),
                }
            },
            upsert=True,
        )
        
        # 加入任务队列
        task_info = {"task_id": task_id, "task_type": "factor_selection", **params}
        await self._task_queue.put(task_info)
        
        return {
            "success": True,
            "task_id": task_id,
            "status": "queued",
            "queue_size": self._task_queue.qsize(),
        }
    
    async def _execute_factor_selection(self, params: dict) -> dict:
        """
        执行因子选股回测
        
        Args:
            params: 回测参数
            
        Returns:
            回测报告
        """
        task_id = params.get("task_id", "unknown")
        
        self.logger.info(
            f"[{task_id}] Executing factor selection: "
            f"{params.get('start_date')} ~ {params.get('end_date')}"
        )
        
        # 更新状态为 running
        await mongo_manager.update_one(
            "backtest_tasks",
            {"task_id": task_id},
            {"$set": {"status": "running", "started_at": datetime.utcnow()}},
        )
        
        # 构建回测配置
        config = {
            "universe": params.get("universe", "all_a"),
            "start_date": params["start_date"],
            "end_date": params["end_date"],
            "initial_cash": params.get("initial_cash", 1000000),
            "rebalance_freq": params.get("rebalance_freq", "monthly"),
            "top_n": params.get("top_n", 20),
            "weight_method": params.get("weight_method", "equal"),
            "factors": params.get("factors", []),
            "exclude": params.get("exclude", ["st", "new_stock"]),
            "benchmark": params.get("benchmark", "000300.SH"),
        }
        
        # 执行组合回测
        backtester = PortfolioBacktester()
        result = await backtester.run(config)
        
        self.logger.info(
            f"[{task_id}] Factor selection completed: "
            f"return={result.get('performance', {}).get('total_return', 0):.2f}%"
        )
        
        return result
    
    async def _execute_backtest(self, params: dict) -> dict:
        """
        执行单股回测
        
        Args:
            params: 回测参数
            
        Returns:
            回测报告
        """
        task_id = params.get("task_id", "unknown")
        ts_code = params["ts_code"]
        start_date = params["start_date"]
        end_date = params["end_date"]
        
        self.logger.info(f"[{task_id}] Executing backtest: {ts_code} ({start_date} ~ {end_date})")
        
        # 更新状态为 running
        await mongo_manager.update_one(
            "backtest_tasks",
            {"task_id": task_id},
            {"$set": {"status": "running", "started_at": datetime.utcnow()}},
        )
        
        # 1. 获取行情数据
        price_data = await self._fetch_price_data(ts_code, start_date, end_date)
        
        if price_data.empty:
            raise ValueError(f"No price data found for {ts_code}")
        
        self.logger.info(f"[{task_id}] Loaded {len(price_data)} days of price data")
        
        # 2. 构建因子数据
        factor_data = FactorData(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            price_data=price_data,
        )
        
        # 3. 自动计算技术指标
        if params.get("auto_technical", True):
            factor_data.add_technical_indicators()
        
        # 4. 配置回测
        weights = params.get("factor_weights", {})
        if not weights:
            weights = {
                "tech_rsi": 0.25,
                "tech_macd_signal": 0.25,
                "tech_price_position": 0.25,
                "tech_vol_ma5": 0.25,
            }
        
        config = BacktestConfig(
            initial_cash=params.get("initial_cash", 100000),
            entry_threshold=params.get("entry_threshold", 0.7),
            exit_threshold=params.get("exit_threshold", 0.3),
            position_size=params.get("position_size", 1.0),
            factor_weights=weights,
        )
        
        # 5. 执行回测
        backtester = VectorizedBacktester(config)
        result = backtester.run(factor_data)
        
        if not result.success:
            raise ValueError(result.error_message)
        
        # 6. 分析绩效
        analyzer = PerformanceAnalyzer()
        metrics = analyzer.analyze(result)
        report = analyzer.generate_report(result, metrics)
        
        self.logger.info(
            f"[{task_id}] Backtest completed: "
            f"return={metrics.total_return_pct:.2f}%, "
            f"sharpe={metrics.sharpe_ratio:.2f}, "
            f"max_dd={metrics.max_drawdown_pct:.2f}%"
        )
        
        return report
    
    async def _fetch_price_data(
        self,
        ts_code: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """从数据库获取行情数据"""
        records = await mongo_manager.find_many(
            "stock_daily",
            {
                "ts_code": ts_code,
                "trade_date": {"$gte": start_date, "$lte": end_date},
            },
            sort=[("trade_date", 1)],
        )
        
        if not records:
            return pd.DataFrame()
        
        df = pd.DataFrame(records)
        
        # 转换日期索引
        df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")
        df.set_index("trade_date", inplace=True)
        df.sort_index(inplace=True)
        
        # 重命名列
        column_map = {
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "vol": "volume",
            "amount": "amount",
            "up_limit": "up_limit",
            "down_limit": "down_limit",
        }
        
        df = df.rename(columns=column_map)
        
        # 确保必要列存在
        required = ["open", "high", "low", "close", "volume"]
        for col in required:
            if col not in df.columns:
                if col == "volume" and "vol" in df.columns:
                    df["volume"] = df["vol"]
                else:
                    raise ValueError(f"Missing required column: {col}")
        
        return df
    
    async def _update_task_result(
        self,
        task_id: str,
        status: str,
        result: dict = None,
        error: str = None,
    ) -> None:
        """更新任务结果"""
        update_data = {
            "status": status,
            "completed_at": datetime.utcnow(),
        }
        
        if result:
            # 转换 numpy 类型为 Python 原生类型，避免 MongoDB 序列化错误
            update_data["result"] = convert_numpy_types(result)
        
        if error:
            update_data["error"] = error
        
        await mongo_manager.update_one(
            "backtest_tasks",
            {"task_id": task_id},
            {"$set": update_data},
        )


# ==================== 启动入口 ====================


async def main():
    """回测节点启动入口"""
    import logging
    from core.logging import setup_logging
    
    setup_logging()
    logger = logging.getLogger("backtest_node")
    
    node = BacktestNode()
    
    try:
        await node.start()
        
        # 保持运行
        logger.info("Backtest Node is running. Press Ctrl+C to stop.")
        while True:
            await asyncio.sleep(3600)
            
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    finally:
        await node.stop()


if __name__ == "__main__":
    asyncio.run(main())
