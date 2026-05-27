"""Additional agent-native tools built on existing StockAgent data."""

from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from core.base import BaseTool, ToolResult
from core.managers import mongo_manager, tushare_manager
from core.rpc import RPCClient


class MarketOverviewInput(BaseModel):
    """Input for market overview."""

    trade_date: Optional[str] = Field(None, description="交易日期 YYYYMMDD，不填取最新")


class MarketOverviewOutput(ToolResult):
    """Market overview output."""

    data: Dict[str, Any] = Field(default_factory=dict)


class StockBasicInput(BaseModel):
    """Input for stock basic query."""

    ts_code: Optional[str] = Field(None, description="股票代码，如 000001.SZ")
    name: Optional[str] = Field(None, description="股票名称，支持模糊搜索")
    limit: int = Field(default=10, ge=1, le=50, description="返回数量")


class StockBasicOutput(ToolResult):
    """Stock basic output."""

    data: List[Dict[str, Any]] = Field(default_factory=list)


class StockBasicTool(BaseTool[StockBasicInput, StockBasicOutput]):
    """Get stock basic information from MongoDB."""

    name = "get_stock_basic"
    description = "获取股票基础信息，包括代码、名称、行业、上市日期等"
    input_model = StockBasicInput
    output_model = StockBasicOutput

    async def execute(self, input_data: StockBasicInput) -> StockBasicOutput:
        if input_data.ts_code:
            ts_code = input_data.ts_code.upper()
            stock = await mongo_manager.find_one(
                "stock_basic",
                {"ts_code": ts_code},
                projection={"_id": 0},
            )
            if stock:
                return StockBasicOutput(data=[stock])
            return StockBasicOutput(
                data=[
                    {
                        "ts_code": ts_code,
                        "name": input_data.name,
                        "data_status": "fallback_minimal",
                        "note": "本地 stock_basic 未同步；当前环境可能只有 Tushare 日线权限。",
                    }
                ]
            )

        query: Dict[str, Any] = {}
        if input_data.name:
            query["name"] = {"$regex": input_data.name}

        data = await mongo_manager.find_many(
            "stock_basic",
            query,
            projection={"_id": 0},
            limit=input_data.limit,
        )
        return StockBasicOutput(data=data)


class StockDailyInput(BaseModel):
    """Input for stock daily query."""

    ts_code: str = Field(..., description="股票代码，如 000001.SZ")
    start_date: Optional[str] = Field(None, description="开始日期 YYYYMMDD")
    end_date: Optional[str] = Field(None, description="结束日期 YYYYMMDD")
    limit: int = Field(default=30, ge=1, le=300, description="返回条数")


class StockDailyOutput(ToolResult):
    """Stock daily output."""

    data: List[Dict[str, Any]] = Field(default_factory=list)


class StockDailyTool(BaseTool[StockDailyInput, StockDailyOutput]):
    """Get stock daily bars from MongoDB, falling back to Tushare daily."""

    name = "get_stock_daily"
    description = "获取股票日线行情数据，包括开高低收、成交量等"
    input_model = StockDailyInput
    output_model = StockDailyOutput

    async def execute(self, input_data: StockDailyInput) -> StockDailyOutput:
        ts_code = input_data.ts_code.upper()
        query: Dict[str, Any] = {"ts_code": ts_code}
        if input_data.start_date:
            query["trade_date"] = {"$gte": input_data.start_date}
        if input_data.end_date:
            query.setdefault("trade_date", {})["$lte"] = input_data.end_date

        data = await mongo_manager.find_many(
            "stock_daily",
            query,
            projection={"_id": 0},
            sort=[("trade_date", -1)],
            limit=input_data.limit,
        )
        if data:
            return StockDailyOutput(data=data)

        start_date = input_data.start_date
        end_date = input_data.end_date or date.today().strftime("%Y%m%d")
        if not start_date:
            start_date = (date.today() - timedelta(days=max(90, input_data.limit * 3))).strftime("%Y%m%d")

        records = await tushare_manager.get_daily(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
        )
        records = sorted(records, key=lambda item: str(item.get("trade_date", "")), reverse=True)
        records = records[: input_data.limit]
        for record in records:
            record["data_status"] = "tushare_daily_fallback"

        if records:
            await mongo_manager.bulk_upsert(
                collection="stock_daily",
                documents=records,
                key_fields=["ts_code", "trade_date"],
                batch_size=500,
            )
        return StockDailyOutput(data=records)


class NewsSentimentInput(BaseModel):
    """Input for news query."""

    keyword: Optional[str] = Field(None, description="关键词")
    limit: int = Field(default=10, ge=1, le=50, description="返回条数")


class NewsSentimentOutput(ToolResult):
    """News output."""

    data: List[Dict[str, Any]] = Field(default_factory=list)


class NewsSentimentTool(BaseTool[NewsSentimentInput, NewsSentimentOutput]):
    """Get latest news from MongoDB."""

    name = "get_news_sentiment"
    description = "获取财经新闻和舆情信息"
    input_model = NewsSentimentInput
    output_model = NewsSentimentOutput

    async def execute(self, input_data: NewsSentimentInput) -> NewsSentimentOutput:
        query: Dict[str, Any] = {}
        if input_data.keyword:
            query["$or"] = [
                {"title": {"$regex": input_data.keyword}},
                {"content": {"$regex": input_data.keyword}},
            ]
        data = await mongo_manager.find_many(
            "news",
            query,
            projection={"_id": 0},
            sort=[("datetime", -1)],
            limit=input_data.limit,
        )
        return NewsSentimentOutput(data=data)


class MarketOverviewTool(BaseTool[MarketOverviewInput, MarketOverviewOutput]):
    """Get broad market overview from MongoDB."""

    name = "get_market_overview"
    description = "获取大盘概览，包括指数、涨跌统计、涨跌停数量、成交额和热门板块"
    input_model = MarketOverviewInput
    output_model = MarketOverviewOutput

    async def execute(self, input_data: MarketOverviewInput) -> MarketOverviewOutput:
        query = {}
        if input_data.trade_date:
            query["trade_date"] = input_data.trade_date

        stats = await mongo_manager.find_one(
            "daily_stats",
            query,
            projection={"_id": 0},
            sort=[("trade_date", -1)],
        )
        if not stats:
            return MarketOverviewOutput(data={"message": "No market stats available"})

        trade_date = stats.get("trade_date", "")
        indexes = await mongo_manager.find_many(
            "index_daily",
            {"trade_date": trade_date, "ts_code": {"$in": ["000001.SH", "399001.SZ", "399006.SZ"]}},
            projection={"_id": 0},
            limit=3,
        )
        hot_sectors = await mongo_manager.find_many(
            "sector_ranking",
            {"trade_date": trade_date, "ranking_type": "industry_top"},
            projection={"_id": 0, "name": 1, "rank": 1, "score": 1},
            sort=[("rank", 1)],
            limit=8,
        )

        return MarketOverviewOutput(
            data={
                "trade_date": trade_date,
                "stats": stats,
                "indexes": indexes,
                "hot_sectors": hot_sectors,
            }
        )


class MarketLatestAnalysisInput(BaseModel):
    """Input for latest market analysis."""

    include_stats: bool = Field(default=True, description="是否包含每日统计数据")


class MarketLatestAnalysisOutput(ToolResult):
    """Latest market analysis output."""

    data: Dict[str, Any] = Field(default_factory=dict)


class MarketLatestAnalysisTool(BaseTool[MarketLatestAnalysisInput, MarketLatestAnalysisOutput]):
    """Get latest sentiment/cycle market analysis."""

    name = "get_market_latest_analysis"
    description = "获取最新市场情绪周期分析，包括情绪分、强度分、周期判断和原因"
    input_model = MarketLatestAnalysisInput
    output_model = MarketLatestAnalysisOutput

    async def execute(self, input_data: MarketLatestAnalysisInput) -> MarketLatestAnalysisOutput:
        latest_stats = await mongo_manager.find_one(
            "daily_stats",
            {},
            projection={"_id": 0},
            sort=[("trade_date", -1)],
        )
        if not latest_stats:
            return MarketLatestAnalysisOutput(data={"message": "No market data available"})

        trade_date = latest_stats.get("trade_date", "")
        analysis = await mongo_manager.find_one(
            "market_analysis",
            {"trade_date": trade_date},
            projection={"_id": 0},
        )

        data = {
            "trade_date": trade_date,
            "analysis": analysis or {},
        }
        if input_data.include_stats:
            data["stats"] = latest_stats

        return MarketLatestAnalysisOutput(data=data)


class FinancialIndicatorInput(BaseModel):
    """Input for financial indicator query."""

    ts_code: str = Field(..., description="股票代码，如 000001.SZ")
    limit: int = Field(default=8, ge=1, le=24, description="返回最近报告期数量")


class FinancialIndicatorOutput(ToolResult):
    """Financial indicator output."""

    data: List[Dict[str, Any]] = Field(default_factory=list)


class FinancialIndicatorTool(BaseTool[FinancialIndicatorInput, FinancialIndicatorOutput]):
    """Get financial indicators from MongoDB."""

    name = "get_financial_indicator"
    description = "获取股票财务指标，包括 ROE、ROA、毛利率、净利率、EPS 等历史报告期数据"
    input_model = FinancialIndicatorInput
    output_model = FinancialIndicatorOutput

    async def execute(self, input_data: FinancialIndicatorInput) -> FinancialIndicatorOutput:
        data = await mongo_manager.find_many(
            "fina_indicator",
            {"ts_code": input_data.ts_code.upper()},
            projection={"_id": 0},
            sort=[("end_date", -1)],
            limit=input_data.limit,
        )
        return FinancialIndicatorOutput(data=data)


class BacktestRecentResultsInput(BaseModel):
    """Input for recent backtest result lookup."""

    ts_code: Optional[str] = Field(None, description="股票代码，可选")
    limit: int = Field(default=5, ge=1, le=20, description="返回数量")


class BacktestRecentResultsOutput(ToolResult):
    """Recent backtest results output."""

    data: List[Dict[str, Any]] = Field(default_factory=list)


class BacktestRecentResultsTool(BaseTool[BacktestRecentResultsInput, BacktestRecentResultsOutput]):
    """Read recent completed backtest results."""

    name = "get_recent_backtest_results"
    description = "读取最近完成的回测结果，用于复盘策略表现和历史任务"
    input_model = BacktestRecentResultsInput
    output_model = BacktestRecentResultsOutput

    async def execute(self, input_data: BacktestRecentResultsInput) -> BacktestRecentResultsOutput:
        query: Dict[str, Any] = {"status": "completed"}
        if input_data.ts_code:
            query["params.ts_code"] = input_data.ts_code.upper()

        data = await mongo_manager.find_many(
            "backtest_tasks",
            query,
            projection={"_id": 0},
            sort=[("completed_at", -1), ("created_at", -1)],
            limit=input_data.limit,
        )
        return BacktestRecentResultsOutput(data=data)


class ListBacktestFactorsInput(BaseModel):
    """Input for available backtest factors."""

    grouped: bool = Field(default=True, description="是否按分类分组返回")


class ListBacktestFactorsOutput(ToolResult):
    """Available factor list output."""

    factors: List[Dict[str, Any]] = Field(default_factory=list)
    grouped: Dict[str, List[Dict[str, Any]]] = Field(default_factory=dict)


class ListBacktestFactorsTool(BaseTool[ListBacktestFactorsInput, ListBacktestFactorsOutput]):
    """List factors available for factor-selection backtests."""

    name = "list_backtest_factors"
    description = "列出因子选股回测可用的因子名称、分类和说明，供构建因子组合策略使用"
    input_model = ListBacktestFactorsInput
    output_model = ListBacktestFactorsOutput

    async def execute(self, input_data: ListBacktestFactorsInput) -> ListBacktestFactorsOutput:
        from nodes.backtest_engine.factor_selection import FactorLibrary

        factors = FactorLibrary.list_factors()
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        if input_data.grouped:
            for factor in factors:
                category = factor.get("category", "other")
                grouped.setdefault(category, []).append(factor)
        return ListBacktestFactorsOutput(factors=factors, grouped=grouped)


class QueryBacktestResultInput(BaseModel):
    """Input for querying a backtest task."""

    task_id: str = Field(..., description="回测任务 ID，例如 bt_xxx 或 fs_xxx")
    user_id: Optional[str] = Field(None, description="当前用户 ID，由运行时注入")


class QueryBacktestResultOutput(ToolResult):
    """Backtest task status/result output."""

    task_id: str = ""
    status: str = ""
    message: str = ""
    result: Dict[str, Any] = Field(default_factory=dict)
    error: Optional[str] = None


class QueryBacktestResultTool(BaseTool[QueryBacktestResultInput, QueryBacktestResultOutput]):
    """Read a backtest task status or result."""

    name = "get_backtest_result"
    description = "查询指定回测任务的状态和结果。任务未完成时返回 pending/queued/running，完成后返回绩效指标和报告。"
    input_model = QueryBacktestResultInput
    output_model = QueryBacktestResultOutput

    async def execute(self, input_data: QueryBacktestResultInput) -> QueryBacktestResultOutput:
        record = await mongo_manager.find_one(
            "backtest_tasks",
            {"task_id": input_data.task_id},
            projection={"_id": 0},
        )
        if not record:
            return QueryBacktestResultOutput(
                success=False,
                task_id=input_data.task_id,
                status="not_found",
                error_message="Backtest task not found",
                error="Backtest task not found",
            )

        owner = record.get("params", {}).get("user_id")
        if owner and input_data.user_id and owner != input_data.user_id:
            return QueryBacktestResultOutput(
                success=False,
                task_id=input_data.task_id,
                status="forbidden",
                error_message="No permission to access this backtest task",
                error="No permission to access this backtest task",
            )

        status = record.get("status", "unknown")
        if status in {"pending", "queued"}:
            message = "回测任务等待中"
        elif status == "running":
            message = "回测任务执行中"
        elif status == "completed":
            message = "回测任务已完成"
        elif status == "failed":
            message = "回测任务失败"
        elif status == "cancelled":
            message = "回测任务已取消"
        else:
            message = "回测任务状态未知"

        return QueryBacktestResultOutput(
            success=status not in {"failed", "forbidden", "not_found"},
            task_id=input_data.task_id,
            status=status,
            message=message,
            result=record.get("result") or {},
            error=record.get("error"),
            error_message=record.get("error") if status == "failed" else None,
        )


class SubmitBacktestInput(BaseModel):
    """Input for submitting an asynchronous backtest task."""

    ts_code: str = Field(..., description="股票代码，如 000001.SZ")
    start_date: str = Field(..., description="开始日期 YYYYMMDD")
    end_date: str = Field(..., description="结束日期 YYYYMMDD")
    initial_cash: float = Field(default=100000.0, ge=10000, le=100000000, description="初始资金")
    entry_threshold: float = Field(default=0.7, ge=0.5, le=0.95, description="买入阈值")
    exit_threshold: float = Field(default=0.3, ge=0.05, le=0.5, description="卖出阈值")
    position_size: float = Field(default=1.0, ge=0.1, le=1.0, description="仓位比例")
    factor_weights: Dict[str, float] = Field(default_factory=dict, description="因子权重")
    auto_technical: bool = Field(default=True, description="是否自动计算技术指标")
    idempotency_key: Optional[str] = Field(None, description="Agent 工具幂等键，防止重复提交")
    user_id: Optional[str] = Field(None, description="当前用户 ID，由运行时注入")


class SubmitBacktestOutput(ToolResult):
    """Submit backtest output."""

    task_id: str = ""
    status: str = ""
    message: str = ""
    rpc_results: List[Dict[str, Any]] = Field(default_factory=list)


class SubmitBacktestTool(BaseTool[SubmitBacktestInput, SubmitBacktestOutput]):
    """Submit an async backtest task through BacktestNode RPC."""

    name = "submit_backtest"
    description = "提交单股回测任务。该工具会占用回测节点计算资源，必须经过用户审批。"
    input_model = SubmitBacktestInput
    output_model = SubmitBacktestOutput

    async def execute(self, input_data: SubmitBacktestInput) -> SubmitBacktestOutput:
        import hashlib

        seed = input_data.idempotency_key or (
            f"{input_data.ts_code}:{input_data.start_date}:{input_data.end_date}:"
            f"{input_data.initial_cash}:{input_data.entry_threshold}:{input_data.exit_threshold}"
        )
        task_id = f"bt_agent_{hashlib.sha256(seed.encode('utf-8')).hexdigest()[:12]}"
        params = {
            "task_id": task_id,
            "user_id": input_data.user_id or "agent",
            "ts_code": input_data.ts_code.upper(),
            "start_date": input_data.start_date,
            "end_date": input_data.end_date,
            "initial_cash": input_data.initial_cash,
            "entry_threshold": input_data.entry_threshold,
            "exit_threshold": input_data.exit_threshold,
            "position_size": input_data.position_size,
            "factor_weights": input_data.factor_weights,
            "auto_technical": input_data.auto_technical,
        }

        rpc_client = RPCClient()
        results = await rpc_client.broadcast_by_type(
            node_type="backtest",
            method="run_backtest",
            params=params,
            timeout=10.0,
            source_node="agent-runtime",
        )

        if not results:
            return SubmitBacktestOutput(
                success=False,
                error_message="No BacktestNode available",
                task_id=task_id,
                status="failed",
                message="未发现可用回测节点，请先启动 NODE_TYPE=backtest",
            )

        first = results[0]
        success = bool(first.get("success"))
        rpc_result = first.get("result", {}) or {}
        return SubmitBacktestOutput(
            success=success,
            error_message=first.get("error") if not success else None,
            task_id=task_id,
            status=rpc_result.get("status", "queued" if success else "failed"),
            message="回测任务已提交" if success else "回测任务提交失败",
            rpc_results=results,
        )


class BacktestFactorConfigInput(BaseModel):
    """Factor config for factor-selection backtest."""

    name: str = Field(..., description="因子名称，例如 momentum_20d、pb、roe")
    weight: float = Field(default=1.0, ge=0, le=1.0, description="因子权重")
    direction: Optional[str] = Field(default=None, description="因子方向：asc 越小越好，desc 越大越好；不填使用默认")


class SubmitFactorSelectionBacktestInput(BaseModel):
    """Input for submitting a factor-selection portfolio backtest."""

    universe: str = Field(default="all_a", description="股票池类型，目前通常使用 all_a")
    start_date: str = Field(..., description="开始日期 YYYYMMDD")
    end_date: str = Field(..., description="结束日期 YYYYMMDD")
    initial_cash: float = Field(default=1000000.0, ge=100000, le=100000000, description="初始资金")
    rebalance_freq: str = Field(default="monthly", description="调仓频率：daily/weekly/monthly/quarterly")
    top_n: int = Field(default=20, ge=1, le=100, description="每期选股数量")
    weight_method: str = Field(default="equal", description="权重方法：equal/factor_weighted")
    factors: List[BacktestFactorConfigInput] = Field(..., min_length=1, description="因子配置列表")
    exclude: List[str] = Field(default=["st", "new_stock"], description="排除规则，例如 st、new_stock")
    benchmark: str = Field(default="000300.SH", description="基准指数代码")
    idempotency_key: Optional[str] = Field(None, description="Agent 工具幂等键，防止重复提交")
    user_id: Optional[str] = Field(None, description="当前用户 ID，由运行时注入")


class SubmitFactorSelectionBacktestOutput(ToolResult):
    """Submit factor-selection backtest output."""

    task_id: str = ""
    status: str = ""
    message: str = ""
    rpc_results: List[Dict[str, Any]] = Field(default_factory=list)


class SubmitFactorSelectionBacktestTool(
    BaseTool[SubmitFactorSelectionBacktestInput, SubmitFactorSelectionBacktestOutput]
):
    """Submit an async factor-selection backtest through BacktestNode RPC."""

    name = "submit_factor_selection_backtest"
    description = "提交因子选股组合回测任务，按多个因子选股并按频率调仓。该工具会占用回测节点计算资源，必须经过用户审批。"
    input_model = SubmitFactorSelectionBacktestInput
    output_model = SubmitFactorSelectionBacktestOutput

    async def execute(self, input_data: SubmitFactorSelectionBacktestInput) -> SubmitFactorSelectionBacktestOutput:
        import hashlib

        factor_payload = [factor.model_dump(exclude_none=True) for factor in input_data.factors]
        seed = input_data.idempotency_key or (
            f"{input_data.universe}:{input_data.start_date}:{input_data.end_date}:"
            f"{input_data.top_n}:{input_data.rebalance_freq}:{factor_payload}"
        )
        task_id = f"fs_agent_{hashlib.sha256(seed.encode('utf-8')).hexdigest()[:12]}"
        params = {
            "task_id": task_id,
            "task_type": "factor_selection",
            "user_id": input_data.user_id or "agent",
            "universe": input_data.universe,
            "start_date": input_data.start_date,
            "end_date": input_data.end_date,
            "initial_cash": input_data.initial_cash,
            "rebalance_freq": input_data.rebalance_freq,
            "top_n": input_data.top_n,
            "weight_method": input_data.weight_method,
            "factors": factor_payload,
            "exclude": input_data.exclude,
            "benchmark": input_data.benchmark,
        }

        rpc_client = RPCClient()
        results = await rpc_client.broadcast_by_type(
            node_type="backtest",
            method="run_factor_selection",
            params=params,
            timeout=10.0,
            source_node="agent-runtime",
        )

        if not results:
            return SubmitFactorSelectionBacktestOutput(
                success=False,
                error_message="No BacktestNode available",
                task_id=task_id,
                status="failed",
                message="未发现可用回测节点，请先启动 NODE_TYPE=backtest",
            )

        first = results[0]
        success = bool(first.get("success"))
        rpc_result = first.get("result", {}) or {}
        return SubmitFactorSelectionBacktestOutput(
            success=success,
            error_message=first.get("error") if not success else None,
            task_id=task_id,
            status=rpc_result.get("status", "queued" if success else "failed"),
            message="因子选股回测任务已提交" if success else "因子选股回测任务提交失败",
            rpc_results=results,
        )
