"""
回测 API

提供量化回测任务的提交、查询和管理接口。

通过 RPC 调用 BacktestNode 执行回测任务。

支持：
- 提交回测任务 (异步执行)
- 查询任务进度和结果
- 取消任务
"""

import uuid
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from enum import Enum

from fastapi import APIRouter, HTTPException, Query, Depends, Request
from pydantic import BaseModel, Field, ValidationError

from core.managers import mongo_manager
from core.rpc import RPCClient
from .auth import get_current_user_id

router = APIRouter(prefix="/backtest", tags=["Backtest"])
logger = logging.getLogger("api.backtest")


# ==================== 数据模型 ====================


class TaskStatus(str, Enum):
    """任务状态"""
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class BacktestRequest(BaseModel):
    """回测请求"""
    ts_code: str = Field(..., description="股票代码", pattern=r"^\d{6}\.(SH|SZ|BJ)$")
    stock_name: Optional[str] = Field(default=None, description="股票名称")
    start_date: str = Field(..., description="开始日期", pattern=r"^\d{8}$")
    end_date: str = Field(..., description="结束日期", pattern=r"^\d{8}$")
    
    # 资金配置
    initial_cash: float = Field(default=100000.0, ge=10000, le=100000000, description="初始资金")
    
    # 信号阈值
    entry_threshold: float = Field(default=0.7, ge=0.5, le=0.95, description="买入阈值")
    exit_threshold: float = Field(default=0.3, ge=0.05, le=0.5, description="卖出阈值")
    
    # 仓位管理
    position_size: float = Field(default=1.0, ge=0.1, le=1.0, description="仓位比例")
    
    # 因子权重 (可选)
    factor_weights: Dict[str, float] = Field(
        default_factory=dict,
        description="因子权重配置，key 为因子名，value 为权重"
    )
    
    # 是否自动计算技术指标
    auto_technical: bool = Field(default=True, description="自动计算技术指标")
    
    class Config:
        json_schema_extra = {
            "example": {
                "ts_code": "000001.SZ",
                "start_date": "20240101",
                "end_date": "20241231",
                "initial_cash": 100000,
                "entry_threshold": 0.7,
                "exit_threshold": 0.3,
                "position_size": 1.0,
                "factor_weights": {
                    "tech_rsi": 0.3,
                    "tech_macd_signal": 0.3,
                    "tech_price_position": 0.4,
                },
                "auto_technical": True,
            }
        }


class FactorConfig(BaseModel):
    """因子配置"""
    name: str = Field(..., description="因子名称")
    weight: float = Field(default=1.0, ge=0, le=1.0, description="因子权重")
    direction: Optional[str] = Field(default=None, description="因子方向: asc(越大越好) / desc(越小越好)")


class FactorSelectionRequest(BaseModel):
    """因子选股回测请求"""
    universe: str = Field(default="all_a", description="股票池类型")
    start_date: str = Field(..., description="开始日期", pattern=r"^\d{8}$")
    end_date: str = Field(..., description="结束日期", pattern=r"^\d{8}$")
    
    initial_cash: float = Field(default=1000000.0, ge=100000, le=100000000, description="初始资金")
    rebalance_freq: str = Field(default="monthly", description="调仓频率: daily/weekly/monthly/quarterly")
    top_n: int = Field(default=20, ge=1, le=100, description="选股数量")
    weight_method: str = Field(default="equal", description="权重方法: equal/factor_weighted")
    
    factors: List[FactorConfig] = Field(..., description="因子配置列表", min_length=1)
    exclude: List[str] = Field(default=["st", "new_stock"], description="排除规则")
    benchmark: str = Field(default="000300.SH", description="基准指数")
    
    class Config:
        json_schema_extra = {
            "example": {
                "universe": "all_a",
                "start_date": "20230101",
                "end_date": "20260101",
                "initial_cash": 1000000,
                "rebalance_freq": "monthly",
                "top_n": 20,
                "weight_method": "equal",
                "factors": [
                    {"name": "momentum_20d", "weight": 0.3},
                    {"name": "pb", "weight": 0.3},
                    {"name": "roe", "weight": 0.4},
                ],
                "exclude": ["st", "new_stock"],
                "benchmark": "000300.SH",
            }
        }


class BacktestTaskResponse(BaseModel):
    """回测任务响应"""
    task_id: str
    status: str
    message: str


# ==================== API 端点 ====================


@router.post("/submit", response_model=BacktestTaskResponse)
async def submit_backtest(
    request: BacktestRequest,
    user_id: str = Depends(get_current_user_id),
):
    """
    提交回测任务
    
    任务将通过 RPC 发送到 BacktestNode 异步执行，
    返回 task_id 用于查询进度。
    
    客户端需要轮询 /status/{task_id} 或 /result/{task_id} 获取结果。
    """
    task_id = f"bt_{uuid.uuid4().hex[:12]}"
    
    logger.info(f"[{task_id}] Backtest request from user {user_id}: {request.ts_code}")
    
    # 构建 RPC 参数
    rpc_params = {
        "task_id": task_id,
        "user_id": user_id,
        "ts_code": request.ts_code,
        "stock_name": request.stock_name,
        "start_date": request.start_date,
        "end_date": request.end_date,
        "initial_cash": request.initial_cash,
        "entry_threshold": request.entry_threshold,
        "exit_threshold": request.exit_threshold,
        "position_size": request.position_size,
        "factor_weights": request.factor_weights,
        "auto_technical": request.auto_technical,
    }
    
    # 通过 RPC 调用 BacktestNode (仅投递任务，不等待执行结果)
    rpc_client = RPCClient()
    
    try:
        results = await rpc_client.broadcast_by_type(
            node_type="backtest",
            method="run_backtest",
            params=rpc_params,
            timeout=10.0,  # 只等待任务投递确认，不等待执行
            source_node="web-node",
        )
        
        if not results:
            raise HTTPException(
                status_code=503,
                detail="No BacktestNode available. Please ensure backtest node is running."
            )
        
        # 取第一个响应
        first_result = results[0]
        
        if not first_result.get("success"):
            error_msg = first_result.get("error", "Unknown error")
            logger.error(f"[{task_id}] RPC failed: {error_msg}")
            raise HTTPException(status_code=500, detail=error_msg)
        
        rpc_response = first_result.get("result", {})
        
        return BacktestTaskResponse(
            task_id=task_id,
            status=rpc_response.get("status", "queued"),
            message="任务已提交到回测节点，请使用 task_id 查询进度",
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"[{task_id}] Failed to submit backtest: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status/{task_id}")
async def get_backtest_status(
    task_id: str,
    user_id: str = Depends(get_current_user_id),
) -> Dict[str, Any]:
    """
    查询回测任务状态
    """
    # 查 MongoDB
    record = await mongo_manager.find_one(
        "backtest_tasks",
        {"task_id": task_id},
    )
    
    if not record:
        raise HTTPException(status_code=404, detail="任务不存在")
    
    # 权限检查 (如果记录中有 user_id)
    if record.get("params", {}).get("user_id") and record["params"]["user_id"] != user_id:
        raise HTTPException(status_code=403, detail="无权访问此任务")
    
    return {
        "task_id": task_id,
        "status": record.get("status"),
        "created_at": record.get("created_at", "").isoformat() if record.get("created_at") else None,
        "started_at": record.get("started_at", "").isoformat() if record.get("started_at") else None,
        "completed_at": record.get("completed_at", "").isoformat() if record.get("completed_at") else None,
        "error": record.get("error"),
    }


@router.get("/result/{task_id}")
async def get_backtest_result(
    task_id: str,
    user_id: str = Depends(get_current_user_id),
) -> Dict[str, Any]:
    """
    获取回测结果
    
    仅当任务完成后可获取。
    """
    record = await mongo_manager.find_one(
        "backtest_tasks",
        {"task_id": task_id},
    )
    
    if not record:
        raise HTTPException(status_code=404, detail="任务不存在")
    
    # 权限检查
    if record.get("params", {}).get("user_id") and record["params"]["user_id"] != user_id:
        raise HTTPException(status_code=403, detail="无权访问此任务")
    
    status = record.get("status")
    
    if status == "pending" or status == "queued":
        return {"task_id": task_id, "status": status, "message": "任务等待中"}
    
    if status == "running":
        return {"task_id": task_id, "status": "running", "message": "任务执行中"}
    
    if status == "failed":
        return {
            "task_id": task_id,
            "status": "failed",
            "error": record.get("error", "Unknown error"),
        }
    
    if status == "cancelled":
        return {"task_id": task_id, "status": "cancelled", "message": "任务已取消"}
    
    return {
        "task_id": task_id,
        "status": "completed",
        "result": record.get("result"),
    }


@router.get("/history")
async def get_backtest_history(
    user_id: str = Depends(get_current_user_id),
    task_type: Optional[str] = Query(default=None, description="任务类型: single/factor_selection"),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> Dict[str, Any]:
    """
    获取用户的回测历史
    
    Args:
        task_type: 可选，筛选任务类型
            - None: 返回所有
            - "single": 单股回测
            - "factor_selection": 因子选股回测
    """
    query: Dict[str, Any] = {"params.user_id": user_id, "status": "completed"}
    
    # 根据任务类型筛选 (task_type 存储在顶层)
    if task_type == "single":
        # 单股回测：task_type 不存在或不等于 factor_selection
        query["$or"] = [
            {"task_type": {"$exists": False}},
            {"task_type": {"$ne": "factor_selection"}},
        ]
    elif task_type == "factor_selection":
        # 因子选股：task_type 等于 factor_selection
        query["task_type"] = "factor_selection"
    
    records = await mongo_manager.find_many(
        "backtest_tasks",
        query,
        sort=[("created_at", -1)],
        skip=offset,
        limit=limit,
    )
    
    # 简化返回数据
    items = []
    for r in records:
        result = r.get("result", {})
        summary = result.get("summary", {})
        metrics = result.get("metrics", {})
        params = r.get("params", {})
        task_type_val = params.get("task_type", "single")
        
        item = {
            "task_id": r.get("task_id"),
            "task_type": task_type_val,
            "start_date": params.get("start_date"),
            "end_date": params.get("end_date"),
            "created_at": r.get("created_at", "").isoformat() if r.get("created_at") else None,
        }
        
        if task_type_val == "factor_selection":
            # 因子选股回测特有字段 - 从 performance 中读取
            performance = result.get("performance", {})
            item.update({
                "top_n": params.get("top_n"),
                "rebalance_freq": params.get("rebalance_freq"),
                "factors_count": len(params.get("factors", [])),
                "total_return_pct": performance.get("total_return"),
                "sharpe_ratio": performance.get("sharpe_ratio"),
                "max_drawdown_pct": performance.get("max_drawdown"),
                "excess_return_pct": performance.get("excess_return"),
            })
        else:
            # 单股回测字段
            item.update({
                "ts_code": params.get("ts_code"),
                "stock_name": params.get("stock_name"),
                "total_return_pct": metrics.get("returns", {}).get("total_return_pct"),
                "sharpe_ratio": metrics.get("risk", {}).get("sharpe_ratio"),
                "max_drawdown_pct": metrics.get("risk", {}).get("max_drawdown_pct"),
            })
        
        items.append(item)
    
    return {
        "total": len(items),
        "items": items,
    }


@router.delete("/{task_id}")
async def cancel_backtest(
    task_id: str,
    user_id: str = Depends(get_current_user_id),
) -> Dict[str, Any]:
    """
    取消回测任务
    
    仅当任务处于 pending/queued 状态时可取消。
    """
    # 查 MongoDB
    record = await mongo_manager.find_one(
        "backtest_tasks",
        {"task_id": task_id},
    )
    
    if not record:
        raise HTTPException(status_code=404, detail="任务不存在")
    
    # 权限检查
    if record.get("params", {}).get("user_id") and record["params"]["user_id"] != user_id:
        raise HTTPException(status_code=403, detail="无权操作此任务")
    
    status = record.get("status")
    if status not in ["pending", "queued"]:
        raise HTTPException(
            status_code=400,
            detail=f"任务状态为 {status}，无法取消"
        )
    
    # 通过 RPC 取消任务
    rpc_client = RPCClient()
    
    try:
        results = await rpc_client.broadcast_by_type(
            node_type="backtest",
            method="cancel_task",
            params={"task_id": task_id},
            timeout=10.0,
            source_node="web-node",
        )
        
        if results and results[0].get("success"):
            return {"task_id": task_id, "status": "cancelled", "message": "任务已取消"}
        
    except Exception as e:
        logger.warning(f"Failed to cancel task via RPC: {e}")
    
    # 直接更新数据库
    await mongo_manager.update_one(
        "backtest_tasks",
        {"task_id": task_id},
        {"$set": {"status": "cancelled", "cancelled_at": datetime.utcnow()}},
    )
    
    return {"task_id": task_id, "status": "cancelled", "message": "任务已取消"}


# ==================== 因子选股回测 API ====================


@router.get("/factors")
async def list_available_factors() -> Dict[str, Any]:
    """
    获取可用的因子列表
    """
    from nodes.backtest_engine.factor_selection import FactorLibrary
    
    factors = FactorLibrary.list_factors()
    
    # 按分类分组
    grouped = {}
    for f in factors:
        category = f["category"]
        if category not in grouped:
            grouped[category] = []
        grouped[category].append(f)
    
    return {
        "factors": factors,
        "grouped": grouped,
    }


@router.post("/factor-selection", response_model=BacktestTaskResponse)
async def submit_factor_selection_backtest(
    raw_request: Request,
    user_id: str = Depends(get_current_user_id),
):
    """
    提交因子选股回测任务
    
    选股策略:
    1. 根据 universe 确定股票池范围 (目前支持全市场 all_a)
    2. 应用排除规则 (ST、次新股等)
    3. 计算每只股票的因子值并标准化
    4. 按综合得分选取 Top N 只股票
    5. 按指定频率调仓
    6. 计算组合收益并与基准对比
    
    返回 task_id 用于查询进度和结果。
    """
    # 解析请求体并验证
    try:
        body = await raw_request.json()
        logger.info(f"Factor selection request body: {body}")
        request = FactorSelectionRequest(**body)
    except ValidationError as e:
        logger.error(f"Validation error: {e.errors()}")
        raise HTTPException(status_code=422, detail=e.errors())
    task_id = f"fs_{uuid.uuid4().hex[:12]}"
    
    logger.info(
        f"[{task_id}] Factor selection backtest from user {user_id}: "
        f"{request.start_date} ~ {request.end_date}, {len(request.factors)} factors"
    )
    
    # 构建 RPC 参数
    rpc_params = {
        "task_id": task_id,
        "user_id": user_id,
        "universe": request.universe,
        "start_date": request.start_date,
        "end_date": request.end_date,
        "initial_cash": request.initial_cash,
        "rebalance_freq": request.rebalance_freq,
        "top_n": request.top_n,
        "weight_method": request.weight_method,
        "factors": [f.model_dump() for f in request.factors],
        "exclude": request.exclude,
        "benchmark": request.benchmark,
    }
    
    # 通过 RPC 调用 BacktestNode
    rpc_client = RPCClient()
    
    try:
        results = await rpc_client.broadcast_by_type(
            node_type="backtest",
            method="run_factor_selection",
            params=rpc_params,
            timeout=10.0,
            source_node="web-node",
        )
        
        if not results:
            raise HTTPException(
                status_code=503,
                detail="No BacktestNode available. Please ensure backtest node is running."
            )
        
        first_result = results[0]
        
        if not first_result.get("success"):
            error_msg = first_result.get("error", "Unknown error")
            logger.error(f"[{task_id}] RPC failed: {error_msg}")
            raise HTTPException(status_code=500, detail=error_msg)
        
        rpc_response = first_result.get("result", {})
        
        return BacktestTaskResponse(
            task_id=task_id,
            status=rpc_response.get("status", "queued"),
            message="因子选股回测任务已提交，请使用 task_id 查询进度",
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"[{task_id}] Failed to submit factor selection: {e}")
        raise HTTPException(status_code=500, detail=str(e))
