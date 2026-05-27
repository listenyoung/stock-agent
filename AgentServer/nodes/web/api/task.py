"""
任务 API

负责:
- 创建分析任务
- 任务派发 (负载均衡)
- 任务查询
- 任务删除
"""

from typing import Optional, List, Dict
from datetime import datetime
import uuid
import logging

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field

from core.protocols import (
    AgentTask,
    TaskType,
    TaskStatus,
)
from core.managers import redis_manager, mongo_manager
from .auth import get_current_user_id


router = APIRouter()
logger = logging.getLogger(__name__)


# ==================== 股票名称缓存 ====================

_stock_names_cache: Dict[str, str] = {}


def _normalize_ts_code(code: str) -> str:
    """
    标准化股票代码（添加后缀）
    
    - 6开头的是上海股票 (.SH)
    - 0/3开头的是深圳股票 (.SZ)
    - 如果已有后缀则直接返回
    """
    if not code:
        return code
    
    # 如果已有后缀，直接返回
    if "." in code:
        return code.upper()
    
    # 根据代码前缀判断市场
    if code.startswith("6"):
        return f"{code}.SH"
    elif code.startswith(("0", "3")):
        return f"{code}.SZ"
    else:
        # 默认返回原始代码
        return code


async def _get_stock_names(ts_codes: List[str]) -> Dict[str, str]:
    """
    批量获取股票名称
    使用缓存减少数据库查询
    
    支持两种格式的股票代码:
    - 简写格式: 002995
    - 完整格式: 002995.SZ
    """
    if not ts_codes:
        return {}
    
    result = {}
    codes_to_fetch = []
    
    # 构建原始代码到标准化代码的映射
    code_mapping: Dict[str, str] = {}  # original -> normalized
    
    for code in ts_codes:
        normalized = _normalize_ts_code(code)
        code_mapping[code] = normalized
        
        # 先从缓存获取（使用标准化代码）
        if normalized in _stock_names_cache:
            result[code] = _stock_names_cache[normalized]
        else:
            codes_to_fetch.append(normalized)
    
    # 批量查询未缓存的
    if codes_to_fetch:
        stocks = await mongo_manager.find_many(
            "stock_basic",
            {"ts_code": {"$in": codes_to_fetch}},
            projection={"ts_code": 1, "name": 1, "_id": 0},
        )
        
        # 构建查询结果映射
        db_results: Dict[str, str] = {}
        for stock in stocks:
            ts_code = stock.get("ts_code", "")
            name = stock.get("name", ts_code)
            db_results[ts_code] = name
            _stock_names_cache[ts_code] = name
        
        # 将结果映射回原始代码
        for original, normalized in code_mapping.items():
            if original not in result:  # 未从缓存获取到的
                if normalized in db_results:
                    result[original] = db_results[normalized]
                else:
                    # 对于没找到的代码，使用代码本身
                    result[original] = original
    
    return result


# ==================== 请求/响应模型 ====================


class CreateTaskRequest(BaseModel):
    """创建任务请求"""
    task_type: TaskType
    ts_codes: List[str] = Field(default_factory=list)
    query: Optional[str] = None
    params: dict = Field(default_factory=dict)
    priority: int = 0


class CreateTaskResponse(BaseModel):
    """创建任务响应"""
    task_id: str
    trace_id: str
    status: TaskStatus
    message: str


class StockNameInfo(BaseModel):
    """股票名称信息"""
    ts_code: str
    name: str


class TaskItem(BaseModel):
    """任务项"""
    task_id: str
    trace_id: str
    task_type: str
    status: str
    ts_codes: List[str]
    stock_names: List[StockNameInfo] = Field(default_factory=list)  # 股票名称信息
    query: Optional[str]
    created_at: datetime
    completed_at: Optional[datetime]
    execution_time_ms: float = 0
    result: Optional[dict] = None
    error_message: Optional[str] = None
    # 新增: 处理节点 ID
    node_id: Optional[str] = None


class TaskListResponse(BaseModel):
    """任务列表响应"""
    tasks: List[TaskItem]
    total: int
    limit: int
    offset: int


# ==================== 任务派发器 ====================


async def dispatch_task(task: AgentTask) -> None:
    """
    派发任务 (带负载均衡)
    
    1. 获取所有活跃的 Inference 节点
    2. 选择负载最低的节点
    3. 将任务推入队列 (可选: 定向分发)
    """
    # 获取所有 Inference 节点
    nodes = await redis_manager.get_all_nodes(node_type="inference")
    
    if not nodes:
        # 没有节点，仍然入队等待
        await redis_manager.enqueue_task(task.model_dump(mode="json"))
        return
    
    # 筛选可用节点
    available_nodes = [n for n in nodes if n.get("status") != "busy"]
    
    if available_nodes:
        # 选择负载最低的节点
        best_node = min(
            available_nodes,
            key=lambda n: n.get("current_tasks", 0) / max(n.get("max_tasks", 1), 1)
        )
        task.target_node_id = best_node.get("node_id")
    
    # 入队
    await redis_manager.enqueue_task(task.model_dump(mode="json"))


# ==================== API 端点 ====================


@router.post("", response_model=CreateTaskResponse)
async def create_task(
    request: Request,
    body: CreateTaskRequest,
    user_id: str = Depends(get_current_user_id),
):
    """创建分析任务"""
    trace_id = getattr(request.state, "trace_id", uuid.uuid4().hex)
    
    # 创建任务
    task = AgentTask(
        task_id=uuid.uuid4().hex,
        trace_id=trace_id,
        task_type=body.task_type,
        ts_codes=body.ts_codes,
        query=body.query,
        params=body.params,
        user_id=user_id,
        priority=body.priority,
    )
    
    # 获取股票名称（创建时获取，存储到任务文档）
    stock_names_map = await _get_stock_names(body.ts_codes) if body.ts_codes else {}
    stock_names_list = [
        {"ts_code": code, "name": stock_names_map.get(code, code)}
        for code in body.ts_codes
    ]
    
    # 保存到数据库
    await mongo_manager.insert_one("tasks", {
        "task_id": task.task_id,
        "trace_id": task.trace_id,
        "task_type": task.task_type.value,
        "status": TaskStatus.PENDING.value,
        "ts_codes": task.ts_codes,
        "stock_names": stock_names_list,  # 存储股票名称
        "query": task.query,
        "params": task.params,
        "user_id": user_id,
        "priority": task.priority,
        "node_id": None,  # 初始为空，由 Inference 节点更新
    })
    
    # 派发任务
    await dispatch_task(task)
    
    return CreateTaskResponse(
        task_id=task.task_id,
        trace_id=task.trace_id,
        status=TaskStatus.QUEUED,
        message="任务已创建并加入队列",
    )


@router.get("", response_model=TaskListResponse)
async def list_tasks(
    status: Optional[TaskStatus] = None,
    task_type: Optional[TaskType] = None,
    limit: int = Query(default=20, le=100),
    offset: int = Query(default=0, ge=0),
    user_id: str = Depends(get_current_user_id),
):
    """获取任务列表"""
    filter_query = {"user_id": user_id}
    
    if status:
        filter_query["status"] = status.value
    if task_type:
        filter_query["task_type"] = task_type.value
    
    # 查询
    tasks = await mongo_manager.find_many(
        "tasks",
        filter_query,
        sort=[("created_at", -1)],
        limit=limit,
        skip=offset,
    )
    
    total = await mongo_manager.count("tasks", filter_query)
    
    return TaskListResponse(
        tasks=[
            TaskItem(
                task_id=t["task_id"],
                trace_id=t.get("trace_id", ""),
                task_type=t["task_type"],
                status=t["status"],
                ts_codes=t.get("ts_codes", []),
                stock_names=[
                    StockNameInfo(ts_code=s.get("ts_code", ""), name=s.get("name", s.get("ts_code", "")))
                    for s in t.get("stock_names", [])
                ],
                query=t.get("query"),
                created_at=t["created_at"],
                completed_at=t.get("completed_at"),
                execution_time_ms=t.get("execution_time_ms", 0),
                result=t.get("result"),
                error_message=t.get("error_message"),
                node_id=t.get("node_id"),  # 返回处理节点
            )
            for t in tasks
        ],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/{task_id}", response_model=TaskItem)
async def get_task(
    task_id: str,
    user_id: str = Depends(get_current_user_id),
):
    """获取任务详情"""
    task = await mongo_manager.find_one(
        "tasks",
        {"task_id": task_id, "user_id": user_id},
    )
    
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")
    
    return TaskItem(
        task_id=task["task_id"],
        trace_id=task.get("trace_id", ""),
        task_type=task["task_type"],
        status=task["status"],
        ts_codes=task.get("ts_codes", []),
        stock_names=[
            StockNameInfo(ts_code=s.get("ts_code", ""), name=s.get("name", s.get("ts_code", "")))
            for s in task.get("stock_names", [])
        ],
        query=task.get("query"),
        created_at=task["created_at"],
        completed_at=task.get("completed_at"),
        execution_time_ms=task.get("execution_time_ms", 0),
        result=task.get("result"),
        error_message=task.get("error_message"),
        node_id=task.get("node_id"),  # 返回处理节点
    )


@router.delete("/{task_id}")
async def cancel_task(
    task_id: str,
    user_id: str = Depends(get_current_user_id),
):
    """取消任务"""
    result = await mongo_manager.update_one(
        "tasks",
        {"task_id": task_id, "user_id": user_id, "status": {"$in": ["pending", "queued"]}},
        {"$set": {"status": TaskStatus.CANCELLED.value}},
    )
    
    if result == 0:
        raise HTTPException(status_code=400, detail="任务无法取消")
    
    return {"message": "任务已取消"}


@router.delete("/{task_id}/delete")
async def delete_task(
    task_id: str,
    user_id: str = Depends(get_current_user_id),
):
    """
    永久删除任务
    
    只能删除已完成、失败或已取消的任务
    """
    # 先检查任务是否存在且属于当前用户
    task = await mongo_manager.find_one(
        "tasks",
        {"task_id": task_id, "user_id": user_id},
    )
    
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")
    
    # 检查任务状态
    deletable_statuses = ["completed", "failed", "cancelled"]
    if task["status"] not in deletable_statuses:
        raise HTTPException(
            status_code=400, 
            detail=f"只能删除已完成/失败/已取消的任务，当前状态: {task['status']}"
        )
    
    # 删除任务
    result = await mongo_manager.delete_one(
        "tasks",
        {"task_id": task_id, "user_id": user_id},
    )
    
    if result == 0:
        raise HTTPException(status_code=500, detail="删除失败")
    
    logger.info(f"Task deleted: {task_id} by user {user_id}")
    
    return {"message": "任务已删除", "task_id": task_id}


# ==================== 快捷分析 ====================


@router.post("/analyze/stock", response_model=CreateTaskResponse)
async def analyze_stock(
    request: Request,
    ts_code: str = Query(..., description="股票代码"),
    analysis_type: str = Query(default="comprehensive", description="分析类型"),
    user_id: str = Depends(get_current_user_id),
):
    """快速个股分析"""
    trace_id = getattr(request.state, "trace_id", uuid.uuid4().hex)
    
    task = AgentTask(
        trace_id=trace_id,
        task_type=TaskType.STOCK_ANALYSIS,
        ts_codes=[ts_code],
        params={"analysis_type": analysis_type},
        user_id=user_id,
    )
    
    # 获取股票名称
    stock_names_map = await _get_stock_names([ts_code])
    stock_name = stock_names_map.get(ts_code, ts_code)
    stock_names_list = [{"ts_code": ts_code, "name": stock_name}]
    
    await mongo_manager.insert_one("tasks", {
        "task_id": task.task_id,
        "trace_id": task.trace_id,
        "task_type": task.task_type.value,
        "status": TaskStatus.PENDING.value,
        "ts_codes": task.ts_codes,
        "stock_names": stock_names_list,  # 存储股票名称
        "params": task.params,
        "user_id": user_id,
        "node_id": None,
    })
    
    await dispatch_task(task)
    
    return CreateTaskResponse(
        task_id=task.task_id,
        trace_id=task.trace_id,
        status=TaskStatus.QUEUED,
        message=f"正在分析 {stock_name}",
    )


@router.post("/analyze/market", response_model=CreateTaskResponse)
async def analyze_market(
    request: Request,
    user_id: str = Depends(get_current_user_id),
):
    """快速大盘分析"""
    trace_id = getattr(request.state, "trace_id", uuid.uuid4().hex)
    
    task = AgentTask(
        trace_id=trace_id,
        task_type=TaskType.MARKET_OVERVIEW,
        user_id=user_id,
    )
    
    await mongo_manager.insert_one("tasks", {
        "task_id": task.task_id,
        "trace_id": task.trace_id,
        "task_type": task.task_type.value,
        "status": TaskStatus.PENDING.value,
        "ts_codes": [],
        "stock_names": [],  # 大盘分析无个股
        "user_id": user_id,
        "node_id": None,
    })
    
    await dispatch_task(task)
    
    return CreateTaskResponse(
        task_id=task.task_id,
        trace_id=task.trace_id,
        status=TaskStatus.QUEUED,
        message="正在分析大盘",
    )


@router.post("/analyze/query", response_model=CreateTaskResponse)
async def query_analysis(
    request: Request,
    query: str = Query(..., description="查询内容"),
    user_id: str = Depends(get_current_user_id),
):
    """自然语言查询"""
    trace_id = getattr(request.state, "trace_id", uuid.uuid4().hex)
    
    task = AgentTask(
        trace_id=trace_id,
        task_type=TaskType.CUSTOM_QUERY,
        query=query,
        user_id=user_id,
    )
    
    await mongo_manager.insert_one("tasks", {
        "task_id": task.task_id,
        "trace_id": task.trace_id,
        "task_type": task.task_type.value,
        "status": TaskStatus.PENDING.value,
        "ts_codes": [],
        "stock_names": [],  # 自定义查询无个股
        "query": query,
        "user_id": user_id,
        "node_id": None,
    })
    
    await dispatch_task(task)
    
    return CreateTaskResponse(
        task_id=task.task_id,
        trace_id=task.trace_id,
        status=TaskStatus.QUEUED,
        message="正在处理查询",
    )
