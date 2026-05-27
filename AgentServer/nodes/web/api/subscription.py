"""
策略订阅 API

简化架构：
- 每种策略类型只有一条策略数据
- 管理员可修改策略参数
- 普通用户只能添加/移除个股
"""

import asyncio
import logging
import uuid
from typing import Optional, List
from datetime import datetime

from fastapi import APIRouter, HTTPException, Path, Body, Depends
from pydantic import BaseModel, Field

from core.managers import mongo_manager
from core.protocols import StrategyType
from core.rpc import rpc_manager
from .auth import get_current_user, require_admin, CurrentUser


logger = logging.getLogger("api.subscription")


router = APIRouter()


# ==================== 已实现的策略类型 ====================

IMPLEMENTED_STRATEGIES = [
    StrategyType.MA5_BUY,      # 5日线低吸
    StrategyType.LIMIT_OPEN,   # 涨跌停打开
    StrategyType.PRICE_CHANGE, # 涨跌幅阈值
]

IMPLEMENTED_STRATEGY_VALUES = [s.value for s in IMPLEMENTED_STRATEGIES]

# 策略元信息（名称、描述、默认参数）
STRATEGY_META = {
    StrategyType.MA5_BUY.value: {
        "name": "5日线低吸",
        "description": "当价格触及5日均线时提醒，适合低吸策略",
        "default_params": {
            "touch_range": 2,
            "max_break_pct": 3,
            "require_stabilize": False,
        },
        "param_schema": [
            {"key": "touch_range", "label": "触及范围 (%)", "type": "number", "default": 2},
            {"key": "max_break_pct", "label": "最大跌破 (%)", "type": "number", "default": 3},
            {"key": "require_stabilize", "label": "需要企稳", "type": "boolean", "default": False},
        ],
    },
    StrategyType.LIMIT_OPEN.value: {
        "name": "涨跌停打开",
        "description": "涨停或跌停打开时提醒，适合打板策略",
        "default_params": {
            "open_threshold": 2,
        },
        "param_schema": [
            {"key": "open_threshold", "label": "开板阈值 (%)", "type": "number", "default": 2},
        ],
    },
    StrategyType.PRICE_CHANGE.value: {
        "name": "涨跌幅阈值",
        "description": "涨跌幅超过阈值时提醒",
        "default_params": {
            "change_threshold": 5,
        },
        "param_schema": [
            {"key": "change_threshold", "label": "涨跌阈值 (%)", "type": "number", "default": 5},
        ],
    },
}


# ==================== RPC 通知 ====================


async def _notify_listeners_refresh(strategy_type: Optional[str] = None) -> None:
    """
    通知所有 Listener 节点刷新策略配置
    
    在策略被修改后调用，确保 Listener 节点能及时感知变更。
    
    Args:
        strategy_type: 可选，指定刷新的策略类型
    """
    try:
        trace_id = uuid.uuid4().hex
        
        # 广播给所有 Listener 节点
        results = await rpc_manager.broadcast_by_type(
            node_type="listener",
            method="refresh_strategies",
            params={"strategy_type": strategy_type},
            trace_id=trace_id,
            source_node="web",
            timeout=5.0,
        )
        
        success_count = sum(1 for r in results if r.get("success"))
        total_count = len(results)
        
        if total_count > 0:
            logger.info(
                f"[{trace_id}] Notified {success_count}/{total_count} Listener nodes "
                f"to refresh strategies"
            )
        else:
            logger.debug(f"[{trace_id}] No Listener nodes to notify")
            
    except Exception as e:
        # 不阻塞主流程，仅记录日志
        logger.warning(f"Failed to notify Listener nodes: {e}")


# ==================== 请求/响应模型 ====================


class StockInfo(BaseModel):
    """股票信息"""
    ts_code: str
    name: str


class SubscriptionResponse(BaseModel):
    """策略订阅响应"""
    subscription_id: str
    strategy_id: str
    strategy_name: str
    strategy_type: str
    watch_list: List[str]  # 保持原有字段兼容
    watch_list_info: List[StockInfo]  # 新增：包含名称的股票列表
    params: dict
    is_active: bool
    created_at: str
    updated_at: str


class StrategyTypeInfo(BaseModel):
    """策略类型信息"""
    type: str
    name: str
    description: str
    param_schema: List[dict]


class UpdateParamsRequest(BaseModel):
    """更新策略参数请求（仅管理员）"""
    params: dict = Field(..., description="策略参数")


class AddStockRequest(BaseModel):
    """添加股票请求"""
    ts_code: str = Field(..., pattern=r"^\d{6}\.(SH|SZ|BJ)$")


class AddStockResponse(BaseModel):
    """添加股票响应"""
    success: bool
    message: str
    watch_list: List[str]


# ==================== 辅助函数 ====================


async def _get_stock_names(ts_codes: List[str]) -> dict:
    """批量获取股票名称"""
    if not ts_codes:
        return {}
    
    stocks = await mongo_manager.find_many(
        "stock_basic",
        {"ts_code": {"$in": ts_codes}},
        projection={"ts_code": 1, "name": 1},
    )
    
    return {s["ts_code"]: s.get("name", s["ts_code"]) for s in stocks}


async def _to_response(record: dict) -> SubscriptionResponse:
    """将 MongoDB 记录转换为响应模型"""
    created_at = record.get("created_at")
    updated_at = record.get("updated_at")
    watch_list = record.get("watch_list", [])
    
    # 获取股票名称
    stock_names = await _get_stock_names(watch_list)
    watch_list_info = [
        StockInfo(ts_code=code, name=stock_names.get(code, code))
        for code in watch_list
    ]
    
    return SubscriptionResponse(
        subscription_id=record.get("subscription_id", ""),
        strategy_id=record.get("strategy_id", ""),
        strategy_name=record.get("strategy_name", ""),
        strategy_type=record.get("strategy_type", ""),
        watch_list=watch_list,
        watch_list_info=watch_list_info,
        params=record.get("params", {}),
        is_active=record.get("is_active", True),
        created_at=created_at.isoformat() if isinstance(created_at, datetime) else str(created_at or ""),
        updated_at=updated_at.isoformat() if isinstance(updated_at, datetime) else str(updated_at or ""),
    )


async def _ensure_strategy_exists(strategy_type: str) -> dict:
    """
    确保策略存在，如果不存在则自动创建
    
    每种策略类型只有一条记录
    """
    import uuid
    
    # 查找现有策略
    record = await mongo_manager.find_one(
        "strategy_subscriptions",
        {"strategy_type": strategy_type},
    )
    
    if record:
        return record
    
    # 不存在则创建
    meta = STRATEGY_META.get(strategy_type, {})
    now = datetime.utcnow()
    
    doc = {
        "subscription_id": uuid.uuid4().hex,
        "strategy_id": uuid.uuid4().hex,
        "strategy_name": meta.get("name", strategy_type),
        "strategy_type": strategy_type,
        "watch_list": [],  # 初始为空，用户添加个股
        "params": meta.get("default_params", {}),
        "is_active": True,
        "created_at": now,
        "updated_at": now,
    }
    
    await mongo_manager.insert_one("strategy_subscriptions", doc)
    return doc


# ==================== API 端点 ====================


@router.get("/types", response_model=List[StrategyTypeInfo])
async def get_available_strategy_types():
    """
    获取可用的策略类型列表
    
    返回已在 Listener 节点实现的策略类型
    """
    result = []
    for st in IMPLEMENTED_STRATEGIES:
        meta = STRATEGY_META.get(st.value, {})
        result.append(StrategyTypeInfo(
            type=st.value,
            name=meta.get("name", st.value),
            description=meta.get("description", ""),
            param_schema=meta.get("param_schema", []),
        ))
    return result


@router.get("", response_model=List[SubscriptionResponse])
async def get_subscriptions(
    is_active: Optional[bool] = None,
    strategy_type: Optional[str] = None,
):
    """
    获取策略订阅列表
    
    每种策略类型只有一条记录
    """
    filter_query = {}
    
    if is_active is not None:
        filter_query["is_active"] = is_active
    if strategy_type:
        filter_query["strategy_type"] = strategy_type
    
    records = await mongo_manager.find_many(
        "strategy_subscriptions",
        filter_query,
        sort=[("strategy_type", 1)],
    )
    
    # 使用 asyncio.gather 并行获取股票名称
    responses = await asyncio.gather(*[_to_response(r) for r in records])
    return list(responses)


@router.get("/{strategy_type}", response_model=SubscriptionResponse)
async def get_subscription_by_type(strategy_type: str = Path(...)):
    """
    获取指定类型的策略
    
    如果不存在会自动创建（使用默认参数）
    """
    if strategy_type not in IMPLEMENTED_STRATEGY_VALUES:
        raise HTTPException(
            status_code=400, 
            detail=f"策略类型 '{strategy_type}' 不存在"
        )
    
    record = await _ensure_strategy_exists(strategy_type)
    return await _to_response(record)


@router.put("/{strategy_type}/params", response_model=SubscriptionResponse)
async def update_strategy_params(
    strategy_type: str = Path(...),
    data: UpdateParamsRequest = Body(...),
    admin: CurrentUser = Depends(require_admin),
):
    """
    更新策略参数（仅管理员）
    
    需要登录且具有管理员权限
    """
    if strategy_type not in IMPLEMENTED_STRATEGY_VALUES:
        raise HTTPException(
            status_code=400, 
            detail=f"策略类型 '{strategy_type}' 不存在"
        )
    
    # 确保策略存在
    record = await _ensure_strategy_exists(strategy_type)
    
    # 更新参数
    await mongo_manager.update_one(
        "strategy_subscriptions",
        {"strategy_type": strategy_type},
        {
            "$set": {
                "params": data.params,
                "updated_at": datetime.utcnow(),
            }
        },
    )
    
    # 通知 Listener 节点刷新
    asyncio.create_task(_notify_listeners_refresh(strategy_type))
    
    # 获取更新后的记录
    updated = await mongo_manager.find_one(
        "strategy_subscriptions",
        {"strategy_type": strategy_type},
    )
    
    return await _to_response(updated)


@router.patch("/{strategy_type}/toggle")
async def toggle_subscription(
    strategy_type: str = Path(...),
    admin: CurrentUser = Depends(require_admin),
):
    """
    切换策略激活状态（仅管理员）
    
    需要登录且具有管理员权限
    """
    if strategy_type not in IMPLEMENTED_STRATEGY_VALUES:
        raise HTTPException(
            status_code=400, 
            detail=f"策略类型 '{strategy_type}' 不存在"
        )
    
    record = await _ensure_strategy_exists(strategy_type)
    new_status = not record.get("is_active", True)
    
    await mongo_manager.update_one(
        "strategy_subscriptions",
        {"strategy_type": strategy_type},
        {
            "$set": {
                "is_active": new_status,
                "updated_at": datetime.utcnow(),
            }
        },
    )
    
    # 通知 Listener 节点刷新
    asyncio.create_task(_notify_listeners_refresh(strategy_type))
    
    return {
        "strategy_type": strategy_type,
        "is_active": new_status,
        "message": "已激活" if new_status else "已停用",
    }


# ==================== 个股管理 ====================


@router.post("/{strategy_type}/stocks", response_model=AddStockResponse)
async def add_stock_to_strategy(
    strategy_type: str = Path(...),
    data: AddStockRequest = Body(...),
):
    """
    向策略添加个股（所有用户可用）
    
    - 自动检查股票代码格式
    - 检查是否已存在，避免重复添加
    - 验证股票是否在 stock_basic 表中存在
    """
    if strategy_type not in IMPLEMENTED_STRATEGY_VALUES:
        raise HTTPException(
            status_code=400, 
            detail=f"策略类型 '{strategy_type}' 不存在"
        )
    
    ts_code = data.ts_code.upper()
    
    # 确保策略存在
    record = await _ensure_strategy_exists(strategy_type)
    watch_list: List[str] = record.get("watch_list", [])
    
    # 检查是否已存在
    if ts_code in watch_list:
        return AddStockResponse(
            success=False,
            message=f"{ts_code} 已在监听列表中",
            watch_list=watch_list,
        )
    
    # 验证股票是否存在
    stock = await mongo_manager.find_one(
        "stock_basic",
        {"ts_code": ts_code},
    )
    
    if not stock:
        raise HTTPException(status_code=400, detail=f"股票 {ts_code} 不存在")
    
    # 添加到 watch_list
    watch_list.append(ts_code)
    
    await mongo_manager.update_one(
        "strategy_subscriptions",
        {"strategy_type": strategy_type},
        {
            "$set": {
                "watch_list": watch_list,
                "updated_at": datetime.utcnow(),
            }
        },
    )
    
    # 通知 Listener 节点刷新
    asyncio.create_task(_notify_listeners_refresh(strategy_type))
    
    stock_name = stock.get("name", ts_code)
    return AddStockResponse(
        success=True,
        message=f"已添加 {stock_name}({ts_code})",
        watch_list=watch_list,
    )


@router.delete("/{strategy_type}/stocks/{ts_code}", response_model=AddStockResponse)
async def remove_stock_from_strategy(
    strategy_type: str = Path(...),
    ts_code: str = Path(..., pattern=r"^\d{6}\.(SH|SZ|BJ)$"),
):
    """从策略移除个股（所有用户可用）"""
    if strategy_type not in IMPLEMENTED_STRATEGY_VALUES:
        raise HTTPException(
            status_code=400, 
            detail=f"策略类型 '{strategy_type}' 不存在"
        )
    
    ts_code = ts_code.upper()
    
    # 确保策略存在
    record = await _ensure_strategy_exists(strategy_type)
    watch_list: List[str] = record.get("watch_list", [])
    
    # 检查是否存在
    if ts_code not in watch_list:
        return AddStockResponse(
            success=False,
            message=f"{ts_code} 不在监听列表中",
            watch_list=watch_list,
        )
    
    # 移除
    watch_list.remove(ts_code)
    
    await mongo_manager.update_one(
        "strategy_subscriptions",
        {"strategy_type": strategy_type},
        {
            "$set": {
                "watch_list": watch_list,
                "updated_at": datetime.utcnow(),
            }
        },
    )
    
    # 通知 Listener 节点刷新
    asyncio.create_task(_notify_listeners_refresh(strategy_type))
    
    return AddStockResponse(
        success=True,
        message=f"已移除 {ts_code}",
        watch_list=watch_list,
    )


# ==================== 管理员初始化 ====================


@router.post("/init", status_code=201)
async def init_all_strategies(
    admin: CurrentUser = Depends(require_admin),
):
    """
    初始化所有策略（仅管理员）
    
    为每种已实现的策略类型创建一条记录（如果不存在）
    需要登录且具有管理员权限
    """
    
    created = []
    existing = []
    
    for st in IMPLEMENTED_STRATEGIES:
        record = await mongo_manager.find_one(
            "strategy_subscriptions",
            {"strategy_type": st.value},
        )
        
        if record:
            existing.append(st.value)
        else:
            await _ensure_strategy_exists(st.value)
            created.append(st.value)
    
    return {
        "message": "初始化完成",
        "created": created,
        "existing": existing,
    }
