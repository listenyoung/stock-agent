"""
股票 API
"""

from typing import Optional, List

from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel

from core.managers import mongo_manager


router = APIRouter()


# ==================== 模型 ====================


class StockBasic(BaseModel):
    """股票基础信息"""
    ts_code: str
    symbol: str
    name: str
    area: Optional[str]
    industry: Optional[str]
    market: Optional[str]
    list_date: Optional[str]


class StockDaily(BaseModel):
    """日线数据"""
    ts_code: str
    trade_date: str
    open: float
    high: float
    low: float
    close: float
    pre_close: Optional[float]
    change: Optional[float]
    pct_chg: Optional[float]
    vol: Optional[float]
    amount: Optional[float]


class StockQuote(BaseModel):
    """股票行情"""
    ts_code: str
    name: str
    price: Optional[float]
    pct_chg: Optional[float]
    vol: Optional[float]
    amount: Optional[float]
    open: Optional[float]
    high: Optional[float]
    low: Optional[float]
    pre_close: Optional[float]


class RealtimeQuoteRequest(BaseModel):
    """实时行情请求"""
    ts_codes: List[str]


# ==================== API 端点 ====================


@router.get("/search", response_model=List[StockBasic])
async def search_stocks(
    keyword: str = Query(..., min_length=1),
    limit: int = Query(default=20, le=50),
):
    """搜索股票"""
    # 支持代码或名称搜索
    filter_query = {
        "$or": [
            {"ts_code": {"$regex": keyword.upper(), "$options": "i"}},
            {"symbol": {"$regex": keyword, "$options": "i"}},
            {"name": {"$regex": keyword, "$options": "i"}},
        ]
    }
    
    stocks = await mongo_manager.find_many(
        "stock_basic",
        filter_query,
        limit=limit,
    )
    
    return [
        StockBasic(
            ts_code=s["ts_code"],
            symbol=s.get("symbol", ""),
            name=s.get("name", ""),
            area=s.get("area"),
            industry=s.get("industry"),
            market=s.get("market"),
            list_date=s.get("list_date"),
        )
        for s in stocks
    ]


@router.get("/{ts_code}/basic", response_model=StockBasic)
async def get_stock_basic(ts_code: str):
    """获取股票基础信息"""
    stock = await mongo_manager.find_one(
        "stock_basic",
        {"ts_code": ts_code.upper()},
    )
    
    if not stock:
        raise HTTPException(status_code=404, detail="股票不存在")
    
    return StockBasic(
        ts_code=stock["ts_code"],
        symbol=stock.get("symbol", ""),
        name=stock.get("name", ""),
        area=stock.get("area"),
        industry=stock.get("industry"),
        market=stock.get("market"),
        list_date=stock.get("list_date"),
    )


@router.get("/{ts_code}/daily", response_model=List[StockDaily])
async def get_stock_daily(
    ts_code: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = Query(default=100, le=500),
):
    """获取日线数据"""
    filter_query = {"ts_code": ts_code.upper()}
    
    if start_date:
        filter_query["trade_date"] = {"$gte": start_date}
    if end_date:
        filter_query.setdefault("trade_date", {})["$lte"] = end_date
    
    records = await mongo_manager.find_many(
        "stock_daily",
        filter_query,
        sort=[("trade_date", -1)],
        limit=limit,
    )
    
    return [
        StockDaily(
            ts_code=r["ts_code"],
            trade_date=r["trade_date"],
            open=r.get("open", 0),
            high=r.get("high", 0),
            low=r.get("low", 0),
            close=r.get("close", 0),
            pre_close=r.get("pre_close"),
            change=r.get("change"),
            pct_chg=r.get("pct_chg"),
            vol=r.get("vol"),
            amount=r.get("amount"),
        )
        for r in records
    ]


@router.post("/realtime", response_model=List[StockQuote])
async def get_realtime_quotes(body: RealtimeQuoteRequest):
    """获取实时行情（从最新日线数据获取）"""
    if not body.ts_codes:
        return []
    
    # 规范化股票代码
    ts_codes = [code.upper() for code in body.ts_codes]
    
    # 获取股票基本信息
    stocks = await mongo_manager.find_many(
        "stock_basic",
        {"ts_code": {"$in": ts_codes}},
    )
    stock_map = {s["ts_code"]: s for s in stocks}
    
    # 批量获取每只股票最新日线，避免 N+1 查询
    daily_records = await mongo_manager.aggregate(
        "stock_daily",
        [
            {"$match": {"ts_code": {"$in": ts_codes}}},
            {"$sort": {"ts_code": 1, "trade_date": -1}},
            {"$group": {"_id": "$ts_code", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
        ],
    )
    daily_map = {}
    for daily in daily_records:
        ts_code = daily.get("ts_code")
        if ts_code and ts_code not in daily_map:
            daily_map[ts_code] = daily
    
    result = []
    for ts_code in ts_codes:
        stock = stock_map.get(ts_code, {})
        daily = daily_map.get(ts_code)

        result.append(StockQuote(
            ts_code=ts_code,
            name=stock.get("name", ts_code),
            price=daily.get("close") if daily else None,
            pct_chg=daily.get("pct_chg") if daily else None,
            vol=daily.get("vol") if daily else None,
            amount=daily.get("amount") if daily else None,
            open=daily.get("open") if daily else None,
            high=daily.get("high") if daily else None,
            low=daily.get("low") if daily else None,
            pre_close=daily.get("pre_close") if daily else None,
        ))
    
    return result


@router.get("/industries", response_model=List[str])
async def get_industries():
    """获取行业列表"""
    industries = await mongo_manager.aggregate(
        "stock_basic",
        [
            {"$match": {"list_status": "L", "industry": {"$ne": None}}},
            {"$group": {"_id": "$industry"}},
            {"$sort": {"_id": 1}},
        ],
    )
    
    return [i["_id"] for i in industries if i["_id"]]


@router.get("/by-industry/{industry}", response_model=List[StockBasic])
async def get_stocks_by_industry(
    industry: str,
    limit: int = Query(default=50, le=100),
):
    """按行业获取股票"""
    stocks = await mongo_manager.find_many(
        "stock_basic",
        {"industry": industry, "list_status": "L"},
        limit=limit,
    )
    
    return [
        StockBasic(
            ts_code=s["ts_code"],
            symbol=s.get("symbol", ""),
            name=s.get("name", ""),
            area=s.get("area"),
            industry=s.get("industry"),
            market=s.get("market"),
            list_date=s.get("list_date"),
        )
        for s in stocks
    ]
