"""
股票相关模型定义
"""

from datetime import datetime, date
from typing import Optional, List
from pydantic import BaseModel, Field
from enum import Enum


class MarketType(str, Enum):
    """市场类型"""
    SH = "SH"  # 上海
    SZ = "SZ"  # 深圳
    BJ = "BJ"  # 北京


class StockStatus(str, Enum):
    """股票状态"""
    LISTED = "L"      # 上市
    DELISTED = "D"    # 退市
    SUSPENDED = "P"   # 暂停上市


class Stock(BaseModel):
    """股票基本信息"""
    ts_code: str = Field(..., description="股票代码，如 000001.SZ")
    symbol: str = Field(..., description="股票代码（无后缀），如 000001")
    name: str = Field(..., description="股票名称")
    area: Optional[str] = Field(None, description="地区")
    industry: Optional[str] = Field(None, description="所属行业")
    fullname: Optional[str] = Field(None, description="股票全称")
    enname: Optional[str] = Field(None, description="英文名称")
    cnspell: Optional[str] = Field(None, description="拼音缩写")
    market: MarketType = Field(..., description="市场类型")
    exchange: Optional[str] = Field(None, description="交易所代码")
    curr_type: str = Field(default="CNY", description="交易货币")
    list_status: StockStatus = Field(default=StockStatus.LISTED, description="上市状态")
    list_date: Optional[date] = Field(None, description="上市日期")
    delist_date: Optional[date] = Field(None, description="退市日期")
    is_hs: Optional[str] = Field(None, description="是否沪深港通标的")
    
    # 元数据
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "ts_code": "000001.SZ",
                "symbol": "000001",
                "name": "平安银行",
                "area": "深圳",
                "industry": "银行",
                "market": "SZ",
                "list_status": "L",
                "list_date": "1991-04-03",
            }
        }
    }


class StockDaily(BaseModel):
    """股票日线行情"""
    ts_code: str = Field(..., description="股票代码")
    trade_date: date = Field(..., description="交易日期")
    open: float = Field(..., description="开盘价")
    high: float = Field(..., description="最高价")
    low: float = Field(..., description="最低价")
    close: float = Field(..., description="收盘价")
    pre_close: float = Field(..., description="昨收价")
    change: float = Field(..., description="涨跌额")
    pct_chg: float = Field(..., description="涨跌幅 (%)")
    vol: float = Field(..., description="成交量 (手)")
    amount: float = Field(..., description="成交额 (千元)")
    
    # 复权因子
    adj_factor: Optional[float] = Field(None, description="复权因子")
    
    # 技术指标 (可选，由后续计算填充)
    ma5: Optional[float] = Field(None, description="5日均线")
    ma10: Optional[float] = Field(None, description="10日均线")
    ma20: Optional[float] = Field(None, description="20日均线")
    ma60: Optional[float] = Field(None, description="60日均线")
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "ts_code": "000001.SZ",
                "trade_date": "2024-01-15",
                "open": 10.50,
                "high": 10.80,
                "low": 10.40,
                "close": 10.75,
                "pre_close": 10.45,
                "change": 0.30,
                "pct_chg": 2.87,
                "vol": 1234567.0,
                "amount": 132456.78,
            }
        }
    }


class StockFinancial(BaseModel):
    """股票财务数据"""
    ts_code: str = Field(..., description="股票代码")
    ann_date: Optional[date] = Field(None, description="公告日期")
    end_date: date = Field(..., description="报告期")
    
    # 利润表
    revenue: Optional[float] = Field(None, description="营业总收入")
    operate_profit: Optional[float] = Field(None, description="营业利润")
    total_profit: Optional[float] = Field(None, description="利润总额")
    net_profit: Optional[float] = Field(None, description="净利润")
    
    # 资产负债表
    total_assets: Optional[float] = Field(None, description="总资产")
    total_liab: Optional[float] = Field(None, description="总负债")
    total_equity: Optional[float] = Field(None, description="股东权益合计")
    
    # 现金流量表
    operate_cash_flow: Optional[float] = Field(None, description="经营活动现金流")
    invest_cash_flow: Optional[float] = Field(None, description="投资活动现金流")
    finance_cash_flow: Optional[float] = Field(None, description="筹资活动现金流")
    
    # 财务比率
    roe: Optional[float] = Field(None, description="净资产收益率 (%)")
    roa: Optional[float] = Field(None, description="总资产收益率 (%)")
    debt_ratio: Optional[float] = Field(None, description="资产负债率 (%)")
    gross_margin: Optional[float] = Field(None, description="毛利率 (%)")
    net_margin: Optional[float] = Field(None, description="净利率 (%)")
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "ts_code": "000001.SZ",
                "end_date": "2023-12-31",
                "revenue": 123456789.0,
                "net_profit": 12345678.0,
                "roe": 12.5,
            }
        }
    }


class StockNews(BaseModel):
    """股票相关新闻/舆情"""
    id: Optional[str] = Field(None, description="新闻ID")
    ts_code: Optional[str] = Field(None, description="相关股票代码")
    title: str = Field(..., description="新闻标题")
    content: Optional[str] = Field(None, description="新闻内容")
    source: Optional[str] = Field(None, description="来源")
    publish_time: datetime = Field(..., description="发布时间")
    
    # 情感分析结果
    sentiment: Optional[str] = Field(None, description="情感倾向: positive/negative/neutral")
    sentiment_score: Optional[float] = Field(None, description="情感得分 [-1, 1]")
    
    # 关键词
    keywords: List[str] = Field(default_factory=list, description="关键词列表")
    
    created_at: datetime = Field(default_factory=datetime.utcnow)


class MarketOverview(BaseModel):
    """大盘概览"""
    trade_date: date = Field(..., description="交易日期")
    
    # 上证指数
    sh_index: float = Field(..., description="上证指数")
    sh_change: float = Field(..., description="上证涨跌幅 (%)")
    
    # 深证成指
    sz_index: float = Field(..., description="深证成指")
    sz_change: float = Field(..., description="深证涨跌幅 (%)")
    
    # 市场统计
    up_count: int = Field(..., description="上涨家数")
    down_count: int = Field(..., description="下跌家数")
    flat_count: int = Field(..., description="平盘家数")
    limit_up_count: int = Field(default=0, description="涨停家数")
    limit_down_count: int = Field(default=0, description="跌停家数")
    
    # 成交数据
    total_volume: float = Field(..., description="总成交量 (亿股)")
    total_amount: float = Field(..., description="总成交额 (亿元)")
    
    # 板块热点
    hot_sectors: List[str] = Field(default_factory=list, description="热门板块")
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
