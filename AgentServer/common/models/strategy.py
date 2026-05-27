"""
策略与分析任务模型定义
"""

from datetime import datetime
from typing import Optional, List, Any, Dict
from pydantic import BaseModel, Field
from enum import Enum


class TaskStatus(str, Enum):
    """任务状态"""
    PENDING = "pending"       # 等待执行
    RUNNING = "running"       # 执行中
    COMPLETED = "completed"   # 已完成
    FAILED = "failed"         # 失败
    CANCELLED = "cancelled"   # 已取消


class TaskType(str, Enum):
    """任务类型"""
    STOCK_ANALYSIS = "stock_analysis"        # 个股分析
    MARKET_OVERVIEW = "market_overview"      # 大盘分析
    STRATEGY_BACKTEST = "strategy_backtest"  # 策略回测
    NEWS_SENTIMENT = "news_sentiment"        # 舆情分析
    REPORT_GENERATE = "report_generate"      # 研报生成
    CUSTOM_QUERY = "custom_query"            # 自定义查询


class AnalysisTask(BaseModel):
    """分析任务"""
    id: Optional[str] = Field(None, alias="_id", description="任务ID")
    user_id: str = Field(..., description="用户ID")
    task_type: TaskType = Field(..., description="任务类型")
    status: TaskStatus = Field(default=TaskStatus.PENDING, description="任务状态")
    
    # 任务输入
    input_params: Dict[str, Any] = Field(default_factory=dict, description="输入参数")
    ts_codes: List[str] = Field(default_factory=list, description="相关股票代码")
    query: Optional[str] = Field(None, description="用户查询（自然语言）")
    
    # 任务输出
    result: Optional[Dict[str, Any]] = Field(None, description="分析结果")
    error_message: Optional[str] = Field(None, description="错误信息")
    
    # 追踪信息
    trace_id: Optional[str] = Field(None, description="追踪ID")
    
    # 时间信息
    created_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = Field(None, description="开始时间")
    completed_at: Optional[datetime] = Field(None, description="完成时间")
    
    # 资源消耗
    llm_tokens_used: int = Field(default=0, description="LLM Token 消耗")
    execution_time_ms: int = Field(default=0, description="执行时间（毫秒）")
    
    model_config = {
        "populate_by_name": True,
        "json_schema_extra": {
            "example": {
                "user_id": "user123",
                "task_type": "stock_analysis",
                "status": "pending",
                "ts_codes": ["000001.SZ"],
                "query": "分析平安银行的投资价值",
            }
        }
    }


class SignalType(str, Enum):
    """信号类型"""
    BUY = "buy"           # 买入信号
    SELL = "sell"         # 卖出信号
    HOLD = "hold"         # 持有
    STRONG_BUY = "strong_buy"   # 强烈买入
    STRONG_SELL = "strong_sell" # 强烈卖出


class StrategyResult(BaseModel):
    """策略分析结果"""
    task_id: str = Field(..., description="关联任务ID")
    ts_code: str = Field(..., description="股票代码")
    
    # 分析概要
    summary: str = Field(..., description="分析摘要")
    signal: SignalType = Field(..., description="交易信号")
    confidence: float = Field(..., ge=0, le=1, description="置信度 [0-1]")
    
    # 目标价位
    target_price: Optional[float] = Field(None, description="目标价")
    stop_loss_price: Optional[float] = Field(None, description="止损价")
    current_price: Optional[float] = Field(None, description="当前价")
    
    # 多维度评分 (0-100)
    scores: Dict[str, float] = Field(
        default_factory=dict,
        description="多维度评分",
        json_schema_extra={
            "example": {
                "fundamental": 75.0,
                "technical": 68.0,
                "sentiment": 82.0,
                "valuation": 65.0,
            }
        }
    )
    
    # 详细分析
    fundamental_analysis: Optional[str] = Field(None, description="基本面分析")
    technical_analysis: Optional[str] = Field(None, description="技术面分析")
    sentiment_analysis: Optional[str] = Field(None, description="舆情分析")
    risk_analysis: Optional[str] = Field(None, description="风险提示")
    
    # 支撑观点的数据
    supporting_data: Dict[str, Any] = Field(
        default_factory=dict,
        description="支撑分析的关键数据"
    )
    
    # 相关研报引用
    report_references: List[Dict[str, str]] = Field(
        default_factory=list,
        description="引用的研报列表"
    )
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "task_id": "task123",
                "ts_code": "000001.SZ",
                "summary": "平安银行基本面稳健，技术面呈现突破形态，建议逢低买入",
                "signal": "buy",
                "confidence": 0.75,
                "target_price": 12.50,
                "stop_loss_price": 9.80,
                "current_price": 10.75,
            }
        }
    }


class Strategy(BaseModel):
    """用户自定义策略"""
    id: Optional[str] = Field(None, alias="_id")
    user_id: str = Field(..., description="用户ID")
    name: str = Field(..., min_length=1, max_length=100, description="策略名称")
    description: Optional[str] = Field(None, description="策略描述")
    
    # 策略配置
    is_active: bool = Field(default=True, description="是否启用")
    is_public: bool = Field(default=False, description="是否公开")
    
    # 选股条件
    stock_pool: List[str] = Field(default_factory=list, description="股票池")
    filters: Dict[str, Any] = Field(
        default_factory=dict,
        description="筛选条件",
        json_schema_extra={
            "example": {
                "roe_min": 10,
                "pe_max": 30,
                "market_cap_min": 100,  # 亿元
            }
        }
    )
    
    # 分析权重
    weights: Dict[str, float] = Field(
        default_factory=lambda: {
            "fundamental": 0.3,
            "technical": 0.3,
            "sentiment": 0.2,
            "valuation": 0.2,
        },
        description="各维度权重"
    )
    
    # 告警设置
    alert_enabled: bool = Field(default=False, description="是否启用告警")
    alert_conditions: Dict[str, Any] = Field(
        default_factory=dict,
        description="告警条件"
    )
    
    # 元数据
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    # 统计信息
    run_count: int = Field(default=0, description="执行次数")
    last_run_at: Optional[datetime] = Field(None, description="最后执行时间")
    
    model_config = {
        "populate_by_name": True,
        "json_schema_extra": {
            "example": {
                "name": "价值成长策略",
                "description": "寻找高ROE、合理PE的成长股",
                "filters": {
                    "roe_min": 15,
                    "pe_max": 25,
                    "revenue_growth_min": 10,
                },
            }
        }
    }


class BacktestResult(BaseModel):
    """回测结果"""
    strategy_id: str = Field(..., description="策略ID")
    
    # 时间范围
    start_date: str = Field(..., description="开始日期")
    end_date: str = Field(..., description="结束日期")
    
    # 收益指标
    total_return: float = Field(..., description="总收益率 (%)")
    annual_return: float = Field(..., description="年化收益率 (%)")
    max_drawdown: float = Field(..., description="最大回撤 (%)")
    sharpe_ratio: float = Field(..., description="夏普比率")
    
    # 交易统计
    trade_count: int = Field(..., description="交易次数")
    win_rate: float = Field(..., description="胜率 (%)")
    profit_factor: float = Field(..., description="盈亏比")
    
    # 基准对比
    benchmark_return: float = Field(..., description="基准收益率 (%)")
    alpha: float = Field(..., description="Alpha")
    beta: float = Field(..., description="Beta")
    
    # 详细数据
    equity_curve: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="权益曲线数据"
    )
    trade_records: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="交易记录"
    )
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
