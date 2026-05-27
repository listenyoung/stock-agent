"""
消息协议定义

定义节点间通信的强类型消息模型。
所有消息必须包含 trace_id 用于分布式追踪。
"""

from enum import Enum
from typing import Optional, List, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field
import uuid


# ==================== 枚举定义 ====================


class NodeType(str, Enum):
    """节点类型"""
    WEB = "web"
    DATA_SYNC = "data_sync"
    MCP = "mcp"
    INFERENCE = "inference"
    LISTENER = "listener"
    BACKTEST = "backtest"


class TaskType(str, Enum):
    """任务类型"""
    STOCK_ANALYSIS = "stock_analysis" # 股票分析
    MARKET_OVERVIEW = "market_overview" # 市场概览
    NEWS_SENTIMENT = "news_sentiment" # 新闻情感
    STRATEGY_BACKTEST = "strategy_backtest" # 策略回测
    CUSTOM_QUERY = "custom_query" # 自定义查询


class TaskStatus(str, Enum):
    """任务状态"""
    PENDING = "pending" # 待处理
    QUEUED = "queued" # 已排队
    RUNNING = "running" # 运行中
    COMPLETED = "completed" # 已完成
    FAILED = "failed" # 失败
    CANCELLED = "cancelled" # 已取消


class SignalType(str, Enum):
    """交易信号"""
    STRONG_BUY = "strong_buy"
    BUY = "buy"
    HOLD = "hold"
    SELL = "sell"
    STRONG_SELL = "strong_sell"


# ==================== 节点信息 ====================


class NodeInfo(BaseModel):
    """节点注册信息"""
    node_id: str
    node_type: NodeType
    host: str
    port: int
    status: str = "online"  # online, busy, offline
    last_heartbeat: datetime = Field(default_factory=datetime.utcnow)
    
    # 负载信息 (Inference 节点)
    current_tasks: int = 0
    max_tasks: int = 5
    
    # RPC 地址 (gRPC)
    rpc_address: Optional[str] = Field(default=None, description="gRPC RPC 地【址 (host:port)")
    
    @property
    def load_ratio(self) -> float:
        """负载比例"""
        return self.current_tasks / self.max_tasks if self.max_tasks > 0 else 0


class NodeHeartbeat(BaseModel):
    """节点心跳"""
    node_id: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    status: str = "alive"
    load: float = 0.0


# ==================== 任务消息 (核心) ====================


class AgentTask(BaseModel):
    """
    Agent 任务消息
    
    从 Web 节点派发到 Inference 节点的任务。
    所有任务必须包含 trace_id。
    """
    # 任务标识
    task_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    trace_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    
    # 任务内容
    task_type: TaskType
    ts_codes: List[str] = Field(default_factory=list)
    query: Optional[str] = None
    params: Dict[str, Any] = Field(default_factory=dict)
    
    # 元信息
    user_id: str
    priority: int = 0  # 优先级，数字越大越优先
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    # 目标节点 (用于定向分发)
    target_node_id: Optional[str] = None


class AgentResponse(BaseModel):
    """
    Agent 响应消息
    
    从 Inference 节点返回到 Web 节点的结果。
    """
    # 关联信息
    task_id: str
    trace_id: str
    
    # 状态
    status: TaskStatus
    
    # 结果
    result: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    
    # 执行信息
    source_node: Optional[str] = None
    execution_time_ms: float = 0
    llm_tokens_used: int = 0
    
    # 时间戳
    completed_at: datetime = Field(default_factory=datetime.utcnow)


# ==================== 进度消息 ====================


class TaskProgress(BaseModel):
    """任务进度消息 (WebSocket 推送)"""
    task_id: str
    trace_id: str
    status: TaskStatus
    progress: float = 0  # 0-100
    current_step: Optional[str] = None
    message: Optional[str] = None
    source_node: Optional[str] = None


class AgentThought(BaseModel):
    """Agent 思考过程 (用于前端展示)"""
    task_id: str
    trace_id: str
    node_name: str
    content: str
    is_final: bool = False
    timestamp: datetime = Field(default_factory=datetime.utcnow)


# ==================== 分析结果 ====================


class AnalysisScore(BaseModel):
    """分析评分"""
    fundamental: Optional[float] = None  # 基本面 0-100
    technical: Optional[float] = None    # 技术面 0-100
    sentiment: Optional[float] = None    # 舆情 0-100
    valuation: Optional[float] = None    # 估值 0-100


class AnalysisResult(BaseModel):
    """分析结果"""
    signal: SignalType
    confidence: float  # 0-1
    scores: AnalysisScore
    summary: str
    
    # 详细分析
    fundamental_analysis: Optional[str] = None
    technical_analysis: Optional[str] = None
    sentiment_analysis: Optional[str] = None
    
    # 目标价
    target_price: Optional[float] = None
    stop_loss_price: Optional[float] = None
    
    # 风险提示
    risks: List[str] = Field(default_factory=list)


# ==================== LangGraph 状态模型 (V3.0) ====================


class AnalysisConflict(BaseModel):
    """分析冲突记录"""
    conflict_type: str = Field(description="冲突类型，如 '基本面vs技术面'")
    description: str = Field(description="冲突描述")
    resolution: str = Field(description="调和结论")


class ConfidenceScore(BaseModel):
    """置信度评分"""
    data_completeness: float = Field(default=50, ge=0, le=100, description="数据完整性 0-100")
    opinion_consistency: float = Field(default=50, ge=0, le=100, description="意见一致性 0-100")
    overall: float = Field(default=50, ge=0, le=100, description="综合置信度 0-100")


class MCPToolCall(BaseModel):
    """MCP 工具调用记录"""
    tool: str = Field(description="工具名称")
    query: str = Field(description="查询描述")
    target_conflict: str = Field(default="", description="针对的矛盾类型")
    expected_evidence: str = Field(default="", description="期望获取的证据")
    result: Optional[Dict[str, Any]] = Field(default=None, description="执行结果")
    success: bool = Field(default=False, description="是否成功")


class StructuredSummary(BaseModel):
    """结构化精简摘要"""
    fundamental_core: str = Field(default="", description="基本面核心结论 (50字内)")
    technical_core: str = Field(default="", description="技术面核心结论 (50字内)")
    sentiment_core: str = Field(default="", description="舆情核心结论 (50字内)")


class ReasoningStep(BaseModel):
    """决策链步骤"""
    step_id: int = Field(description="步骤编号")
    node_name: str = Field(description="节点名称")
    action: str = Field(description="执行动作")
    reasoning: str = Field(description="推理过程")
    result_summary: str = Field(default="", description="结果摘要")
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class RoundSummary(BaseModel):
    """
    每轮分析的决策摘要
    
    用于跨轮次记忆，让 Supervisor 知道上一轮的问题和本轮的改进
    """
    round_id: int = Field(description="轮次编号 (1=初始分析, 2+=补充分析)")
    
    # 该轮的分析结论
    fundamental_conclusion: str = Field(default="", description="基本面结论")
    technical_conclusion: str = Field(default="", description="技术面结论") 
    sentiment_conclusion: str = Field(default="", description="舆情结论")
    
    # 该轮发现的问题
    conflicts_found: List[str] = Field(default_factory=list, description="发现的矛盾点")
    confidence_score: float = Field(default=50, description="该轮置信度")
    
    # 该轮的决策
    decision: str = Field(default="", description="该轮决策")
    unresolved_issues: List[str] = Field(default_factory=list, description="未解决的问题")
    
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class SupplementaryData(BaseModel):
    """
    补充数据 (来自 MCP 搜索)
    
    与初始数据 (initial_data) 区分，用于增量分析
    """
    source: str = Field(description="数据来源 (如 get_stock_daily, get_news_sentiment)")
    target_conflict: str = Field(default="", description="针对的矛盾类型")
    content: str = Field(description="数据内容摘要")
    raw_data: Optional[Dict[str, Any]] = Field(default=None, description="原始数据")
    fetched_at: datetime = Field(default_factory=datetime.utcnow)


class StockAnalysisState(BaseModel):
    """
    股票分析状态 (LangGraph State) V3.2
    
    增强特性：
    - 结构化精简摘要
    - MCP 工具调用记录
    - 完整决策链追踪
    - 跨轮次记忆 (reasoning_steps)
    - 初始数据与补充数据分离
    """
    # ====== 追踪信息 ======
    trace_id: str = Field(default_factory=lambda: uuid.uuid4().hex, description="分布式追踪ID")
    
    # ====== 输入参数 ======
    ts_code: str = Field(description="股票代码")
    task_id: str = Field(description="任务ID")
    
    # ====== 数据采集结果 (初始数据) ======
    stock: Optional[Dict[str, Any]] = Field(default=None, description="股票基本信息")
    stock_name: str = Field(default="", description="股票名称")
    daily_data: List[Dict[str, Any]] = Field(default_factory=list, description="日线数据")
    fina_data: List[Dict[str, Any]] = Field(default_factory=list, description="财务指标数据")
    
    # ====== 补充数据 (V3.2 新增) ======
    supplementary_data: List[SupplementaryData] = Field(
        default_factory=list, 
        description="MCP 搜索返回的补充数据"
    )
    
    # ====== 三方分析结果 ======
    fundamental_res: str = Field(default="", description="基本面分析结果")
    technical_res: str = Field(default="", description="技术面分析结果")
    sentiment_res: str = Field(default="", description="舆情分析结果")
    
    # ====== 首轮分析结果备份 (V3.2 新增) ======
    initial_fundamental_res: str = Field(default="", description="首轮基本面分析")
    initial_technical_res: str = Field(default="", description="首轮技术面分析")
    initial_sentiment_res: str = Field(default="", description="首轮舆情分析")
    
    # ====== 结构化精简 (V3.1 新增) ======
    structured_summary: StructuredSummary = Field(
        default_factory=StructuredSummary, 
        description="各维度核心结论精简"
    )
    
    # ====== Supervisor 输出 ======
    analysis_conflicts: List[AnalysisConflict] = Field(default_factory=list, description="逻辑冲突列表")
    confidence_score: ConfidenceScore = Field(default_factory=ConfidenceScore, description="置信度评分")
    final_decision: str = Field(default="", description="最终决策")
    decision_reason: str = Field(default="", description="决策理由")
    
    # ====== MCP 搜索结果 (V3.1 新增) ======
    mcp_tool_calls: List[MCPToolCall] = Field(default_factory=list, description="MCP 工具调用记录")
    mcp_evidence: List[Dict[str, Any]] = Field(default_factory=list, description="MCP 补充证据")
    
    # ====== 决策链追踪 (V3.1 新增) ======
    reasoning_chain: List[ReasoningStep] = Field(default_factory=list, description="完整决策链")
    
    # ====== 跨轮次记忆 (V3.2 新增) ======
    reasoning_steps: List[RoundSummary] = Field(
        default_factory=list, 
        description="每轮分析的决策摘要，用于 Supervisor 判断矛盾是否解决"
    )
    
    # ====== 最终输出 ======
    signal: str = Field(default="hold", description="投资信号")
    confidence: float = Field(default=0.5, ge=0, le=1, description="置信度")
    summary: str = Field(default="", description="综合摘要")
    scores: Dict[str, int] = Field(default_factory=dict, description="各维度评分")
    risks: List[str] = Field(default_factory=list, description="风险提示")
    
    # ====== 控制流 ======
    retry_count: int = Field(default=0, description="重试次数")
    needs_refinement: bool = Field(default=False, description="是否需要补充数据")
    refinement_queries: List[Dict[str, Any]] = Field(default_factory=list, description="需要补充的查询列表")
    error: Optional[str] = Field(default=None, description="错误信息")
    
    def add_reasoning_step(self, node_name: str, action: str, reasoning: str, result_summary: str = "") -> None:
        """添加决策链步骤"""
        step = ReasoningStep(
            step_id=len(self.reasoning_chain) + 1,
            node_name=node_name,
            action=action,
            reasoning=reasoning,
            result_summary=result_summary,
        )
        self.reasoning_chain.append(step)
    
    def save_round_summary(self) -> None:
        """保存当前轮次的决策摘要"""
        round_id = len(self.reasoning_steps) + 1
        
        summary = RoundSummary(
            round_id=round_id,
            fundamental_conclusion=self.structured_summary.fundamental_core,
            technical_conclusion=self.structured_summary.technical_core,
            sentiment_conclusion=self.structured_summary.sentiment_core,
            conflicts_found=[c.description for c in self.analysis_conflicts],
            confidence_score=self.confidence_score.overall if self.confidence_score else 50,
            decision=self.final_decision,
            unresolved_issues=[
                c.description for c in self.analysis_conflicts 
                if "未解决" in c.resolution or "需进一步" in c.resolution
            ],
        )
        self.reasoning_steps.append(summary)
    
    def get_previous_issues(self) -> List[str]:
        """获取上一轮未解决的问题"""
        if not self.reasoning_steps:
            return []
        last_round = self.reasoning_steps[-1]
        return last_round.unresolved_issues + last_round.conflicts_found
    
    def is_refinement_round(self) -> bool:
        """是否为补充分析轮次"""
        return self.retry_count > 0
    
    class Config:
        """Pydantic 配置"""
        extra = "allow"  # 允许额外字段


# ==================== 策略监听 (Listener Node) ====================


class StrategyType(str, Enum):
    """策略类型"""
    LIMIT_OPEN = "limit_open"           # 涨跌停打开
    PRICE_CHANGE = "price_change"       # 涨跌幅阈值
    VOLUME_SURGE = "volume_surge"       # 放量突破
    MA_CROSS = "ma_cross"               # 均线交叉
    MA5_BUY = "ma5_buy"                 # 5日线低吸
    CUSTOM = "custom"                   # 自定义策略


class StrategySubscription(BaseModel):
    """
    策略订阅配置
    
    用于 Listener 节点监听市场数据并触发预警。
    """
    # 基础信息
    subscription_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    strategy_id: str = Field(default_factory=lambda: uuid.uuid4().hex, description="策略唯一标识")
    strategy_name: str = Field(default="", description="策略名称")
    strategy_type: StrategyType = Field(default=StrategyType.CUSTOM, description="策略类型")
    
    # 监听范围
    watch_list: List[str] = Field(
        default_factory=list, 
        description="监听股票列表，必须包含 'ALL' 才表示全市场监听，空列表表示不监听任何股票"
    )
    
    # 策略参数
    params: Dict[str, Any] = Field(
        default_factory=dict,
        description="策略参数，如 {'threshold': 3.0} 表示涨幅超过3%触发"
    )
    
    # 状态
    is_active: bool = Field(default=True, description="是否激活")
    user_id: Optional[str] = Field(default=None, description="所属用户")
    
    # 时间戳
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    def is_all_market(self) -> bool:
        """是否监听全市场 (必须明确包含 'ALL' 标识)"""
        return "ALL" in self.watch_list


class StrategyAlert(BaseModel):
    """
    策略触发预警
    
    当策略条件满足时生成的预警消息。
    """
    alert_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    trace_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    
    # 关联信息
    subscription_id: str = Field(description="订阅配置ID")
    strategy_id: str = Field(description="策略ID")
    strategy_name: str = Field(default="", description="策略名称")
    
    # 触发信息
    ts_code: str = Field(description="股票代码")
    stock_name: str = Field(default="", description="股票名称")
    trigger_price: float = Field(description="触发时价格")
    trigger_reason: str = Field(description="触发原因")
    
    # 附加数据
    extra_data: Dict[str, Any] = Field(default_factory=dict, description="额外数据")
    
    # 时间戳
    triggered_at: datetime = Field(default_factory=datetime.utcnow)


class MarketSnapshot(BaseModel):
    """
    市场快照 (内存缓存用)
    
    每次轮询获取的全市场实时数据快照。
    """
    snapshot_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    snapshot_time: datetime = Field(default_factory=datetime.utcnow)
    
    # 数据
    quotes: Dict[str, Dict[str, Any]] = Field(
        default_factory=dict,
        description="实时行情数据，key 为 ts_code"
    )
    limit_stocks: Dict[str, Dict[str, Any]] = Field(
        default_factory=dict,
        description="今日涨跌停股票，key 为 ts_code"
    )
    
    # 统计
    total_stocks: int = Field(default=0, description="股票总数")
    up_count: int = Field(default=0, description="上涨家数")
    down_count: int = Field(default=0, description="下跌家数")
    limit_up_count: int = Field(default=0, description="涨停家数")
    limit_down_count: int = Field(default=0, description="跌停家数")


# ==================== 工具调用 (MCP) ====================


class ToolRequest(BaseModel):
    """MCP 工具调用请求"""
    message_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    trace_id: str
    tool_name: str
    arguments: Dict[str, Any] = Field(default_factory=dict)
    timeout: int = 30


class ToolResponse(BaseModel):
    """MCP 工具调用响应"""
    request_id: str
    success: bool
    result: Optional[Any] = None
    error: Optional[str] = None
    execution_time_ms: float = 0


# ==================== 兼容性别名 ====================
# 保持向后兼容

TaskMessage = AgentTask
ResultMessage = AgentResponse
TaskProgressMessage = TaskProgress
"""Shared protocol schemas for StockAgent nodes."""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class NodeType(str, Enum):
    WEB = "web"
    DATA_SYNC = "data_sync"
    MCP = "mcp"
    INFERENCE = "inference"
    LISTENER = "listener"
    BACKTEST = "backtest"


class TaskType(str, Enum):
    STOCK_ANALYSIS = "stock_analysis"
    MARKET_OVERVIEW = "market_overview"
    CUSTOM_QUERY = "custom_query"


class TaskStatus(str, Enum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class SignalType(str, Enum):
    STRONG_BUY = "strong_buy"
    BUY = "buy"
    HOLD = "hold"
    SELL = "sell"
    STRONG_SELL = "strong_sell"


class NodeInfo(BaseModel):
    node_id: str
    node_type: NodeType
    host: str
    port: int
    status: str = "online"
    last_heartbeat: datetime = Field(default_factory=datetime.utcnow)
    current_tasks: int = 0
    max_tasks: int = 1
    rpc_address: Optional[str] = None


class AgentTask(BaseModel):
    task_id: str
    trace_id: str
    task_type: TaskType
    ts_codes: List[str] = Field(default_factory=list)
    query: Optional[str] = None
    params: Dict[str, Any] = Field(default_factory=dict)
    user_id: Optional[str] = None
    priority: int = 0
    target_node_id: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)


class AgentResponse(BaseModel):
    task_id: str
    trace_id: str
    status: TaskStatus
    result: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    source_node: Optional[str] = None
    execution_time_ms: float = 0
    created_at: datetime = Field(default_factory=datetime.utcnow)


class TaskProgress(BaseModel):
    task_id: str
    trace_id: str
    status: TaskStatus
    progress: float = 0
    message: str = ""
    source_node: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)


class AnalysisResult(BaseModel):
    task_id: str
    signal: SignalType = SignalType.HOLD
    score: float = 0
    summary: str = ""
    details: Dict[str, Any] = Field(default_factory=dict)


class ToolRequest(BaseModel):
    message_id: str
    trace_id: str = ""
    tool_name: str
    arguments: Dict[str, Any] = Field(default_factory=dict)
    user_id: Optional[str] = None


class ToolResponse(BaseModel):
    request_id: str
    success: bool
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    execution_time_ms: float = 0
