# Inference 节点 (V3.2)

## 概述

Inference 节点负责执行股票分析的核心推理逻辑，基于 LangGraph 状态机架构实现多维度分析、冲突调和、动态纠偏与跨轮次增量分析。

## 架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                     Inference Node (V3.2)                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│    ┌─────────────┐                                               │
│    │ data_collect │ ← 数据采集 (股票基本信息 + 日线数据)          │
│    └──────┬──────┘                                               │
│           │                                                       │
│    ┌──────┼──────┬──────────────┐                                │
│    ▼      ▼      ▼              │                                │
│ ┌─────┐ ┌─────┐ ┌─────┐        │ ← 并行多维度分析 (Round 1)     │
│ │fund │ │tech │ │sent │        │                                │
│ └──┬──┘ └──┬──┘ └──┬──┘        │                                │
│    └───────┼───────┘            │                                │
│            ▼                    │                                │
│    ┌──────────────┐             │                                │
│    │  supervisor  │ ← 逻辑对冲 + 保存 RoundSummary               │
│    └──────┬───────┘             │                                │
│           ▼                     │                                │
│    ┌──────────────┐             │                                │
│    │ check_result │ ← 置信度阈值检查                             │
│    └──────┬───────┘             │                                │
│           │                     │                                │
│    ┌──────┴──────┐              │                                │
│    │ [条件边缘]  │              │                                │
│    ├─────────────┤              │                                │
│    │             │              │                                │
│    ▼             ▼              │                                │
│ confidence    confidence        │                                │
│  >= 60%        < 60%           │                                │
│    │             │              │                                │
│    │    ┌────────┴────────┐    │                                │
│    │    │ query_refinement │ ← 生成 MCP 工具调用指令            │
│    │    └────────┬────────┘    │                                │
│    │             ▼              │                                │
│    │    ┌────────────────┐     │                                │
│    │    │   mcp_search   │ ← 执行工具 → SupplementaryData       │
│    │    └────────┬───────┘     │                                │
│    │             ▼              │                                │
│    │    ┌────────┼────────┐    │                                │
│    │    │ fund │tech│sent │ ← 增量分析 (Round 2)                │
│    │    └────────┼────────┘    │                                │
│    │             ▼              │                                │
│    │    ┌────────────────┐     │                                │
│    │    │   supervisor   │ ← 对比 reasoning_steps               │
│    │    └────────┬───────┘     │                                │
│    │             │              │                                │
│    ▼             ▼              │                                │
│ ┌────────┐                                                       │
│ │ output │ ← 构建最终报告 + round_history                        │
│ └────────┘                                                       │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

## 核心节点

### 1. data_collect_node
- **职责**: 数据采集
- **输入**: `ts_code`, `task_id`
- **输出**: `stock`, `stock_name`, `daily_data`
- **数据源**: MongoDB (`stock_basic`, `stock_daily`)

### 2. fundamental_node
- **职责**: 基本面分析
- **输入**: `stock`, `ts_code`
- **输出**: `fundamental_res`
- **提示词**: `core/prompts/stock_analysis/fundamental.yaml`

### 3. technical_node
- **职责**: 技术面分析
- **输入**: `daily_data`, `ts_code`
- **输出**: `technical_res`
- **计算**: MA5, MA20, 量比
- **提示词**: `core/prompts/stock_analysis/technical.yaml`

### 4. sentiment_node
- **职责**: 舆情分析
- **输入**: `stock_name`, `ts_code`
- **输出**: `sentiment_res`
- **数据源**: MongoDB (`news`)
- **提示词**: `core/prompts/stock_analysis/sentiment.yaml`

### 5. supervisor_node (V3.1 增强)
- **职责**: 
  - 结构化精简三方意见 (每方 ≤50 字)
  - 识别逻辑冲突 (量价背离、资金背离等)
  - 冲突调和
  - 置信度评估
  - 最终决策
- **输入**: `fundamental_res`, `technical_res`, `sentiment_res`, `mcp_evidence`
- **输出**: 
  - `structured_summary`: 精简摘要
  - `analysis_conflicts`: 冲突列表
  - `confidence_score`: 置信度评分
  - `final_decision`: 投资信号
- **提示词**: `core/prompts/stock_analysis/supervisor.yaml`

### 6. check_result_node
- **职责**: 置信度阈值检查
- **阈值**: 60% (可配置)
- **输出**: `needs_refinement` (bool)
- **决策逻辑**:
  - `confidence >= 60%` → 输出结果
  - `confidence < 60%` → 触发数据补充

### 7. query_refinement_node (V3.1 增强)
- **职责**: 生成 MCP 工具调用指令
- **输入**: `analysis_conflicts`, `confidence_score`, `structured_summary`
- **输出**: `refinement_queries` (工具调用列表)
- **提示词**: `core/prompts/stock_analysis/refinement.yaml`

### 8. mcp_search_node (V3.1 新增)
- **职责**: 执行 MCP 工具调用
- **支持工具**:
  - `get_stock_daily`: 获取日线数据
  - `get_financial_indicator`: 获取财务指标
  - `get_news_sentiment`: 获取新闻舆情
  - `search_similar_reports`: 搜索相似研报
- **输出**: `mcp_tool_calls`, `mcp_evidence`
- **提示词**: `core/prompts/stock_analysis/mcp_search.yaml`

## 状态模型 (StockAnalysisState V3.2)

```python
class StockAnalysisState(BaseModel):
    # 追踪信息
    trace_id: str          # 分布式追踪 ID
    
    # 输入参数
    ts_code: str           # 股票代码
    task_id: str           # 任务 ID
    
    # 数据采集结果 (初始数据)
    stock: Dict            # 股票基本信息
    stock_name: str        # 股票名称
    daily_data: List       # 日线数据
    
    # 补充数据 (V3.2 新增)
    supplementary_data: List[SupplementaryData]  # MCP 搜索返回的补充数据
    
    # 三方分析结果
    fundamental_res: str   # 基本面分析 (当前轮)
    technical_res: str     # 技术面分析 (当前轮)
    sentiment_res: str     # 舆情分析 (当前轮)
    
    # 首轮分析备份 (V3.2 新增)
    initial_fundamental_res: str  # 首轮基本面分析
    initial_technical_res: str    # 首轮技术面分析
    initial_sentiment_res: str    # 首轮舆情分析
    
    # 结构化精简
    structured_summary: StructuredSummary
    
    # Supervisor 输出
    analysis_conflicts: List[AnalysisConflict]  # 逻辑冲突
    confidence_score: ConfidenceScore           # 置信度
    final_decision: str                         # 最终决策
    
    # MCP 搜索结果
    mcp_tool_calls: List[MCPToolCall]  # 工具调用记录
    mcp_evidence: List[Dict]           # 补充证据
    
    # 决策链追踪
    reasoning_chain: List[ReasoningStep]
    
    # 跨轮次记忆 (V3.2 新增)
    reasoning_steps: List[RoundSummary]  # 每轮决策摘要
    
    # 控制流
    retry_count: int       # 重试次数 (max: 2)
    needs_refinement: bool # 是否需要补充数据
    
    # 辅助方法 (V3.2)
    def is_refinement_round(self) -> bool     # 是否为补充分析轮次
    def get_previous_issues(self) -> List[str] # 获取上轮未解决问题
    def save_round_summary(self) -> None       # 保存当前轮次摘要
```

## 新增模型 (V3.2)

### RoundSummary
```python
class RoundSummary(BaseModel):
    """每轮分析的决策摘要，用于跨轮次记忆"""
    round_id: int                    # 轮次编号
    fundamental_conclusion: str      # 基本面结论
    technical_conclusion: str        # 技术面结论
    sentiment_conclusion: str        # 舆情结论
    conflicts_found: List[str]       # 发现的矛盾点
    confidence_score: float          # 该轮置信度
    decision: str                    # 该轮决策
    unresolved_issues: List[str]     # 未解决的问题
```

### SupplementaryData
```python
class SupplementaryData(BaseModel):
    """补充数据，与初始数据区分"""
    source: str                      # 数据来源 (get_stock_daily, get_news_sentiment 等)
    target_conflict: str             # 针对的矛盾类型
    content: str                     # 数据内容摘要
    raw_data: Optional[Dict]         # 原始数据
```

## 逻辑对冲规则

| 背离类型 | 识别特征 | 置信度惩罚 |
|---------|---------|-----------|
| **量价背离** | 股价上涨但成交量萎缩 >30% | -20分 |
| **德不配位** | 情绪极度亢奋但基本面证伪 | -25分 |
| **机构出逃** | 高位横盘 + 大单持续流出 | -30分 |
| **防御性上涨** | 市场退潮期防御板块独涨 | -15分 |
| **消息驱动** | 无基本面支撑的纯概念炒作 | -20分 |

## 日志规范

所有日志包含 `trace_id` 用于分布式追踪:

```
[node_name] trace_id=abc123 | message
```

示例:
```
[supervisor] trace_id=a1b2c3d4 | Synthesizing analysis for 贵州茅台
[supervisor] trace_id=a1b2c3d4 | Conflicts found: 2
[supervisor] trace_id=a1b2c3d4 |   Conflict 1: [量价背离] 股价上涨但量能萎缩...
[check_result] trace_id=a1b2c3d4 | Confidence=45, threshold=60, retry=0
[check_result] trace_id=a1b2c3d4 | Needs refinement: True
[mcp_search] trace_id=a1b2c3d4 | Executing 2 MCP tool calls
```

## 决策链追踪

每个节点执行后记录 `ReasoningStep`:

```python
{
    "step_id": 1,
    "node_name": "data_collect",
    "action": "数据采集",
    "reasoning": "获取 600519.SH 的基本信息和近60日行情数据",
    "result_summary": "获取 60 条日线数据",
    "timestamp": "2026-02-02T12:00:00"
}
```

## 配置文件

### supervisor.yaml
```yaml
confidence_threshold: 60
max_retry_count: 2
```

### refinement.yaml
```yaml
max_queries: 3
min_confidence_for_skip: 60
```

## 使用示例

```python
from nodes.inference.graph import StockAnalysisGraph

graph = StockAnalysisGraph()
await graph.initialize()

result = await graph.analyze_stock(
    ts_code="600519.SH",
    task_id="task_001",
    progress_callback=lambda p, m: print(f"{p}%: {m}")
)

print(f"Signal: {result['signal']}")
print(f"Confidence: {result['confidence']}")
print(f"Conflicts: {len(result['analysis_conflicts'])}")
print(f"Retries: {result['retry_count']}")
```

## 增量分析逻辑 (V3.2)

### 首轮分析 vs 补充分析

| 特性 | 首轮分析 (Round 1) | 补充分析 (Round 2+) |
|------|-------------------|---------------------|
| 数据来源 | 初始数据 (daily_data) | 初始数据 + supplementary_data |
| Prompt 模式 | 标准分析 | 增量分析 (显式说明修正) |
| 上下文 | 无 | 上轮结论 + 上轮问题 |
| 输出要求 | 核心结论 | 必须说明"结论是否修正" |

### 增量分析 Prompt 输出格式

```
【针对上轮问题的回应】
问题: [Supervisor 提出的问题]
新证据: [补充数据中的关键信息]
结论修正: [是/否，以及修正后的结论]

【更新后的分析】
...
```

### Supervisor 跨轮次判断

Supervisor 在 Round 2+ 时会：
1. 接收 `reasoning_steps` (历史决策摘要)
2. 接收 `previous_issues` (上轮未解决问题)
3. 判断 "第二轮搜索是否解决了第一轮提出的矛盾"
4. 在输出中包含 `上轮问题解决情况`:
   ```json
   {
     "resolved": ["已解决的问题"],
     "unresolved": ["仍未解决的问题"],
     "summary": "本轮分析是否有效解决了上轮矛盾"
   }
   ```

## 版本历史

- **V3.0**: Supervisor + Review 架构
- **V3.1**: 
  - 添加 MCP Search 节点
  - 结构化精简三方意见
  - 完整决策链追踪
  - 增强日志规范 (trace_id)
  - 矛盾点针对性搜索
- **V3.2**:
  - 跨轮次记忆 (reasoning_steps)
  - 初始数据与补充数据分离
  - 增量分析 Prompt 升级
  - Supervisor 判断问题解决情况
  - 首轮分析结果备份