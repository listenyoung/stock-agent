"""LangGraph-style execution plan metadata.

The current runtime is intentionally lightweight, but this module makes the
agent lifecycle explicit so future LangGraph migration, replay, and RL data
extraction share the same conceptual nodes.
"""

from typing import List

from pydantic import BaseModel


class AgentGraphNode(BaseModel):
    """A logical node in the agent execution graph."""

    name: str
    description: str


class AgentGraphEdge(BaseModel):
    """A directed edge in the agent execution graph."""

    source: str
    target: str


class AgentGraphSpec(BaseModel):
    """Serializable graph specification for frontend visualization."""

    nodes: List[AgentGraphNode]
    edges: List[AgentGraphEdge]


def get_default_agent_graph() -> AgentGraphSpec:
    """Return the default StockAgent runtime graph."""
    nodes = [
        AgentGraphNode(name="start_run", description="创建 run、保存用户消息并发出启动事件"),
        AgentGraphNode(name="load_memory", description="读取短期历史和长期记忆"),
        AgentGraphNode(name="plan", description="生成结构化计划和风险等级"),
        AgentGraphNode(name="compress_context", description="按 token 预算压缩消息、记忆和工具结果"),
        AgentGraphNode(name="execute_tools", description="执行只读工具并记录 tool trace"),
        AgentGraphNode(name="run_sub_agents", description="并行执行 Market/Technical/News/Fundamental/Backtest 子 Agent，并由 RiskAgent 审查"),
        AgentGraphNode(name="reflect", description="反思工具信息是否足够以及缺失项"),
        AgentGraphNode(name="final_answer", description="基于工具观测生成最终回答"),
        AgentGraphNode(name="critic", description="审查最终回答的完整性、事实性和风险提示"),
        AgentGraphNode(name="write_memory", description="将有价值的交互摘要写入长期记忆"),
        AgentGraphNode(name="complete_run", description="完成 run 并保存最终 trace"),
    ]
    edges = [
        AgentGraphEdge(source="start_run", target="load_memory"),
        AgentGraphEdge(source="load_memory", target="plan"),
        AgentGraphEdge(source="plan", target="compress_context"),
        AgentGraphEdge(source="compress_context", target="execute_tools"),
        AgentGraphEdge(source="execute_tools", target="run_sub_agents"),
        AgentGraphEdge(source="run_sub_agents", target="reflect"),
        AgentGraphEdge(source="reflect", target="final_answer"),
        AgentGraphEdge(source="final_answer", target="critic"),
        AgentGraphEdge(source="critic", target="write_memory"),
        AgentGraphEdge(source="write_memory", target="complete_run"),
    ]
    return AgentGraphSpec(nodes=nodes, edges=edges)
