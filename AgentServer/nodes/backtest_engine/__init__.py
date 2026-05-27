"""
量化回测引擎模块

设计理念：数据与逻辑解耦
- 独立节点设计：回测引擎作为独立计算单元，接受"决策矩阵"
- 因子输入标准化：支持技术指标、舆情情绪、财报估值等多因子

核心组件：
- BacktestNode: 回测引擎节点
- FactorData: 标准化因子数据容器
- VectorizedBacktester: 向量化回测引擎
- PerformanceAnalyzer: 绩效评估与回撤系统
- CustomFactorInterface: 自定义因子接口（扩展用）
"""

from .factors import FactorData, CustomFactorInterface
from .backtester import VectorizedBacktester, BacktestConfig, BacktestResult
from .performance import PerformanceAnalyzer, PerformanceMetrics
from .node import BacktestNode

__all__ = [
    # 节点
    "BacktestNode",
    # 因子系统
    "FactorData",
    "CustomFactorInterface",
    # 回测引擎
    "VectorizedBacktester",
    "BacktestConfig",
    "BacktestResult",
    # 绩效分析
    "PerformanceAnalyzer",
    "PerformanceMetrics",
]
