"""
统一日志模块
支持 Loki 异步日志推送，带 trace_id 追踪
"""

from common.logger.loki_logger import (
    get_logger,
    setup_loki_handler,
    TraceContext,
    get_trace_id,
    set_trace_id,
)

__all__ = [
    "get_logger",
    "setup_loki_handler",
    "TraceContext",
    "get_trace_id",
    "set_trace_id",
]
