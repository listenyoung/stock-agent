"""
Loki Logger - 统一日志格式与 Loki 集成
支持:
- JSON 格式日志
- trace_id 请求追踪
- 异步 Loki 推送
"""

import logging
import json
import uuid
import sys
from datetime import datetime, timezone
from contextvars import ContextVar
from typing import Optional, Any
from functools import wraps

# Trace ID 上下文变量
_trace_id_var: ContextVar[Optional[str]] = ContextVar("trace_id", default=None)


def get_trace_id() -> str:
    """获取当前请求的 trace_id"""
    trace_id = _trace_id_var.get()
    if trace_id is None:
        trace_id = generate_trace_id()
        _trace_id_var.set(trace_id)
    return trace_id


def set_trace_id(trace_id: str) -> None:
    """设置当前请求的 trace_id"""
    _trace_id_var.set(trace_id)


def generate_trace_id() -> str:
    """生成唯一的 trace_id"""
    return str(uuid.uuid4())


class TraceContext:
    """
    Trace ID 上下文管理器
    用于在请求处理过程中自动管理 trace_id
    
    Usage:
        with TraceContext() as trace_id:
            logger.info("Processing request", extra={"trace_id": trace_id})
    """
    
    def __init__(self, trace_id: Optional[str] = None):
        self.trace_id = trace_id or generate_trace_id()
        self._token = None
    
    def __enter__(self) -> str:
        self._token = _trace_id_var.set(self.trace_id)
        return self.trace_id
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._token is not None:
            _trace_id_var.reset(self._token)
        return False


class JsonFormatter(logging.Formatter):
    """
    JSON 格式日志格式化器
    输出格式适配 Loki/Grafana 日志查询
    """
    
    def __init__(
        self,
        service_name: str = "stock-agent",
        include_trace_id: bool = True,
    ):
        super().__init__()
        self.service_name = service_name
        self.include_trace_id = include_trace_id
    
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "service": self.service_name,
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # 添加 trace_id
        if self.include_trace_id:
            trace_id = getattr(record, "trace_id", None) or get_trace_id()
            if trace_id:
                log_data["trace_id"] = trace_id
        
        # 添加额外字段
        if hasattr(record, "extra_data") and record.extra_data:
            log_data["extra"] = record.extra_data
        
        # 添加异常信息
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        return json.dumps(log_data, ensure_ascii=False, default=str)


class LokiHandler(logging.Handler):
    """
    Loki 日志处理器
    异步发送日志到 Loki
    """
    
    def __init__(
        self,
        url: str,
        labels: Optional[dict] = None,
        batch_size: int = 100,
        flush_interval: float = 5.0,
    ):
        super().__init__()
        self.url = url
        self.labels = labels or {}
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self._buffer: list = []
        self._initialized = False
    
    def _lazy_init(self):
        """延迟初始化，避免导入时的循环依赖"""
        if self._initialized:
            return
        
        try:
            import logging_loki
            self._loki_handler = logging_loki.LokiHandler(
                url=f"{self.url}/loki/api/v1/push",
                tags=self.labels,
                version="1",
            )
            self._initialized = True
        except ImportError:
            # 如果没有安装 loki 库，使用标准输出
            self._loki_handler = None
            self._initialized = True
    
    def emit(self, record: logging.LogRecord):
        self._lazy_init()
        
        if self._loki_handler:
            try:
                self._loki_handler.emit(record)
            except Exception:
                # 发送失败时降级到标准输出
                self.handleError(record)
        else:
            # Loki 不可用时输出到 stderr
            try:
                msg = self.format(record)
                sys.stderr.write(msg + "\n")
            except Exception:
                self.handleError(record)


class StockAgentLogger(logging.Logger):
    """
    Stock Agent 专用 Logger
    自动注入 trace_id 和服务上下文
    """
    
    def __init__(self, name: str, level: int = logging.NOTSET):
        super().__init__(name, level)
    
    def _log(
        self,
        level: int,
        msg: object,
        args,
        exc_info=None,
        extra: Optional[dict] = None,
        stack_info: bool = False,
        stacklevel: int = 1,
    ):
        # 自动注入 trace_id
        if extra is None:
            extra = {}
        
        if "trace_id" not in extra:
            extra["trace_id"] = get_trace_id()
        
        # 保存额外数据
        extra_data = {k: v for k, v in extra.items() if k not in ("trace_id",)}
        if extra_data:
            extra["extra_data"] = extra_data
        
        super()._log(level, msg, args, exc_info, extra, stack_info, stacklevel + 1)


# 设置自定义 Logger 类
logging.setLoggerClass(StockAgentLogger)

# 缓存已创建的 logger
_loggers: dict[str, logging.Logger] = {}


def get_logger(
    name: str,
    level: int = logging.INFO,
    service_name: str = "stock-agent",
) -> logging.Logger:
    """
    获取统一配置的 Logger 实例
    
    Args:
        name: Logger 名称，通常使用 __name__
        level: 日志级别
        service_name: 服务名称，用于日志标签
    
    Returns:
        配置好的 Logger 实例
    """
    if name in _loggers:
        return _loggers[name]
    
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # 避免重复添加 handler
    if not logger.handlers:
        # 控制台 Handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(JsonFormatter(service_name=service_name))
        logger.addHandler(console_handler)
    
    _loggers[name] = logger
    return logger


def setup_loki_handler(
    logger: logging.Logger,
    loki_url: str,
    service_name: str = "stock-agent",
    extra_labels: Optional[dict] = None,
) -> None:
    """
    为 Logger 添加 Loki Handler
    
    Args:
        logger: Logger 实例
        loki_url: Loki 服务地址
        service_name: 服务名称
        extra_labels: 额外的标签
    """
    labels = {
        "service": service_name,
        "environment": "production",
    }
    if extra_labels:
        labels.update(extra_labels)
    
    loki_handler = LokiHandler(url=loki_url, labels=labels)
    loki_handler.setFormatter(JsonFormatter(service_name=service_name))
    logger.addHandler(loki_handler)


def log_execution_time(logger: Optional[logging.Logger] = None):
    """
    装饰器：记录函数执行时间
    
    Usage:
        @log_execution_time()
        async def my_function():
            pass
    """
    def decorator(func):
        nonlocal logger
        if logger is None:
            logger = get_logger(func.__module__)
        
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            import time
            start = time.perf_counter()
            try:
                result = await func(*args, **kwargs)
                elapsed = time.perf_counter() - start
                logger.info(
                    f"{func.__name__} completed",
                    extra={"duration_ms": round(elapsed * 1000, 2)},
                )
                return result
            except Exception as e:
                elapsed = time.perf_counter() - start
                logger.error(
                    f"{func.__name__} failed: {e}",
                    extra={"duration_ms": round(elapsed * 1000, 2)},
                    exc_info=True,
                )
                raise
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            import time
            start = time.perf_counter()
            try:
                result = func(*args, **kwargs)
                elapsed = time.perf_counter() - start
                logger.info(
                    f"{func.__name__} completed",
                    extra={"duration_ms": round(elapsed * 1000, 2)},
                )
                return result
            except Exception as e:
                elapsed = time.perf_counter() - start
                logger.error(
                    f"{func.__name__} failed: {e}",
                    extra={"duration_ms": round(elapsed * 1000, 2)},
                    exc_info=True,
                )
                raise
        
        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper
    
    return decorator
