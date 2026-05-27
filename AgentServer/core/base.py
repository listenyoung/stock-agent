"""
基类定义

所有 Tool、Node、Collector 的基类。
"""

from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Optional, Dict, Any, Type
from datetime import datetime
import time
import logging

from pydantic import BaseModel


# ==================== 工具基类 ====================


InputT = TypeVar("InputT", bound=BaseModel)
OutputT = TypeVar("OutputT", bound=BaseModel)


class ToolResult(BaseModel):
    """工具执行结果"""
    success: bool = True
    error_message: Optional[str] = None
    execution_time_ms: float = 0


class BaseTool(ABC, Generic[InputT, OutputT]):
    """
    工具基类
    
    所有 MCP 工具必须继承此类。
    
    Example:
        class GetStockBasicInput(BaseModel):
            ts_code: str
        
        class GetStockBasicOutput(ToolResult):
            data: dict
        
        class GetStockBasicTool(BaseTool[GetStockBasicInput, GetStockBasicOutput]):
            name = "get_stock_basic"
            description = "获取股票基础信息"
            input_model = GetStockBasicInput
            output_model = GetStockBasicOutput
            
            async def execute(self, input: GetStockBasicInput) -> GetStockBasicOutput:
                data = await tushare_manager.get_stock_basic(input.ts_code)
                return GetStockBasicOutput(data=data)
    """
    
    name: str
    description: str
    input_model: Type[InputT]
    output_model: Type[OutputT]
    
    def __init__(self):
        self.logger = logging.getLogger(f"tool.{self.name}")
    
    @abstractmethod
    async def execute(self, input_data: InputT) -> OutputT:
        """
        执行工具
        
        子类必须实现此方法。
        """
        raise NotImplementedError
    
    async def __call__(self, input_data: InputT) -> OutputT:
        """调用工具"""
        start_time = time.time()
        
        try:
            result = await self.execute(input_data)
            result.execution_time_ms = (time.time() - start_time) * 1000
            return result
        except Exception as e:
            self.logger.exception(f"Tool execution failed: {e}")
            return self.output_model(
                success=False,
                error_message=str(e),
                execution_time_ms=(time.time() - start_time) * 1000,
            )
    
    def get_schema(self) -> dict:
        """获取工具 Schema (用于 LLM Function Calling)"""
        return {
            "name": self.name,
            "description": self.description,
            "parameters": self.input_model.model_json_schema(),
        }


# ==================== 采集器基类 ====================


class BaseCollector(ABC):
    """
    采集器基类
    
    所有数据采集器必须继承此类。
    
    调度时间支持:
    - 通过 default_schedule 类属性设置默认值
    - 通过 @property schedule 从配置读取，未配置则使用默认值
    
    Example:
        class StockBasicCollector(BaseCollector):
            name = "stock_basic"
            description = "采集股票基础信息"
            default_schedule = "0 9 * * 1-5"  # 默认 cron 表达式
            
            @property
            def schedule(self) -> str:
                return settings.data_sync.stock_basic_schedule or self.default_schedule
            
            async def collect(self) -> dict:
                data = await tushare_manager.get_stock_basic()
                await mongo_manager.bulk_upsert("stock_basic", data)
                return {"count": len(data)}
    """
    
    name: str
    description: str
    default_schedule: str  # 默认 cron 表达式
    
    @property
    def schedule(self) -> str:
        """
        获取调度时间
        
        子类可重写此 property 从配置读取。
        """
        return self.default_schedule
    
    def __init__(self):
        self.logger = logging.getLogger(f"collector.{self.name}")
        self._last_run: Optional[datetime] = None
        self._last_result: Optional[dict] = None
    
    @abstractmethod
    async def collect(self) -> Dict[str, Any]:
        """
        执行采集
        
        子类必须实现此方法。
        
        Returns:
            采集结果，至少包含 count 字段
        """
        raise NotImplementedError
    
    async def run(self) -> dict:
        """运行采集器"""
        start_time = time.time()
        
        try:
            result = await self.collect()
            
            duration_ms = (time.time() - start_time) * 1000
            
            self._last_run = datetime.utcnow()
            self._last_result = {
                "success": True,
                "count": result.get("count", 0),
                "duration_ms": duration_ms,
                **result,
            }
            
            return self._last_result
            
        except Exception as e:
            self.logger.exception(f"Collector failed: {e}")
            
            duration_ms = (time.time() - start_time) * 1000
            
            self._last_run = datetime.utcnow()
            self._last_result = {
                "success": False,
                "error": str(e),
                "duration_ms": duration_ms,
            }
            
            return self._last_result
    
    @property
    def status(self) -> dict:
        """获取采集器状态"""
        return {
            "name": self.name,
            "description": self.description,
            "schedule": self.schedule,
            "last_run": self._last_run.isoformat() if self._last_run else None,
            "last_result": self._last_result,
        }
