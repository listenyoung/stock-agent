"""
分析智能体节点实现
"""

import asyncio
from typing import Optional
from datetime import datetime
import json

from nodes.base import BaseNode
from core.protocols import (
    NodeType,
    AgentTask,
    AgentResponse,
    TaskProgress,
    TaskStatus,
)
from core.managers import (
    redis_manager,
    mongo_manager,
    llm_manager,
    milvus_manager,
)

from .graph import StockAnalysisGraph


class InferenceNode(BaseNode):
    """
    分析智能体节点
    
    职责:
    - 从 Redis 队列消费分析任务
    - 使用 LangGraph 工作流执行分析
    - 支持动态扩容 (启动多个实例)
    - 通过 gRPC RPC 接收远程调用
    """
    
    node_type = NodeType.INFERENCE
    DEFAULT_RPC_PORT = 50052  # InferenceNode 默认 RPC 端口
    
    def __init__(
        self,
        node_id: Optional[str] = None,
        max_concurrent_tasks: int = 5,
        rpc_port: int = 0,
    ):
        from core.settings import settings
        super().__init__(node_id, rpc_port or settings.rpc.inference_port)
        
        self._max_tasks = max_concurrent_tasks
        self._semaphore: Optional[asyncio.Semaphore] = None
        
        # 分析图
        self._graph: Optional[StockAnalysisGraph] = None
    
    async def start(self) -> None:
        """启动推理节点"""
        # 按依赖顺序初始化管理器
        self.logger.info("Initializing managers...")
        await redis_manager.initialize()      # 任务队列
        await mongo_manager.initialize()      # 任务持久化
        await llm_manager.initialize()        # LLM
        await milvus_manager.initialize()     # 向量检索
        
        # 启动 RPC 服务器
        await self._start_rpc_server()
        
        # 创建并发限制
        self._semaphore = asyncio.Semaphore(self._max_tasks)
        
        # 初始化分析图
        self._graph = StockAnalysisGraph()
        await self._graph.initialize()
        
        self.logger.info(
            f"Inference node started, max_concurrent_tasks={self._max_tasks}, "
            f"rpc={self._rpc_address}"
        )
    
    async def stop(self) -> None:
        """停止节点"""
        # 等待当前任务完成
        while self._current_tasks > 0:
            self.logger.info(f"Waiting for {self._current_tasks} tasks to complete...")
            await asyncio.sleep(1)
    
    async def run(self) -> None:
        """节点主循环"""
        self.logger.info("Inference node listening for tasks...")
        
        while self._running:
            try:
                # 检查是否有空余容量
                if self._current_tasks >= self._max_tasks:
                    await asyncio.sleep(0.5)
                    continue
                
                # 从队列获取任务
                task_json = await redis_manager.dequeue_task(timeout=5)
                
                if task_json:
                    task_data = json.loads(task_json)
                    
                    # 跳过工具调用请求 (给 MCP 节点处理)
                    if task_data.get("tool_name"):
                        continue
                    
                    task = AgentTask(**task_data)
                    
                    # 异步处理任务
                    asyncio.create_task(self._process_task(task))
                    
            except Exception as e:
                self.logger.error(f"Error in main loop: {e}")
                await asyncio.sleep(1)
    
    async def _process_task(self, task: AgentTask) -> None:
        """处理单个任务"""
        async with self._semaphore:
            self._current_tasks += 1
            
            # 设置 trace_id (用于日志)
            self.set_trace_id(task.trace_id)
            
            start_time = datetime.utcnow()
            
            try:
                self.logger.info(f"Processing task: {task.task_id}, type={task.task_type}")
                
                # 更新任务状态为运行中
                await mongo_manager.update_one(
                    "tasks",
                    {"task_id": task.task_id},
                    {"$set": {
                        "status": TaskStatus.RUNNING.value,
                        "started_at": start_time,
                        "node_id": self.node_id,
                    }},
                )
                
                # 发送进度通知
                await self._publish_progress(task, 0, "开始分析...")
                
                # 执行分析
                result = await self._execute_analysis(task)
                
                # 计算执行时间
                execution_time_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
                
                # 更新任务结果
                await mongo_manager.update_one(
                    "tasks",
                    {"task_id": task.task_id},
                    {"$set": {
                        "status": TaskStatus.COMPLETED.value,
                        "completed_at": datetime.utcnow(),
                        "result": result,
                        "execution_time_ms": execution_time_ms,
                    }},
                )
                
                # 发布结果
                response = AgentResponse(
                    task_id=task.task_id,
                    trace_id=task.trace_id,
                    status=TaskStatus.COMPLETED,
                    result=result,
                    source_node=self.node_id,
                    execution_time_ms=execution_time_ms,
                )
                
                await redis_manager.publish_result(task.task_id, response)
                
                self.logger.info(
                    f"Task completed: {task.task_id}, duration={execution_time_ms:.2f}ms"
                )
                
            except Exception as e:
                self.logger.exception(f"Task failed: {task.task_id}, error={e}")
                
                # 更新任务状态为失败
                await mongo_manager.update_one(
                    "tasks",
                    {"task_id": task.task_id},
                    {"$set": {
                        "status": TaskStatus.FAILED.value,
                        "error_message": str(e),
                    }},
                )
                
                # 发布失败结果
                response = AgentResponse(
                    task_id=task.task_id,
                    trace_id=task.trace_id,
                    status=TaskStatus.FAILED,
                    error_message=str(e),
                    source_node=self.node_id,
                )
                
                await redis_manager.publish_result(task.task_id, response)
                
            finally:
                self._current_tasks -= 1
                self.clear_trace_id()
    
    async def _execute_analysis(self, task: AgentTask) -> dict:
        """执行分析任务"""
        task_type = task.task_type.value
        
        if task_type == "stock_analysis":
            return await self._analyze_stock(task)
        elif task_type == "market_overview":
            return await self._analyze_market(task)
        elif task_type == "custom_query":
            return await self._custom_query(task)
        else:
            raise ValueError(f"Unsupported task type: {task_type}")
    
    async def _analyze_stock(self, task: AgentTask) -> dict:
        """个股分析"""
        if not task.ts_codes:
            raise ValueError("No stock codes provided")
        
        ts_code = task.ts_codes[0]
        
        result = await self._graph.analyze_stock(
            ts_code=ts_code,
            task_id=task.task_id,
            progress_callback=lambda p, m: self._publish_progress(task, p, m),
        )
        
        return result
    
    async def _analyze_market(self, task: AgentTask) -> dict:
        """大盘分析"""
        result = await self._graph.analyze_market(
            task_id=task.task_id,
            progress_callback=lambda p, m: self._publish_progress(task, p, m),
        )
        
        return result
    
    async def _custom_query(self, task: AgentTask) -> dict:
        """自然语言查询"""
        if not task.query:
            raise ValueError("No query provided")
        
        result = await self._graph.custom_query(
            query=task.query,
            task_id=task.task_id,
            progress_callback=lambda p, m: self._publish_progress(task, p, m),
        )
        
        return result
    
    async def _publish_progress(
        self,
        task: AgentTask,
        progress: float,
        message: str,
    ) -> None:
        """发布进度更新"""
        progress_msg = TaskProgress(
            task_id=task.task_id,
            trace_id=task.trace_id,
            status=TaskStatus.RUNNING,
            progress=progress,
            message=message,
            source_node=self.node_id,
        )
        
        await redis_manager.publish_result(task.task_id, progress_msg)


def main():
    """入口函数"""
    import os
    
    max_tasks = int(os.environ.get("MAX_CONCURRENT_TASKS", 5))
    
    node = InferenceNode(max_concurrent_tasks=max_tasks)
    asyncio.run(node.main())


if __name__ == "__main__":
    main()
