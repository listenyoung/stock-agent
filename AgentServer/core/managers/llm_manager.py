"""
LLM 管理器

负责:
- 多 Provider 支持 (OpenAI, DashScope, DeepSeek, Ollama, 智谱)
- 并发控制
- Token 统计
"""

import asyncio
from typing import Optional, List, Dict, Any, AsyncGenerator

from .base import BaseManager
from ..settings import settings


class LLMManager(BaseManager):
    """
    LLM 资源管理器
    
    支持多种 Provider:
    - openai: OpenAI API (GPT 系列)
    - dashscope: 阿里云 DashScope (通义千问)
    - deepseek: DeepSeek API
    - zhipu: 智谱 AI (GLM 系列)
    - ollama: 本地 Ollama
    
    注意: DeepSeek 不支持 Embedding API，需要单独配置 embedding_provider
    """
    
    def __init__(self):
        super().__init__()
        self._config = settings.llm
        self._client = None
        self._embedding_client = None  # 独立的 Embedding 客户端
        self._semaphore: Optional[asyncio.Semaphore] = None
        
        # Token 统计
        self._total_prompt_tokens = 0
        self._total_completion_tokens = 0
    
    async def initialize(self) -> None:
        """初始化 LLM 客户端"""
        if self._initialized:
            return
        
        provider = self._config.provider
        self.logger.info(f"Initializing LLM provider: {provider}")
        
        # 初始化并发限制
        self._semaphore = asyncio.Semaphore(self._config.max_concurrent_requests)
        
        if provider == "openai":
            await self._init_openai()
        elif provider == "dashscope":
            await self._init_dashscope()
        elif provider == "deepseek":
            await self._init_deepseek()
        elif provider == "zhipu":
            await self._init_zhipu()
        elif provider == "ollama":
            await self._init_ollama()
        else:
            raise ValueError(f"Unsupported LLM provider: {provider}")
        
        # 初始化独立的 Embedding 客户端（如果配置了）
        await self._init_embedding_client()
        
        self._initialized = True
        self.logger.info(f"LLM initialized, model={self._config.model_name} ✓")
    
    async def _init_openai(self) -> None:
        """初始化 OpenAI"""
        from openai import AsyncOpenAI
        
        self._client = AsyncOpenAI(
            api_key=self._config.api_key.get_secret_value() if self._config.api_key else None,
            base_url=self._config.api_base,
        )
    
    async def _init_dashscope(self) -> None:
        """初始化 DashScope"""
        from openai import AsyncOpenAI
        
        # DashScope 兼容 OpenAI API
        self._client = AsyncOpenAI(
            api_key=self._config.api_key.get_secret_value() if self._config.api_key else None,
            base_url=self._config.api_base or "https://dashscope.aliyuncs.com/compatible-mode/v1",
        )
    
    async def _init_deepseek(self) -> None:
        """初始化 DeepSeek"""
        from openai import AsyncOpenAI
        
        # DeepSeek 兼容 OpenAI API
        self._client = AsyncOpenAI(
            api_key=self._config.api_key.get_secret_value() if self._config.api_key else None,
            base_url=self._config.api_base or "https://api.deepseek.com/v1",
        )
    
    async def _init_zhipu(self) -> None:
        """初始化智谱 AI"""
        from openai import AsyncOpenAI
        
        # 智谱 AI 兼容 OpenAI API
        self._client = AsyncOpenAI(
            api_key=self._config.api_key.get_secret_value() if self._config.api_key else None,
            base_url=self._config.api_base or "https://open.bigmodel.cn/api/paas/v4",
        )
    
    async def _init_ollama(self) -> None:
        """初始化 Ollama"""
        from openai import AsyncOpenAI
        
        # Ollama 也兼容 OpenAI API
        self._client = AsyncOpenAI(
            api_key="ollama",  # Ollama 不需要 key
            base_url=self._config.api_base or "http://localhost:11434/v1",
        )
    
    async def _init_embedding_client(self) -> None:
        """
        初始化独立的 Embedding 客户端
        
        如果配置了 embedding_provider，使用独立的客户端
        否则使用主 LLM 客户端
        """
        embedding_provider = self._config.embedding_provider
        
        if not embedding_provider:
            # 没有配置独立 provider，使用主客户端
            self._embedding_client = self._client
            return
        
        from openai import AsyncOpenAI
        
        # 获取 Embedding API Key
        embedding_api_key = None
        if self._config.embedding_api_key:
            embedding_api_key = self._config.embedding_api_key.get_secret_value()
        
        # 根据 provider 配置 base_url
        base_url_map = {
            "openai": "https://api.openai.com/v1",
            "dashscope": "https://dashscope.aliyuncs.com/compatible-mode/v1",
            "zhipu": "https://open.bigmodel.cn/api/paas/v4",
            "ollama": "http://localhost:11434/v1",
        }
        
        base_url = self._config.embedding_api_base or base_url_map.get(embedding_provider)
        
        self._embedding_client = AsyncOpenAI(
            api_key=embedding_api_key or "ollama",
            base_url=base_url,
        )
        
        self.logger.info(
            f"Embedding client initialized: provider={embedding_provider}, "
            f"model={self._config.embedding_model}"
        )
    
    async def shutdown(self) -> None:
        """关闭"""
        self._client = None
        self._initialized = False
        self.logger.info(
            f"LLM shutdown. Total tokens: prompt={self._total_prompt_tokens}, "
            f"completion={self._total_completion_tokens}"
        )
    
    async def health_check(self) -> bool:
        """健康检查"""
        if not self._initialized or self._client is None:
            return False
        
        try:
            # 简单调用测试
            await self.chat([{"role": "user", "content": "hi"}], max_tokens=5)
            return True
        except Exception:
            return False
    
    # ==================== Chat 接口 ====================
    
    async def chat(
        self,
        messages: List[Dict[str, str]],
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        **kwargs,
    ) -> str:
        """
        Chat 调用
        
        Args:
            messages: 消息列表
            model: 模型名称，默认使用配置
            temperature: 温度
            max_tokens: 最大 token 数
            
        Returns:
            模型响应文本
        """
        self._ensure_initialized()
        
        async with self._semaphore:
            response = await self._client.chat.completions.create(
                model=model or self._config.model_name,
                messages=messages,
                temperature=temperature or self._config.temperature,
                max_tokens=max_tokens or self._config.max_tokens,
                **kwargs,
            )
        
        # 统计 Token
        if response.usage:
            self._total_prompt_tokens += response.usage.prompt_tokens
            self._total_completion_tokens += response.usage.completion_tokens
        
        return response.choices[0].message.content
    
    async def chat_stream(
        self,
        messages: List[Dict[str, str]],
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        **kwargs,
    ) -> AsyncGenerator[str, None]:
        """
        流式 Chat 调用
        
        Yields:
            模型响应文本片段
        """
        self._ensure_initialized()
        
        async with self._semaphore:
            stream = await self._client.chat.completions.create(
                model=model or self._config.model_name,
                messages=messages,
                temperature=temperature or self._config.temperature,
                max_tokens=max_tokens or self._config.max_tokens,
                stream=True,
                **kwargs,
            )
            
            async for chunk in stream:
                if chunk.choices and chunk.choices[0].delta.content:
                    yield chunk.choices[0].delta.content

    async def create_chat_completion(
        self,
        messages: List[Dict[str, Any]],
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        **kwargs,
    ) -> Any:
        """
        创建原始 Chat Completion 响应。

        Agent Runtime 使用这个方法读取 function/tool calls、usage 和其他
        provider-specific 字段；普通业务仍可继续使用 chat/chat_stream。
        """
        self._ensure_initialized()

        async with self._semaphore:
            response = await self._client.chat.completions.create(
                model=model or self._config.model_name,
                messages=messages,
                temperature=temperature if temperature is not None else self._config.temperature,
                max_tokens=max_tokens or self._config.max_tokens,
                **kwargs,
            )

        if response.usage:
            self._total_prompt_tokens += response.usage.prompt_tokens
            self._total_completion_tokens += response.usage.completion_tokens

        return response
    
    # ==================== Embedding 接口 ====================
    
    async def embedding(
        self,
        texts: List[str],
        model: Optional[str] = None,
    ) -> List[List[float]]:
        """
        获取文本向量
        
        使用独立的 embedding_client（如果配置了 embedding_provider）
        否则使用主 LLM 客户端
        
        Args:
            texts: 文本列表
            model: 模型名称
            
        Returns:
            向量列表
        """
        self._ensure_initialized()
        
        # 使用 Embedding 专用客户端
        client = self._embedding_client or self._client
        
        async with self._semaphore:
            response = await client.embeddings.create(
                model=model or self._config.embedding_model,
                input=texts,
            )
        
        return [item.embedding for item in response.data]
    
    # ==================== 统计 ====================
    
    def get_token_usage(self) -> Dict[str, int]:
        """获取 Token 使用统计"""
        return {
            "prompt_tokens": self._total_prompt_tokens,
            "completion_tokens": self._total_completion_tokens,
            "total_tokens": self._total_prompt_tokens + self._total_completion_tokens,
        }
    
    def get_status(self) -> dict:
        """获取状态"""
        status = super().get_status()
        status.update({
            "provider": self._config.provider,
            "model": self._config.model_name,
            "token_usage": self.get_token_usage(),
        })
        return status


# ==================== 全局单例 ====================
llm_manager = LLMManager()
