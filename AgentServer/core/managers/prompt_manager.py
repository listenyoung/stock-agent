"""
提示词管理器 (V3.0)

功能：
1. 基于 Jinja2 的模板渲染引擎 (StrictUndefined 模式)
2. 在 initialize() 时预加载所有 YAML 到内存
3. 支持模板变量替换，缺失变量会抛出明确错误
4. 预编译模板提升运行时性能
"""

from typing import Dict, Any, Optional
from pathlib import Path
import yaml

from jinja2 import Environment, BaseLoader, TemplateNotFound, StrictUndefined, Template

from core.managers.base import BaseManager


class PromptManager(BaseManager):
    """
    提示词管理器 (V3.0)
    
    特性：
    - 启动时预加载所有 YAML 配置到内存
    - 预编译 Jinja2 模板
    - StrictUndefined 模式：缺失变量会抛出 UndefinedError
    
    Usage:
        await prompt_manager.initialize()
        
        # 获取渲染后的提示词 (缺失变量会报错)
        prompt = prompt_manager.get_prompt(
            "stock_analysis/fundamental",
            ts_code="000001.SZ",
            industry="银行"
        )
        
        # 获取系统提示词
        system = prompt_manager.get_system_prompt("stock_analysis/fundamental")
        
        # 列出所有提示词
        names = prompt_manager.list_prompts()
    """
    
    def __init__(self):
        super().__init__()
        # 原始配置存储
        self._configs: Dict[str, Dict[str, Any]] = {}
        # 预编译的模板缓存
        self._templates: Dict[str, Template] = {}
        # Jinja2 环境
        self._env: Optional[Environment] = None
        # prompts 目录路径
        self._prompts_dir: Optional[Path] = None
    
    async def initialize(self) -> None:
        """
        初始化：预加载所有 YAML 配置并编译模板
        
        执行步骤：
        1. 扫描 core/prompts/ 目录
        2. 加载所有 YAML 文件到内存
        3. 预编译所有 Jinja2 模板
        """
        if self._initialized:
            return
        
        # 确定 prompts 目录路径
        self._prompts_dir = Path(__file__).parent.parent / "prompts"
        
        if not self._prompts_dir.exists():
            self._prompts_dir.mkdir(parents=True, exist_ok=True)
            self.logger.warning(f"Created prompts directory: {self._prompts_dir}")
        
        # 创建 Jinja2 环境 (StrictUndefined 模式)
        self._env = Environment(
            undefined=StrictUndefined,  # 缺失变量抛出错误
            autoescape=False,
            trim_blocks=True,
            lstrip_blocks=True,
        )
        
        # 递归加载所有 YAML 文件
        yaml_files = list(self._prompts_dir.rglob("*.yaml"))
        self.logger.info(f"Found {len(yaml_files)} YAML files in {self._prompts_dir}")
        
        for yaml_file in yaml_files:
            try:
                self._load_yaml_file_sync(yaml_file)
            except Exception as e:
                self.logger.error(f"Failed to load {yaml_file}: {e}")
        
        # 预编译所有模板
        self._precompile_templates()
        
        self._initialized = True
        self.logger.info(
            f"PromptManager initialized: {len(self._configs)} prompts loaded, "
            f"{len(self._templates)} templates compiled"
        )
    
    def _load_yaml_file_sync(self, file_path: Path) -> None:
        """同步加载单个 YAML 文件"""
        with open(file_path, "r", encoding="utf-8") as f:
            content = yaml.safe_load(f)
        
        if not content:
            self.logger.debug(f"Empty YAML file: {file_path}")
            return
        
        # 计算相对路径作为 key 前缀
        # e.g., core/prompts/stock_analysis/fundamental.yaml -> stock_analysis/fundamental
        relative_path = file_path.relative_to(self._prompts_dir)
        prefix = str(relative_path.with_suffix("")).replace("\\", "/")
        
        # 处理 YAML 结构
        if isinstance(content, dict):
            # 单个提示词配置 (有 template 或 system_prompt 字段)
            if "template" in content or "system_prompt" in content:
                self._configs[prefix] = content
                self.logger.debug(f"Loaded prompt config: {prefix}")
            else:
                # 多个提示词配置 (嵌套结构)
                for key, value in content.items():
                    if isinstance(value, dict):
                        full_key = f"{prefix}/{key}"
                        self._configs[full_key] = value
                        self.logger.debug(f"Loaded prompt config: {full_key}")
    
    def _precompile_templates(self) -> None:
        """预编译所有 Jinja2 模板"""
        for name, config in self._configs.items():
            # 编译 template (优先) 或 user_prompt
            template_str = config.get("template") or config.get("user_prompt", "")
            if template_str:
                try:
                    compiled = self._env.from_string(template_str)
                    self._templates[name] = compiled
                    self.logger.debug(f"Compiled template: {name}")
                except Exception as e:
                    self.logger.error(f"Failed to compile template {name}: {e}")
            
            # 编译 system_prompt (V3.2 支持)
            system_prompt_str = config.get("system_prompt", "")
            if system_prompt_str and ("{{" in system_prompt_str or "{%" in system_prompt_str):
                try:
                    compiled = self._env.from_string(system_prompt_str)
                    self._templates[f"{name}:system"] = compiled
                    self.logger.debug(f"Compiled system_prompt template: {name}")
                except Exception as e:
                    self.logger.error(f"Failed to compile system_prompt {name}: {e}")
    
    async def shutdown(self) -> None:
        """关闭管理器，释放内存"""
        self._configs.clear()
        self._templates.clear()
        self._env = None
        self._initialized = False
        self.logger.info("PromptManager shutdown")
    
    async def health_check(self) -> bool:
        """健康检查"""
        return self._initialized and len(self._configs) > 0
    
    def get_prompt(
        self,
        name: str,
        **kwargs: Any,
    ) -> str:
        """
        获取渲染后的提示词
        
        Args:
            name: 提示词名称 (e.g., "stock_analysis/fundamental")
            **kwargs: 模板变量
            
        Returns:
            渲染后的提示词字符串
            
        Raises:
            KeyError: 提示词不存在
            jinja2.UndefinedError: 缺失必需的模板变量
        """
        self._ensure_initialized()
        
        if name not in self._templates:
            if name in self._configs:
                # 配置存在但无 template
                return ""
            raise KeyError(f"Prompt not found: {name}. Available: {list(self._configs.keys())}")
        
        # 使用预编译的模板渲染
        template = self._templates[name]
        return template.render(**kwargs)
    
    def get_system_prompt(self, name: str, **kwargs: Any) -> str:
        """
        获取系统提示词 (支持 Jinja2 模板渲染)
        
        Args:
            name: 提示词名称
            **kwargs: 模板变量 (用于 system_prompt 中的 Jinja2 语法)
            
        Returns:
            渲染后的系统提示词字符串
            
        Raises:
            KeyError: 提示词不存在
        """
        self._ensure_initialized()
        
        if name not in self._configs:
            raise KeyError(f"Prompt not found: {name}. Available: {list(self._configs.keys())}")
        
        system_key = f"{name}:system"
        
        # 如果有预编译的 system_prompt 模板，使用模板渲染
        if system_key in self._templates:
            return self._templates[system_key].render(**kwargs)
        
        # 否则返回原始字符串
        return self._configs[name].get("system_prompt", "")
    
    def get_config(self, name: str) -> Dict[str, Any]:
        """
        获取完整的提示词配置
        
        Args:
            name: 提示词名称
            
        Returns:
            完整配置字典 (包含 template, system_prompt, output_format 等)
            
        Raises:
            KeyError: 提示词不存在
        """
        self._ensure_initialized()
        
        if name not in self._configs:
            raise KeyError(f"Prompt not found: {name}. Available: {list(self._configs.keys())}")
        
        return self._configs[name].copy()  # 返回副本防止意外修改
    
    def list_prompts(self) -> list:
        """列出所有已加载的提示词名称"""
        return list(self._configs.keys())
    
    def has_prompt(self, name: str) -> bool:
        """检查提示词是否存在"""
        return name in self._configs
    
    def get_stats(self) -> Dict[str, Any]:
        """获取管理器统计信息"""
        return {
            "initialized": self._initialized,
            "prompts_count": len(self._configs),
            "templates_count": len(self._templates),
            "prompts_dir": str(self._prompts_dir) if self._prompts_dir else None,
        }
    
    async def reload(self) -> None:
        """
        热重载所有提示词配置
        
        用于开发调试，无需重启服务即可更新提示词
        """
        self._configs.clear()
        self._templates.clear()
        
        yaml_files = list(self._prompts_dir.rglob("*.yaml"))
        for yaml_file in yaml_files:
            try:
                self._load_yaml_file_sync(yaml_file)
            except Exception as e:
                self.logger.error(f"Failed to reload {yaml_file}: {e}")
        
        self._precompile_templates()
        self.logger.info(f"Reloaded {len(self._configs)} prompts")


# ==================== 全局单例 ====================
prompt_manager = PromptManager()
