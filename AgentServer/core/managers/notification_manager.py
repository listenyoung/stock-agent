"""
通知管理器 (企业微信 Webhook)

负责:
- 发送企业微信 Markdown 消息
- 消息频率控制
- 失败重试
"""

import asyncio
from typing import Optional, Dict, Any, List
from datetime import datetime
import httpx

from .base import BaseManager
from ..settings import settings
from ..protocols import StrategyAlert


class NotificationManager(BaseManager):
    """
    通知管理器
    
    支持发送 Markdown 格式的企业微信 Webhook 消息。
    内置消息频率控制，防止刷屏。
    
    Example:
        await notification_manager.send_alert(alert)
        await notification_manager.send_markdown("### 标题\n内容")
    """
    
    def __init__(self):
        super().__init__()
        self._config = settings.notification
        self._client: Optional[httpx.AsyncClient] = None
        self._last_send_time: Dict[str, datetime] = {}  # 按策略ID记录最后发送时间
        self._send_queue: List[Dict[str, Any]] = []
        self._lock = asyncio.Lock()
    
    async def initialize(self) -> None:
        """初始化 HTTP 客户端"""
        if self._initialized:
            return
        
        if not self._config.is_configured:
            self.logger.warning("Notification webhook not configured, skipping initialization")
            self._initialized = True
            return
        
        self.logger.info("Initializing NotificationManager...")
        
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(10.0),
            headers={"Content-Type": "application/json"},
        )
        
        self._initialized = True
        self.logger.info("NotificationManager initialized ✓")
    
    async def shutdown(self) -> None:
        """关闭 HTTP 客户端"""
        if self._client:
            await self._client.aclose()
            self._client = None
        self._initialized = False
        self.logger.info("NotificationManager shutdown")
    
    async def health_check(self) -> bool:
        """健康检查"""
        return self._initialized
    
    async def send_alert(self, alert: StrategyAlert) -> bool:
        """
        发送策略预警消息
        
        Args:
            alert: 预警对象
            
        Returns:
            是否发送成功
        """
        self.logger.info(
            f"[NOTIFY] send_alert called: ts_code={alert.ts_code}, "
            f"strategy={alert.strategy_name}, reason={alert.trigger_reason[:30]}..."
        )
        
        if not self._config.enabled:
            self.logger.warning("[NOTIFY] Notification disabled (config.enabled=False)")
            return False
            
        if not self._config.is_configured:
            self.logger.warning("[NOTIFY] Notification not configured (webhook missing)")
            return False
        
        # 频率控制
        async with self._lock:
            last_time = self._last_send_time.get(alert.strategy_id)
            if last_time:
                elapsed = (datetime.utcnow() - last_time).total_seconds()
                if elapsed < self._config.min_interval:
                    self.logger.warning(
                        f"[NOTIFY] Rate limited: strategy={alert.strategy_id}, "
                        f"elapsed={elapsed:.1f}s < min_interval={self._config.min_interval}s"
                    )
                    return False
        
        # 构建消息（使用纯文本格式，兼容性更好）
        text_content = self._build_alert_text(alert)
        self.logger.info(f"[NOTIFY] Sending text message, length={len(text_content)}")
        
        success = await self.send_text(text_content)
        
        if success:
            async with self._lock:
                self._last_send_time[alert.strategy_id] = datetime.utcnow()
            self.logger.info(f"[NOTIFY] ★ Alert sent successfully: {alert.ts_code}")
        else:
            self.logger.error(f"[NOTIFY] Failed to send alert: {alert.ts_code}")
        
        return success
    
    async def send_markdown(self, content: str) -> bool:
        """
        发送 Markdown 消息
        
        Args:
            content: Markdown 内容
            
        Returns:
            是否发送成功
        """
        if not self._config.is_configured:
            self.logger.warning("Webhook not configured, cannot send message")
            return False
        
        self._ensure_initialized()
        
        payload = {
            "msgtype": "markdown",
            "markdown": {
                "content": content
            }
        }
        
        try:
            response = await self._client.post(
                self._config.wecom_webhook,
                json=payload,
            )
            response.raise_for_status()
            
            result = response.json()
            if result.get("errcode") == 0:
                self.logger.info(f"Notification sent successfully: {content[:50]}...")
                return True
            else:
                self.logger.error(f"Notification failed: {result}")
                return False
                
        except httpx.HTTPError as e:
            self.logger.error(f"HTTP error sending notification: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Error sending notification: {e}")
            return False
    
    async def send_text(self, content: str, mentioned_list: Optional[List[str]] = None) -> bool:
        """
        发送文本消息
        
        Args:
            content: 文本内容
            mentioned_list: 需要 @ 的用户ID列表，使用 "@all" 表示 @ 所有人
            
        Returns:
            是否发送成功
        """
        if not self._config.is_configured:
            self.logger.warning("Webhook not configured, cannot send message")
            return False
        
        self._ensure_initialized()
        
        payload = {
            "msgtype": "text",
            "text": {
                "content": content,
            }
        }
        
        # if mentioned_list:
        #     payload["text"]["mentioned_list"] = mentioned_list
        
        try:
            response = await self._client.post(
                self._config.wecom_webhook,
                json=payload,
            )
            response.raise_for_status()
            
            result = response.json()
            return result.get("errcode") == 0
            
        except Exception as e:
            self.logger.error(f"Error sending text notification: {e}")
            return False
    
    def _build_alert_text(self, alert: StrategyAlert) -> str:
        """
        构建预警消息的纯文本内容 (企业微信兼容格式)
        
        Args:
            alert: 预警对象
            
        Returns:
            纯文本格式的消息内容
        """
        # 根据触发类型选择图标
        limit_type = alert.extra_data.get("limit_type", "")
        if limit_type == "up":
            icon = "🔔"
            title = "涨停打开提醒"
        elif limit_type == "down":
            icon = "🔔"
            title = "跌停打开提醒"
        else:
            icon = "🔔"
            title = alert.strategy_name
        
        # 时间
        time_str = alert.triggered_at.strftime('%H:%M:%S')
        
        # 获取涨跌停价格
        limit_price = alert.extra_data.get("up_limit") or alert.extra_data.get("down_limit", 0)
        limit_price_str = f"{limit_price:.2f}" if limit_price else "-"
        
        # 纯文本格式
        content = (
            f"{icon} {title}\n"
            f"股票: {alert.stock_name} ({alert.ts_code})\n"
            f"策略: {alert.strategy_name}\n"
            f"涨跌停价格: {limit_price_str}\n"
            f"当前价格: {alert.trigger_price:.2f}\n"
            f"触发原因: {alert.trigger_reason}\n"
            f"时间: {time_str}"
        )
        
        return content
    
    def _build_alert_markdown(self, alert: StrategyAlert) -> str:
        """
        构建预警消息的 Markdown 内容 (备用)
        
        Args:
            alert: 预警对象
            
        Returns:
            Markdown 格式的消息内容
        """
        time_str = alert.triggered_at.strftime('%H:%M:%S')
        
        content = f"""📢 **{alert.strategy_name}**
> **{alert.stock_name}** ({alert.ts_code})
> 当前价: ¥{alert.trigger_price:.2f}
> {alert.trigger_reason}
> 时间: {time_str}"""
        
        return content


# 全局单例
notification_manager = NotificationManager()
