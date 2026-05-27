"""
WebSocket 端点

负责:
- 实时推送任务进度
- 推送分析结果
- 心跳保活
"""

import asyncio
import json
from contextlib import suppress
from typing import Dict, Set

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
from jose import JWTError, jwt

from core.settings import settings
from core.managers import redis_manager


router = APIRouter()


# 连接管理
class ConnectionManager:
    """WebSocket 连接管理器"""
    
    def __init__(self):
        # user_id -> [websocket, ...]
        self._connections: Dict[str, Set[WebSocket]] = {}
        # task_id -> [user_id, ...]
        self._task_subscribers: Dict[str, Set[str]] = {}
    
    async def connect(self, websocket: WebSocket, user_id: str) -> None:
        """连接"""
        await websocket.accept()
        
        if user_id not in self._connections:
            self._connections[user_id] = set()
        self._connections[user_id].add(websocket)
    
    def disconnect(self, websocket: WebSocket, user_id: str) -> None:
        """断开连接"""
        if user_id in self._connections:
            self._connections[user_id].discard(websocket)
            if not self._connections[user_id]:
                del self._connections[user_id]
                self.unsubscribe_user(user_id)
    
    async def send_to_user(self, user_id: str, message: dict) -> None:
        """发送消息给用户"""
        if user_id in self._connections:
            for ws in list(self._connections[user_id]):
                try:
                    await ws.send_json(message)
                except Exception:
                    self._connections[user_id].discard(ws)
    
    def subscribe_task(self, user_id: str, task_id: str) -> None:
        """订阅任务"""
        if task_id not in self._task_subscribers:
            self._task_subscribers[task_id] = set()
        self._task_subscribers[task_id].add(user_id)
    
    def unsubscribe_task(self, user_id: str, task_id: str) -> None:
        """取消订阅"""
        if task_id in self._task_subscribers:
            self._task_subscribers[task_id].discard(user_id)
            if not self._task_subscribers[task_id]:
                del self._task_subscribers[task_id]

    def unsubscribe_user(self, user_id: str) -> None:
        """取消用户的所有任务订阅"""
        for task_id in list(self._task_subscribers.keys()):
            self._task_subscribers[task_id].discard(user_id)
            if not self._task_subscribers[task_id]:
                del self._task_subscribers[task_id]

    def is_subscribed(self, user_id: str, task_id: str) -> bool:
        """检查用户是否订阅了任务"""
        return user_id in self._task_subscribers.get(task_id, set())
    
    async def broadcast_task_update(self, task_id: str, message: dict) -> None:
        """广播任务更新"""
        if task_id in self._task_subscribers:
            for user_id in list(self._task_subscribers[task_id]):
                await self.send_to_user(user_id, message)


manager = ConnectionManager()


def verify_token(token: str) -> str:
    """验证 Token 并返回 user_id"""
    try:
        payload = jwt.decode(
            token,
            settings.jwt_secret.get_secret_value(),
            algorithms=[settings.jwt_algorithm],
        )
        user_id = payload.get("sub")
        if user_id:
            return user_id
    except JWTError:
        pass
    return ""


async def listen_redis_results(user_id: str, websocket: WebSocket):
    """监听 Redis 结果并推送"""
    pattern = f"{settings.redis.result_channel_prefix}:*"
    pubsub = redis_manager.client.pubsub()
    
    try:
        await pubsub.psubscribe(pattern)
        
        async for message in pubsub.listen():
            if message.get("type") != "pmessage":
                continue
            
            channel = message.get("channel", "")
            task_id = str(channel).rsplit(":", 1)[-1]
            if not task_id or not manager.is_subscribed(user_id, task_id):
                continue
            
            raw_data = message.get("data")
            try:
                payload = json.loads(raw_data)
            except (TypeError, json.JSONDecodeError):
                payload = {"raw": raw_data}
            
            await websocket.send_json({
                "type": "task_update",
                "task_id": task_id,
                "data": payload,
            })
    except asyncio.CancelledError:
        raise
    finally:
        with suppress(Exception):
            await pubsub.punsubscribe(pattern)
        with suppress(Exception):
            await pubsub.close()


@router.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket,
    token: str = Query(...),
):
    """WebSocket 端点"""
    # 验证 Token
    user_id = verify_token(token)
    if not user_id:
        await websocket.close(code=4001, reason="Invalid token")
        return
    
    # 连接
    await manager.connect(websocket, user_id)
    
    # 发送连接确认
    await websocket.send_json({
        "type": "connected",
        "user_id": user_id,
    })
    redis_listener_task = asyncio.create_task(
        listen_redis_results(user_id, websocket)
    )
    
    try:
        while True:
            # 接收消息
            data = await websocket.receive_text()
            message = json.loads(data)
            
            msg_type = message.get("type")
            
            if msg_type == "ping":
                await websocket.send_json({"type": "pong"})
                
            elif msg_type == "subscribe":
                task_id = message.get("task_id")
                if task_id:
                    manager.subscribe_task(user_id, task_id)
                    await websocket.send_json({
                        "type": "subscribed",
                        "task_id": task_id,
                    })
                    
            elif msg_type == "unsubscribe":
                task_id = message.get("task_id")
                if task_id:
                    manager.unsubscribe_task(user_id, task_id)
                    
    finally:
        redis_listener_task.cancel()
        with suppress(asyncio.CancelledError):
            await redis_listener_task
        manager.disconnect(websocket, user_id)


# 导出路由
websocket_router = router
