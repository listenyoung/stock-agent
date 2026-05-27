"""
用户 API
"""

from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from core.managers import mongo_manager
from .auth import get_current_user_id


router = APIRouter()


# ==================== 模型 ====================


class UserInfo(BaseModel):
    """用户信息"""
    user_id: str
    username: str
    email: str
    nickname: Optional[str]
    avatar: Optional[str]
    watchlist: List[str]
    preferences: dict
    is_admin: bool = False


class UpdateUserRequest(BaseModel):
    """更新用户请求"""
    nickname: Optional[str] = None
    avatar: Optional[str] = None


class UserPreferences(BaseModel):
    """用户偏好"""
    theme: Optional[str] = None
    language: Optional[str] = None
    notification_enabled: Optional[bool] = None


# ==================== API 端点 ====================


@router.get("/me", response_model=UserInfo)
async def get_current_user(user_id: str = Depends(get_current_user_id)):
    """获取当前用户信息"""
    user = await mongo_manager.find_one(
        "users",
        {"user_id": user_id},
        projection={"password_hash": 0},
    )
    
    if not user:
        raise HTTPException(status_code=404, detail="用户不存在")
    
    return UserInfo(
        user_id=user["user_id"],
        username=user["username"],
        email=user["email"],
        nickname=user.get("nickname"),
        avatar=user.get("avatar"),
        watchlist=user.get("watchlist", []),
        preferences=user.get("preferences", {}),
        is_admin=user.get("is_admin", False),
    )


@router.put("/me")
async def update_user(
    body: UpdateUserRequest,
    user_id: str = Depends(get_current_user_id),
):
    """更新用户信息"""
    update_data = {}
    
    if body.nickname is not None:
        update_data["nickname"] = body.nickname
    if body.avatar is not None:
        update_data["avatar"] = body.avatar
    
    if update_data:
        await mongo_manager.update_one(
            "users",
            {"user_id": user_id},
            {"$set": update_data},
        )
    
    return {"message": "更新成功"}


# ==================== 自选股 ====================


@router.get("/me/watchlist", response_model=List[str])
async def get_watchlist(user_id: str = Depends(get_current_user_id)):
    """获取自选股列表"""
    user = await mongo_manager.find_one(
        "users",
        {"user_id": user_id},
        projection={"watchlist": 1},
    )
    
    return user.get("watchlist", []) if user else []


@router.post("/me/watchlist")
async def add_to_watchlist(
    ts_code: str,
    user_id: str = Depends(get_current_user_id),
):
    """添加自选股"""
    await mongo_manager.update_one(
        "users",
        {"user_id": user_id},
        {"$addToSet": {"watchlist": ts_code}},
    )
    
    return {"message": f"已添加 {ts_code}"}


@router.delete("/me/watchlist/{ts_code}")
async def remove_from_watchlist(
    ts_code: str,
    user_id: str = Depends(get_current_user_id),
):
    """移除自选股"""
    await mongo_manager.update_one(
        "users",
        {"user_id": user_id},
        {"$pull": {"watchlist": ts_code}},
    )
    
    return {"message": f"已移除 {ts_code}"}


# ==================== 偏好设置 ====================


@router.put("/me/preferences")
async def update_preferences(
    body: UserPreferences,
    user_id: str = Depends(get_current_user_id),
):
    """更新偏好设置"""
    update_data = {}
    
    if body.theme is not None:
        update_data["preferences.theme"] = body.theme
    if body.language is not None:
        update_data["preferences.language"] = body.language
    if body.notification_enabled is not None:
        update_data["preferences.notification_enabled"] = body.notification_enabled
    
    if update_data:
        await mongo_manager.update_one(
            "users",
            {"user_id": user_id},
            {"$set": update_data},
        )
    
    return {"message": "设置已更新"}
