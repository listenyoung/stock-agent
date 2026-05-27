"""
用户模型定义
"""

from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field, EmailStr
from bson import ObjectId


class PyObjectId(str):
    """MongoDB ObjectId 的 Pydantic 兼容类型"""
    
    @classmethod
    def __get_validators__(cls):
        yield cls.validate
    
    @classmethod
    def validate(cls, v, handler):
        if isinstance(v, ObjectId):
            return str(v)
        if isinstance(v, str) and ObjectId.is_valid(v):
            return v
        raise ValueError("Invalid ObjectId")


class UserBase(BaseModel):
    """用户基础信息"""
    username: str = Field(..., min_length=3, max_length=50, description="用户名")
    email: EmailStr = Field(..., description="邮箱地址")
    nickname: Optional[str] = Field(None, max_length=100, description="昵称")
    avatar: Optional[str] = Field(None, description="头像URL")
    is_active: bool = Field(default=True, description="是否激活")
    is_superuser: bool = Field(default=False, description="是否管理员")


class User(UserBase):
    """用户模型（不含密码）"""
    id: Optional[PyObjectId] = Field(default=None, alias="_id")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    last_login: Optional[datetime] = None
    
    # 用户偏好
    watchlist: List[str] = Field(default_factory=list, description="自选股列表")
    preferences: dict = Field(default_factory=dict, description="用户偏好设置")
    
    model_config = {
        "populate_by_name": True,
        "json_encoders": {ObjectId: str},
        "json_schema_extra": {
            "example": {
                "username": "trader001",
                "email": "trader@example.com",
                "nickname": "量化交易员",
                "is_active": True,
                "watchlist": ["000001.SZ", "600000.SH"],
            }
        },
    }


class UserInDB(User):
    """数据库中的用户模型（含密码哈希）"""
    hashed_password: str = Field(..., description="密码哈希")


class UserCreate(BaseModel):
    """用户创建请求"""
    username: str = Field(..., min_length=3, max_length=50)
    email: EmailStr
    password: str = Field(..., min_length=8, max_length=100)
    nickname: Optional[str] = None


class UserUpdate(BaseModel):
    """用户更新请求"""
    nickname: Optional[str] = None
    avatar: Optional[str] = None
    preferences: Optional[dict] = None
    watchlist: Optional[List[str]] = None


class UserLogin(BaseModel):
    """用户登录请求"""
    username: str
    password: str


class TokenPayload(BaseModel):
    """JWT Token 载荷"""
    sub: str  # user_id
    exp: datetime
    type: str = "access"  # access 或 refresh
