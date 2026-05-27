"""
认证 API
"""

from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel, EmailStr
from jose import JWTError, jwt

from core.settings import settings
from core.managers import mongo_manager

import bcrypt

router = APIRouter()

# OAuth2
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login")


# ==================== 模型 ====================


class RegisterRequest(BaseModel):
    """注册请求"""
    username: str
    email: EmailStr
    password: str
    nickname: Optional[str] = None


class RegisterResponse(BaseModel):
    """注册响应"""
    user_id: str
    username: str
    message: str


class CurrentUser(BaseModel):
    """当前登录用户信息"""
    user_id: str
    username: str
    is_admin: bool = False


class TokenResponse(BaseModel):
    """Token 响应"""
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    user_id: str
    username: str


class RefreshRequest(BaseModel):
    """刷新 Token 请求"""
    refresh_token: str


# ==================== 工具函数 ====================


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """验证密码"""
    return bcrypt.checkpw(
        plain_password.encode('utf-8'),
        hashed_password.encode('utf-8')
    )


def hash_password(password: str) -> str:
    """加密密码"""
    return bcrypt.hashpw(
        password.encode('utf-8'),
        bcrypt.gensalt()
    ).decode('utf-8')


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """创建访问 Token"""
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=settings.jwt_expire_minutes))
    to_encode.update({"exp": expire, "type": "access"})
    
    return jwt.encode(
        to_encode,
        settings.jwt_secret.get_secret_value(),
        algorithm=settings.jwt_algorithm,
    )


def create_refresh_token(data: dict) -> str:
    """创建刷新 Token"""
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(days=30)
    to_encode.update({"exp": expire, "type": "refresh"})
    
    return jwt.encode(
        to_encode,
        settings.jwt_secret.get_secret_value(),
        algorithm=settings.jwt_algorithm,
    )


async def get_current_user_id(token: str = Depends(oauth2_scheme)) -> str:
    """获取当前用户 ID"""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="无效的认证凭据",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    try:
        payload = jwt.decode(
            token,
            settings.jwt_secret.get_secret_value(),
            algorithms=[settings.jwt_algorithm],
        )
        
        user_id: str = payload.get("sub")
        token_type: str = payload.get("type")
        
        if user_id is None or token_type != "access":
            raise credentials_exception
            
    except JWTError:
        raise credentials_exception
    
    return user_id


async def get_current_user(token: str = Depends(oauth2_scheme)) -> CurrentUser:
    """获取当前用户（含管理员信息）"""
    user_id = await get_current_user_id(token)
    
    user = await mongo_manager.find_one(
        "users",
        {"user_id": user_id},
        projection={"user_id": 1, "username": 1, "is_admin": 1},
    )
    
    if not user:
        raise HTTPException(status_code=404, detail="用户不存在")
    
    return CurrentUser(
        user_id=user["user_id"],
        username=user["username"],
        is_admin=user.get("is_admin", False),
    )


def require_admin(current_user: CurrentUser = Depends(get_current_user)) -> CurrentUser:
    """要求管理员权限"""
    if not current_user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="需要管理员权限"
        )
    return current_user


# ==================== API 端点 ====================


@router.post("/register", response_model=RegisterResponse)
async def register(body: RegisterRequest):
    """用户注册"""
    # 检查用户名是否存在
    existing = await mongo_manager.find_one(
        "users",
        {"$or": [{"username": body.username}, {"email": body.email}]},
    )
    
    if existing:
        if existing.get("username") == body.username:
            raise HTTPException(status_code=400, detail="用户名已存在")
        else:
            raise HTTPException(status_code=400, detail="邮箱已被注册")
    
    # 创建用户
    import uuid
    user_id = uuid.uuid4().hex
    
    await mongo_manager.insert_one("users", {
        "user_id": user_id,
        "username": body.username,
        "email": body.email,
        "password_hash": hash_password(body.password),
        "nickname": body.nickname or body.username,
        "avatar": None,
        "watchlist": [],
        "preferences": {},
        "is_admin": False,  # 新用户默认非管理员
    })
    
    return RegisterResponse(
        user_id=user_id,
        username=body.username,
        message="注册成功",
    )


@router.post("/login", response_model=TokenResponse)
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    """用户登录"""
    import logging
    logger = logging.getLogger("auth")
    
    logger.info(f"Login attempt: username={form_data.username}")
    
    user = await mongo_manager.find_one(
        "users",
        {"username": form_data.username},
    )
    
    if not user:
        logger.warning(f"User not found: {form_data.username}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="用户名或密码错误",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    logger.info(f"User found: {user.get('username')}, checking password...")
    
    if not verify_password(form_data.password, user["password_hash"]):
        logger.warning(f"Password mismatch for user: {form_data.username}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="用户名或密码错误",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    logger.info(f"Login successful: {form_data.username}")
    
    # 更新最后登录时间
    await mongo_manager.update_one(
        "users",
        {"user_id": user["user_id"]},
        {"$set": {"last_login": datetime.utcnow()}},
    )
    
    # 生成 Token
    token_data = {"sub": user["user_id"]}
    
    return TokenResponse(
        access_token=create_access_token(token_data),
        refresh_token=create_refresh_token(token_data),
        user_id=user["user_id"],
        username=user["username"],
    )


@router.post("/refresh", response_model=TokenResponse)
async def refresh_token(body: RefreshRequest):
    """刷新 Token"""
    try:
        payload = jwt.decode(
            body.refresh_token,
            settings.jwt_secret.get_secret_value(),
            algorithms=[settings.jwt_algorithm],
        )
        
        user_id: str = payload.get("sub")
        token_type: str = payload.get("type")
        
        if not user_id or token_type != "refresh":
            raise HTTPException(status_code=401, detail="无效的刷新 Token")
            
    except JWTError:
        raise HTTPException(status_code=401, detail="无效的刷新 Token")
    
    # 获取用户
    user = await mongo_manager.find_one("users", {"user_id": user_id})
    if not user:
        raise HTTPException(status_code=401, detail="用户不存在")
    
    # 生成新 Token
    token_data = {"sub": user_id}
    
    return TokenResponse(
        access_token=create_access_token(token_data),
        refresh_token=create_refresh_token(token_data),
        user_id=user_id,
        username=user["username"],
    )


@router.post("/logout")
async def logout(user_id: str = Depends(get_current_user_id)):
    """登出"""
    # 这里可以实现 Token 黑名单等逻辑
    return {"message": "已登出"}


@router.post("/change-password")
async def change_password(
    old_password: str,
    new_password: str,
    user_id: str = Depends(get_current_user_id),
):
    """修改密码"""
    user = await mongo_manager.find_one("users", {"user_id": user_id})
    
    if not user or not verify_password(old_password, user["password_hash"]):
        raise HTTPException(status_code=400, detail="原密码错误")
    
    await mongo_manager.update_one(
        "users",
        {"user_id": user_id},
        {"$set": {"password_hash": hash_password(new_password)}},
    )
    
    return {"message": "密码修改成功"}
