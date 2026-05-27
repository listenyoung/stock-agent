"""
设置用户为管理员

用法:
    python scripts/set_admin.py <username>
    python scripts/set_admin.py --list          # 列出所有管理员
    python scripts/set_admin.py --remove <username>  # 移除管理员权限
"""

import asyncio
import sys
import os

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.managers import mongo_manager


async def list_admins():
    """列出所有管理员"""
    await mongo_manager.initialize()
    
    admins = await mongo_manager.find_many(
        "users",
        {"is_admin": True},
        projection={"user_id": 1, "username": 1, "email": 1},
    )
    
    print("\n" + "=" * 50)
    print("当前管理员列表")
    print("=" * 50)
    
    if not admins:
        print("  (暂无管理员)")
    else:
        for admin in admins:
            print(f"  - {admin['username']} ({admin.get('email', 'N/A')})")
    
    print()


async def set_admin(username: str, is_admin: bool = True):
    """设置/移除用户管理员权限"""
    await mongo_manager.initialize()
    
    # 查找用户
    user = await mongo_manager.find_one(
        "users",
        {"username": username},
    )
    
    if not user:
        print(f"\n错误: 用户 '{username}' 不存在")
        return False
    
    # 更新管理员状态
    await mongo_manager.update_one(
        "users",
        {"username": username},
        {"$set": {"is_admin": is_admin}},
    )
    
    action = "设置为管理员" if is_admin else "移除管理员权限"
    print(f"\n成功: 已将用户 '{username}' {action}")
    
    return True


async def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return
    
    arg = sys.argv[1]
    
    if arg == "--list":
        await list_admins()
    elif arg == "--remove":
        if len(sys.argv) < 3:
            print("错误: 请指定要移除管理员权限的用户名")
            return
        await set_admin(sys.argv[2], is_admin=False)
    elif arg.startswith("-"):
        print(f"未知选项: {arg}")
        print(__doc__)
    else:
        await set_admin(arg, is_admin=True)


if __name__ == "__main__":
    asyncio.run(main())
