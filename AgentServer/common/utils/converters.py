"""
类型转换工具函数

提供各种数据类型转换的通用方法
"""

from typing import Any

import numpy as np
import pandas as pd


def convert_numpy_types(obj: Any) -> Any:
    """
    递归转换 numpy 类型为 Python 原生类型
    
    MongoDB、JSON 等无法直接序列化 numpy 类型，需要转换为 Python 原生类型。
    
    Args:
        obj: 待转换的对象，可以是 dict、list 或任意 numpy 类型
        
    Returns:
        转换后的 Python 原生类型对象
        
    Examples:
        >>> convert_numpy_types(np.int64(42))
        42
        >>> convert_numpy_types({'a': np.float64(1.5)})
        {'a': 1.5}
    """
    if isinstance(obj, dict):
        return {k: convert_numpy_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(item) for item in obj]
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif pd.isna(obj):
        return None
    else:
        return obj


def safe_float(value: Any, default: float = 0.0) -> float:
    """
    安全转换为 float
    
    Args:
        value: 待转换的值
        default: 转换失败时的默认值
        
    Returns:
        转换后的 float 值
    """
    try:
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return default
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_int(value: Any, default: int = 0) -> int:
    """
    安全转换为 int
    
    Args:
        value: 待转换的值
        default: 转换失败时的默认值
        
    Returns:
        转换后的 int 值
    """
    try:
        if value is None:
            return default
        return int(value)
    except (ValueError, TypeError):
        return default
