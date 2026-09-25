"""查询结果模型。"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Source(str, Enum):
    """结果来源，用于解释降级判定。"""

    UPSTREAM = "upstream"  # 本次实时调用上游获得
    CACHE = "cache"        # 命中新鲜缓存
    STALE = "stale"        # 上游不可用，回退到过期但仍可接受的陈旧数据


@dataclass(frozen=True)
class QueryResult:
    key: str
    value: dict
    source: Source
    age: float  # 数据产生至今的秒数，便于判断陈旧程度
