"""带新鲜期与陈旧宽限期的缓存。"""
from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass(frozen=True)
class CacheEntry:
    key: str
    value: dict
    stored_at: float  # monotonic 时间戳

    def age(self, now: float | None = None) -> float:
        return (now if now is not None else time.monotonic()) - self.stored_at


class TTLCache:
    """简单 TTL 缓存。

    语义：
    - age <= cache_ttl：新鲜，直接命中；
    - cache_ttl < age <= cache_ttl + stale_ttl：陈旧，仅作降级回退；
    - 更老：视为不存在，返回 None。
    """

    def __init__(self, cache_ttl: float, stale_ttl: float) -> None:
        self._cache_ttl = cache_ttl
        self._stale_ttl = stale_ttl
        self._entries: dict[str, CacheEntry] = {}

    def get_fresh(self, key: str) -> CacheEntry | None:
        entry = self._entries.get(key)
        if entry is not None and entry.age() <= self._cache_ttl:
            return entry
        return None

    def get_stale(self, key: str) -> CacheEntry | None:
        entry = self._entries.get(key)
        if entry is not None and entry.age() <= self._cache_ttl + self._stale_ttl:
            return entry
        return None

    def put(self, key: str, value: dict) -> None:
        self._entries[key] = CacheEntry(key=key, value=value, stored_at=time.monotonic())
