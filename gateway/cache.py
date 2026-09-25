"""带新鲜期与陈旧窗口的缓存。"""

import time
from dataclasses import dataclass


@dataclass(frozen=True)
class CacheEntry:
    payload: dict
    stored_at: float


class TtlCache:
    """两级语义：

    - [stored_at, stored_at+ttl)：新鲜，直接命中返回；
    - [stored_at+ttl, stored_at+ttl+stale_ttl)：陈旧，仅在上游失败时回退；
    - 超过 stale_ttl：彻底失效，返回 None。
    """

    def __init__(self, ttl: float, stale_ttl: float) -> None:
        self.ttl = ttl
        self.stale_ttl = stale_ttl
        self._store: dict[str, CacheEntry] = {}

    def get_fresh(self, key: str, now: float | None = None) -> dict | None:
        now = time.monotonic() if now is None else now
        entry = self._store.get(key)
        if entry is not None and now - entry.stored_at < self.ttl:
            return entry.payload
        return None

    def get_stale(self, key: str, now: float | None = None) -> dict | None:
        now = time.monotonic() if now is None else now
        entry = self._store.get(key)
        if entry is not None and now - entry.stored_at < self.ttl + self.stale_ttl:
            return entry.payload
        return None

    def put(self, key: str, payload: dict, now: float | None = None) -> None:
        now = time.monotonic() if now is None else now
        self._store[key] = CacheEntry(payload=payload, stored_at=now)

    def invalidate(self, key: str) -> None:
        self._store.pop(key, None)
