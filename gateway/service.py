"""聚合查询服务：编排缓存、合并、限流与降级。"""

import asyncio
import time

from gateway.cache import TtlCache
from gateway.coalescer import RequestCoalescer
from gateway.config import Settings
from gateway.limiter import (
    ConcurrencyLimiter,
    QueueFullError,
    QueueTimeoutError,
)
from gateway.types import (
    MalformedResponseError,
    QueryResult,
    Reason,
    UpstreamError,
)
from gateway.upstream import FakeUpstream

_REQUIRED_FIELDS = ("key", "value", "epoch")


class AggregationService:
    """对外入口：query(key) -> QueryResult。

    流程：新鲜缓存 -> 同键合并 -> (限流 + 超时) 上游调用 ->
    成功写缓存 / 失败按类型归一化，优先回退陈旧数据。
    """

    def __init__(self, settings: Settings, upstream: FakeUpstream) -> None:
        self.settings = settings
        self.upstream = upstream
        self.cache = TtlCache(settings.cache_ttl, settings.stale_ttl)
        self.coalescer = RequestCoalescer()
        self.limiter = ConcurrencyLimiter(
            settings.max_concurrent, settings.max_queue, settings.queue_timeout
        )

    async def _fetch_bounded(self, key: str) -> dict:
        """在 flight 任务内执行：限流 + 超时 + 响应校验。"""
        await self.limiter.acquire()
        try:
            async with asyncio.timeout(self.settings.upstream_timeout):
                payload = await self.upstream.fetch(key)
        finally:
            await self.limiter.release()
        if not all(f in payload for f in _REQUIRED_FIELDS):
            raise MalformedResponseError(
                f"missing fields: expect {_REQUIRED_FIELDS}, got {sorted(payload)}"
            )
        return payload

    def _stale_or(
        self, key: str, reason: Reason, detail: str
    ) -> QueryResult:
        """有陈旧数据则降级返回（reason=STALE_FALLBACK，detail 记录原始原因）。"""
        stale = self.cache.get_stale(key)
        if stale is not None:
            return QueryResult(
                ok=True,
                reason=Reason.STALE_FALLBACK,
                key=key,
                payload=stale,
                stale=True,
                detail=f"degraded: {reason.value}; serving stale cache ({detail})",
                served_at=time.time(),
            )
        return QueryResult(
            ok=False, reason=reason, key=key, detail=detail, served_at=time.time()
        )

    async def query(self, key: str) -> QueryResult:
        fresh = self.cache.get_fresh(key)
        if fresh is not None:
            return QueryResult(
                ok=True,
                reason=Reason.CACHE_HIT,
                key=key,
                payload=fresh,
                served_at=time.time(),
            )
        try:
            payload, shared = await self.coalescer.run(
                key, lambda: self._fetch_bounded(key)
            )
        except asyncio.CancelledError:
            # 共享 flight 被外部取消（如服务关闭）而本任务并未被取消时，归一化为 CANCELLED
            task = asyncio.current_task()
            if task is not None and task.cancelling() > 0:
                raise  # 本任务真的被取消，按 asyncio 语义向上传播
            return QueryResult(
                ok=False,
                reason=Reason.CANCELLED,
                key=key,
                detail="shared upstream call was cancelled",
                served_at=time.time(),
            )
        except QueueFullError as exc:
            return QueryResult(
                ok=False, reason=Reason.QUEUE_FULL, key=key,
                detail=str(exc), served_at=time.time(),
            )
        except QueueTimeoutError as exc:
            return QueryResult(
                ok=False, reason=Reason.QUEUE_TIMEOUT, key=key,
                detail=str(exc), served_at=time.time(),
            )
        except TimeoutError:
            return self._stale_or(
                key, Reason.UPSTREAM_TIMEOUT,
                f"no response within {self.settings.upstream_timeout}s",
            )
        except UpstreamError as exc:
            return self._stale_or(key, Reason.UPSTREAM_ERROR, str(exc))
        except MalformedResponseError as exc:
            return self._stale_or(key, Reason.MALFORMED_RESPONSE, str(exc))

        if not shared:
            self.cache.put(key, payload)
        return QueryResult(
            ok=True,
            reason=Reason.COALESCED if shared else Reason.FRESH,
            key=key,
            payload=payload,
            served_at=time.time(),
        )

    async def aclose(self) -> None:
        """取消所有进行中的上游调用并等待收尾，确保无悬挂任务。"""
        await self.coalescer.aclose()
