"""聚合器：单飞合并 + 有界并发 + 排队背压 + 超时/异常归一化 + 陈旧回退。

语义说明：
- 请求合并（单飞）：同一 key 的并发请求共享同一次上游调用；
  上游结果只写入缓存、不缓存失败，失败不会污染后续请求。
- 背压：同时执行的上游调用不超过 max_concurrent；超出后排队，
  排队数超过 max_queue 的请求立即以 OVERLOADED 拒绝。
- 取消：等待者被取消只影响自身；当某 key 的最后一个等待者取消时，
  对应的上游任务会被取消，及时释放资源，不留悬挂任务。
- 降级：上游超时/错误/畸形时，若存在陈旧宽限期内的缓存则回退为
  STALE 结果（带 age，可解释），否则抛出对应 kind 的 GatewayError。
"""
from __future__ import annotations

import asyncio
import logging

from .cache import TTLCache
from .config import GatewayConfig
from .errors import ErrorKind, GatewayError
from .fake_upstream import FakeUpstream, UpstreamFailure
from .models import QueryResult, Source

logger = logging.getLogger("gateway.aggregator")


class _InFlight:
    """同一 key 的一次进行中的上游调用及其等待者。"""

    def __init__(self) -> None:
        self.future: asyncio.Future[QueryResult] = asyncio.get_running_loop().create_future()
        self.task: asyncio.Task[None] | None = None
        self.waiters: int = 0


class Aggregator:
    def __init__(self, upstream: FakeUpstream, cache: TTLCache, config: GatewayConfig) -> None:
        self._upstream = upstream
        self._cache = cache
        self._config = config
        self._semaphore = asyncio.Semaphore(config.max_concurrent)
        self._queue_waiters = 0
        self._inflight: dict[str, _InFlight] = {}

    async def query(self, key: str) -> QueryResult:
        fresh = self._cache.get_fresh(key)
        if fresh is not None:
            return QueryResult(key=key, value=fresh.value, source=Source.CACHE, age=fresh.age())

        inf = self._inflight.get(key)
        if inf is None:
            inf = _InFlight()
            self._inflight[key] = inf
            inf.task = asyncio.create_task(self._run(key, inf), name=f"fetch:{key}")
        inf.waiters += 1
        try:
            # shield：单个等待者取消不会取消共享的 future
            return await asyncio.shield(inf.future)
        finally:
            inf.waiters -= 1
            if inf.waiters == 0 and inf.task is not None and not inf.task.done():
                # 最后一个等待者离开：取消上游任务，释放并发额度
                inf.task.cancel()

    async def _run(self, key: str, inf: _InFlight) -> None:
        try:
            result = await self._execute(key)
        except asyncio.CancelledError:
            if not inf.future.done():
                inf.future.cancel()
            raise
        except Exception as exc:
            if inf.waiters > 0 and not inf.future.done():
                inf.future.set_exception(exc)
        else:
            if inf.waiters > 0 and not inf.future.done():
                inf.future.set_result(result)
        finally:
            if self._inflight.get(key) is inf:
                del self._inflight[key]

    async def _execute(self, key: str) -> QueryResult:
        # 有界并发 + 排队背压：额度打满后排队，队列也满则快速拒绝
        if self._semaphore.locked():
            if self._queue_waiters >= self._config.max_queue:
                raise GatewayError(
                    ErrorKind.OVERLOADED,
                    f"queue full ({self._queue_waiters}/{self._config.max_queue})",
                )
            self._queue_waiters += 1
            try:
                await self._semaphore.acquire()
            finally:
                self._queue_waiters -= 1
        else:
            await self._semaphore.acquire()
        try:
            return await self._call_upstream(key)
        finally:
            self._semaphore.release()

    async def _call_upstream(self, key: str) -> QueryResult:
        try:
            async with asyncio.timeout(self._config.upstream_timeout):
                raw = await self._upstream.fetch(key)
        except TimeoutError:
            return self._degrade_or_raise(key, ErrorKind.UPSTREAM_TIMEOUT, "upstream timed out")
        except UpstreamFailure as exc:
            return self._degrade_or_raise(key, ErrorKind.UPSTREAM_ERROR, str(exc))
        if not isinstance(raw, dict) or "key" not in raw or "value" not in raw:
            return self._degrade_or_raise(
                key, ErrorKind.MALFORMED_RESPONSE, f"unexpected payload: {raw!r}"
            )
        self._cache.put(key, raw)
        return QueryResult(key=key, value=raw, source=Source.UPSTREAM, age=0.0)

    def _degrade_or_raise(self, key: str, kind: ErrorKind, detail: str) -> QueryResult:
        stale = self._cache.get_stale(key)
        if stale is not None:
            logger.info(
                "degrade to stale: key=%r kind=%s age=%.3fs detail=%s",
                key, kind.value, stale.age(), detail,
            )
            return QueryResult(key=key, value=stale.value, source=Source.STALE, age=stale.age())
        raise GatewayError(kind, detail)

    async def aclose(self) -> None:
        """取消所有进行中的上游任务并等待收尾，确保无悬挂任务。"""
        tasks = [inf.task for inf in self._inflight.values() if inf.task is not None]
        for t in tasks:
            t.cancel()
        for t in tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        self._inflight.clear()
