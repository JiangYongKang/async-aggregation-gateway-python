"""聚合查询服务：编排缓存、合并、熔断、限流与降级。"""

import asyncio
import logging
import time
from collections.abc import Callable

from gateway.breaker import CircuitBreaker, CircuitState
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

log = logging.getLogger("gateway.service")

_REQUIRED_FIELDS = ("key", "value", "epoch")

# 只有真正穿透上游后出现的这三类失败才计入熔断
_UPSTREAM_FAILURES = (TimeoutError, UpstreamError, MalformedResponseError)


class AggregationService:
    """对外入口：query(key) -> QueryResult。

    优先级：新鲜缓存 -> 熔断短路（OPEN：旧数据降级，否则快速拒绝）->
    同键合并（HALF_OPEN 时并发请求合到唯一一次恢复试探）->
    (限流 + 超时) 上游调用 -> 成功写缓存并恢复熔断 / 失败计熔断，
    优先回退陈旧数据。
    """

    def __init__(
        self,
        settings: Settings,
        upstream: FakeUpstream,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.settings = settings
        self.upstream = upstream
        clock = clock or time.monotonic
        self.cache = TtlCache(settings.cache_ttl, settings.stale_ttl)
        self.coalescer = RequestCoalescer()
        self.limiter = ConcurrencyLimiter(
            settings.max_concurrent, settings.max_queue, settings.queue_timeout
        )
        self.breaker = CircuitBreaker(
            threshold=settings.failure_threshold,
            window=settings.failure_window,
            cooldown=settings.circuit_cooldown,
            clock=clock,
        )
        self._clock = clock

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

    async def _run_leader(self, key: str) -> dict:
        """共享上游调用的实际执行体（仅由 flight 的 leader 任务运行）。

        成功时在这里完成“写缓存 + 熔断恢复”，失败时在这里计一次熔断失败：
        这些副作用归属于调用本身而非某个等待者，因此等待者中途取消不会
        让一次最终成功的共享调用丢掉结果。
        """
        try:
            payload = await self._fetch_bounded(key)
        except _UPSTREAM_FAILURES:
            self.breaker.record_failure(key)
            log.info("key=%s 上游失败已计入熔断，窗口内失败数=%d",
                     key, self.breaker.failure_count(key))
            raise
        self.breaker.record_success(key)
        self.cache.put(key, payload)
        return payload

    def _degrade_or_reject(
        self, key: str, reason: Reason, detail: str
    ) -> QueryResult:
        """有（未彻底过期的）旧数据则带着原始原因降级，否则按原始原因拒绝。"""
        stale = self.cache.get_stale(key)
        if stale is not None:
            return QueryResult(
                ok=True,
                reason=Reason.STALE_FALLBACK,
                key=key,
                payload=stale,
                stale=True,
                detail=f"degraded: {reason.value}; serving stale cache ({detail})",
                served_at=self._clock(),
            )
        return QueryResult(
            ok=False, reason=reason, key=key, detail=detail,
            served_at=self._clock(),
        )

    def _short_circuit(self, key: str, retry_after: float) -> QueryResult:
        """熔断 OPEN：不穿透上游，旧数据降级，否则快速拒绝（区分于其他原因）。"""
        detail = (
            f"circuit open: {self.breaker.failure_count(key)} consecutive "
            f"upstream failure(s); upstream probing paused, retry_after={retry_after:.2f}s"
        )
        log.info("key=%s 熔断短路，不穿透上游，剩余冷却 %.2fs", key, retry_after)
        return self._degrade_or_reject(key, Reason.CIRCUIT_OPEN, detail)

    async def query(self, key: str) -> QueryResult:
        # 1) 新鲜缓存优先：任何熔断状态下都直接命中，不穿透上游
        fresh = self.cache.get_fresh(key)
        if fresh is not None:
            return QueryResult(
                ok=True,
                reason=Reason.CACHE_HIT,
                key=key,
                payload=fresh,
                served_at=self._clock(),
            )

        # 2) 熔断判定（按 key 独立）
        state, retry_after, probe_permitted = self.breaker.check(key)
        if state == CircuitState.OPEN:
            return self._short_circuit(key, retry_after)
        if state == CircuitState.HALF_OPEN:
            if probe_permitted:
                log.info("key=%s 冷却到期，放一次恢复试探，真正穿透上游", key)
            else:
                log.info("key=%s 恢复试探进行中，本请求合并到该次试探，不额外穿透", key)

        # 3) 同键合并：HALF_OPEN 下首个请求成为唯一试探，其余合并
        try:
            payload, shared = await self.coalescer.run(
                key, lambda: self._run_leader(key)
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
                served_at=self._clock(),
            )
        except QueueFullError as exc:
            return QueryResult(
                ok=False, reason=Reason.QUEUE_FULL, key=key,
                detail=str(exc), served_at=self._clock(),
            )
        except QueueTimeoutError as exc:
            return QueryResult(
                ok=False, reason=Reason.QUEUE_TIMEOUT, key=key,
                detail=str(exc), served_at=self._clock(),
            )
        except TimeoutError:
            return self._degrade_or_reject(
                key, Reason.UPSTREAM_TIMEOUT,
                f"no response within {self.settings.upstream_timeout}s",
            )
        except UpstreamError as exc:
            return self._degrade_or_reject(key, Reason.UPSTREAM_ERROR, str(exc))
        except MalformedResponseError as exc:
            return self._degrade_or_reject(key, Reason.MALFORMED_RESPONSE, str(exc))

        # 成功结果（含缓存写入与熔断恢复）已由 leader 任务内部完成
        return QueryResult(
            ok=True,
            reason=Reason.COALESCED if shared else Reason.FRESH,
            key=key,
            payload=payload,
            served_at=self._clock(),
        )

    async def aclose(self) -> None:
        """取消所有进行中的上游调用并等待收尾，确保无悬挂任务。"""
        await self.coalescer.aclose()
