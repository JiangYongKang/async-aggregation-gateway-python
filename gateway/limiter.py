"""有界并发 + 排队背压。"""

import asyncio


class QueueFullError(Exception):
    """排队名额已满，快速拒绝。"""


class QueueTimeoutError(Exception):
    """排队等待超时。"""


class ConcurrencyLimiter:
    """max_concurrent 个执行名额 + max_queue 个排队名额，超出快速拒绝。

    取消安全：等待中的任务被取消时，排队计数在 finally 中回收，不泄漏。
    """

    def __init__(self, max_concurrent: int, max_queue: int, queue_timeout: float) -> None:
        self.max_concurrent = max_concurrent
        self.max_queue = max_queue
        self.queue_timeout = queue_timeout
        self._active = 0
        self._queued = 0
        self._cond = asyncio.Condition()

    @property
    def queued(self) -> int:
        return self._queued

    @property
    def active(self) -> int:
        return self._active

    async def acquire(self) -> None:
        """获取执行名额；满员抛 QueueFullError，超时抛 QueueTimeoutError。"""
        async with self._cond:
            if self._active < self.max_concurrent:
                self._active += 1
                return
            if self._queued >= self.max_queue:
                raise QueueFullError(
                    f"queue full: {self._queued}/{self.max_queue} waiting, "
                    f"{self._active}/{self.max_concurrent} running"
                )
            self._queued += 1
            try:
                async with asyncio.timeout(self.queue_timeout):
                    await self._cond.wait_for(
                        lambda: self._active < self.max_concurrent
                    )
                self._active += 1
            except TimeoutError as exc:
                raise QueueTimeoutError(
                    f"queue wait exceeded {self.queue_timeout}s"
                ) from exc
            finally:
                self._queued -= 1

    async def release(self) -> None:
        async with self._cond:
            self._active -= 1
            self._cond.notify()
