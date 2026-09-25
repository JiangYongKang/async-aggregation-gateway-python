"""同键请求合并（singleflight）。"""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass


@dataclass
class _Flight:
    task: asyncio.Task
    waiters: int = 0


class RequestCoalescer:
    """同一起点的并发请求共享同一次上游调用。

    语义：
    - 首个到达者创建 flight，后续同键请求合并等待同一 task；
    - 任一等待者被取消不影响共享调用本身（shield 隔离）；
    - 所有等待者都取消后，底层 task 被取消，不留悬挂任务；
    - 一次失败只影响当前这批等待者：flight 立即移除，后续请求重新发起，
      失败结果不会写入缓存、不会污染后续请求。
    """

    def __init__(self) -> None:
        self._inflight: dict[str, _Flight] = {}

    @property
    def inflight_count(self) -> int:
        return len(self._inflight)

    async def run(
        self, key: str, factory: Callable[[], Awaitable[dict]]
    ) -> tuple[dict, bool]:
        """返回 (payload, shared)。shared=True 表示合并到了已有调用。"""
        flight = self._inflight.get(key)
        shared = flight is not None
        if flight is None:
            flight = _Flight(task=asyncio.create_task(factory()))
            self._inflight[key] = flight
        flight.waiters += 1
        try:
            return await asyncio.shield(flight.task), shared
        finally:
            flight.waiters -= 1
            if flight.waiters == 0:
                # 只有创建者负责从表中移除；无人等待时取消未完成的上游调用
                if self._inflight.get(key) is flight:
                    self._inflight.pop(key, None)
                if not flight.task.done():
                    flight.task.cancel()

    async def aclose(self) -> None:
        """取消所有进行中的 flight 并等待收尾，确保无悬挂任务。"""
        flights = list(self._inflight.values())
        self._inflight.clear()
        for flight in flights:
            flight.task.cancel()
        await asyncio.gather(
            *(f.task for f in flights), return_exceptions=True
        )
