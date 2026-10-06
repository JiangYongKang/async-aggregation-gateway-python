"""同键请求合并（singleflight）。"""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass


@dataclass
class _Flight:
    task: asyncio.Task
    waiters: int = 0


def _swallow_task_result(task: asyncio.Task) -> None:
    """消费已结束任务可能无人读取的异常，避免 "exception never retrieved" 告警。"""
    if task.cancelled():
        return
    try:
        task.exception()
    except asyncio.CancelledError:
        pass


class RequestCoalescer:
    """同一起点的并发请求共享同一次上游调用。

    语义：
    - 首个到达者创建 flight，后续同键请求合并等待同一 task；
    - 任一等待者被取消不影响共享调用本身（shield 隔离）；
    - 所有等待者都取消后，若调用尚未结束则取消它，不留悬挂任务；
      若调用已经结束（例如恰好在全部取消前成功），结果保留——成功调用
      的写缓存/熔断记录等副作用在 leader 任务内部完成，不依赖某个等待者
      存活，因此“共享调用最终成功”时成功结果不会因等待者提前取消而丢失；
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
            flight.task.add_done_callback(_swallow_task_result)
            self._inflight[key] = flight
        flight.waiters += 1
        try:
            return await asyncio.shield(flight.task), shared
        finally:
            flight.waiters -= 1
            if flight.waiters == 0:
                # 最后一个离开者负责摘除 flight；仅当调用仍未结束时才取消，
                # 已结束（成功/失败）的结果与其副作用原样保留
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
