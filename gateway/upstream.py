"""进程内假上游：可编程的延迟、抖动、失败与畸形响应。"""

import asyncio
import random

from gateway.types import MalformedResponseError, UpstreamError


class FakeUpstream:
    """模拟真实数据源。

    - latency/jitter 模拟响应时间抖动
    - fail_keys/malformed_keys/hang_keys 模拟异常上游
    - call_count/inflight/max_inflight 供测试断言穿透次数与并发上限
    - gate 闸门：设置后所有调用阻塞直到 gate.set()，用于制造“长时间无响应”
    """

    def __init__(
        self,
        latency: float = 0.05,
        jitter: float = 0.0,
        fail_keys: set[str] | None = None,
        malformed_keys: set[str] | None = None,
        hang_keys: set[str] | None = None,
    ) -> None:
        self.latency = latency
        self.jitter = jitter
        self.fail_keys = fail_keys or set()
        self.malformed_keys = malformed_keys or set()
        self.hang_keys = hang_keys or set()
        self.call_count = 0
        self.inflight = 0
        self.max_inflight = 0
        self._gate: asyncio.Event | None = None

    def set_gate(self, gate: asyncio.Event | None) -> None:
        """设置/清除闸门：设置后所有调用阻塞直到 gate.set()。"""
        self._gate = gate

    async def fetch(self, key: str) -> dict:
        """拉取数据，正常返回 {"key","value","epoch"}；异常按 key 配置抛出。"""
        self.call_count += 1
        self.inflight += 1
        self.max_inflight = max(self.max_inflight, self.inflight)
        try:
            if self._gate is not None:
                await self._gate.wait()
            delay = self.latency + (random.uniform(0, self.jitter) if self.jitter else 0.0)
            if key in self.hang_keys:
                delay = max(delay, 3600.0)  # 挂起：依赖调用方超时/取消兜底
            await asyncio.sleep(delay)
            if key in self.fail_keys:
                raise UpstreamError(f"upstream rejected key={key!r}")
            if key in self.malformed_keys:
                raise MalformedResponseError(f"malformed payload for key={key!r}")
            return {"key": key, "value": f"value-of-{key}", "epoch": self.call_count}
        finally:
            self.inflight -= 1
