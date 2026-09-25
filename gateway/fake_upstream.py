"""进程内假上游：模拟延迟、抖动、失败与畸形响应。"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field


class UpstreamFailure(Exception):
    """假上游主动抛出的业务错误。"""


@dataclass
class UpstreamBehavior:
    """单个 key 的行为脚本。

    - latency: 基础响应延迟（秒）。
    - jitter: 每次调用附加的 [0, jitter) 随机抖动（秒）。
    - fail: 为 True 时抛出 UpstreamFailure。
    - malformed: 为 True 时返回畸形数据（缺字段/类型错误）。
    - hang: 为 True 时永不返回（用于模拟上游长时间无响应）。
    """

    latency: float = 0.01
    jitter: float = 0.0
    fail: bool = False
    malformed: bool = False
    hang: bool = False


class FakeUpstream:
    """可脚本化的假上游。

    通过 set_behavior 为指定 key 配置行为；call_count 记录真实调用次数，
    用于验证请求合并没有重复穿透。内部用 asyncio 模拟，取消会立即生效。
    """

    def __init__(self, default: UpstreamBehavior | None = None, seed: int = 42) -> None:
        import random

        self._default = default or UpstreamBehavior()
        self._behaviors: dict[str, UpstreamBehavior] = {}
        self._rng = random.Random(seed)
        self.call_count: dict[str, int] = {}

    def set_behavior(self, key: str, behavior: UpstreamBehavior) -> None:
        self._behaviors[key] = behavior

    def calls(self, key: str) -> int:
        return self.call_count.get(key, 0)

    async def fetch(self, key: str) -> dict:
        """模拟一次上游调用，返回 {"key", "value", "seq"} 字典。"""
        b = self._behaviors.get(key, self._default)
        self.call_count[key] = self.call_count.get(key, 0) + 1
        seq = self.call_count[key]

        if b.hang:
            # 永不返回，直到被取消（CancelledError 会正常传播）
            await asyncio.Event().wait()
        delay = b.latency + (self._rng.random() * b.jitter if b.jitter > 0 else 0.0)
        if delay > 0:
            await asyncio.sleep(delay)
        if b.fail:
            raise UpstreamFailure(f"upstream failed for key={key!r} (call #{seq})")
        if b.malformed:
            return {"unexpected": "shape", "seq": seq}  # 缺少 key/value 字段
        return {"key": key, "value": f"value-of-{key}#{seq}", "seq": seq}
