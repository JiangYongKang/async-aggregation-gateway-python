"""按数据源（key）的失败短路与自动恢复试探。

状态机（每个 key 独立，互不影响）：

- CLOSED：正常放行。窗口内累计失败次数达到阈值 -> OPEN；
- OPEN：冷却中，check 返回剩余冷却时间，调用方不再穿透上游；
  冷却到期后 check 将其转为 HALF_OPEN；
- HALF_OPEN：允许且仅允许一次恢复试探（只对首个判定者返回 HALF_OPEN，
  其余并发请求由 singleflight 合并到这次试探）；
  试探成功 record_success -> CLOSED；试探失败 record_failure ->
  重新 OPEN 并重新计时。
"""

import enum
import time
from collections.abc import Callable
from dataclasses import dataclass, field


class CircuitState(enum.Enum):
    """熔断器状态。"""

    CLOSED = "closed"          # 正常放行
    OPEN = "open"              # 冷却短路中，不穿透上游
    HALF_OPEN = "half_open"    # 冷却到期，允许且仅允许一次恢复试探


@dataclass
class _KeyState:
    """单个数据源的短路状态。"""

    failure_times: list[float] = field(default_factory=list)
    state: CircuitState = CircuitState.CLOSED
    opened_at: float = 0.0


class CircuitBreaker:
    """按 key 维护失败计数与短路状态。

    只统计“真正穿透到上游”的调用结果：短路快速拒绝、排队背压、客户端取消
    都不应计入失败，由调用方决定何时 record_*。
    """

    def __init__(
        self,
        threshold: int,
        window: float,
        cooldown: float,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.threshold = threshold
        self.window = window
        self.cooldown = cooldown
        self._clock = clock or time.monotonic
        self._states: dict[str, _KeyState] = {}

    def _get(self, key: str) -> _KeyState:
        st = self._states.get(key)
        if st is None:
            st = _KeyState()
            self._states[key] = st
        return st

    def _prune(self, st: _KeyState, now: float) -> None:
        """丢弃窗口外的旧失败计数。"""
        if self.window > 0:
            cutoff = now - self.window
            st.failure_times = [t for t in st.failure_times if t > cutoff]

    def check(self, key: str) -> tuple[CircuitState, float, bool]:
        """返回 (状态, OPEN 时剩余冷却秒数；其余为 0.0, 是否拿到唯一试探许可)。

        冷却到期的 OPEN 在此被推进为 HALF_OPEN：只有推进它的那一次调用
        probe_permitted=True（即唯一的恢复试探许可）；HALF_OPEN 稳态期间
        到达的请求 probe_permitted=False，由调用方经 singleflight 合并到
        已在进行的那次试探上，因此同一时刻只有一个请求真正穿透上游。
        """
        now = self._clock()
        st = self._states.get(key)
        if st is None or st.state == CircuitState.CLOSED:
            return CircuitState.CLOSED, 0.0, False
        if st.state == CircuitState.OPEN:
            elapsed = now - st.opened_at
            if elapsed >= self.cooldown:
                st.state = CircuitState.HALF_OPEN
                return CircuitState.HALF_OPEN, 0.0, True
            return CircuitState.OPEN, self.cooldown - elapsed, False
        return CircuitState.HALF_OPEN, 0.0, False

    def record_success(self, key: str) -> None:
        """记录上游成功（含恢复试探成功）：计数清零并恢复正常。"""
        st = self._states.get(key)
        if st is not None:
            st.state = CircuitState.CLOSED
            st.failure_times.clear()
            st.opened_at = 0.0

    def record_failure(self, key: str) -> None:
        """记录一次真正穿透上游后的失败。

        - HALF_OPEN 试探失败：立即重新 OPEN 并重新计时；
        - CLOSED 下失败：计入窗口计数，达到阈值转入 OPEN。
        """
        now = self._clock()
        st = self._get(key)
        if st.state == CircuitState.HALF_OPEN:
            st.state = CircuitState.OPEN
            st.opened_at = now
            return
        self._prune(st, now)
        st.failure_times.append(now)
        if len(st.failure_times) >= self.threshold:
            st.state = CircuitState.OPEN
            st.opened_at = now

    # ---- 供观测/日志使用的只读接口 ----

    def state_of(self, key: str) -> CircuitState:
        """不推进状态的前提下返回当前状态（冷却到期也保持 OPEN 字面量直到 check）。"""
        st = self._states.get(key)
        return CircuitState.CLOSED if st is None else st.state

    def failure_count(self, key: str) -> int:
        """窗口内当前有效失败次数。"""
        st = self._states.get(key)
        if st is None:
            return 0
        self._prune(st, self._clock())
        return len(st.failure_times)
