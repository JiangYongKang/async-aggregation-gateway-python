"""按数据源（key）的失败短路（circuit breaker）与自动恢复试探。

状态机（每个 key 独立）：

- CLOSED：正常放行；连续上游失败（超时/报错/畸形）计数达到阈值 → OPEN；
- OPEN：冷却期内直接拒绝（不穿透上游、不写缓存、不计入失败次数）；
  冷却到期后进入 HALF_OPEN；
- HALF_OPEN：只放行一个恢复试探（probe），其余同 key 请求合并到这次试探上；
  试探成功 → CLOSED 并清零失败计数；试探失败 → 重新 OPEN 并重新计时。

取消（CancelledError）与本地背压（queue_full/queue_timeout）不算上游失败，
不会推进短路计数。
"""

import enum
import time
from dataclasses import dataclass


class Action(enum.Enum):
    """check() 的判定结果。"""

    ALLOW = "allow"    # 正常放行
    PROBE = "probe"    # 放行，且本次调用是恢复试探（持有试探名额）
    JOIN = "join"      # 已有试探在途，调用方应合并到该试探的 flight 上
    REJECT = "reject"  # 短路中，快速拒绝（retry_after 提示恢复时间）


@dataclass(frozen=True)
class Decision:
    action: Action
    retry_after: float = 0.0


@dataclass
class _KeyState:
    failures: int = 0            # 连续上游失败次数（成功即清零）
    opened_at: float | None = None  # 进入 OPEN 的时刻；None 表示 CLOSED
    probe_in_flight: bool = False   # HALF_OPEN 下是否已有试探在途


class CircuitBreaker:
    """按 key 的短路器；全部方法同步，天然与单事件循环的调用方无竞态。"""

    def __init__(self, failure_threshold: int, cooldown: float) -> None:
        self.failure_threshold = failure_threshold
        self.cooldown = cooldown
        self._states: dict[str, _KeyState] = {}

    def _state(self, key: str) -> _KeyState:
        return self._states.setdefault(key, _KeyState())

    def check(self, key: str, now: float | None = None) -> Decision:
        """请求前判定：放行 / 成为试探 / 合并到在途试探 / 短路拒绝。"""
        now = time.monotonic() if now is None else now
        st = self._state(key)
        if st.opened_at is None:
            return Decision(Action.ALLOW)
        elapsed = now - st.opened_at
        if elapsed < self.cooldown:
            return Decision(Action.REJECT, retry_after=self.cooldown - elapsed)
        # 冷却结束，进入 HALF_OPEN：同一时刻只放一个试探
        if st.probe_in_flight:
            return Decision(Action.JOIN)
        st.probe_in_flight = True
        return Decision(Action.PROBE)

    def on_success(self, key: str) -> None:
        """上游调用成功：清零失败计数并闭合电路（试探成功即恢复正常）。"""
        st = self._state(key)
        st.failures = 0
        st.opened_at = None
        st.probe_in_flight = False

    def on_failure(self, key: str, now: float | None = None) -> None:
        """上游调用失败（超时/报错/畸形）：累计并在达到阈值时开路。

        试探失败（当前处于 OPEN/HALF_OPEN）时重新计时冷却。
        """
        now = time.monotonic() if now is None else now
        st = self._state(key)
        st.failures += 1
        if st.opened_at is not None:
            # 恢复试探失败：继续短路并重新计时
            st.opened_at = now
            st.probe_in_flight = False
        elif st.failures >= self.failure_threshold:
            st.opened_at = now

    def probe_done(self, key: str) -> None:
        """试探 flight 结束（含被取消）时释放试探名额，不改变成败计数。

        on_success/on_failure 已处理的情况重复调用是幂等的。
        """
        st = self._state(key)
        st.probe_in_flight = False

    def state_of(self, key: str, now: float | None = None) -> str:
        """供测试/观测：closed / open / half_open。"""
        now = time.monotonic() if now is None else now
        st = self._states.get(key)
        if st is None or st.opened_at is None:
            return "closed"
        if now - st.opened_at >= self.cooldown:
            return "half_open"
        return "open"
