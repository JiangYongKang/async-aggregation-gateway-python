"""对外结果与原因定义。"""

import enum
from dataclasses import dataclass, field


class Reason(enum.Enum):
    """结果来源/失败原因，要求对外可区分。"""

    FRESH = "fresh"                    # 本次上游调用成功
    CACHE_HIT = "cache_hit"            # 命中新鲜缓存
    COALESCED = "coalesced"            # 合并到进行中的同键请求
    STALE_FALLBACK = "stale_fallback"  # 上游失败/超时，回退陈旧数据
    UPSTREAM_TIMEOUT = "upstream_timeout"
    UPSTREAM_ERROR = "upstream_error"
    MALFORMED_RESPONSE = "malformed_response"
    QUEUE_FULL = "queue_full"          # 排队名额已满，快速拒绝
    QUEUE_TIMEOUT = "queue_timeout"    # 排队超时
    CANCELLED = "cancelled"            # 客户端取消
    CIRCUIT_OPEN = "circuit_open"      # 连续失败触发短路，冷却期内快速拒绝


@dataclass(frozen=True)
class QueryResult:
    """统一对外结果。ok=False 时 payload 为 None，reason 给出可区分原因。"""

    ok: bool
    reason: Reason
    key: str
    payload: dict | None = None
    stale: bool = False
    detail: str = ""
    served_at: float = 0.0


class UpstreamError(Exception):
    """上游主动返回的业务错误。"""


class MalformedResponseError(Exception):
    """上游返回数据无法解析/校验。"""
