"""归一化的对外错误类型。"""
from __future__ import annotations

from enum import Enum


class ErrorKind(str, Enum):
    """可区分的对外失败原因。"""

    OVERLOADED = "overloaded"            # 并发与队列均打满，快速拒绝
    UPSTREAM_TIMEOUT = "upstream_timeout"  # 上游超时且无陈旧数据可回退
    UPSTREAM_ERROR = "upstream_error"      # 上游返回错误且无陈旧数据可回退
    MALFORMED_RESPONSE = "malformed_response"  # 上游返回畸形数据且无陈旧数据可回退


class GatewayError(Exception):
    """对外统一的异常类型，kind 区分原因。"""

    def __init__(self, kind: ErrorKind, detail: str = "") -> None:
        super().__init__(f"{kind.value}: {detail}")
        self.kind = kind
        self.detail = detail
