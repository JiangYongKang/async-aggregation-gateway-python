"""配置项定义。"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class GatewayConfig:
    """网关运行参数。

    - cache_ttl: 新鲜缓存有效期（秒）。
    - stale_ttl: 缓存过期后仍可作为陈旧数据回退的宽限期（秒）。
    - max_concurrent: 同时允许执行（含合并后触发上游）的最大请求数。
    - max_queue: 并发打满后允许排队等待的最大请求数，超出即快速拒绝。
    - upstream_timeout: 单次上游调用的超时时间（秒）。
    """

    cache_ttl: float = 1.0
    stale_ttl: float = 5.0
    max_concurrent: int = 4
    max_queue: int = 8
    upstream_timeout: float = 2.0
