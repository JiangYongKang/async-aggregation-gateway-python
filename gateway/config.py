"""配置项定义。"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    """网关运行参数（全部为秒/个数，便于测试注入小数值）。"""

    cache_ttl: float = 5.0            # 缓存新鲜期
    stale_ttl: float = 30.0           # 过期后仍可作为陈旧数据回退的窗口
    upstream_timeout: float = 2.0     # 单次上游调用超时
    max_concurrent: int = 4           # 同时访问上游的最大并发数
    max_queue: int = 8                # 排队等待名额，超出即快速拒绝
    queue_timeout: float = 5.0        # 排队最长等待时间
    failure_threshold: int = 3        # 同一key连续上游失败达到此次数后短路
    cooldown: float = 10.0            # 短路冷却时间，到期后放行单个恢复试探
