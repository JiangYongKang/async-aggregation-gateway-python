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
    failure_threshold: int = 3        # 同一数据源连续失败多少次后触发短路
    failure_window: float = 10.0      # 失败计数的有效窗口：窗口外的旧失败不计入
    circuit_cooldown: float = 10.0    # 短路冷却时长，到期后放一次恢复试探
