# 异步聚合查询网关

基于 FastAPI + asyncio 的异步聚合查询服务。用进程内假上游（`FakeUpstream`）模拟真实
数据源的延迟、抖动、失败、畸形响应与挂起，使并发访问、上游抖动与客户端取消下的
结果和资源占用**可预期、可复现**。

## 架构

```
GET /query/{key}
      │
      ▼
AggregationService.query
      │  1) 新鲜缓存命中 → CACHE_HIT
      │  2) 同键合并（RequestCoalescer, singleflight）
      │  3) 限流 + 排队（ConcurrencyLimiter）
      │  4) 超时包裹的上游调用 + 响应校验
      ▼
FakeUpstream（进程内假上游）
```

模块划分：

| 文件 | 职责 |
|---|---|
| `gateway/config.py` | 全部可调参数（`Settings`） |
| `gateway/types.py` | 对外结果 `QueryResult` 与可区分原因 `Reason` |
| `gateway/cache.py` | 新鲜期 + 陈旧窗口两级缓存 |
| `gateway/coalescer.py` | 同键请求合并（singleflight） |
| `gateway/limiter.py` | 有界并发 + 排队背压 |
| `gateway/upstream.py` | 可编程假上游（延迟/抖动/失败/畸形/挂起/闸门） |
| `gateway/service.py` | 编排：缓存 → 合并 → 限流 → 归一化与降级 |
| `main.py` | FastAPI 接线与状态码映射 |

## 请求合并规则

- 同一起点（key）的并发请求共享**同一次**上游调用：首个到达者发起，后续请求合并
  等待（结果 `reason=coalesced`），不重复穿透。
- 缓存失效瞬间的并发同键请求同样只穿透一次。
- 任一等待者被客户端取消**不会**误杀共享调用（shield 隔离），其余等待者照常拿到结果；
  当所有等待者都取消后，底层上游调用被取消，不留悬挂任务。
- 一次失败只影响当前这批等待者：失败结果**不写入缓存**，flight 立即移除，
  后续请求重新发起新调用，不会被旧失败污染。

## 缓存与降级语义

- `[0, cache_ttl)`：新鲜，直接命中（`cache_hit`）。
- `[cache_ttl, cache_ttl + stale_ttl)`：陈旧，**仅**在上游失败/超时/畸形时回退
  （`stale_fallback`，`stale=true`，`detail` 中记录原始失败原因，可解释、不静默）。
- 超过 `stale_ttl`：彻底失效，不再回退。
- 降级判定规则：上游超时 → 有陈旧数据则 `stale_fallback`，否则 `upstream_timeout`；
  上游错误、畸形响应同理。本地背压（`queue_full`/`queue_timeout`）**不**触发陈旧回退，
  直接快速拒绝。

## 背压规则

- 同时访问上游的调用不超过 `max_concurrent`；超出者在队列中等待，队列名额
  不超过 `max_queue`。
- 队列已满 → 立即拒绝，`reason=queue_full`（HTTP 503）。
- 排队超过 `queue_timeout` → `reason=queue_timeout`（HTTP 503）。
- 上游超过 `upstream_timeout` 无响应 → 取消该次调用并释放并发名额。
- 客户端取消或等待结束时，排队计数与并发名额在 `finally` 中回收，无资源泄漏。

## 对外原因（Reason）与状态码

| reason | 含义 | HTTP |
|---|---|---|
| `fresh` | 本次上游调用成功 | 200 |
| `cache_hit` | 命中新鲜缓存 | 200 |
| `coalesced` | 合并到进行中的同键调用 | 200 |
| `stale_fallback` | 上游异常，回退陈旧数据（detail 含原始原因） | 200 |
| `upstream_timeout` | 上游超时且无陈旧数据 | 504 |
| `upstream_error` | 上游业务错误且无陈旧数据 | 502 |
| `malformed_response` | 响应缺字段/无法解析且无陈旧数据 | 502 |
| `queue_full` | 排队名额已满，快速拒绝 | 503 |
| `queue_timeout` | 排队等待超时 | 503 |
| `cancelled` | 共享调用被外部取消（如服务关闭） | 499 |

## 配置项（`Settings`）

| 参数 | 默认 | 说明 |
|---|---|---|
| `cache_ttl` | 5.0s | 缓存新鲜期 |
| `stale_ttl` | 30.0s | 过期后仍可作为陈旧数据回退的窗口 |
| `upstream_timeout` | 2.0s | 单次上游调用超时 |
| `max_concurrent` | 4 | 同时访问上游的最大并发数 |
| `max_queue` | 8 | 排队等待名额，超出快速拒绝 |
| `queue_timeout` | 5.0s | 排队最长等待时间 |

## 本地验证

```bash
uv sync                 # 安装依赖
uv run pytest tests/    # 运行全部测试（日志实时打印输入与判定依据）
uv run uvicorn main:app # 启动服务后访问 GET /query/{key}
```

测试覆盖：并发同键合并、缓存命中、缓存失效后并发共享、上游失败不污染、上游超时、
陈旧数据回退（含判定依据）、畸形响应归一化、排队打满快速拒绝、排队超时、有界并发
峰值、客户端取消释放资源、部分取消不影响共享调用、HTTP 状态码映射。
每个用例日志均打印「输入」与「判定依据」，连续多次运行结果一致。
