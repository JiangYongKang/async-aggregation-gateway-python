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
      │  2) 短路检查（CircuitBreaker）：开路中 → CIRCUIT_OPEN / 陈旧降级
      │  3) 同键合并（RequestCoalescer, singleflight；恢复试探也走这里）
      │  4) 限流 + 排队（ConcurrencyLimiter）
      │  5) 超时包裹的上游调用 + 响应校验
      ▼
FakeUpstream（进程内假上游）
```

模块划分：

| 文件 | 职责 |
|---|---|
| `gateway/config.py` | 全部可调参数（`Settings`） |
| `gateway/types.py` | 对外结果 `QueryResult` 与可区分原因 `Reason` |
| `gateway/cache.py` | 新鲜期 + 陈旧窗口两级缓存 |
| `gateway/breaker.py` | 按 key 的失败短路（circuit breaker）与恢复试探 |
| `gateway/coalescer.py` | 同键请求合并（singleflight） |
| `gateway/limiter.py` | 有界并发 + 排队背压 |
| `gateway/upstream.py` | 可编程假上游（延迟/抖动/失败/畸形/挂起/闸门） |
| `gateway/service.py` | 编排：缓存 → 短路 → 合并 → 限流 → 归一化与降级 |
| `main.py` | FastAPI 接线与状态码映射 |

## 请求合并规则

- 同一起点（key）的并发请求共享**同一次**上游调用：首个到达者发起，后续请求合并
  等待（结果 `reason=coalesced`），不重复穿透。
- 缓存失效瞬间的并发同键请求同样只穿透一次。
- 任一等待者被客户端取消**不会**误杀共享调用（shield 隔离），其余等待者照常拿到结果；
  当所有等待者都取消后，底层上游调用被取消，不留悬挂任务。
- 一次失败只影响当前这批等待者：失败结果**不写入缓存**，flight 立即移除，
  后续请求重新发起新调用，不会被旧失败污染。
- 共享调用的**成功结果由 flight 自身写入缓存**：只要这次调用最终成功，
  即使发起者或中途等待的某个请求被客户端取消，新鲜期内的后续查询仍能
  `cache_hit` 复用该结果，不会重复穿透上游。

## 失败短路与自动恢复（Circuit Breaker）

按起点（key）独立计数与开合，某个起点持续挂掉时不再重复穿透上游：

- **CLOSED（正常）**：上游超时 / 上游报错 / 响应畸形按**每次上游调用**计一次连续失败
  （与等待者数量无关；客户端取消、本地背压 `queue_full`/`queue_timeout` 不计入）。
  连续失败达到 `failure_threshold` → 进入 OPEN。
- **OPEN（短路）**：`cooldown` 秒内对该 key 的请求**不穿透上游**，快速返回
  `reason=circuit_open`（HTTP 503），`detail` 说明是因连续失败被短路并给出
  恢复倒计时（`retry in X.XXs`）。短路拒绝**不写缓存、不计入失败次数、不延后冷却**。
- **HALF_OPEN（恢复试探）**：冷却到期后，同一时刻**只放行一个请求**真正试探上游；
  并发的同 key 请求通过 singleflight **合并到这次试探上**，不会趁恢复瞬间打爆上游；
  试探同样走限流器，不突破 `max_concurrent` 整体并发上限。
  - 试探成功 → 电路闭合、失败计数清零，结果正常写缓存，后续请求立即恢复
    `cache_hit`/`fresh` 正常路径；
  - 试探失败 → 重新 OPEN 并从失败时刻**重新计时**冷却。
- 短路/试探只影响出问题的那个 key，其余 key 的查询完全不受影响。

### 与现有能力的优先级关系

一次查询的判定顺序（前者优先）：

1. **新鲜缓存**：命中即 `cache_hit`，不触碰短路器与上游；
2. **短路检查**：OPEN 中 → 有未彻底过期的旧数据则按降级规则返回
   `stale_fallback`（`detail` 含 `circuit_open`），否则 `circuit_open` 快速拒绝；
3. **请求合并**：ALLOW / 恢复试探 / 合并到在途试探，同 key 共享一次调用；
4. **排队背压**：`max_concurrent` / `max_queue` / `queue_timeout` 约束穿透上游的
   调用（含恢复试探），打满即 `queue_full` / `queue_timeout`；
5. **上游调用**：超时/报错/畸形 → 计入短路失败次数，并按降级规则回退陈旧数据。

即：缓存降级优先于短路拒绝（有旧数据就不直接报错），短路优先于合并与排队
（开路期间不占用合并 flight 与排队名额），试探穿透受排队背压约束。

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
| `circuit_open` | 连续失败触发短路，冷却期内快速拒绝（detail 含恢复倒计时） | 503 |

## 配置项（`Settings`）

| 参数 | 默认 | 说明 |
|---|---|---|
| `cache_ttl` | 5.0s | 缓存新鲜期 |
| `stale_ttl` | 30.0s | 过期后仍可作为陈旧数据回退的窗口 |
| `upstream_timeout` | 2.0s | 单次上游调用超时 |
| `max_concurrent` | 4 | 同时访问上游的最大并发数 |
| `max_queue` | 8 | 排队等待名额，超出快速拒绝 |
| `queue_timeout` | 5.0s | 排队最长等待时间 |
| `failure_threshold` | 3 | 同一 key 连续上游失败达到此次数后进入短路 |
| `cooldown` | 10.0s | 短路冷却时间，到期后放行单个恢复试探 |

## 本地验证

```bash
uv sync                 # 安装依赖
uv run pytest tests/    # 运行全部测试（日志实时打印输入与判定依据）
uv run uvicorn main:app # 启动服务后访问 GET /query/{key}
```

测试覆盖：并发同键合并、缓存命中、缓存失效后并发共享、上游失败不污染、上游超时、
陈旧数据回退（含判定依据）、畸形响应归一化、排队打满快速拒绝、排队超时、有界并发
峰值、客户端取消释放资源、部分取消不影响共享调用、HTTP 状态码映射，
以及失败短路专项（`tests/test_circuit_breaker.py`）：连续失败进入短路、
短路拒绝不计失败不延后冷却、恢复试探成功/失败、恢复瞬间并发只穿透一次、
短路期间旧数据降级、共享成功结果在发起者取消后仍入缓存供新鲜期复用。
每个用例日志均打印「输入」与「判定依据」，连续多次运行结果一致：

```bash
uv run pytest tests/ -q && uv run pytest tests/ -q   # 连跑两次，结果应一致
```
