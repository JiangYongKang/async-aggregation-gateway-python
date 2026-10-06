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
      │  1) 新鲜缓存命中 → CACHE_HIT（任何熔断状态下都不穿透）
      │  2) 熔断判定（CircuitBreaker，按 key 独立）
      │     OPEN → 旧数据降级 STALE_FALLBACK，否则快速拒绝 CIRCUIT_OPEN
      │  3) 同键合并（RequestCoalescer, singleflight）
      │     HALF_OPEN 时并发请求合到唯一一次恢复试探
      │  4) 限流 + 排队（ConcurrencyLimiter）
      │  5) 超时包裹的上游调用 + 响应校验
      │     成功：写缓存并关闭熔断；失败：计一次熔断失败并旧数据降级
      ▼
FakeUpstream（进程内假上游）
```

模块划分：

| 文件 | 职责 |
|---|---|
| `gateway/config.py` | 全部可调参数（`Settings`） |
| `gateway/types.py` | 对外结果 `QueryResult` 与可区分原因 `Reason` |
| `gateway/cache.py` | 新鲜期 + 陈旧窗口两级缓存 |
| `gateway/breaker.py` | 按数据源的失败短路（CLOSED/OPEN/HALF_OPEN 状态机） |
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

## 失败短路与自动恢复（按数据源）

每个数据源（key）各自维护一个 `CLOSED → OPEN → HALF_OPEN → CLOSED/OPEN` 状态机，
互不影响：

- **CLOSED（正常）**：在 `failure_window` 窗口内，同一 key 真正穿透上游后连续出现
  上游超时（`upstream_timeout`）、上游业务错误（`upstream_error`）或畸形响应
  （`malformed_response`），次数累计达到 `failure_threshold` 即转入 **OPEN**。
- **OPEN（短路冷却）**：`circuit_cooldown` 时间内针对该 key 的请求**不向上游穿透**，
  直接走降级判定——有未彻底过期的旧数据就返回 `stale_fallback`
  （`detail` 里同时带有 `circuit_open` 原因与剩余冷却时间），没有任何可用数据才快速
  拒绝为 `circuit_open`（HTTP 503，`detail` 含连续失败次数与 `retry_after=剩余秒数`）。
- **HALF_OPEN（恢复试探）**：冷却到期后，熔断器**只发放一次试探许可**：
  - 首个请求真正去打上游，同一时刻到达的其他相同请求经 singleflight **合并到这一次
    试探**上，不会趁恢复瞬间一起穿透；试探仍受 `max_concurrent` 并发上限与排队规则
    约束，不会突破整体背压；
  - 试探**成功**：计数清零、转回 CLOSED，leader 任务立即写缓存，后续请求立刻恢复
    `cache_hit`；
  - 试探**失败**：立即重新 OPEN 并以当前时刻**重新计时**冷却，窗口外到达的请求继续
    短路。

计数口径与副作用：

- 只有“真正打到上游”的那三类失败才计入熔断；**短路快速拒绝本身不写缓存、也不算又
  失败一次**，不会自我续命；排队背压（`queue_full`/`queue_timeout`）与客户端取消
  同样不计入。
- 短路与试探**只针对出问题的 key**，其他 key 的查询、缓存命中、合并与并发名额完全不受影响。
- 成功结果（写缓存 + 熔断恢复）在共享调用的 leader 任务**内部**完成，不依赖某个等待者
  存活：即使发起该调用或中途等待它的请求提前取消，只要共享调用最终成功，新鲜期内后续
  查询仍直接命中该次结果，不会重复穿透上游。

## 优先级关系（新能力与既有能力如何组合）

单次 `query(key)` 的判定顺序：

1. **新鲜缓存**：任何熔断状态下，新鲜缓存命中（`cache_hit`）永远最高优先，不穿透；
2. **熔断 OPEN**：不穿透，先看旧数据——陈旧窗口内 → `stale_fallback`（detail 注明
   是因为 `circuit_open` 降级），否则 → `circuit_open` 快速拒绝；
3. **同键合并**：CLOSED 正常合并、HALF_OPEN 时并发请求合并到唯一恢复试探；
4. **限流排队**：真正要穿透的调用（含恢复试探）仍走 `max_concurrent`/`max_queue`，
   背压原因与熔断原因彼此独立、可区分；
5. **上游调用 + 校验**：成功写缓存并恢复熔断；三类上游失败计熔断，并同样按“旧数据
   降级，否则按原始原因拒绝”归一化。

即：缓存命中 > 熔断短路（含旧数据降级）> 请求合并 > 排队背压 > 上游调用。

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
| `circuit_open` | 连续失败触发短路、冷却中不穿透上游（detail 含 retry_after） | 503 |
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
| `failure_threshold` | 3 | 同一数据源在窗口内连续失败多少次后触发短路 |
| `failure_window` | 10.0s | 失败计数有效窗口：窗口外的旧失败不计入阈值 |
| `circuit_cooldown` | 10.0s | 短路冷却时长，到期后自动放一次恢复试探 |

## 本地验证

```bash
uv sync                 # 安装依赖
uv run pytest tests/    # 运行全部测试（日志实时打印输入与判定依据）
uv run uvicorn main:app # 启动服务后访问 GET /query/{key}
```

测试覆盖：并发同键合并、缓存命中、缓存失效后并发共享、上游失败不污染、上游超时、
陈旧数据回退（含判定依据）、畸形响应归一化、排队打满快速拒绝、排队超时、有界并发
峰值、客户端取消释放资源、部分取消不影响共享调用、HTTP 状态码映射；
以及失败短路专项（`tests/test_circuit.py`）：连续失败后进入短路、短路按数据源隔离、
冷却结束后恢复试探成功与试探失败（重新计时）、恢复瞬间并发只穿透一次且不突破并发
上限、短路期间旧数据降级/过期后快速拒绝、共享成功结果在等待者中途取消后仍于新鲜期
内命中且不重复穿透。
每个用例日志均打印「输入」与「判定依据」，连续多次运行结果一致（可重复执行
`uv run pytest tests/` 验证，当前共 21 个用例）。
