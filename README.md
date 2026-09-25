# 异步聚合查询网关

基于 FastAPI + asyncio 的异步聚合查询服务，用进程内假上游（`FakeUpstream`）模拟真实数据源，
使并发访问、上游抖动与客户端取消下的结果和资源占用可预期、可复现。

## 结构

- `gateway/config.py` — 配置项（`GatewayConfig`）
- `gateway/errors.py` — 归一化错误（`GatewayError` + `ErrorKind`）
- `gateway/models.py` — 查询结果（`QueryResult` + `Source`）
- `gateway/fake_upstream.py` — 可脚本化的假上游（延迟/抖动/失败/畸形/挂起）
- `gateway/cache.py` — 带新鲜期与陈旧宽限期的 TTL 缓存
- `gateway/aggregator.py` — 核心：单飞合并 + 背压 + 超时/异常归一化 + 陈旧回退
- `main.py` — HTTP 入口

## 规则说明

### 请求合并（单飞）
- 同一 key 的并发请求共享同一次上游调用，不重复穿透；可用 `GET /upstream/stats` 验证调用次数。
- 命中新鲜缓存（`age <= cache_ttl`）直接返回 `source=cache`。
- 失败结果**不写入缓存**，一次失败不会污染后续请求；上游恢复后自动重新调用。

### 缓存过期与陈旧回退
- `cache_ttl < age <= cache_ttl + stale_ttl`：缓存进入陈旧期，仅用于降级。
- 上游超时/错误/畸形时，若存在陈旧期内的数据，则降级返回 `source=stale` 并携带 `age`（秒），
  判定依据会打印日志（`degrade to stale: key=... kind=... age=...`），绝不静默返回过期数据。
- 无陈旧数据可回退时，抛出对应 `GatewayError`，对外映射为可区分的错误原因。

### 背压与快速拒绝
- 同时进行的上游调用不超过 `max_concurrent`（信号量）。
- 额度打满后请求排队，排队数超过 `max_queue` 立即以 `overloaded`（HTTP 503）拒绝。

### 取消与资源释放
- 等待者通过 `asyncio.shield` 挂在共享 future 上：单个客户端取消只影响自身，其余等待者照常拿到结果。
- 当某 key 的最后一个等待者取消时，对应上游任务被取消，立即释放并发额度，不留悬挂任务。
- 上游挂起由 `upstream_timeout` 兜底，超时后任务必然结束。

### 错误归一化（`ErrorKind`）
| kind | 含义 | HTTP |
|---|---|---|
| `overloaded` | 并发与队列均打满，快速拒绝 | 503 |
| `upstream_timeout` | 上游超时且无陈旧数据 | 503 |
| `upstream_error` | 上游返回错误且无陈旧数据 | 502 |
| `malformed_response` | 上游返回畸形数据且无陈旧数据 | 502 |

## 配置项（`GatewayConfig`）
| 字段 | 默认 | 说明 |
|---|---|---|
| `cache_ttl` | 1.0s | 新鲜缓存有效期 |
| `stale_ttl` | 5.0s | 过期后仍可作为陈旧回退的宽限期 |
| `max_concurrent` | 4 | 最大并发上游调用数 |
| `max_queue` | 8 | 最大排队数，超出即拒绝 |
| `upstream_timeout` | 2.0s | 单次上游调用超时 |

## HTTP 接口
- `GET /query/{key}` → `{key, value, source, age}`；失败时 `{error, detail}` + 上表状态码
- `POST /upstream/behavior/{key}`，body：`{latency, jitter, fail, malformed, hang}` — 配置假上游行为
- `GET /upstream/stats` → 各 key 的真实上游调用次数

## 本地验证

```bash
uv run pytest tests/        # 全部用例，日志打印每个用例的输入与判定依据
uv run python main.py       # 启动服务（127.0.0.1，随机端口见输出）
```

手动复现示例（另开终端，端口以启动输出为准）：

```bash
# 上游失败 -> 502 upstream_error
curl -X POST localhost:PORT/upstream/behavior/k1 -H 'content-type: application/json' -d '{"fail": true}'
curl localhost:PORT/query/k1

# 上游挂起 -> 503 upstream_timeout
curl -X POST localhost:PORT/upstream/behavior/k2 -H 'content-type: application/json' -d '{"hang": true}'
curl localhost:PORT/query/k2
```
