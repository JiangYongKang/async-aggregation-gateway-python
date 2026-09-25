"""异步聚合查询网关的 HTTP 入口。

- GET /query/{key}：聚合查询（合并/缓存/背压/降级）。
- POST /upstream/behavior/{key}：配置假上游行为，便于本地复现各种场景。
- GET /upstream/stats：查看各 key 的真实上游调用次数（验证请求合并）。
"""
from __future__ import annotations

from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from gateway.aggregator import Aggregator
from gateway.cache import TTLCache
from gateway.config import GatewayConfig
from gateway.errors import ErrorKind, GatewayError
from gateway.fake_upstream import FakeUpstream, UpstreamBehavior
from gateway.models import QueryResult

_CONFIG = GatewayConfig()
_UPSTREAM = FakeUpstream()
_AGGREGATOR = Aggregator(_UPSTREAM, TTLCache(_CONFIG.cache_ttl, _CONFIG.stale_ttl), _CONFIG)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await _AGGREGATOR.aclose()


app = FastAPI(lifespan=lifespan)


class BehaviorIn(BaseModel):
    latency: float = 0.01
    jitter: float = 0.0
    fail: bool = False
    malformed: bool = False
    hang: bool = False


def _result_payload(r: QueryResult) -> dict:
    return {"key": r.key, "value": r.value, "source": r.source.value, "age": round(r.age, 6)}


@app.exception_handler(GatewayError)
async def gateway_error_handler(request: Request, exc: GatewayError) -> JSONResponse:
    status = 503 if exc.kind in (ErrorKind.OVERLOADED, ErrorKind.UPSTREAM_TIMEOUT) else 502
    return JSONResponse(status_code=status, content={"error": exc.kind.value, "detail": exc.detail})


@app.get("/query/{key}")
async def query(key: str) -> dict:
    return _result_payload(await _AGGREGATOR.query(key))


@app.post("/upstream/behavior/{key}")
async def set_behavior(key: str, body: BehaviorIn) -> dict:
    _UPSTREAM.set_behavior(key, UpstreamBehavior(**body.model_dump()))
    return {"key": key, "behavior": body.model_dump()}


@app.get("/upstream/stats")
async def upstream_stats() -> dict:
    return {"call_count": dict(_UPSTREAM.call_count)}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
