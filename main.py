from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from gateway import AggregationService, Reason, Settings
from gateway.upstream import FakeUpstream

settings = Settings()
upstream = FakeUpstream()
service = AggregationService(settings, upstream)

# 对外原因 -> HTTP 状态码：成功类 200，背压 503，上游类 502/504
_STATUS_BY_REASON = {
    Reason.QUEUE_FULL: 503,
    Reason.QUEUE_TIMEOUT: 503,
    Reason.UPSTREAM_TIMEOUT: 504,
    Reason.UPSTREAM_ERROR: 502,
    Reason.MALFORMED_RESPONSE: 502,
    Reason.CANCELLED: 499,
    Reason.CIRCUIT_OPEN: 503,
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await service.aclose()


app = FastAPI(lifespan=lifespan)


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/query/{key}")
async def query(key: str):
    result = await service.query(key)
    status = 200 if result.ok else _STATUS_BY_REASON.get(result.reason, 500)
    return JSONResponse(
        status_code=status,
        content={
            "ok": result.ok,
            "reason": result.reason.value,
            "key": result.key,
            "payload": result.payload,
            "stale": result.stale,
            "detail": result.detail,
        },
    )


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
