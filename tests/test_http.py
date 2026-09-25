"""HTTP 层测试：验证对外结果与错误原因在 API 上可区分。"""
from __future__ import annotations

import asyncio
import logging

import httpx
import pytest

from main import app

logger = logging.getLogger("test")


@pytest.fixture
async def client():
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def test_concurrent_same_key_over_http_shares_one_call(client):
    logger.info("input: 5 concurrent GET /query/http-merge")
    responses = await asyncio.gather(*[client.get("/query/http-merge") for _ in range(5)])
    stats = (await client.get("/upstream/stats")).json()
    logger.info("evidence: statuses=%s call_count=%s",
                [r.status_code for r in responses], stats["call_count"].get("http-merge"))
    assert all(r.status_code == 200 for r in responses)
    assert stats["call_count"]["http-merge"] == 1


async def test_upstream_error_maps_to_502_with_distinct_reason(client):
    await client.post("/upstream/behavior/http-fail", json={"fail": True, "latency": 0})
    r = await client.get("/query/http-fail")
    logger.info("evidence: status=%d body=%s", r.status_code, r.json())
    assert r.status_code == 502
    assert r.json()["error"] == "upstream_error"


async def test_hanging_upstream_maps_to_timeout_reason(client):
    await client.post("/upstream/behavior/http-hang", json={"hang": True})
    r = await client.get("/query/http-hang")
    logger.info("evidence: status=%d body=%s", r.status_code, r.json())
    assert r.status_code == 503
    assert r.json()["error"] == "upstream_timeout"
