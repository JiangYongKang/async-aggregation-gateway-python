"""HTTP 层端到端测试：状态码与原因可区分。"""

import logging

import httpx
import pytest

from main import app, service, upstream

log = logging.getLogger("test.http")


@pytest.fixture
def client():
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


async def test_query_ok(client):
    log.info("用例=HTTP 正常查询 | 输入=GET /query/a | 判定依据=200 且 reason=fresh")
    async with client:
        resp = await client.get("/query/a")
    log.info("实际: status=%d body=%s", resp.status_code, resp.json())
    assert resp.status_code == 200
    assert resp.json()["reason"] == "fresh"


async def test_query_upstream_error_maps_502(client):
    upstream.fail_keys.add("bad")
    service.cache.invalidate("bad")
    log.info("用例=HTTP 上游失败 | 输入=GET /query/bad (fail_keys) | 判定依据=502 且 reason=upstream_error")
    async with client:
        resp = await client.get("/query/bad")
    log.info("实际: status=%d body=%s", resp.status_code, resp.json())
    upstream.fail_keys.discard("bad")
    assert resp.status_code == 502
    assert resp.json()["reason"] == "upstream_error"
