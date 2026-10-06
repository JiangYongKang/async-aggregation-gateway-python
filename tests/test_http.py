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


async def test_query_circuit_open_maps_503(client):
    upstream.fail_keys.add("broke")
    service.cache.invalidate("broke")
    log.info("用例=HTTP 熔断短路 | 输入=连续 GET /query/broke 至第 4 次（阈值 3） | "
             "判定依据=前 3 次 502，第 4 次 503 且 reason=circuit_open，detail 含 retry_after")
    statuses: list[int] = []
    async with client:
        for _ in range(3):
            statuses.append((await client.get("/query/broke")).status_code)
        resp = await client.get("/query/broke")
    body = resp.json()
    log.info("实际: statuses=%s last_status=%d body=%s", statuses, resp.status_code, body)
    upstream.fail_keys.discard("broke")
    assert statuses == [502, 502, 502]
    assert resp.status_code == 503
    assert body["reason"] == "circuit_open"
    assert "retry_after" in body["detail"]
