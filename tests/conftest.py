from __future__ import annotations

import logging

import pytest

from gateway.aggregator import Aggregator
from gateway.cache import TTLCache
from gateway.config import GatewayConfig
from gateway.fake_upstream import FakeUpstream, UpstreamBehavior


@pytest.fixture
def config() -> GatewayConfig:
    return GatewayConfig(cache_ttl=0.05, stale_ttl=1.0, max_concurrent=2, max_queue=1, upstream_timeout=0.2)


@pytest.fixture
def upstream() -> FakeUpstream:
    return FakeUpstream(default=UpstreamBehavior(latency=0.02))


@pytest.fixture
async def aggregator(config, upstream):
    agg = Aggregator(upstream, TTLCache(config.cache_ttl, config.stale_ttl), config)
    yield agg
    await agg.aclose()


logging.basicConfig(level=logging.INFO, format="%(name)s | %(message)s", force=True)


@pytest.fixture(autouse=True)
def log_case(request, caplog):
    caplog.set_level(logging.INFO)
    logging.getLogger("gateway.aggregator").setLevel(logging.INFO)
    print(f"\n=== case: {request.node.name} ===")
