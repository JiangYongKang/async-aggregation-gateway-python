"""聚合器行为测试：每个用例打印输入与判定依据，断言结果一致且原因可区分。"""
from __future__ import annotations

import asyncio
import logging

import pytest

from gateway.errors import ErrorKind, GatewayError
from gateway.fake_upstream import UpstreamBehavior
from gateway.models import Source

logger = logging.getLogger("test")


async def test_concurrent_same_key_requests_share_one_upstream_call(aggregator, upstream):
    n = 10
    logger.info("input: %d concurrent query('a'), upstream latency=20ms", n)
    results = await asyncio.gather(*[aggregator.query("a") for _ in range(n)])
    seqs = {r.value["seq"] for r in results}
    logger.info("evidence: upstream_calls=%d distinct_seqs=%s sources=%s",
                upstream.calls("a"), seqs, {r.source for r in results})
    assert upstream.calls("a") == 1, "并发同键请求必须共享同一次上游调用"
    assert len(seqs) == 1, "所有请求必须拿到同一份结果"
    assert all(r.source == Source.UPSTREAM for r in results)


async def test_fresh_cache_hit_avoids_upstream(aggregator, upstream):
    await aggregator.query("a")
    logger.info("input: second query within cache_ttl")
    r = await aggregator.query("a")
    logger.info("evidence: source=%s upstream_calls=%d", r.source, upstream.calls("a"))
    assert r.source == Source.CACHE
    assert upstream.calls("a") == 1, "新鲜缓存命中不得再次穿透上游"


async def test_upstream_failure_falls_back_to_stale_cache(aggregator, upstream, config):
    first = await aggregator.query("a")
    await asyncio.sleep(config.cache_ttl + 0.02)  # 让缓存进入陈旧期
    upstream.set_behavior("a", UpstreamBehavior(latency=0.01, fail=True))
    logger.info("input: cache expired, upstream now fails")
    r = await aggregator.query("a")
    logger.info("evidence: source=%s age=%.3f stale_ttl=%.2f value=%s",
                r.source, r.age, config.stale_ttl, r.value)
    assert r.source == Source.STALE, "上游失败且陈旧数据在宽限期内必须降级返回"
    assert r.value == first.value, "降级必须返回上次成功的数据，不得静默返回错误"
    assert 0 < r.age <= config.cache_ttl + config.stale_ttl


async def test_upstream_error_without_cache_is_normalized(aggregator, upstream):
    upstream.set_behavior("b", UpstreamBehavior(latency=0.0, fail=True))
    logger.info("input: query('b') with failing upstream and empty cache")
    with pytest.raises(GatewayError) as exc_info:
        await aggregator.query("b")
    logger.info("evidence: kind=%s detail=%s", exc_info.value.kind, exc_info.value.detail)
    assert exc_info.value.kind == ErrorKind.UPSTREAM_ERROR


async def test_failure_is_not_cached_and_does_not_poison_later_results(aggregator, upstream):
    upstream.set_behavior("b", UpstreamBehavior(latency=0.0, fail=True))
    with pytest.raises(GatewayError):
        await aggregator.query("b")
    upstream.set_behavior("b", UpstreamBehavior(latency=0.0))
    logger.info("input: retry after upstream recovers")
    r = await aggregator.query("b")
    logger.info("evidence: source=%s upstream_calls=%d", r.source, upstream.calls("b"))
    assert r.source == Source.UPSTREAM
    assert upstream.calls("b") == 2, "失败不得被缓存，恢复后必须重新调用上游"


async def test_upstream_timeout_is_normalized(aggregator, upstream):
    upstream.set_behavior("c", UpstreamBehavior(hang=True))
    logger.info("input: query('c') with hanging upstream, timeout=0.2s")
    with pytest.raises(GatewayError) as exc_info:
        await aggregator.query("c")
    logger.info("evidence: kind=%s", exc_info.value.kind)
    assert exc_info.value.kind == ErrorKind.UPSTREAM_TIMEOUT


async def test_malformed_response_is_normalized(aggregator, upstream):
    upstream.set_behavior("d", UpstreamBehavior(latency=0.0, malformed=True))
    logger.info("input: query('d') with malformed upstream payload")
    with pytest.raises(GatewayError) as exc_info:
        await aggregator.query("d")
    logger.info("evidence: kind=%s detail=%s", exc_info.value.kind, exc_info.value.detail)
    assert exc_info.value.kind == ErrorKind.MALFORMED_RESPONSE


async def test_queue_overflow_is_rejected_fast_with_distinct_reason(aggregator, upstream, config):
    keys = [f"e{i}" for i in range(config.max_concurrent + config.max_queue + 1)]
    for k in keys:
        upstream.set_behavior(k, UpstreamBehavior(latency=0.15))
    logger.info("input: %d concurrent distinct keys, max_concurrent=%d max_queue=%d",
                len(keys), config.max_concurrent, config.max_queue)
    results = await asyncio.gather(*[aggregator.query(k) for k in keys], return_exceptions=True)
    rejected = [r for r in results if isinstance(r, GatewayError)]
    ok = [r for r in results if not isinstance(r, GatewayError)]
    logger.info("evidence: rejected=%d kinds=%s ok=%d", len(rejected),
                [r.kind for r in rejected], len(ok))
    assert len(rejected) == 1, "超出 并发+队列 承载的请求必须被快速拒绝"
    assert all(r.kind == ErrorKind.OVERLOADED for r in rejected), "拒绝原因必须可区分"
    assert all(r.source == Source.UPSTREAM for r in ok)


async def test_client_cancel_releases_resources_and_leaves_no_dangling_tasks(aggregator, upstream):
    upstream.set_behavior("f", UpstreamBehavior(hang=True))
    logger.info("input: query('f') against hanging upstream, client cancels after 20ms")
    task = asyncio.create_task(aggregator.query("f"))
    await asyncio.sleep(0.02)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0.01)
    dangling = [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]
    logger.info("evidence: inflight=%s dangling_tasks=%s", list(aggregator._inflight), dangling)
    assert not aggregator._inflight, "取消后不得残留进行中的合并任务"
    assert not dangling, "取消后不得存在悬挂任务"


async def test_cancel_of_one_waiter_does_not_affect_others(aggregator, upstream):
    upstream.set_behavior("g", UpstreamBehavior(latency=0.1))
    logger.info("input: two merged waiters, one cancels midway")
    t1 = asyncio.create_task(aggregator.query("g"))
    t2 = asyncio.create_task(aggregator.query("g"))
    await asyncio.sleep(0.02)
    t1.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t1
    r2 = await t2
    logger.info("evidence: survivor source=%s upstream_calls=%d", r2.source, upstream.calls("g"))
    assert r2.source == Source.UPSTREAM, "其余等待者必须正常拿到结果"
    assert upstream.calls("g") == 1
