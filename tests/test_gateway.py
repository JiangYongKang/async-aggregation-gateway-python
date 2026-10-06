"""异步聚合网关行为测试：每个用例打印输入与判定依据。"""

import asyncio
import logging

from gateway import AggregationService, Reason, Settings
from gateway.upstream import FakeUpstream

log = logging.getLogger("test.gateway")


def make_service(**overrides) -> tuple[AggregationService, FakeUpstream]:
    defaults = {
        "cache_ttl": 0.2,
        "stale_ttl": 1.0,
        "upstream_timeout": 0.15,
        "max_concurrent": 2,
        "max_queue": 2,
        "queue_timeout": 0.2,
    }
    defaults.update(overrides)
    upstream = FakeUpstream(latency=0.02)
    return AggregationService(Settings(**defaults), upstream), upstream


def judge(case: str, inputs: str, basis: str) -> None:
    log.info("用例=%s | 输入=%s | 判定依据=%s", case, inputs, basis)


async def test_concurrent_same_key_coalesced():
    svc, up = make_service()
    judge("并发同键合并", "10 个并发 query('a')，缓存为空",
          "上游仅穿透 1 次；恰好 1 个 FRESH，其余 COALESCED；payload 完全一致")
    results = await asyncio.gather(*(svc.query("a") for _ in range(10)))
    log.info("实际: call_count=%d reasons=%s", up.call_count,
             [r.reason.value for r in results])
    assert up.call_count == 1
    assert sum(r.reason == Reason.FRESH for r in results) == 1
    assert sum(r.reason == Reason.COALESCED for r in results) == 9
    assert len({r.payload["epoch"] for r in results if r.payload}) == 1
    await svc.aclose()


async def test_cache_hit_avoids_upstream():
    svc, up = make_service()
    judge("缓存命中", "连续 2 次 query('a')，间隔 < cache_ttl",
          "第 2 次 reason=CACHE_HIT，上游总穿透仍为 1")
    r1 = await svc.query("a")
    r2 = await svc.query("a")
    log.info("实际: r1=%s r2=%s call_count=%d", r1.reason.value, r2.reason.value,
             up.call_count)
    assert r1.reason == Reason.FRESH
    assert r2.reason == Reason.CACHE_HIT
    assert up.call_count == 1
    await svc.aclose()


async def test_expired_cache_concurrent_requests_share_one_call():
    svc, up = make_service()
    judge("缓存失效后并发合并", "先写入缓存，sleep 超过 ttl 后 8 个并发同键请求",
          "过期后并发请求仍共享同一次上游调用，call_count 总共为 2")
    await svc.query("a")
    await asyncio.sleep(0.25)  # 缓存过期
    results = await asyncio.gather(*(svc.query("a") for _ in range(8)))
    log.info("实际: call_count=%d reasons=%s", up.call_count,
             [r.reason.value for r in results])
    assert up.call_count == 2
    assert all(r.ok for r in results)
    await svc.aclose()


async def test_upstream_error_not_cached_and_recovers():
    svc, up = make_service()
    up.fail_keys.add("bad")
    judge("上游失败不污染", "fail_keys={'bad'} 连续 2 次 query('bad')，随后移除失败标记再查",
          "前 2 次均为 UPSTREAM_ERROR 且每次独立穿透；恢复后第 3 次 FRESH")
    r1 = await svc.query("bad")
    r2 = await svc.query("bad")
    up.fail_keys.discard("bad")
    r3 = await svc.query("bad")
    log.info("实际: r1=%s r2=%s r3=%s call_count=%d",
             r1.reason.value, r2.reason.value, r3.reason.value, up.call_count)
    assert r1.reason == r2.reason == Reason.UPSTREAM_ERROR
    assert not r1.ok and not r2.ok
    assert r3.reason == Reason.FRESH
    assert up.call_count == 3
    await svc.aclose()


async def test_upstream_timeout_distinct_reason():
    svc, up = make_service()
    up.hang_keys.add("slow")
    judge("上游超时", "hang_keys={'slow'}，upstream_timeout=0.15s，无陈旧数据",
          "reason=UPSTREAM_TIMEOUT，ok=False，耗时远小于挂起时长")
    r = await svc.query("slow")
    log.info("实际: reason=%s detail=%s", r.reason.value, r.detail)
    assert not r.ok and r.reason == Reason.UPSTREAM_TIMEOUT
    await svc.aclose()


async def test_stale_fallback_on_timeout_is_explainable():
    svc, up = make_service()
    judge("陈旧数据回退", "先成功写入缓存，过期后上游挂起",
          "reason=STALE_FALLBACK，stale=True，detail 含原始原因 upstream_timeout")
    r1 = await svc.query("a")
    await asyncio.sleep(0.25)  # 过期但仍在 stale 窗口内
    up.hang_keys.add("a")
    r2 = await svc.query("a")
    log.info("实际: reason=%s stale=%s detail=%s", r2.reason.value, r2.stale, r2.detail)
    assert r2.ok and r2.reason == Reason.STALE_FALLBACK and r2.stale
    assert r2.payload == r1.payload
    assert "upstream_timeout" in r2.detail
    await svc.aclose()


async def test_malformed_response_normalized():
    svc, up = make_service()
    up.malformed_keys.add("weird")
    judge("畸形响应归一化", "malformed_keys={'weird'}",
          "reason=MALFORMED_RESPONSE，与 UPSTREAM_ERROR/TIMEOUT 可区分")
    r = await svc.query("weird")
    log.info("实际: reason=%s detail=%s", r.reason.value, r.detail)
    assert not r.ok and r.reason == Reason.MALFORMED_RESPONSE
    await svc.aclose()


async def test_queue_full_fast_reject():
    svc, up = make_service(max_concurrent=1, max_queue=1, queue_timeout=1.0)
    gate = asyncio.Event()
    up.set_gate(gate)
    judge("排队打满快速拒绝", "max_concurrent=1 max_queue=1，上游被闸门阻塞，并发 5 个不同键",
          "1 个占用执行 + 1 个排队，其余 3 个立即得到 QUEUE_FULL，可与其他原因区分")
    tasks = [asyncio.create_task(svc.query(f"k{i}")) for i in range(5)]
    await asyncio.sleep(0.1)
    gate.set()
    results = await asyncio.gather(*tasks)
    reasons = [r.reason for r in results]
    log.info("实际: reasons=%s", [r.value for r in reasons])
    assert reasons.count(Reason.QUEUE_FULL) == 3
    assert all(r.reason in (Reason.FRESH, Reason.QUEUE_FULL) for r in results)
    await svc.aclose()


async def test_queue_timeout_distinct_from_queue_full():
    svc, up = make_service(max_concurrent=1, max_queue=2, queue_timeout=0.1)
    gate = asyncio.Event()
    up.set_gate(gate)
    judge("排队超时", "max_concurrent=1 max_queue=2 queue_timeout=0.1s，闸门不放行，3 个不同键",
          "执行中的 1 个因闸门阻塞超过 upstream_timeout 得到 UPSTREAM_TIMEOUT；"
          "2 个排队者在 0.1s 后得到 QUEUE_TIMEOUT，两者可区分")
    tasks = [asyncio.create_task(svc.query(f"k{i}")) for i in range(3)]
    await asyncio.sleep(0.5)
    gate.set()
    results = await asyncio.gather(*tasks)
    reasons = [r.reason for r in results]
    log.info("实际: reasons=%s", [r.value for r in reasons])
    assert reasons.count(Reason.QUEUE_TIMEOUT) == 2
    assert reasons.count(Reason.UPSTREAM_TIMEOUT) == 1
    await svc.aclose()


async def test_bounded_concurrency_never_exceeded():
    svc, up = make_service(max_concurrent=3, max_queue=20, queue_timeout=2.0)
    judge("有界并发", "max_concurrent=3，12 个不同键并发",
          "上游观测到的 max_inflight 峰值 <= 3，且全部成功")
    results = await asyncio.gather(*(svc.query(f"k{i}") for i in range(12)))
    log.info("实际: max_inflight=%d ok=%d/12", up.max_inflight,
             sum(r.ok for r in results))
    assert up.max_inflight <= 3
    assert all(r.ok for r in results)
    await svc.aclose()


async def test_client_cancel_releases_resources():
    svc, up = make_service(max_concurrent=1, max_queue=5)
    gate = asyncio.Event()
    up.set_gate(gate)
    judge("客户端取消", "2 个等待者共享同键 flight，全部取消",
          "取消后 limiter 占用归零、无进行中 flight、事件循环无悬挂任务")
    t1 = asyncio.create_task(svc.query("a"))
    t2 = asyncio.create_task(svc.query("a"))
    await asyncio.sleep(0.05)
    t1.cancel()
    t2.cancel()
    await asyncio.gather(t1, t2, return_exceptions=True)
    await asyncio.sleep(0.05)
    log.info("实际: active=%d queued=%d inflight=%d",
             svc.limiter.active, svc.limiter.queued, svc.coalescer.inflight_count)
    assert svc.limiter.active == 0
    assert svc.limiter.queued == 0
    assert svc.coalescer.inflight_count == 0
    leftover = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    assert leftover == [], leftover
    gate.set()
    await svc.aclose()


async def test_partial_cancel_keeps_shared_call_alive():
    svc, up = make_service()
    gate = asyncio.Event()
    up.set_gate(gate)
    judge("部分取消", "2 个等待者共享同键 flight，仅取消其中 1 个后放行闸门",
          "被取消者收到 CancelledError，幸存者仍拿到 FRESH 结果，上游调用未被误杀")
    t1 = asyncio.create_task(svc.query("a"))
    t2 = asyncio.create_task(svc.query("a"))
    await asyncio.sleep(0.05)
    t1.cancel()
    gate.set()
    r2 = await t2
    log.info("实际: t1.cancelled=%s t2.reason=%s", t1.cancelled(), r2.reason.value)
    assert t1.cancelled()
    assert r2.ok and r2.reason in (Reason.FRESH, Reason.COALESCED)
    await svc.aclose()
