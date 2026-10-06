"""按数据源失败短路与自动恢复试探的行为测试。

每个用例打印「输入」与「判定依据」；全部使用小数值配置与显式时序，
不依赖随机抖动，连续多次运行结果一致。
"""

import asyncio
import logging

from gateway import Reason
from gateway.config import Settings
from gateway.service import AggregationService
from gateway.upstream import FakeUpstream

log = logging.getLogger("test.circuit")


def make_service(**overrides) -> tuple[AggregationService, FakeUpstream]:
    defaults = {
        "cache_ttl": 0.2,
        "stale_ttl": 1.0,
        "upstream_timeout": 0.15,
        "max_concurrent": 2,
        "max_queue": 4,
        "queue_timeout": 1.0,
        "failure_threshold": 3,
        "failure_window": 10.0,
        "circuit_cooldown": 0.3,
    }
    defaults.update(overrides)
    upstream = FakeUpstream(latency=0.02)
    return AggregationService(Settings(**defaults), upstream), upstream


def judge(case: str, inputs: str, basis: str) -> None:
    log.info("用例=%s | 输入=%s | 判定依据=%s", case, inputs, basis)


async def test_consecutive_failures_open_circuit():
    svc, up = make_service()
    up.fail_keys.add("bad")
    judge("连续失败触发短路",
          "fail_keys={'bad'}，阈值=3，连续 4 次 query('bad')",
          "前 3 次 UPSTREAM_ERROR 且各穿透一次；第 4 次 CIRCUIT_OPEN、"
          "call_count 不再增加、不写缓存；detail 含 circuit open 与 retry_after")
    r1 = await svc.query("bad")
    r2 = await svc.query("bad")
    r3 = await svc.query("bad")
    calls_before = up.call_count
    r4 = await svc.query("bad")
    log.info("实际: reasons=%s call_count=%d->%d breaker=%s",
             [r.reason.value for r in (r1, r2, r3, r4)],
             calls_before, up.call_count,
             svc.breaker.state_of("bad").value)
    assert [r.reason for r in (r1, r2, r3)] == [Reason.UPSTREAM_ERROR] * 3
    assert calls_before == 3
    assert not r4.ok and r4.reason == Reason.CIRCUIT_OPEN
    assert up.call_count == 3  # 短路不再穿透
    assert "circuit open" in r4.detail and "retry_after" in r4.detail
    assert svc.cache.get_fresh("bad") is None  # 短路不写缓存
    assert svc.breaker.state_of("bad").value == "open"
    await svc.aclose()


async def test_other_keys_unaffected_by_open_circuit():
    svc, up = make_service()
    up.fail_keys.add("bad")
    judge("短路按数据源隔离",
          "'bad' 连续失败 3 次进入短路后查询 'good'",
          "'bad' 为 CIRCUIT_OPEN 且不穿透；'good' 正常 FRESH，互不影响")
    for _ in range(4):
        await svc.query("bad")
    r = await svc.query("good")
    log.info("实际: bad_calls=%d good=%s breaker(good)=%s",
             up.call_count, r.reason.value,
             svc.breaker.state_of("good").value)
    assert r.ok and r.reason == Reason.FRESH
    assert svc.breaker.state_of("good").value == "closed"
    await svc.aclose()


async def test_stale_data_served_during_open_then_reject_when_expired():
    # 该用例冷却时间取长一些，保证陈旧数据彻底过期时短路仍在生效
    svc, up = make_service(stale_ttl=0.8, circuit_cooldown=2.0)
    judge("短路期间旧数据降级，过期后快速拒绝",
          "先成功 query('a') 写缓存；过期后令 a 失败，连打至短路；"
          "冷却中继续查（陈旧窗口内 / 陈旧窗口外）",
          "窗口内：STALE_FALLBACK 且 detail 含 circuit_open；穿透只发生 3 次失败；"
          "陈旧数据彻底过期后：CIRCUIT_OPEN 快速拒绝且仍不穿透")
    r0 = await svc.query("a")
    await asyncio.sleep(0.25)  # 新鲜期外、陈旧窗口内
    up.fail_keys.add("a")
    deg = [await svc.query("a") for _ in range(5)]  # 3 次失败打穿 + 2 次短路
    calls = up.call_count
    log.info("实际: reasons=%s call_count=%d", [r.reason.value for r in deg], calls)
    assert deg[0].reason == deg[1].reason == deg[2].reason == Reason.STALE_FALLBACK
    for r in deg[3:]:
        assert r.ok and r.stale and r.reason == Reason.STALE_FALLBACK
        assert "circuit_open" in r.detail
    assert calls == 4  # 1 次成功 + 3 次失败，短路的两次不穿透
    await asyncio.sleep(0.7)  # 超过 stale_ttl，旧数据彻底失效；冷却仍有 2s
    r = await svc.query("a")
    log.info("实际: 陈旧数据过期后 reason=%s call_count=%d detail=%s",
             r.reason.value, up.call_count, r.detail)
    assert not r.ok and r.reason == Reason.CIRCUIT_OPEN
    assert up.call_count == calls
    assert r0.payload is not None
    up.fail_keys.discard("a")
    await svc.aclose()


async def test_recovery_probe_success_restores_normal():
    svc, up = make_service()
    up.fail_keys.add("bad")
    judge("冷却后恢复试探成功",
          "3 次失败进入短路，冷却 0.3s 后移除失败标记，再查 2 次",
          "冷却后首个请求 HALF_OPEN 试探，FRESH 成功并关闭熔断；紧接的请求"
          "CACHE_HIT 直接命中，两次都不额外穿透（call_count 只 +1）")
    for _ in range(3):
        await svc.query("bad")
    assert up.call_count == 3
    await asyncio.sleep(0.35)  # 超过冷却
    up.fail_keys.discard("bad")
    r1 = await svc.query("bad")  # 唯一一次恢复试探
    r2 = await svc.query("bad")  # 试探成功后立即恢复正常缓存命中
    log.info("实际: probe=%s next=%s call_count=%d breaker=%s",
             r1.reason.value, r2.reason.value, up.call_count,
             svc.breaker.state_of("bad").value)
    assert r1.ok and r1.reason == Reason.FRESH
    assert r2.ok and r2.reason == Reason.CACHE_HIT
    assert up.call_count == 4
    assert svc.breaker.state_of("bad").value == "closed"
    await svc.aclose()


async def test_recovery_probe_failure_reopens_and_rearms():
    svc, up = make_service()
    up.fail_keys.add("bad")
    judge("冷却后恢复试探失败",
          "3 次失败进入短路，冷却后保持失败：试探 1 次，立即再查，再冷却后恢复",
          "试探失败 -> UPSTREAM_ERROR 且重新 OPEN 重新计时；并发窗口外的立即请求"
          "仍 CIRCUIT_OPEN；第二个冷却到期后试探成功 -> FRESH")
    for _ in range(3):
        await svc.query("bad")
    assert up.call_count == 3
    await asyncio.sleep(0.35)
    probe = await svc.query("bad")     # HALF_OPEN 试探，失败
    immediate = await svc.query("bad")  # 应重新短路
    log.info("实际: probe=%s immediate=%s call_count=%d breaker=%s",
             probe.reason.value, immediate.reason.value, up.call_count,
             svc.breaker.state_of("bad").value)
    assert not probe.ok and probe.reason == Reason.UPSTREAM_ERROR
    assert not immediate.ok and immediate.reason == Reason.CIRCUIT_OPEN
    assert "retry_after" in immediate.detail
    assert up.call_count == 4
    assert svc.breaker.state_of("bad").value == "open"
    await asyncio.sleep(0.35)  # 重新计时的冷却到期
    up.fail_keys.discard("bad")
    r = await svc.query("bad")
    log.info("实际: 再次冷却后试探=%s call_count=%d", r.reason.value, up.call_count)
    assert r.ok and r.reason == Reason.FRESH
    assert up.call_count == 5
    await svc.aclose()


async def test_concurrent_requests_share_single_recovery_probe():
    svc, up = make_service(max_concurrent=2, max_queue=8)
    gate = asyncio.Event()
    up.set_gate(gate)
    up.fail_keys.add("bad")
    judge("恢复瞬间并发只穿透一次且不突破并发上限",
          "3 次失败进入短路；冷却后闸门关闭，同时发起 6 个 'bad' 与 3 个其他键，"
          "然后放行闸门",
          "'bad' 只产生 1 次恢复试探（1 FRESH + 5 COALESCED），其他键各自穿透；"
          "上游观测 max_inflight 不超过 max_concurrent=2；试探成功后后续查询 CACHE_HIT")
    for _ in range(3):
        await svc.query("bad")
    await asyncio.sleep(0.35)
    up.fail_keys.discard("bad")
    tasks = [asyncio.create_task(svc.query("bad")) for _ in range(6)]
    others = [asyncio.create_task(svc.query(f"x{i}")) for i in range(3)]
    await asyncio.sleep(0.1)  # 等所有请求进入合并/排队
    gate.set()
    bad_results = await asyncio.gather(*tasks)
    await asyncio.gather(*others)
    log.info("实际: bad_reasons=%s max_inflight=%d call_count=%d",
             [r.reason.value for r in bad_results], up.max_inflight, up.call_count)
    assert sum(r.reason == Reason.FRESH for r in bad_results) == 1
    assert sum(r.reason == Reason.COALESCED for r in bad_results) == 5
    assert up.max_inflight <= 2
    assert up.call_count == 3 + 1 + 3  # 3 旧失败 + 1 试探 + 3 个其他键
    follow = await svc.query("bad")
    log.info("实际: 试探成功后后续查询=%s call_count=%d",
             follow.reason.value, up.call_count)
    assert follow.reason == Reason.CACHE_HIT
    assert up.call_count == 7
    await svc.aclose()


async def test_shared_success_survives_cancelling_waiter():
    svc, up = make_service()
    gate = asyncio.Event()
    up.set_gate(gate)
    judge("共享成功不因子请求取消而丢失",
          "2 个请求共享同键 flight（发起者 + 1 等待者），上游返回前取消发起者，"
          "随后放行闸门；幸存者拿结果后再做第 3 次查询",
          "被取消的发起者不影响共享调用；幸存者成功；成功结果已由 leader 任务写入"
          "缓存，第 3 次查询在新鲜期内 CACHE_HIT，上游总穿透仅 1 次")
    t1 = asyncio.create_task(svc.query("a"))  # 发起者（leader 所在 flight）
    t2 = asyncio.create_task(svc.query("a"))  # 合并等待者
    await asyncio.sleep(0.05)
    t1.cancel()  # 发起者提前取消
    gate.set()   # 共享调用最终成功
    cancelled = await asyncio.gather(t1, return_exceptions=True)
    r2 = await t2
    r3 = await svc.query("a")
    log.info("实际: t1=%s r2=%s r3=%s call_count=%d",
             "CancelledError" if isinstance(cancelled[0], asyncio.CancelledError)
             else getattr(cancelled[0], "reason", cancelled[0]),
             r2.reason.value, r3.reason.value, up.call_count)
    assert isinstance(cancelled[0], asyncio.CancelledError)
    assert r2.ok
    assert r3.ok and r3.reason == Reason.CACHE_HIT
    assert up.call_count == 1
    await svc.aclose()
