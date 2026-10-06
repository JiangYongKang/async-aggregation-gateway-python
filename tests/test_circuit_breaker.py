"""按数据源失败短路（circuit breaker）行为测试：每个用例打印输入与判定依据。"""

import asyncio
import logging

from test_gateway import judge, make_service

from gateway import Reason

log = logging.getLogger("test.circuit")


async def test_circuit_opens_after_consecutive_failures():
    svc, up = make_service(failure_threshold=3, cooldown=0.3)
    up.fail_keys.add("bad")
    judge("连续失败进入短路", "failure_threshold=3，fail_keys={'bad'}，连续 query('bad')",
          "前 3 次均 UPSTREAM_ERROR 且各穿透 1 次；第 4 次 CIRCUIT_OPEN 快速拒绝，"
          "不再穿透，detail 说明短路原因与恢复倒计时，状态为 open")
    results = [await svc.query("bad") for _ in range(3)]
    r4 = await svc.query("bad")
    log.info("实际: 前3次=%s 第4次=%s detail=%s call_count=%d state=%s",
             [r.reason.value for r in results], r4.reason.value, r4.detail,
             up.call_count, svc.breaker.state_of("bad"))
    assert [r.reason for r in results] == [Reason.UPSTREAM_ERROR] * 3
    assert r4.reason == Reason.CIRCUIT_OPEN and not r4.ok
    assert up.call_count == 3  # 第 4 次未穿透
    assert "circuit open" in r4.detail and "retry in" in r4.detail
    assert svc.breaker.state_of("bad") == "open"
    await svc.aclose()


async def test_circuit_open_rejection_not_counted_and_cooldown_not_extended():
    svc, up = make_service(failure_threshold=2, cooldown=0.25)
    up.fail_keys.add("bad")
    judge("短路拒绝不计失败不延后冷却",
          "failure_threshold=2 cooldown=0.25s；开路后在冷却期内再查 2 次，"
          "随后在原始冷却到期点之后查询（上游已恢复）",
          "冷却期内的 2 次均为 CIRCUIT_OPEN 且不穿透；若拒绝被计入失败会重新计时，"
          "则 T+0.3s 时仍在冷却中——正确行为是 T+0.3s 放行试探并 FRESH")
    await svc.query("bad")
    await svc.query("bad")  # 达到阈值，T 时刻开路
    await asyncio.sleep(0.1)
    r1 = await svc.query("bad")   # T+0.1，冷却期内
    await asyncio.sleep(0.1)
    r2 = await svc.query("bad")   # T+0.2，冷却期内
    await asyncio.sleep(0.1)      # T+0.3 > T+0.25，原始冷却已到期
    up.fail_keys.discard("bad")
    r3 = await svc.query("bad")
    log.info("实际: r1=%s r2=%s r3=%s call_count=%d",
             r1.reason.value, r2.reason.value, r3.reason.value, up.call_count)
    assert r1.reason == r2.reason == Reason.CIRCUIT_OPEN
    assert r3.reason == Reason.FRESH  # 冷却未被拒绝延后，试探成功
    assert up.call_count == 3  # 2 次失败 + 1 次试探，拒绝未穿透
    await svc.aclose()


async def test_half_open_probe_success_recovers():
    svc, up = make_service(failure_threshold=2, cooldown=0.2)
    up.fail_keys.add("bad")
    judge("恢复试探成功", "failure_threshold=2 cooldown=0.2s；开路后移除失败标记，"
          "sleep 超过冷却时间再连续查 2 次",
          "冷却后第 1 次为恢复试探：穿透 1 次且 FRESH，电路闭合；"
          "第 2 次立即 CACHE_HIT，恢复正常缓存命中")
    await svc.query("bad")
    await svc.query("bad")
    assert svc.breaker.state_of("bad") == "open"
    up.fail_keys.discard("bad")
    await asyncio.sleep(0.25)
    r1 = await svc.query("bad")
    r2 = await svc.query("bad")
    log.info("实际: 试探=%s 随后=%s call_count=%d state=%s",
             r1.reason.value, r2.reason.value, up.call_count,
             svc.breaker.state_of("bad"))
    assert r1.reason == Reason.FRESH and r1.ok
    assert r2.reason == Reason.CACHE_HIT
    assert up.call_count == 3
    assert svc.breaker.state_of("bad") == "closed"
    await svc.aclose()


async def test_half_open_probe_failure_reopens():
    svc, up = make_service(failure_threshold=2, cooldown=0.2)
    up.fail_keys.add("bad")
    judge("恢复试探失败", "failure_threshold=2 cooldown=0.2s；开路后上游仍失败，"
          "sleep 超过冷却时间后查询，随后立即再查",
          "冷却后第 1 次为试探：穿透 1 次且 UPSTREAM_ERROR，电路重新开路并重新计时；"
          "紧接着的查询再次 CIRCUIT_OPEN 且不穿透")
    await svc.query("bad")
    await svc.query("bad")
    await asyncio.sleep(0.25)
    r1 = await svc.query("bad")   # 试探，仍失败
    r2 = await svc.query("bad")   # 重新开路后的冷却期内
    log.info("实际: 试探=%s 随后=%s detail=%s call_count=%d state=%s",
             r1.reason.value, r2.reason.value, r2.detail, up.call_count,
             svc.breaker.state_of("bad"))
    assert r1.reason == Reason.UPSTREAM_ERROR
    assert r2.reason == Reason.CIRCUIT_OPEN
    assert up.call_count == 3  # 仅试探穿透 1 次
    assert svc.breaker.state_of("bad") == "open"
    await svc.aclose()


async def test_recovery_probe_coalesces_concurrent_requests():
    svc, up = make_service(failure_threshold=2, cooldown=0.2, max_concurrent=2)
    up.fail_keys.add("bad")
    judge("恢复瞬间并发只穿透一次",
          "failure_threshold=2 cooldown=0.2s；开路后移除失败标记并架起闸门，"
          "冷却到期后 6 个并发 query('bad')，随后放行闸门",
          "6 个请求共享同一次试探：上游仅穿透 1 次（1 个 FRESH + 5 个 COALESCED），"
          "上游并发峰值不超过 max_concurrent=2")
    await svc.query("bad")
    await svc.query("bad")
    up.fail_keys.discard("bad")
    gate = asyncio.Event()
    up.set_gate(gate)
    await asyncio.sleep(0.25)  # 冷却到期
    tasks = [asyncio.create_task(svc.query("bad")) for _ in range(6)]
    await asyncio.sleep(0.05)  # 全部到达：1 个试探在途，5 个已合并
    gate.set()
    results = await asyncio.gather(*tasks)
    log.info("实际: reasons=%s call_count=%d max_inflight=%d",
             [r.reason.value for r in results], up.call_count, up.max_inflight)
    assert up.call_count == 3  # 2 次失败 + 1 次试探
    assert sum(r.reason == Reason.FRESH for r in results) == 1
    assert sum(r.reason == Reason.COALESCED for r in results) == 5
    assert all(r.ok for r in results)
    assert up.max_inflight <= 2
    await svc.aclose()


async def test_circuit_open_serves_stale_fallback():
    svc, up = make_service(failure_threshold=2, cooldown=0.3)
    judge("短路期间旧数据降级", "先成功写入缓存，sleep 使缓存过新鲜期但仍在陈旧窗口内，"
          "随后上游连续失败达到阈值开路，再查询",
          "开路后的查询不穿透上游，按降级规则返回 STALE_FALLBACK，"
          "detail 中可见原始原因 circuit_open")
    r0 = await svc.query("a")
    assert r0.reason == Reason.FRESH
    await asyncio.sleep(0.25)  # 过新鲜期（ttl=0.2），仍在陈旧窗口（stale_ttl=1.0）
    up.fail_keys.add("a")
    r1 = await svc.query("a")   # 失败 1，降级
    r2 = await svc.query("a")   # 失败 2，达到阈值开路，降级
    calls_before = up.call_count
    r3 = await svc.query("a")   # 短路中：不穿透，旧数据降级
    log.info("实际: r1=%s r2=%s r3=%s detail=%s call_count=%d->%d",
             r1.reason.value, r2.reason.value, r3.reason.value, r3.detail,
             calls_before, up.call_count)
    assert r1.reason == r2.reason == Reason.STALE_FALLBACK
    assert r3.reason == Reason.STALE_FALLBACK and r3.ok and r3.stale
    assert "circuit_open" in r3.detail
    assert r3.payload == r0.payload
    assert up.call_count == calls_before  # 短路查询未穿透
    await svc.aclose()


async def test_circuit_isolated_per_key():
    svc, up = make_service(failure_threshold=2, cooldown=0.3)
    up.fail_keys.add("bad")
    judge("短路按起点隔离", "fail_keys={'bad'}，'bad' 连续失败开路后查询 'good'",
          "'bad' 被 CIRCUIT_OPEN 短路，'good' 不受影响正常 FRESH")
    await svc.query("bad")
    await svc.query("bad")
    r_bad = await svc.query("bad")
    r_good = await svc.query("good")
    log.info("实际: bad=%s good=%s state(bad)=%s state(good)=%s",
             r_bad.reason.value, r_good.reason.value,
             svc.breaker.state_of("bad"), svc.breaker.state_of("good"))
    assert r_bad.reason == Reason.CIRCUIT_OPEN
    assert r_good.reason == Reason.FRESH and r_good.ok
    assert svc.breaker.state_of("good") == "closed"
    await svc.aclose()


async def test_shared_success_cached_despite_creator_cancel():
    svc, up = make_service()
    gate = asyncio.Event()
    up.set_gate(gate)
    judge("共享成功结果不因取消丢失",
          "闸门阻塞下 2 个请求共享同键 flight，取消发起者后放行闸门，"
          "幸存者在新鲜期内再查一次",
          "发起者取消不误杀共享调用：幸存者拿到 COALESCED 结果；"
          "成功结果由 flight 写入缓存，后续查询 CACHE_HIT 且不重复穿透")
    t1 = asyncio.create_task(svc.query("a"))
    t2 = asyncio.create_task(svc.query("a"))
    await asyncio.sleep(0.05)
    t1.cancel()
    gate.set()
    r2 = await t2
    await asyncio.gather(t1, return_exceptions=True)
    r3 = await svc.query("a")
    log.info("实际: t1.cancelled=%s t2=%s t3=%s call_count=%d",
             t1.cancelled(), r2.reason.value, r3.reason.value, up.call_count)
    assert t1.cancelled()
    assert r2.ok and r2.reason == Reason.COALESCED
    assert r3.reason == Reason.CACHE_HIT
    assert up.call_count == 1  # 成功结果入缓存，未重复穿透
    await svc.aclose()
