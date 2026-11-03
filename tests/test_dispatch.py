"""Tests for the shared dispatch queue (DispatchQueue / DispatchWorker)."""

import asyncio
import contextlib
import json
import time

import pytest
import redis.asyncio as aioredis

from redis_semaphore import (
    DispatchConfig,
    DispatchQueue,
    DispatchWorker,
    Job,
    JobError,
    JobFailedError,
    JobTimeoutError,
    QueueFullError,
)

pytestmark = pytest.mark.timeout(30)

# Keep the production *proportions* (reclaim_idle > leader_ttl, defaults
# 30 > 10): inverting them would re-elect slower than reclaim and hide the
# post-crash reordering window that production configs have.
FAST = {
    "leader_ttl": 0.5,
    "election_interval": 0.05,
    "reclaim_idle": 0.8,
    "reclaim_interval": 0.1,
    "read_timeout": 0.1,
}


def _config(**kwargs) -> DispatchConfig:
    return DispatchConfig(namespace="t-dispatch", **{**FAST, **kwargs})


@contextlib.asynccontextmanager
async def running(*workers: DispatchWorker):
    tasks = [asyncio.create_task(worker.run()) for worker in workers]
    try:
        yield tasks
    finally:
        for worker in workers:
            worker.stop()
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 10)


async def until(predicate, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("condition not reached")
        await asyncio.sleep(0.02)


async def until_length(queue: DispatchQueue, name: str, length: int) -> None:
    for _ in range(250):
        if await queue.alength(name) >= length:
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"queue {name!r} never reached {length} jobs")


async def echo(job: Job):
    return {"payload": job.payload, "queue": job.queue}


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "kwargs",
    [
        {"namespace": ""},
        {"group": ""},
        {"max_queue_size": 0},
        {"max_queues_per_worker": 0},
        {"max_admitted": 0},
        {"leader_ttl": 0},
        {"election_interval": 0},
        {"reclaim_idle": 0},
        {"reclaim_interval": -1},
        {"reply_ttl": 0},
        {"read_timeout": 0},
    ],
)
def test_config_validation(kwargs):
    with pytest.raises(ValueError):
        DispatchConfig(**kwargs)


def test_worker_validation(redis_client, async_redis_client):
    with pytest.raises(TypeError, match="async"):
        DispatchWorker(redis_client, ["q"], echo)
    with pytest.raises(ValueError, match="empty"):
        DispatchWorker(async_redis_client, [], echo)
    with pytest.raises(ValueError, match="queue name"):
        DispatchWorker(async_redis_client, [""], echo)


async def test_submit_validation(async_redis_client):
    queue = DispatchQueue(async_redis_client, _config())
    with pytest.raises(ValueError, match="queue name"):
        await queue.asubmit("", {})
    with pytest.raises(ValueError, match="timeout"):
        await queue.asubmit("q", {}, timeout=0)


def test_key_names(redis_client):
    assert DispatchQueue(redis_client, _config()).stream_key("q") == "t-dispatch:q:stream"
    tagged = DispatchQueue(redis_client, _config(cluster_hash_tags=True))
    assert tagged.stream_key("q") == "{t-dispatch:q}:stream"


# ---------------------------------------------------------------------------
# Basic flow
# ---------------------------------------------------------------------------


async def test_submit_roundtrip(async_redis_client):
    config = _config()
    worker = DispatchWorker(async_redis_client, ["q1", "q2"], echo, config)
    queue = DispatchQueue(async_redis_client, config)
    async with running(worker):
        assert await queue.asubmit("q1", {"x": 1}, timeout=5) == {
            "payload": {"x": 1},
            "queue": "q1",
        }
        assert (await queue.asubmit("q2", [1, 2], timeout=5))["queue"] == "q2"
    # Finished jobs leave the stream.
    assert await queue.alength("q1") == 0


async def test_sync_submit(redis_client, async_redis_client):
    config = _config()
    worker = DispatchWorker(async_redis_client, ["sync"], echo, config)
    queue = DispatchQueue(redis_client, config)
    async with running(worker):
        result = await asyncio.to_thread(queue.submit, "sync", "hello", timeout=5)
        assert result["payload"] == "hello"
        assert queue.length("sync") == 0


async def test_handler_errors(async_redis_client):
    async def handler(job: Job):
        if job.payload == "plain":
            raise ValueError("boom")
        raise JobError("no quota", data={"retry_after": 1.5})

    config = _config()
    queue = DispatchQueue(async_redis_client, config)
    async with running(DispatchWorker(async_redis_client, ["err"], handler, config)):
        with pytest.raises(JobFailedError) as plain:
            await queue.asubmit("err", "plain", timeout=5)
        assert plain.value.error_type == "ValueError"
        assert "boom" in str(plain.value)
        assert plain.value.data == {}

        with pytest.raises(JobFailedError) as structured:
            await queue.asubmit("err", "structured", timeout=5)
        assert structured.value.error_type == "JobError"
        assert structured.value.data == {"retry_after": 1.5}


async def test_non_json_result_fails_job(async_redis_client):
    async def handler(job: Job):
        return object()

    config = _config()
    queue = DispatchQueue(async_redis_client, config)
    async with running(DispatchWorker(async_redis_client, ["bad"], handler, config)):
        with pytest.raises(JobFailedError, match="TypeError"):
            await queue.asubmit("bad", None, timeout=5)


async def test_submit_timeout_without_worker(async_redis_client):
    queue = DispatchQueue(async_redis_client, _config())
    start = time.monotonic()
    with pytest.raises(JobTimeoutError):
        await queue.asubmit("nobody", {}, timeout=0.3)
    assert time.monotonic() - start < 1.0


async def test_queue_full(async_redis_client):
    queue = DispatchQueue(async_redis_client, _config(max_queue_size=1))
    first = asyncio.create_task(queue.asubmit("full", 1, timeout=5))
    await until_length(queue, "full", 1)
    with pytest.raises(QueueFullError):
        await queue.asubmit("full", 2, timeout=5)
    first.cancel()
    await asyncio.gather(first, return_exceptions=True)


async def test_expired_job_is_dropped(async_redis_client):
    calls = []

    async def handler(job: Job):
        calls.append(job.payload)
        return None

    config = _config()
    queue = DispatchQueue(async_redis_client, config)
    with pytest.raises(JobTimeoutError):
        await queue.asubmit("late", "expired", timeout=0.2)
    async with running(DispatchWorker(async_redis_client, ["late"], handler, config)):
        assert await queue.asubmit("late", "fresh", timeout=5) is None
    assert calls == ["fresh"]
    assert await queue.alength("late") == 0


async def test_malformed_entry_is_dropped(async_redis_client):
    config = _config()
    queue = DispatchQueue(async_redis_client, config)
    await async_redis_client.xadd(queue.stream_key("junk"), {"garbage": "1"})
    async with running(DispatchWorker(async_redis_client, ["junk"], echo, config)):
        assert (await queue.asubmit("junk", "ok", timeout=5))["payload"] == "ok"
    assert await queue.alength("junk") == 0


# ---------------------------------------------------------------------------
# Ordering and admission
# ---------------------------------------------------------------------------


async def test_fifo_across_workers(async_redis_client):
    """Jobs are admitted in stream order even with several competing workers."""
    config = _config()
    queue = DispatchQueue(async_redis_client, config)
    submits = [asyncio.create_task(queue.asubmit("fifo", i, timeout=10)) for i in range(15)]
    await until_length(queue, "fifo", 15)
    stream_order = [
        json.loads(fields[b"payload"])
        for _, fields in await async_redis_client.xrange(queue.stream_key("fifo"))
    ]

    admitted = []

    async def handler(job: Job):
        admitted.append(job.payload)
        await asyncio.sleep(0.01)
        job.admit()
        await asyncio.sleep(0.05)
        return job.payload

    workers = [DispatchWorker(async_redis_client, ["fifo"], handler, config) for _ in range(3)]
    async with running(*workers):
        assert sorted(await asyncio.gather(*submits)) == list(range(15))
    assert admitted == stream_order


async def test_admission_serialized_work_concurrent(async_redis_client):
    state = {"before_admit": 0, "max_before_admit": 0}

    async def handler(job: Job):
        state["before_admit"] += 1
        state["max_before_admit"] = max(state["max_before_admit"], state["before_admit"])
        await asyncio.sleep(0.05)
        state["before_admit"] -= 1
        job.admit()
        await asyncio.sleep(0.5)
        return job.payload

    config = _config()
    queue = DispatchQueue(async_redis_client, config)
    workers = [DispatchWorker(async_redis_client, ["conc"], handler, config) for _ in range(2)]
    async with running(*workers):
        await until(lambda: any(w.leading for w in workers))
        start = time.monotonic()
        await asyncio.gather(*(queue.asubmit("conc", i, timeout=10) for i in range(6)))
        elapsed = time.monotonic() - start
    assert state["max_before_admit"] == 1
    # Sequential would take 6 * 0.55 = 3.3 s.
    assert elapsed < 2.0


async def test_without_admit_jobs_run_one_by_one(async_redis_client):
    state = {"active": 0, "max_active": 0}

    async def handler(job: Job):
        state["active"] += 1
        state["max_active"] = max(state["max_active"], state["active"])
        await asyncio.sleep(0.05)
        state["active"] -= 1

    config = _config()
    queue = DispatchQueue(async_redis_client, config)
    async with running(DispatchWorker(async_redis_client, ["seq"], handler, config)):
        await asyncio.gather(*(queue.asubmit("seq", i, timeout=10) for i in range(5)))
    assert state["max_active"] == 1


async def test_election_interval_defaults_to_a_third_of_leader_ttl(async_redis_client):
    """Probing busy queues much faster than leader_ttl only burns Redis."""
    worker = DispatchWorker(async_redis_client, ["q"], echo, DispatchConfig(leader_ttl=9.0))
    assert worker._election_interval == pytest.approx(3.0)


async def test_max_admitted_caps_concurrency(async_redis_client):
    """With max_admitted, the leader starts the next job only when a handler
    slot frees up; without it, admitted jobs run unbounded."""
    state = {"active": 0, "max": 0}

    async def handler(job: Job):
        job.admit()
        state["active"] += 1
        state["max"] = max(state["max"], state["active"])
        await asyncio.sleep(0.15)
        state["active"] -= 1
        return job.payload

    config = _config(max_admitted=2)
    queue = DispatchQueue(async_redis_client, config)
    async with running(DispatchWorker(async_redis_client, ["capped"], handler, config)):
        results = await asyncio.gather(*(queue.asubmit("capped", i, timeout=10) for i in range(6)))
    assert sorted(results) == list(range(6))
    assert state["max"] == 2


async def test_dispatch_job_metrics(async_redis_client):
    from redis_semaphore.metrics import get_metrics, set_metrics

    class Collector:
        def __init__(self):
            self.jobs = []

        @property
        def enabled(self):
            return True

        def inc_dispatch_job(self, queue, namespace, result):
            self.jobs.append((queue, namespace, result))

        def __getattr__(self, name):  # the semaphore hooks used by elections
            return lambda *args, **kwargs: None

    async def handler(job: Job):
        if job.payload == "boom":
            raise ValueError("boom")
        return "ok"

    collector = Collector()
    previous = get_metrics()
    set_metrics(collector)
    try:
        config = _config()
        queue = DispatchQueue(async_redis_client, config)
        async with running(DispatchWorker(async_redis_client, ["measured"], handler, config)):
            assert await queue.asubmit("measured", "fine", timeout=5) == "ok"
            with pytest.raises(JobFailedError):
                await queue.asubmit("measured", "boom", timeout=5)
    finally:
        set_metrics(previous)
    assert ("measured", "t-dispatch", "completed") in collector.jobs
    assert ("measured", "t-dispatch", "failed") in collector.jobs


def test_record_job_tolerates_old_collectors():
    """Custom collectors written before inc_dispatch_job existed must not break."""
    from redis_semaphore.dispatch import _record_job
    from redis_semaphore.metrics import get_metrics, set_metrics

    class Old:
        @property
        def enabled(self):
            return True

    previous = get_metrics()
    set_metrics(Old())
    try:
        _record_job("q", "ns", "completed")  # must not raise
    finally:
        set_metrics(previous)


async def test_queues_spread_over_workers(async_redis_client):
    config = _config(max_queues_per_worker=2)
    queues = ["a", "b", "c", "d"]
    workers = [DispatchWorker(async_redis_client, queues, echo, config) for _ in range(2)]
    async with running(*workers):
        await until(lambda: sum(len(w.leading) for w in workers) == 4)
        assert [len(w.leading) for w in workers] == [2, 2]
        assert sorted(q for w in workers for q in w.leading) == queues


# ---------------------------------------------------------------------------
# Failure handling
# ---------------------------------------------------------------------------


async def test_reclaims_job_of_dead_worker(async_redis_client):
    """A job taken by a worker that died is re-run by the queue's leader."""
    seen = []

    async def handler(job: Job):
        seen.append(job.reclaimed)
        return "done"

    config = _config()
    queue = DispatchQueue(async_redis_client, config)
    stream = queue.stream_key("orphan")
    await async_redis_client.xgroup_create(stream, config.group, id="0", mkstream=True)
    sec, usec = await async_redis_client.time()
    now_ms = sec * 1000 + usec // 1000
    fields = {
        "id": "job-1",
        "payload": json.dumps("payload"),
        "created": str(now_ms),
        "deadline": str(now_ms + 10_000),
    }
    pipe = async_redis_client.pipeline(transaction=True)
    pipe.xadd(stream, fields)
    pipe.xreadgroup(config.group, "ghost", {stream: ">"}, count=1)  # taken, never answered
    await pipe.execute()

    async def consumers() -> set[str]:
        return {
            c["name"].decode() if isinstance(c["name"], bytes) else c["name"]
            for c in await async_redis_client.xinfo_consumers(stream, config.group)
        }

    async with running(DispatchWorker(async_redis_client, ["orphan"], handler, config)):
        reply = await async_redis_client.blpop(["t-dispatch:orphan:reply:job-1"], timeout=5)
        assert reply is not None
        assert json.loads(reply[1]) == {"ok": True, "result": "done"}
        assert seen == [True]
        assert await queue.alength("orphan") == 0
        # The dead consumer is pruned once it holds nothing and stays idle.
        deadline = time.monotonic() + 5
        while "ghost" in await consumers():
            assert time.monotonic() < deadline, "dead consumer was not pruned"
            await asyncio.sleep(0.1)


async def test_long_running_job_is_not_reclaimed(async_redis_client):
    """The owner touches its jobs, so slow jobs (> reclaim_idle) run only once."""
    calls = []

    async def handler(job: Job):
        calls.append(job.reclaimed)
        job.admit()
        await asyncio.sleep(2.0)
        return "slow"

    config = _config()
    queue = DispatchQueue(async_redis_client, config)
    workers = [DispatchWorker(async_redis_client, ["slow"], handler, config) for _ in range(2)]
    async with running(*workers):
        assert await queue.asubmit("slow", None, timeout=10) == "slow"
    assert calls == [False]


async def test_live_job_is_not_reclaimed_by_its_own_worker(async_redis_client, monkeypatch):
    """When the touch loop falls behind, XAUTOCLAIM hands the worker its own
    still-running job; it must not be started a second time."""
    calls = []

    async def handler(job: Job):
        calls.append(job.reclaimed)
        job.admit()
        await asyncio.sleep(1.5)  # > reclaim_idle, and never touched
        return "slow"

    async def no_touch(self):
        await asyncio.Event().wait()

    monkeypatch.setattr(DispatchWorker, "_touch_loop", no_touch)
    config = _config()
    queue = DispatchQueue(async_redis_client, config)
    async with running(DispatchWorker(async_redis_client, ["self-reclaim"], handler, config)):
        assert await queue.asubmit("self-reclaim", None, timeout=10) == "slow"
    assert calls == [False]


async def test_second_cancel_still_cleans_up(async_redis_client):
    """asyncio.wait_for(run, grace)-style double cancellation must not abort
    the cleanup halfway (leaked handlers, leadership held until its TTL)."""
    started = asyncio.Event()

    async def handler(job: Job):
        job.admit()
        started.set()
        await asyncio.sleep(30)  # runs until the hard shutdown cancels it
        return "never"

    config = _config()
    queue = DispatchQueue(async_redis_client, config)
    worker = DispatchWorker(async_redis_client, ["grace"], handler, config)
    run = asyncio.create_task(worker.run())
    submit = asyncio.create_task(queue.asubmit("grace", None, timeout=30))
    await asyncio.wait_for(started.wait(), 5)
    run.cancel()  # first cancel: graceful, would wait for the admitted handler
    with pytest.raises((asyncio.TimeoutError, asyncio.CancelledError)):
        await asyncio.wait_for(run, timeout=0.3)  # grace elapsed: second cancel
    assert worker.health().running is False
    assert worker.leading == []
    # Cleanup ran to completion: the worker is reusable.
    run2 = asyncio.create_task(worker.run())
    await until(lambda: worker.leading == ["grace"])
    worker.stop()
    await asyncio.wait_for(run2, 5)
    submit.cancel()
    await asyncio.gather(submit, return_exceptions=True)


async def test_lost_leadership_hands_back_unadmitted_job(async_redis_client):
    """A job waiting for admission on a leader that lost its Mutex is re-run."""
    calls = []

    async def handler(job: Job):
        calls.append(job.reclaimed)
        if len(calls) == 1:
            await asyncio.Event().wait()  # never admitted; cancelled on loss
        return "second run"

    config = _config()
    queue = DispatchQueue(async_redis_client, config)
    worker = DispatchWorker(async_redis_client, ["lost"], handler, config)
    async with running(worker):
        submit = asyncio.create_task(queue.asubmit("lost", None, timeout=10))
        await until(lambda: len(calls) == 1)
        # Simulate the Mutex expiring server-side (e.g. after a network split).
        await async_redis_client.delete("t-dispatch:lost:leader:owners")
        assert await submit == "second run"
    assert calls == [False, True]


async def test_stop_lets_admitted_jobs_finish(async_redis_client):
    admitted = asyncio.Event()

    async def handler(job: Job):
        job.admit()
        admitted.set()
        await asyncio.sleep(0.5)
        return "finished"

    config = _config()
    queue = DispatchQueue(async_redis_client, config)
    worker = DispatchWorker(async_redis_client, ["stop"], handler, config)
    run = asyncio.create_task(worker.run())
    submit = asyncio.create_task(queue.asubmit("stop", None, timeout=10))
    await asyncio.wait_for(admitted.wait(), 5)
    worker.stop()
    await asyncio.wait_for(run, 5)
    assert await submit == "finished"
    assert worker.leading == []


async def test_stop_before_run_is_honoured(async_redis_client):
    """stop() from a signal handler that fires before run() starts is not lost."""
    worker = DispatchWorker(async_redis_client, ["early"], echo, _config())
    worker.stop()
    await asyncio.wait_for(worker.run(), 5)
    # The worker can be started again afterwards.
    run = asyncio.create_task(worker.run())
    await until(lambda: worker.leading == ["early"])
    worker.stop()
    await asyncio.wait_for(run, 5)


async def test_submit_survives_transient_wait_error(async_redis_client, monkeypatch):
    """A connection blip while waiting for the reply must not lose the result."""
    import redis

    config = _config()
    queue = DispatchQueue(async_redis_client, config)
    real_blmove = async_redis_client.blmove
    failures = {"left": 1}

    async def flaky_blmove(*args, **kwargs):
        if failures["left"]:
            failures["left"] -= 1
            raise redis.ConnectionError("blip")
        return await real_blmove(*args, **kwargs)

    monkeypatch.setattr(async_redis_client, "blmove", flaky_blmove)
    async with running(DispatchWorker(async_redis_client, ["blip"], echo, config)):
        assert (await queue.asubmit("blip", "x", timeout=10))["payload"] == "x"
    assert failures["left"] == 0


async def test_submit_survives_lost_reply(async_redis_client, monkeypatch):
    """A reply lost in flight (connection dropped after the server already
    served the read) is still there for the retry: the read is BLMOVE back
    onto the same list, not a consuming BLPOP."""
    import redis

    config = _config()
    queue = DispatchQueue(async_redis_client, config)
    real_blmove = async_redis_client.blmove
    lost = {"left": 1}

    async def lossy_blmove(*args, **kwargs):
        result = await real_blmove(*args, **kwargs)
        if result is not None and lost["left"]:
            lost["left"] -= 1
            raise redis.ConnectionError("reply lost in flight")
        return result

    monkeypatch.setattr(async_redis_client, "blmove", lossy_blmove)
    async with running(DispatchWorker(async_redis_client, ["lossy"], echo, config)):
        assert (await queue.asubmit("lossy", "x", timeout=10))["payload"] == "x"
    assert lost["left"] == 0


async def test_submit_survives_client_unblock(async_redis_client, monkeypatch):
    """UNBLOCKED (sent to blocked clients during failovers) is transient:
    the submitter retries the wait instead of dying with a permanent error."""
    import redis

    config = _config()
    queue = DispatchQueue(async_redis_client, config)
    real_blmove = async_redis_client.blmove
    kicks = {"left": 1}

    async def kicked_blmove(*args, **kwargs):
        if kicks["left"]:
            kicks["left"] -= 1
            raise redis.ResponseError("UNBLOCKED client unblocked via CLIENT UNBLOCK")
        return await real_blmove(*args, **kwargs)

    monkeypatch.setattr(async_redis_client, "blmove", kicked_blmove)
    async with running(DispatchWorker(async_redis_client, ["kicked"], echo, config)):
        assert (await queue.asubmit("kicked", "x", timeout=10))["payload"] == "x"
    assert kicks["left"] == 0


async def test_retried_enqueue_does_not_duplicate_job(async_redis_client):
    """An enqueue resent after a lost reply reuses the entry it already made."""
    from redis_semaphore.lua_scripts import (
        LuaScriptRegistry,
        LuaScriptRunner,
        ScriptClientAdapter,
    )

    config = _config()
    queue = DispatchQueue(async_redis_client, config)
    runner = LuaScriptRunner(LuaScriptRegistry())
    adapter = ScriptClientAdapter(async_redis_client)
    args = (
        queue.stream_key("dup"),
        queue._keys.submit_marker("dup", "job-1"),
        0,
        60_000,
        "job-1",
        json.dumps({"x": 1}),
        "",
    )
    first = await runner.dispatch_enqueue_async(adapter, *args)
    second = await runner.dispatch_enqueue_async(adapter, *args)  # the resent call
    assert first == second
    assert await queue.alength("dup") == 1


async def test_enqueue_stamps_server_time(async_redis_client):
    """created/deadline come from the Redis server clock, not the producer's
    wall clock, so skewed pod clocks cannot poison deadlines."""
    config = _config()
    queue = DispatchQueue(async_redis_client, config)
    with pytest.raises(JobTimeoutError):
        await queue.asubmit("stamp", None, timeout=0.3)
    ((_, fields),) = await async_redis_client.xrange(queue.stream_key("stamp"))
    sec, usec = await async_redis_client.time()
    now_ms = sec * 1000 + usec // 1000
    created_ms = float(fields[b"created"])
    deadline_ms = float(fields[b"deadline"])
    assert abs(created_ms - now_ms) < 2_000
    assert abs(deadline_ms - created_ms - 300) < 50


def test_job_remaining_uses_measured_clock_offset():
    """remaining compares server-clock deadlines against server-clock 'now'."""
    server_ahead = 3600.0  # the server clock is an hour ahead of this machine
    job = Job(
        queue="q",
        entry_id="1-1",
        id="j",
        payload=None,
        created=time.time() + server_ahead,
        deadline=time.time() + server_ahead + 100,
        clock_offset=server_ahead,
    )
    assert job.remaining is not None
    assert 95 < job.remaining <= 100


async def test_run_twice_is_rejected(async_redis_client):
    worker = DispatchWorker(async_redis_client, ["twice"], echo, _config())
    async with running(worker):
        await asyncio.sleep(0.05)
        with pytest.raises(RuntimeError, match="already running"):
            await worker.run()


async def test_resp3_client_roundtrip(async_resp3_client):
    """RESP3 (protocol=3) XREADGROUP replies are dicts; the worker must cope."""
    config = _config()
    queue = DispatchQueue(async_resp3_client, config)
    async with running(DispatchWorker(async_resp3_client, ["resp3"], echo, config)):
        for i in range(3):
            result = await queue.asubmit("resp3", {"i": i}, timeout=5)
            assert result["payload"] == {"i": i}
    assert await queue.alength("resp3") == 0


async def test_redis_without_xautoclaim_stops_worker(async_redis_client, monkeypatch):
    """On Redis < 6.2 the worker stops with a clear permanent error instead of
    thrashing through endless re-elections."""
    import redis as redis_pkg

    from redis_semaphore.errors import CommandDeniedError

    async def no_xautoclaim(*args, **kwargs):
        raise redis_pkg.ResponseError("ERR unknown command 'XAUTOCLAIM'")

    monkeypatch.setattr(async_redis_client, "xautoclaim", no_xautoclaim)
    worker = DispatchWorker(async_redis_client, ["old-redis"], echo, _config())
    with pytest.raises(CommandDeniedError, match="6.2"):
        await asyncio.wait_for(worker.run(), 10)
    assert worker.health().running is False


async def test_worker_survives_redis_errors(async_redis_client):
    """A failing leader gives up its queue; the worker re-elects later."""
    config = _config()
    queue = DispatchQueue(async_redis_client, config)
    broken = aioredis.Redis(host="localhost", port=1, socket_connect_timeout=0.1)
    worker = DispatchWorker(broken, ["down"], echo, config)
    run = asyncio.create_task(worker.run())
    await asyncio.sleep(0.3)
    assert not run.done()  # elections fail quietly
    worker.stop()
    await asyncio.wait_for(run, 5)
    await broken.aclose()
    assert await queue.alength("down") == 0


# ---------------------------------------------------------------------------
# Health check: queue status and worker health
# ---------------------------------------------------------------------------


def test_leader_owners_key_matches_mutex(redis_client):
    """The status check must look at the exact key the leadership Mutex uses."""
    from redis_semaphore import Mutex
    from redis_semaphore.dispatch import _Keys

    for tags in (False, True):
        config = _config(cluster_hash_tags=tags)
        mutex = Mutex(
            redis_client,
            _Keys.leader_name("q"),
            namespace=config.namespace,
            cluster_hash_tags=tags,
        )
        assert _Keys(config).leader_owners("q") == mutex.owners_key


async def test_status_of_unknown_queue(async_redis_client):
    status = await DispatchQueue(async_redis_client, _config()).astatus("never-used")
    assert status.name == "never-used"
    assert (status.length, status.in_progress, status.waiting) == (0, 0, 0)
    assert status.oldest_age is None
    assert status.has_leader is False


async def test_status_without_worker(async_redis_client):
    queue = DispatchQueue(async_redis_client, _config())
    submits = [asyncio.create_task(queue.asubmit("idle", i, timeout=10)) for i in range(3)]
    await until_length(queue, "idle", 3)
    await asyncio.sleep(0.3)
    status = await queue.astatus("idle")
    assert (status.length, status.in_progress, status.waiting) == (3, 0, 3)
    assert status.oldest_age is not None
    assert status.oldest_age >= 0.25
    assert status.has_leader is False  # nobody serves it: a health alarm
    for submit in submits:
        submit.cancel()
    await asyncio.gather(*submits, return_exceptions=True)


async def test_status_with_worker(async_redis_client):
    release = asyncio.Event()

    async def handler(job: Job):
        await release.wait()  # the head job waits for "admission"
        return job.payload

    config = _config()
    queue = DispatchQueue(async_redis_client, config)
    worker = DispatchWorker(async_redis_client, ["busy"], handler, config)
    async with running(worker):
        submits = [asyncio.create_task(queue.asubmit("busy", i, timeout=10)) for i in range(4)]
        await until_length(queue, "busy", 4)
        await until(lambda: worker.health().jobs_in_progress == 1)

        status = await queue.astatus("busy")
        assert status.has_leader is True
        assert (status.length, status.in_progress, status.waiting) == (4, 1, 3)

        health = worker.health()
        assert health.running and health.healthy
        assert health.leading == ("busy",)
        assert health.jobs_in_progress == 1
        assert health.last_loop_age is not None
        assert health.last_loop_age < 1.0

        release.set()
        assert sorted(await asyncio.gather(*submits)) == [0, 1, 2, 3]
        empty = await queue.astatus("busy")
        assert (empty.length, empty.oldest_age) == (0, None)

    # Leadership is released on stop.
    assert (await queue.astatus("busy")).has_leader is False


async def test_status_many_and_sync(redis_client, async_redis_client):
    config = _config()
    async with running(DispatchWorker(async_redis_client, ["m1"], echo, config)):
        statuses = await DispatchQueue(async_redis_client, config).astatus_many(["m1", "m2", "m1"])
        assert list(statuses) == ["m1", "m2"]
        sync = await asyncio.to_thread(
            DispatchQueue(redis_client, config).status_many, ["m1", "m2"]
        )
    for name in ("m1", "m2"):
        assert statuses[name].length == sync[name].length == 0
    assert sync["m2"].has_leader is False


async def test_worker_health_lifecycle(async_redis_client):
    worker = DispatchWorker(async_redis_client, ["life"], echo, _config(), worker_id="w-1")
    before = worker.health()
    assert before.worker_id == "w-1"
    assert (before.running, before.healthy, before.last_loop_age) == (False, False, None)

    run = asyncio.create_task(worker.run())
    await until(lambda: worker.health().leading == ("life",))
    assert worker.health().healthy is True
    worker.stop()
    await asyncio.wait_for(run, 5)

    after = worker.health()
    assert (after.running, after.stopping, after.healthy) == (False, False, False)
    assert after.leading == ()


async def test_worker_health_reports_redis_errors(async_redis_client):
    broken = aioredis.Redis(host="localhost", port=1, socket_connect_timeout=0.1)
    worker = DispatchWorker(broken, ["down"], echo, _config())
    run = asyncio.create_task(worker.run())
    await until(lambda: worker.health().last_error is not None)
    health = worker.health()
    assert health.running is True
    assert health.healthy is False  # Redis unreachable: not ready
    assert "Error" in (health.last_error or "")
    worker.stop()
    await asyncio.wait_for(run, 5)
    await broken.aclose()


# ---------------------------------------------------------------------------
# RequeueJob: put a job back into its queue (e.g. after an upstream 429)
# ---------------------------------------------------------------------------


def test_requeue_job_validation():
    from redis_semaphore import RequeueJob

    assert RequeueJob().delay == 0.0
    with pytest.raises(ValueError):
        RequeueJob(delay=-1)


async def test_requeue_before_admit_keeps_place_in_queue(async_redis_client):
    """A job requeued before admit() runs again before any later job."""
    from redis_semaphore import RequeueJob

    config = _config()
    queue = DispatchQueue(async_redis_client, config)
    submits = [asyncio.create_task(queue.asubmit("requeue", i, timeout=10)) for i in range(4)]
    await until_length(queue, "requeue", 4)
    stream_order = [
        json.loads(fields[b"payload"])
        for _, fields in await async_redis_client.xrange(queue.stream_key("requeue"))
    ]
    first = stream_order[0]
    runs: list[tuple[int, bool]] = []

    async def handler(job: Job):
        runs.append((job.payload, job.reclaimed))
        if job.payload == first and not job.reclaimed:
            raise RequeueJob()  # e.g. no quota upstream: stay at the head
        job.admit()
        return job.payload

    async with running(DispatchWorker(async_redis_client, ["requeue"], handler, config)):
        assert sorted(await asyncio.gather(*submits)) == [0, 1, 2, 3]
    assert runs == [(first, False), (first, True)] + [(p, False) for p in stream_order[1:]]
    assert await queue.alength("requeue") == 0


async def test_requeue_after_admit_runs_again(async_redis_client):
    """After admit() (e.g. 429 from the API) the job is re-run, not failed."""
    from redis_semaphore import RequeueJob

    attempts: dict[int, int] = {}

    async def handler(job: Job):
        attempts[job.payload] = attempts.get(job.payload, 0) + 1
        job.admit()
        await asyncio.sleep(0.05)
        if attempts[job.payload] < 3:
            raise RequeueJob()
        return attempts[job.payload]

    config = _config()
    queue = DispatchQueue(async_redis_client, config)
    async with running(DispatchWorker(async_redis_client, ["retry"], handler, config)):
        results = await asyncio.gather(*(queue.asubmit("retry", i, timeout=10) for i in range(3)))
    assert results == [3, 3, 3]
    assert await queue.alength("retry") == 0


async def test_requeue_delay_holds_the_queue(async_redis_client):
    """Requeued with a delay before admit(): the queue waits, order is kept."""
    from redis_semaphore import RequeueJob

    config = _config()
    queue = DispatchQueue(async_redis_client, config)
    starts: list[tuple[int, float]] = []

    async def handler(job: Job):
        starts.append((job.payload, time.monotonic()))
        if job.payload == "a" and not job.reclaimed:
            raise RequeueJob(delay=0.5)
        return job.payload

    first = asyncio.create_task(queue.asubmit("delay", "a", timeout=10))
    await until_length(queue, "delay", 1)
    second = asyncio.create_task(queue.asubmit("delay", "b", timeout=10))
    await until_length(queue, "delay", 2)
    async with running(DispatchWorker(async_redis_client, ["delay"], handler, config)):
        assert await asyncio.gather(first, second) == ["a", "b"]
    assert [p for p, _ in starts] == ["a", "a", "b"]
    assert starts[1][1] - starts[0][1] >= 0.45


async def test_requeue_is_bounded_by_deadline(async_redis_client):
    """A job that keeps requeueing ends when its submitter gives up."""
    from redis_semaphore import RequeueJob

    async def handler(job: Job):
        raise RequeueJob(delay=0.05)

    config = _config()
    queue = DispatchQueue(async_redis_client, config)
    async with running(DispatchWorker(async_redis_client, ["forever"], handler, config)):
        with pytest.raises(JobTimeoutError):
            await queue.asubmit("forever", None, timeout=0.5)
        # The expired job is dropped instead of being requeued forever.
        for _ in range(100):
            if await queue.alength("forever") == 0:
                break
            await asyncio.sleep(0.05)
    assert await queue.alength("forever") == 0


async def test_stop_cuts_requeue_delay_short(async_redis_client):
    from redis_semaphore import RequeueJob

    admitted = asyncio.Event()

    async def handler(job: Job):
        job.admit()
        admitted.set()
        raise RequeueJob(delay=30)

    config = _config()
    queue = DispatchQueue(async_redis_client, config)
    worker = DispatchWorker(async_redis_client, ["cut"], handler, config)
    run = asyncio.create_task(worker.run())
    submit = asyncio.create_task(queue.asubmit("cut", None, timeout=10))
    await asyncio.wait_for(admitted.wait(), 5)
    start = time.monotonic()
    worker.stop()
    await asyncio.wait_for(run, 5)
    assert time.monotonic() - start < 3
    # Handed back, still queued for another worker.
    assert (await queue.astatus("cut")).length == 1
    submit.cancel()
    await asyncio.gather(submit, return_exceptions=True)


async def test_requeue_is_not_reported_as_abandoned(async_redis_client, caplog):
    from redis_semaphore import RequeueJob

    async def handler(job: Job):
        if not job.reclaimed:
            raise RequeueJob()
        return "ok"

    config = _config()
    queue = DispatchQueue(async_redis_client, config)
    with caplog.at_level("WARNING", logger="redis_semaphore"):
        async with running(DispatchWorker(async_redis_client, ["quiet"], handler, config)):
            assert await queue.asubmit("quiet", None, timeout=10) == "ok"
    assert not [r for r in caplog.records if "abandoned" in r.getMessage()]
