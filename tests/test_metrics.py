"""Tests for metrics integration."""

import asyncio
import threading
import time

import pytest

from redis_semaphore import AcquireTimeoutError, Mutex, Semaphore, SemaphoreConfig
from redis_semaphore.logger import get_logger, set_logger
from redis_semaphore.metrics import PrometheusMetrics, _NoopMetrics, get_metrics, set_metrics


class DummyMetrics:
    def __init__(self) -> None:
        self.events: list[tuple[str, tuple, dict]] = []

    @property
    def enabled(self) -> bool:
        return True

    def set_slots_used(self, name: str, namespace: str, used: int, limit: int) -> None:
        self.events.append(("set_slots_used", (name, namespace, used, limit), {}))

    def set_waiting(self, name: str, namespace: str, waiting: int) -> None:
        self.events.append(("set_waiting", (name, namespace, waiting), {}))

    def observe_wait_seconds(
        self, name: str, namespace: str, wait_seconds: float, result: str
    ) -> None:
        self.events.append(("observe_wait_seconds", (name, namespace, result), {}))

    def inc_acquire(self, name: str, namespace: str, result: str) -> None:
        self.events.append(("inc_acquire", (name, namespace, result), {}))

    def inc_queue_total(self, name: str, namespace: str) -> None:
        self.events.append(("inc_queue_total", (name, namespace), {}))

    def inc_lock_lost(self, name: str, namespace: str) -> None:
        self.events.append(("inc_lock_lost", (name, namespace), {}))


@pytest.fixture
def metrics():
    old = get_metrics()
    dummy = DummyMetrics()
    set_metrics(dummy)
    try:
        yield dummy
    finally:
        set_metrics(old)


def _has_event(events, name: str, *contains) -> bool:
    for event_name, args, _ in events:
        if event_name != name:
            continue
        if all(item in args for item in contains):
            return True
    return False


def test_metrics_success(redis_client, metrics: DummyMetrics):
    config = SemaphoreConfig(name="metrics-success", limit=1)
    sem = Semaphore(redis_client, config)

    result = sem.acquire(blocking=False)
    assert result.success is True
    sem.release()

    assert _has_event(metrics.events, "inc_acquire", "metrics-success", "success")
    assert _has_event(metrics.events, "set_slots_used", "metrics-success")


def test_metrics_busy(redis_client, metrics: DummyMetrics):
    config = SemaphoreConfig(name="metrics-busy", limit=1)
    sem1 = Semaphore(redis_client, config)
    sem2 = Semaphore(redis_client, config)

    assert sem1.acquire(blocking=False).success is True
    assert sem2.acquire(blocking=False).success is False
    sem1.release()

    assert _has_event(metrics.events, "inc_acquire", "metrics-busy", "busy")


def test_metrics_timeout(redis_client, metrics: DummyMetrics):
    config = SemaphoreConfig(
        name="metrics-timeout",
        limit=1,
        acquire_timeout=0.2,
        retry_interval=0.05,
    )
    sem1 = Semaphore(redis_client, config)
    sem2 = Semaphore(redis_client, config)

    assert sem1.acquire(blocking=False).success is True

    with pytest.raises(AcquireTimeoutError):
        sem2.acquire(blocking=True)

    sem1.release()

    assert _has_event(metrics.events, "inc_acquire", "metrics-timeout", "timeout")
    assert _has_event(metrics.events, "observe_wait_seconds", "metrics-timeout", "timeout")


def test_metrics_waiting_gauge(redis_client, metrics: DummyMetrics):
    config = SemaphoreConfig(
        name="metrics-waiting",
        limit=1,
        acquire_timeout=0.2,
        retry_interval=0.05,
    )
    sem1 = Semaphore(redis_client, config)
    sem2 = Semaphore(redis_client, config)

    assert sem1.acquire(blocking=False).success is True
    with pytest.raises(AcquireTimeoutError):
        sem2.acquire(blocking=True)
    sem1.release()

    assert _has_event(metrics.events, "set_waiting", "metrics-waiting", 1)
    assert _has_event(metrics.events, "set_waiting", "metrics-waiting", 0)
    assert _has_event(metrics.events, "inc_queue_total", "metrics-waiting")


def test_metrics_lock_lost(redis_client, metrics: DummyMetrics):
    sem = Mutex(redis_client, "metrics-lock-lost", refresh_interval=0.05)
    sem.acquire(blocking=False)

    redis_client.zrem(sem.owners_key, sem.identifier)
    time.sleep(0.2)

    assert _has_event(metrics.events, "inc_lock_lost", "metrics-lock-lost")


@pytest.mark.asyncio
async def test_async_metrics_lock_lost(async_redis_client, metrics: DummyMetrics):
    sem = Mutex(async_redis_client, "metrics-lock-lost-async", refresh_interval=0.05)
    await sem.aacquire(blocking=False)

    await async_redis_client.zrem(sem.owners_key, sem.identifier)
    await asyncio.sleep(0.2)

    assert _has_event(metrics.events, "inc_lock_lost", "metrics-lock-lost-async")


def test_metrics_wait_success(redis_client, metrics: DummyMetrics):
    config = SemaphoreConfig(
        name="metrics-wait-success",
        limit=1,
        acquire_timeout=1.0,
        retry_interval=0.05,
    )
    sem1 = Semaphore(redis_client, config)
    sem2 = Semaphore(redis_client, config)

    assert sem1.acquire(blocking=False).success is True

    def release_later():
        time.sleep(0.2)
        sem1.release()

    thread = threading.Thread(target=release_later)
    thread.start()

    assert sem2.acquire(blocking=True).success is True
    sem2.release()
    thread.join()

    assert _has_event(metrics.events, "observe_wait_seconds", "metrics-wait-success", "success")


@pytest.mark.asyncio
async def test_async_metrics_wait_success(async_redis_client, metrics: DummyMetrics):
    class DebugLogger:
        def __init__(self) -> None:
            self.debug_calls = 0

        def debug(self, msg: str, *args, **kwargs) -> None:
            self.debug_calls += 1

        def info(self, msg: str, *args, **kwargs) -> None:
            return None

        def warning(self, msg: str, *args, **kwargs) -> None:
            return None

        def error(self, msg: str, *args, **kwargs) -> None:
            return None

        def exception(self, msg: str, *args, **kwargs) -> None:
            return None

        def isEnabledFor(self, level: int) -> bool:
            return True

    old_logger = get_logger()
    debug_logger = DebugLogger()
    set_logger(debug_logger)

    config = SemaphoreConfig(
        name="async-metrics-wait-success",
        limit=1,
        retry_interval=0.01,
    )
    sem1 = Semaphore(async_redis_client, config)
    sem2 = Semaphore(async_redis_client, config)

    await sem1.aacquire(blocking=False)
    task = asyncio.create_task(sem2.aacquire(blocking=True))
    await asyncio.sleep(0.05)
    await sem1.arelease()
    result = await task
    await sem2.arelease()

    assert result.success is True
    assert _has_event(
        metrics.events,
        "observe_wait_seconds",
        "async-metrics-wait-success",
        "success",
    )
    assert debug_logger.debug_calls > 0
    set_logger(old_logger)


@pytest.mark.asyncio
async def test_async_metrics_busy(async_redis_client, metrics: DummyMetrics):
    config = SemaphoreConfig(name="async-metrics-busy", limit=1)
    sem1 = Semaphore(async_redis_client, config)
    sem2 = Semaphore(async_redis_client, config)

    assert (await sem1.aacquire(blocking=False)).success is True
    assert (await sem2.aacquire(blocking=False)).success is False
    await sem1.arelease()

    assert _has_event(metrics.events, "inc_acquire", "async-metrics-busy", "busy")


@pytest.mark.asyncio
async def test_async_metrics_timeout(async_redis_client, metrics: DummyMetrics):
    config = SemaphoreConfig(
        name="async-metrics-timeout",
        limit=1,
        acquire_timeout=0.1,
        retry_interval=0.01,
    )

    sem1 = Semaphore(async_redis_client, config)
    sem2 = Semaphore(async_redis_client, config)

    await sem1.aacquire(blocking=False)
    with pytest.raises(AcquireTimeoutError):
        await sem2.aacquire(blocking=True)
    await sem1.arelease()

    assert _has_event(metrics.events, "inc_acquire", "async-metrics-timeout", "timeout")
    assert _has_event(
        metrics.events,
        "observe_wait_seconds",
        "async-metrics-timeout",
        "timeout",
    )


def test_prometheus_metrics_basic():
    pytest.importorskip("prometheus_client")
    metrics = PrometheusMetrics()
    assert metrics.enabled is True
    metrics.set_slots_used("name", "ns", 1, 2)
    metrics.set_waiting("name", "ns", 3)
    metrics.observe_wait_seconds("name", "ns", 0.1, "success")
    metrics.inc_acquire("name", "ns", "success")
    metrics.inc_queue_total("name", "ns")
    metrics.inc_lock_lost("name", "ns")


def test_noop_metrics_methods():
    metrics = _NoopMetrics()
    assert metrics.enabled is False
    metrics.set_slots_used("name", "ns", 1, 2)
    metrics.set_waiting("name", "ns", 0)
    metrics.observe_wait_seconds("name", "ns", 0.1, "success")
    metrics.inc_acquire("name", "ns", "success")
    metrics.inc_queue_total("name", "ns")
    metrics.inc_lock_lost("name", "ns")
