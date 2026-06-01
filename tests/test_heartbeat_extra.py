"""Additional heartbeat coverage tests."""

import asyncio
import threading
import time

import pytest

from redis_semaphore.errors import PermanentBackendError, RedisConnectionError
from redis_semaphore.heartbeat import Heartbeat
from redis_semaphore.semaphore import _is_permanent_backend_error


def test_sync_heartbeat_start_idempotent():
    """Starting twice should not create a new thread."""
    heartbeat = Heartbeat(lambda: True, interval=0.01, identifier="id")
    heartbeat.start()
    thread = heartbeat._thread
    heartbeat.start()

    assert heartbeat._thread is thread
    heartbeat.stop()


def test_sync_heartbeat_exception_does_not_mark_lost():
    """Exceptions in refresh are swallowed and do not mark lock lost."""
    calls = 0

    def refresh_fn() -> bool:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("boom")
        return True

    heartbeat = Heartbeat(refresh_fn, interval=0.01, identifier="id")
    heartbeat.start()
    time.sleep(0.05)
    heartbeat.stop()

    assert heartbeat.is_lock_lost is False


@pytest.mark.asyncio
async def test_async_heartbeat_start_idempotent():
    """Starting twice should not create a new task."""

    async def refresh_fn() -> bool:
        return True

    heartbeat = Heartbeat(refresh_fn, interval=0.01, identifier="id")
    await heartbeat.astart()
    task = heartbeat._task
    await heartbeat.astart()

    assert heartbeat._task is task
    await heartbeat.astop()


@pytest.mark.asyncio
async def test_async_heartbeat_exception_does_not_mark_lost():
    """Exceptions in refresh are swallowed and do not mark lock lost."""
    calls = 0
    triggered = asyncio.Event()

    async def refresh_fn() -> bool:
        nonlocal calls
        calls += 1
        if calls == 1:
            triggered.set()
            raise RuntimeError("boom")
        return True

    heartbeat = Heartbeat(refresh_fn, interval=0.01, identifier="id")
    await heartbeat.astart()
    await asyncio.wait_for(triggered.wait(), timeout=1.0)
    await heartbeat.astop()

    assert heartbeat.is_lock_lost is False


def test_sync_heartbeat_connection_loss_escalates_after_lock_timeout():
    """Persistent connection errors escalate to lock-lost after lock_timeout."""
    lost = threading.Event()

    def refresh_fn() -> bool:
        raise RuntimeError("connection lost")

    heartbeat = Heartbeat(
        refresh_fn,
        interval=0.05,
        identifier="id",
        on_lock_lost=lambda _identifier: lost.set(),
        lock_timeout=0.3,
        retry_step=0.02,
    )
    start = time.monotonic()
    heartbeat.start()
    assert lost.wait(timeout=2.0) is True
    elapsed = time.monotonic() - start
    heartbeat.stop()

    assert heartbeat.is_lock_lost is True
    # Conservative deadline: must not escalate before lock_timeout elapsed.
    assert elapsed >= 0.25


def test_sync_heartbeat_transient_error_recovers():
    """A short connection blip that recovers before the deadline keeps the lock."""
    calls = {"n": 0}
    lost = threading.Event()

    def refresh_fn() -> bool:
        calls["n"] += 1
        if calls["n"] <= 3:
            raise RuntimeError("blip")
        return True

    heartbeat = Heartbeat(
        refresh_fn,
        interval=0.05,
        identifier="id",
        on_lock_lost=lambda _identifier: lost.set(),
        lock_timeout=1.0,
        retry_step=0.02,
    )
    heartbeat.start()
    time.sleep(0.4)
    heartbeat.stop()

    assert lost.is_set() is False
    assert heartbeat.is_lock_lost is False
    assert calls["n"] >= 4


def test_sync_heartbeat_permanent_error_marks_lost_immediately():
    """A permanent refresh error escalates to lock-lost without waiting the deadline."""
    lost = threading.Event()

    def refresh_fn() -> bool:
        raise PermanentBackendError("ACL denied ZADD")

    heartbeat = Heartbeat(
        refresh_fn,
        interval=0.05,
        identifier="id",
        on_lock_lost=lambda _identifier: lost.set(),
        lock_timeout=5.0,  # large deadline - must NOT be waited out
        retry_step=0.02,
        is_fatal_error=_is_permanent_backend_error,
    )
    start = time.monotonic()
    heartbeat.start()
    assert lost.wait(timeout=1.0) is True
    elapsed = time.monotonic() - start
    heartbeat.stop()

    assert heartbeat.is_lock_lost is True
    # Fired on the first attempt, far below the 5s deadline.
    assert elapsed < 1.0


def test_sync_heartbeat_transient_error_is_not_fatal():
    """With the predicate in place, transient errors still retry until the deadline."""
    lost = threading.Event()

    def refresh_fn() -> bool:
        raise RedisConnectionError("connection reset")

    heartbeat = Heartbeat(
        refresh_fn,
        interval=0.05,
        identifier="id",
        on_lock_lost=lambda _identifier: lost.set(),
        lock_timeout=0.4,
        retry_step=0.02,
        is_fatal_error=_is_permanent_backend_error,
    )
    heartbeat.start()
    # Must not escalate before the deadline (transient is not fatal).
    assert lost.wait(timeout=0.2) is False
    # Eventually escalates once the deadline passes.
    assert lost.wait(timeout=1.0) is True
    heartbeat.stop()


@pytest.mark.asyncio
async def test_async_heartbeat_permanent_error_marks_lost_immediately():
    """A permanent async refresh error escalates immediately, ignoring the deadline."""
    lost = asyncio.Event()

    async def refresh_fn() -> bool:
        raise PermanentBackendError("ACL denied ZADD")

    async def on_lost(_identifier: str) -> None:
        lost.set()

    heartbeat = Heartbeat(
        refresh_fn,
        interval=0.05,
        identifier="id",
        on_lock_lost=on_lost,
        lock_timeout=5.0,
        retry_step=0.02,
        is_fatal_error=_is_permanent_backend_error,
    )
    await heartbeat.astart()
    await asyncio.wait_for(lost.wait(), timeout=1.0)
    await heartbeat.astop()

    assert heartbeat.is_lock_lost is True


@pytest.mark.asyncio
async def test_async_heartbeat_connection_loss_escalates_after_lock_timeout():
    """Persistent async connection errors escalate to lock-lost after lock_timeout."""
    lost = asyncio.Event()

    async def refresh_fn() -> bool:
        raise RuntimeError("connection lost")

    async def on_lost(_identifier: str) -> None:
        lost.set()

    heartbeat = Heartbeat(
        refresh_fn,
        interval=0.05,
        identifier="id",
        on_lock_lost=on_lost,
        lock_timeout=0.3,
        retry_step=0.02,
    )
    await heartbeat.astart()
    await asyncio.wait_for(lost.wait(), timeout=2.0)
    await heartbeat.astop()

    assert heartbeat.is_lock_lost is True


@pytest.mark.asyncio
async def test_async_heartbeat_cancel_async_is_best_effort():
    """cancel_async() cancels the task synchronously without awaiting."""

    async def refresh_fn() -> bool:
        return True

    heartbeat = Heartbeat(refresh_fn, interval=0.5, identifier="id")
    await heartbeat.astart()
    task = heartbeat._task
    assert task is not None

    heartbeat.cancel_async()
    assert heartbeat._task is None

    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_async_heartbeat_cancel_propagates():
    """CancelledError is re-raised from the heartbeat loop."""

    async def refresh_fn() -> bool:
        return True

    heartbeat = Heartbeat(refresh_fn, interval=0.5, identifier="id")
    await heartbeat.astart()

    task = heartbeat._task
    assert task is not None
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
