"""Additional heartbeat coverage tests."""

import asyncio
import time

import pytest

from redis_semaphore.heartbeat import Heartbeat


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
