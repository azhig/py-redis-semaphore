"""Tests for mixed sync/async mode handling and lock-lost waits."""

import asyncio
import time

import pytest

from redis_semaphore import AcquireMode, MixedModeError, Semaphore, SemaphoreConfig


@pytest.mark.asyncio
async def test_mixed_mode_sync_then_async(redis_client):
    config = SemaphoreConfig(name="mixed-sync-async", limit=1)
    sem = Semaphore(redis_client, config)
    sem.acquire(blocking=False)

    with pytest.raises(MixedModeError):
        await sem.aacquire(blocking=False)

    sem.release()


@pytest.mark.asyncio
async def test_mixed_mode_async_then_sync(async_redis_client):
    config = SemaphoreConfig(name="mixed-async-sync", limit=1)
    sem = Semaphore(async_redis_client, config)
    await sem.aacquire(blocking=False)

    with pytest.raises(MixedModeError):
        sem.acquire(blocking=False)

    await sem.arelease()


def test_wait_for_lock_lost_timeout(redis_client):
    config = SemaphoreConfig(name="lock-lost-timeout", limit=1)
    sem = Semaphore(redis_client, config)

    assert sem.wait_for_lock_lost(timeout=0.01) is False


def test_wait_for_lock_lost_true(redis_client):
    config = SemaphoreConfig(name="lock-lost-true", limit=1)
    sem = Semaphore(redis_client, config)

    sem.acquire(blocking=False)
    redis_client.zrem(sem.owners_key, sem.identifier)
    assert sem.refresh() is False

    assert sem.wait_for_lock_lost(timeout=0.1) is True


def test_notify_queue_is_bounded(redis_client):
    """The BLPOP notification list must not grow unboundedly under release churn.

    With no waiters consuming tokens, every release LPUSHes one; without the
    LTRIM cap the list would grow by one per release forever.
    """
    limit = 3
    config = SemaphoreConfig(
        name="notify-bounded",
        limit=limit,
        acquire_mode=AcquireMode.BLPOP,
        namespace="t-notify",
    )
    redis_client.delete(f"t-notify:{config.name}:queue")

    sem = Semaphore(redis_client, config)
    for _ in range(50):
        assert sem.acquire(blocking=False).success is True
        sem.release()

    assert redis_client.llen(f"t-notify:{config.name}:queue") <= limit


@pytest.mark.asyncio
async def test_await_lock_lost_timeout(async_redis_client):
    config = SemaphoreConfig(name="lock-lost-async-timeout", limit=1)
    sem = Semaphore(async_redis_client, config)

    try:
        assert await sem.await_lock_lost(timeout=0.01) is False
    finally:
        sem._lock_lost_event.set()


@pytest.mark.asyncio
async def test_await_lock_lost_true(async_redis_client):
    class DummyClient:
        pass

    config = SemaphoreConfig(name="lock-lost-async-true", limit=1)
    sem = Semaphore(DummyClient(), config)
    sem._lock_lost_event.set()

    assert await sem.await_lock_lost(timeout=0.1) is True


@pytest.mark.asyncio
async def test_await_lock_lost_wakes_promptly(async_redis_client):
    """A waiter parked on await_lock_lost must wake via the asyncio.Event as
    soon as the loss is signalled from an async path, not on a poll tick."""
    config = SemaphoreConfig(name="lock-lost-prompt", limit=1, lock_timeout=30.0)
    sem = Semaphore(async_redis_client, config)
    await sem.aacquire()

    waiter = asyncio.create_task(sem.await_lock_lost(timeout=5.0))
    await asyncio.sleep(0.05)  # let the waiter park on the event

    await async_redis_client.zrem(sem.owners_key, sem.identifier)
    start = time.monotonic()
    assert await sem.arefresh() is False  # detects the loss, sets the events

    assert await asyncio.wait_for(waiter, timeout=1.0) is True
    assert time.monotonic() - start < 0.5
