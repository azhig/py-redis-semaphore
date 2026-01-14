"""Async heartbeat tests."""

import asyncio

import pytest

from redis_semaphore import Semaphore, SemaphoreConfig


@pytest.mark.asyncio
async def test_async_heartbeat_keeps_lock(async_redis_client):
    """Async heartbeat should keep the lock from expiring."""
    config = SemaphoreConfig(
        name="test-async-heartbeat",
        limit=1,
        lock_timeout=0.2,
        refresh_interval=0.05,
    )

    sem1 = Semaphore(async_redis_client, config)
    await sem1.aacquire(blocking=False)

    await asyncio.sleep(0.4)

    sem2 = Semaphore(async_redis_client, config)
    result = await sem2.aacquire(blocking=False)
    assert result.success is False

    await sem1.arelease()
    result = await sem2.aacquire(blocking=False)
    assert result.success is True
    await sem2.arelease()


@pytest.mark.asyncio
async def test_async_heartbeat_lock_lost_callback(async_redis_client):
    """Lock lost callback fires when lock is removed."""
    event = asyncio.Event()

    async def on_lock_lost(identifier: str) -> None:
        event.set()

    config = SemaphoreConfig(
        name="test-async-heartbeat-lost",
        limit=1,
        lock_timeout=0.5,
        refresh_interval=0.05,
    )

    sem = Semaphore(async_redis_client, config, on_lock_lost=on_lock_lost)
    await sem.aacquire(blocking=False)

    await async_redis_client.zrem(sem.owners_key, sem.identifier)

    await asyncio.wait_for(event.wait(), timeout=1.0)
    assert sem.is_lost is True
