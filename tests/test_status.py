"""Tests for the public status() / cleanup() API."""

import time

import pytest

from redis_semaphore import Semaphore, SemaphoreConfig, SemaphoreStatus


def test_status_reports_occupancy(redis_client):
    """status() reflects live occupancy and ownership without acquiring."""
    config = SemaphoreConfig(name="status-occupancy", limit=2, namespace="t-status")
    redis_client.delete(f"t-status:{config.name}:owners")

    observer = Semaphore(redis_client, config)
    empty = observer.status()
    assert isinstance(empty, SemaphoreStatus)
    assert empty.used_slots == 0
    assert empty.available == 2
    assert empty.is_owner is False
    assert empty.expires_at is None

    holder = Semaphore(redis_client, config)
    assert holder.acquire(blocking=False).success is True

    held = holder.status()
    assert held.used_slots == 1
    assert held.available == 1
    assert held.is_owner is True
    assert held.expires_at is not None

    # A non-owner observer sees occupancy but not ownership.
    seen = observer.status()
    assert seen.used_slots == 1
    assert seen.is_owner is False

    holder.release()
    assert observer.status().used_slots == 0


@pytest.mark.asyncio
async def test_status_async_reports_occupancy(async_redis_client):
    config = SemaphoreConfig(name="status-occupancy-async", limit=1, namespace="t-status")
    await async_redis_client.delete(f"t-status:{config.name}:owners")

    sem = Semaphore(async_redis_client, config)
    assert (await sem.astatus()).is_owner is False

    await sem.aacquire(blocking=False)
    held = await sem.astatus()
    assert held.used_slots == 1
    assert held.is_owner is True
    assert held.expires_at is not None

    await sem.arelease()
    assert (await sem.astatus()).used_slots == 0


def test_cleanup_removes_expired_entries(redis_client):
    """cleanup() purges expired owners but keeps live ones."""
    config = SemaphoreConfig(name="cleanup-expired", limit=5, namespace="t-cleanup")
    owners_key = f"t-cleanup:{config.name}:owners"
    redis_client.delete(owners_key)

    now_ms = int(time.time() * 1000)
    # Two already-expired members and one far in the future.
    redis_client.zadd(
        owners_key,
        {"dead-1": now_ms - 10_000, "dead-2": now_ms - 1, "alive": now_ms + 60_000},
    )

    sem = Semaphore(redis_client, config)
    removed = sem.cleanup()
    assert removed == 2
    assert redis_client.zcard(owners_key) == 1

    # Idempotent: nothing left to remove.
    assert sem.cleanup() == 0
    redis_client.delete(owners_key)


@pytest.mark.asyncio
async def test_cleanup_async_removes_expired_entries(async_redis_client):
    config = SemaphoreConfig(name="cleanup-expired-async", limit=5, namespace="t-cleanup")
    owners_key = f"t-cleanup:{config.name}:owners"
    await async_redis_client.delete(owners_key)

    now_ms = int(time.time() * 1000)
    await async_redis_client.zadd(owners_key, {"dead": now_ms - 5_000, "alive": now_ms + 60_000})

    sem = Semaphore(async_redis_client, config)
    assert await sem.acleanup() == 1
    assert await async_redis_client.zcard(owners_key) == 1
    await async_redis_client.delete(owners_key)
