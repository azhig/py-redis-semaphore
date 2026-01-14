"""Tests for asynchronous Mutex."""

import pytest

from redis_semaphore import Mutex, NotAcquiredError


@pytest.mark.asyncio
async def test_async_mutex_exclusive(async_redis_client):
    """Async mutex allows only a single owner."""
    mutex1 = Mutex(async_redis_client, "test-async-mutex")
    result1 = await mutex1.aacquire(blocking=False)
    assert result1.success is True

    mutex2 = Mutex(async_redis_client, "test-async-mutex")
    result2 = await mutex2.aacquire(blocking=False)
    assert result2.success is False

    await mutex1.arelease()

    result3 = await mutex2.aacquire(blocking=False)
    assert result3.success is True
    await mutex2.arelease()


@pytest.mark.asyncio
async def test_async_mutex_context_manager(async_redis_client):
    """Async mutex works as a context manager."""
    async with Mutex(async_redis_client, "test-async-mutex-context") as lock:
        assert lock.is_acquired is True
        assert lock.fencing_token is not None

    assert lock.is_acquired is False


@pytest.mark.asyncio
async def test_async_mutex_reentrant(async_redis_client):
    """Re-entrant acquire returns the same fencing token."""
    mutex = Mutex(async_redis_client, "test-async-mutex-reentrant")
    first = await mutex.aacquire(blocking=False)
    second = await mutex.aacquire(blocking=False)

    assert first.success is True
    assert second.success is True
    assert second.fencing_token is not None
    assert first.fencing_token is not None
    assert second.fencing_token > first.fencing_token

    await mutex.arelease()


@pytest.mark.asyncio
async def test_async_release_without_acquire(async_redis_client):
    """Release without acquire raises an error."""
    mutex = Mutex(async_redis_client, "test-async-mutex-release")

    with pytest.raises(NotAcquiredError):
        await mutex.arelease()
