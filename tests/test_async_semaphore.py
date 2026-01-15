"""Tests for asynchronous Semaphore."""

import pytest

from redis_semaphore import (
    AcquireError,
    AcquireResult,
    AcquireTimeoutError,
    Semaphore,
    SemaphoreConfig,
)


@pytest.mark.asyncio
async def test_async_acquire_release(async_redis_client):
    """Async acquire and release works."""
    config = SemaphoreConfig(name="test-async-sem", limit=1)
    sem = Semaphore(async_redis_client, config)

    result = await sem.aacquire(blocking=False)
    assert result.success is True
    assert sem.is_acquired is True

    released = await sem.arelease()
    assert released is True
    assert sem.is_acquired is False


@pytest.mark.asyncio
async def test_async_context_manager(async_redis_client):
    """Async context manager usage works."""
    config = SemaphoreConfig(name="test-async-context", limit=1)

    async with Semaphore(async_redis_client, config) as sem:
        assert sem.is_acquired is True
        assert sem.fencing_token is not None

    assert sem.is_acquired is False


@pytest.mark.asyncio
async def test_async_counting_semaphore(async_redis_client):
    """Counting semaphore allows up to limit holders."""
    config = SemaphoreConfig(name="test-async-counting", limit=2)

    sem1 = Semaphore(async_redis_client, config)
    sem2 = Semaphore(async_redis_client, config)
    sem3 = Semaphore(async_redis_client, config)

    assert (await sem1.aacquire(blocking=False)).success is True
    assert (await sem2.aacquire(blocking=False)).success is True
    assert (await sem3.aacquire(blocking=False)).success is False

    await sem1.arelease()
    assert (await sem3.aacquire(blocking=False)).success is True

    await sem2.arelease()
    await sem3.arelease()


@pytest.mark.asyncio
async def test_async_acquire_timeout(async_redis_client):
    """Async acquire timeout raises error."""
    config = SemaphoreConfig(
        name="test-async-timeout",
        limit=1,
        acquire_timeout=0.3,
        retry_interval=0.05,
    )

    sem1 = Semaphore(async_redis_client, config)
    await sem1.aacquire(blocking=False)

    sem2 = Semaphore(async_redis_client, config)
    with pytest.raises(AcquireTimeoutError):
        await sem2.aacquire(blocking=True)

    await sem1.arelease()


@pytest.mark.asyncio
async def test_async_reentrant_acquire(async_redis_client):
    """Re-entrant acquire returns the same token."""
    config = SemaphoreConfig(name="test-async-reentrant", limit=1)
    sem = Semaphore(async_redis_client, config)

    first = await sem.aacquire(blocking=False)
    second = await sem.aacquire(blocking=False)

    assert first.success is True
    assert second.success is True
    assert second.fencing_token is not None
    assert first.fencing_token is not None
    assert second.fencing_token > first.fencing_token

    await sem.arelease()


@pytest.mark.asyncio
async def test_async_refresh_without_acquire(async_redis_client):
    """Refresh should fail when lock is not acquired."""
    config = SemaphoreConfig(name="test-async-refresh-unacquired", limit=1)
    sem = Semaphore(async_redis_client, config)

    assert await sem.arefresh() is False


@pytest.mark.asyncio
async def test_async_context_manager_acquire_failure(async_redis_client):
    """__aenter__ raises AcquireError when acquire returns failure."""
    config = SemaphoreConfig(
        name="test-async-context-fail",
        limit=1,
        acquire_timeout=0.01,
        retry_interval=0.01,
    )

    class FailingSemaphore(Semaphore):
        async def aacquire(self, blocking: bool = True) -> AcquireResult:
            return AcquireResult(
                success=False,
                identifier=None,
                fencing_token=None,
                expires_at=None,
            )

    sem = FailingSemaphore(async_redis_client, config)
    with pytest.raises(AcquireError):
        await sem.__aenter__()


@pytest.mark.asyncio
async def test_async_blpop_acquire_release(async_redis_client):
    """Test async BLPOP mode acquire/release."""
    from redis_semaphore import AcquireMode

    config = SemaphoreConfig(
        name="test-async-blpop",
        limit=1,
        acquire_mode=AcquireMode.BLPOP,
        blpop_timeout=1.0,
    )

    sem = Semaphore(async_redis_client, config)
    result = await sem.aacquire(blocking=False)
    assert result.success is True
    await sem.arelease()


@pytest.mark.asyncio
async def test_async_blpop_waiter_notification(async_redis_client):
    """Test that async release notifies waiting BLPOP."""
    import asyncio

    from redis_semaphore import AcquireMode

    config = SemaphoreConfig(
        name="test-async-blpop-notify",
        limit=1,
        acquire_mode=AcquireMode.BLPOP,
        blpop_timeout=5.0,
    )

    sem1 = Semaphore(async_redis_client, config)
    await sem1.aacquire(blocking=False)

    acquired = asyncio.Event()

    async def waiter():
        sem2 = Semaphore(async_redis_client, config)
        await sem2.aacquire(blocking=True)
        acquired.set()
        await sem2.arelease()

    task = asyncio.create_task(waiter())

    # Give task time to start waiting
    await asyncio.sleep(0.1)

    # Release should notify the waiter
    await sem1.arelease()

    # Wait for waiter to acquire
    try:
        await asyncio.wait_for(acquired.wait(), timeout=1.0)
    except asyncio.TimeoutError:
        pytest.fail("Waiter did not acquire in time")

    await task


@pytest.mark.asyncio
async def test_async_polling_with_backoff(async_redis_client):
    """Test async polling with exponential backoff."""
    import time

    from redis_semaphore import AcquireMode

    config = SemaphoreConfig(
        name="test-async-polling-backoff",
        limit=1,
        acquire_mode=AcquireMode.POLLING,
        retry_interval=0.05,
        retry_interval_max=0.2,
        retry_backoff_multiplier=2.0,
        acquire_timeout=0.5,
    )

    sem1 = Semaphore(async_redis_client, config)
    await sem1.aacquire(blocking=False)

    sem2 = Semaphore(async_redis_client, config)
    start = time.monotonic()
    with pytest.raises(AcquireTimeoutError):
        await sem2.aacquire(blocking=True)
    elapsed = time.monotonic() - start

    assert 0.4 < elapsed < 0.7

    await sem1.arelease()
