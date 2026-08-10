"""Tests for reacquire_policy."""

import pytest

from redis_semaphore import (
    AlreadyAcquiredError,
    ReacquirePolicy,
    Semaphore,
    SemaphoreConfig,
)


def test_renew_is_default_and_renews_token(redis_client):
    sem = Semaphore(redis_client, SemaphoreConfig(name="renew", limit=1))
    assert sem.config.reacquire_policy is ReacquirePolicy.RENEW

    first = sem.acquire()
    second = sem.acquire()  # re-entrant: renews TTL, issues a NEW token
    try:
        assert second.success
        assert second.fencing_token > first.fencing_token
    finally:
        sem.release()

    # No hold count: the single release freed the slot.
    assert sem.status().used_slots == 0


def test_forbid_raises_on_reacquire(redis_client):
    sem = Semaphore(
        redis_client,
        SemaphoreConfig(name="forbid", limit=1, reacquire_policy=ReacquirePolicy.FORBID),
    )
    sem.acquire()
    try:
        with pytest.raises(AlreadyAcquiredError):
            sem.acquire()
        with pytest.raises(AlreadyAcquiredError):
            sem.acquire(blocking=False)
    finally:
        sem.release()


def test_forbid_allows_acquire_after_release(redis_client):
    sem = Semaphore(
        redis_client,
        SemaphoreConfig(name="forbid-cycle", limit=1, reacquire_policy=ReacquirePolicy.FORBID),
    )
    assert sem.acquire().success
    sem.release()
    assert sem.acquire().success
    sem.release()


@pytest.mark.asyncio
async def test_forbid_raises_on_reacquire_async(async_redis_client):
    sem = Semaphore(
        async_redis_client,
        SemaphoreConfig(name="aforbid", limit=1, reacquire_policy=ReacquirePolicy.FORBID),
    )
    await sem.aacquire()
    try:
        with pytest.raises(AlreadyAcquiredError):
            await sem.aacquire()
    finally:
        await sem.arelease()
