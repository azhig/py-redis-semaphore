"""Tests for Redis key hygiene: TTLs on service keys and Cluster hash tags."""

import pytest

from redis_semaphore import Semaphore, SemaphoreConfig


def test_owners_key_gets_ttl_on_acquire(redis_client):
    sem = Semaphore(redis_client, SemaphoreConfig(name="ttl", limit=1, lock_timeout=5.0))
    sem.acquire()
    try:
        pttl = redis_client.pttl(sem.owners_key)
        # max_expires - now + lock_timeout margin => about 2 * lock_timeout
        assert 0 < pttl <= 10_000
    finally:
        sem.release()


def test_owners_key_ttl_extended_on_refresh(redis_client):
    sem = Semaphore(redis_client, SemaphoreConfig(name="ttl-refresh", limit=1, lock_timeout=5.0))
    sem.acquire()
    try:
        assert sem.refresh() is True
        pttl = redis_client.pttl(sem.owners_key)
        assert 5_000 < pttl <= 10_000
    finally:
        sem.release()


def test_owners_key_ttl_covers_longest_lived_member(redis_client):
    """The key TTL must never undercut a live member with a longer TTL."""
    long_holder = Semaphore(
        redis_client, SemaphoreConfig(name="mixed-ttl", limit=2, lock_timeout=8.0)
    )
    short_holder = Semaphore(
        redis_client, SemaphoreConfig(name="mixed-ttl", limit=2, lock_timeout=2.0)
    )
    long_holder.acquire()
    short_holder.acquire()  # acquires last, with the shorter lock_timeout
    try:
        pttl = redis_client.pttl(long_holder.owners_key)
        # Long holder lives ~8s; the key must outlive it.
        assert pttl > 7_500
    finally:
        short_holder.release()
        long_holder.release()


def test_queue_key_gets_ttl_on_notify(redis_client):
    sem = Semaphore(redis_client, SemaphoreConfig(name="queue-ttl", limit=1, lock_timeout=5.0))
    sem.acquire()
    sem.release()  # release LPUSHes a wakeup token and must set a TTL
    pttl = redis_client.pttl(sem.queue_key)
    assert 0 < pttl <= 5_000


def test_fencing_key_intentionally_has_no_ttl(redis_client):
    """Fencing counter must survive; expiring it would reset monotonicity."""
    sem = Semaphore(redis_client, SemaphoreConfig(name="fencing-ttl", limit=1))
    sem.acquire()
    sem.release()
    assert redis_client.pttl(sem.fencing_key) == -1


def test_cluster_hash_tags_key_format(redis_client):
    sem = Semaphore(
        redis_client,
        SemaphoreConfig(name="tagged", limit=1, cluster_hash_tags=True),
    )
    assert sem.owners_key == "{semaphore:tagged}:owners"
    assert sem.fencing_key == "{semaphore:tagged}:fencing"
    assert sem.queue_key == "{semaphore:tagged}:queue"


def test_cluster_hash_tags_full_cycle(redis_client):
    sem = Semaphore(
        redis_client,
        SemaphoreConfig(name="tagged-cycle", limit=1, cluster_hash_tags=True),
    )
    result = sem.acquire()
    assert result.success
    assert redis_client.exists("{semaphore:tagged-cycle}:owners") == 1
    status = sem.status()
    assert status.is_owner
    assert sem.refresh() is True
    assert sem.release() is True


def test_default_keys_unchanged(redis_client):
    """Without the flag the legacy key format must stay intact."""
    sem = Semaphore(redis_client, SemaphoreConfig(name="legacy", limit=1))
    assert sem.owners_key == "semaphore:legacy:owners"
    assert sem.fencing_key == "semaphore:legacy:fencing"
    assert sem.queue_key == "semaphore:legacy:queue"


@pytest.mark.asyncio
async def test_owners_key_ttl_async(async_redis_client):
    sem = Semaphore(async_redis_client, SemaphoreConfig(name="attl", limit=1, lock_timeout=5.0))
    await sem.aacquire()
    try:
        pttl = await async_redis_client.pttl(sem.owners_key)
        assert 0 < pttl <= 10_000
    finally:
        await sem.arelease()
