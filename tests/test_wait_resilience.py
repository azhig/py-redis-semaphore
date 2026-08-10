"""Tests for wait-phase resilience in BLPOP mode.

The wait phase must tolerate transient backend errors (a socket read timeout
racing the BLPOP reply, a connection blip) instead of aborting a blocking
acquire, and must cap each BLPOP below the client's socket_timeout.
"""

import threading
import time

import pytest
import redis

from redis_semaphore import (
    AcquireTimeoutError,
    PermanentBackendError,
    Semaphore,
    SemaphoreConfig,
)


class FlakyBlpopClient:
    """Delegating wrapper whose blpop raises for the first N calls."""

    def __init__(self, inner, failures: int, exc_factory=None):
        self._inner = inner
        self._failures = failures
        self._exc_factory = exc_factory or (
            lambda: redis.exceptions.TimeoutError("injected timeout")
        )
        self.blpop_calls = 0

    def blpop(self, *args, **kwargs):
        self.blpop_calls += 1
        if self._failures > 0:
            self._failures -= 1
            raise self._exc_factory()
        return self._inner.blpop(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._inner, name)


def test_blocking_acquire_times_out_cleanly_despite_wait_errors(redis_client):
    """Every BLPOP raises TimeoutError; acquire must still end in
    AcquireTimeoutError (not a connection error) at ~acquire_timeout."""
    holder = Semaphore(redis_client, SemaphoreConfig(name="flaky", limit=1))
    holder.acquire()
    try:
        waiter = Semaphore(
            FlakyBlpopClient(redis_client, failures=10_000),
            SemaphoreConfig(name="flaky", limit=1, acquire_timeout=1.0, blpop_timeout=0.2),
        )
        start = time.monotonic()
        with pytest.raises(AcquireTimeoutError):
            waiter.acquire()
        assert time.monotonic() - start < 3.0
    finally:
        holder.release()


def test_blocking_acquire_recovers_after_transient_wait_error(redis_client):
    """One injected wait error, then the holder releases: the waiter must
    acquire successfully."""
    holder = Semaphore(redis_client, SemaphoreConfig(name="flaky-once", limit=1))
    holder.acquire()

    flaky = FlakyBlpopClient(redis_client, failures=1)
    waiter = Semaphore(
        flaky,
        SemaphoreConfig(name="flaky-once", limit=1, acquire_timeout=5.0, blpop_timeout=0.2),
    )

    releaser = threading.Timer(0.3, holder.release)
    releaser.start()
    try:
        result = waiter.acquire()
        assert result.success
        assert flaky.blpop_calls >= 1
    finally:
        releaser.cancel()
        waiter.release()


def test_permanent_error_during_wait_still_raises(redis_client):
    """A permanent rejection (e.g. ACL NOPERM on BLPOP) must not be swallowed."""
    holder = Semaphore(redis_client, SemaphoreConfig(name="denied", limit=1))
    holder.acquire()
    try:
        waiter = Semaphore(
            FlakyBlpopClient(
                redis_client,
                failures=1,
                exc_factory=lambda: redis.exceptions.NoPermissionError("NOPERM"),
            ),
            SemaphoreConfig(name="denied", limit=1, acquire_timeout=2.0),
        )
        with pytest.raises(PermanentBackendError):
            waiter.acquire()
    finally:
        holder.release()


@pytest.mark.asyncio
async def test_async_blocking_acquire_times_out_cleanly(async_redis_client):
    holder = Semaphore(async_redis_client, SemaphoreConfig(name="aflaky", limit=1))
    await holder.aacquire()

    # Inject the failure on the client instance itself so the adapter still
    # recognizes it as an async Redis client.
    async def failing_blpop(*args, **kwargs):
        raise redis.exceptions.TimeoutError("injected timeout")

    async_redis_client.blpop = failing_blpop
    try:
        waiter = Semaphore(
            async_redis_client,
            SemaphoreConfig(name="aflaky", limit=1, acquire_timeout=1.0, blpop_timeout=0.2),
        )
        with pytest.raises(AcquireTimeoutError):
            await waiter.aacquire()
    finally:
        del async_redis_client.blpop
        await holder.arelease()


def test_blpop_wait_capped_below_socket_timeout():
    """The per-wait BLPOP timeout must stay below the client socket_timeout."""
    client = redis.Redis(socket_timeout=1.0)
    sem = Semaphore(client, SemaphoreConfig(name="capped", limit=1, blpop_timeout=5.0))
    assert sem._blpop_wait_cap == pytest.approx(0.8)

    from redis_semaphore.semaphore import _AcquireState

    state = _AcquireState(start_time=time.monotonic())
    assert sem._blpop_timeout_for(state) == pytest.approx(0.8)


def test_blpop_wait_cap_absent_without_socket_timeout():
    client = redis.Redis()  # no socket_timeout configured
    sem = Semaphore(client, SemaphoreConfig(name="uncapped", limit=1))
    assert sem._blpop_wait_cap is None


def test_socket_timeout_race_survives_end_to_end(redis_client):
    """Integration for the original bug: socket_timeout == blpop_timeout used
    to raise RedisConnectionError from a blocking acquire after ~socket_timeout;
    now the wait is capped and acquire times out on schedule."""
    import os

    holder = Semaphore(redis_client, SemaphoreConfig(name="race", limit=1))
    holder.acquire()
    try:
        waiter_client = redis.Redis(
            port=int(os.environ.get("REDIS_PORT", "6379")), db=15, socket_timeout=1.0
        )
        waiter = Semaphore(
            waiter_client,
            SemaphoreConfig(name="race", limit=1, acquire_timeout=2.5, blpop_timeout=1.0),
        )
        start = time.monotonic()
        with pytest.raises(AcquireTimeoutError):
            waiter.acquire()
        elapsed = time.monotonic() - start
        assert 2.0 <= elapsed <= 4.0
        waiter_client.close()
    finally:
        holder.release()
