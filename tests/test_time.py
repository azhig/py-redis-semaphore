"""Tests for the time source machinery (server time in Lua vs client time)."""

import time

import pytest

from redis_semaphore import Semaphore, SemaphoreConfig, TimeSource


class _NoopClient:
    """Client stub: time-source resolution never talks to Redis."""


def test_server_time_passes_empty_arg():
    """SERVER mode (default) sends '' so the script reads TIME on the server."""
    config = SemaphoreConfig(name="server-time", limit=1)
    sem = Semaphore(_NoopClient(), config)

    assert config.time_source is TimeSource.SERVER
    assert sem._now_ms_arg() == ""


def test_client_time_passes_local_milliseconds():
    config = SemaphoreConfig(name="client-time", limit=1, time_source=TimeSource.CLIENT)
    sem = Semaphore(_NoopClient(), config)

    now_arg = sem._now_ms_arg()
    assert isinstance(now_arg, int)
    assert abs(now_arg - time.time() * 1000) < 5_000


def test_use_server_time_is_deprecated_alias():
    with pytest.deprecated_call():
        config = SemaphoreConfig(name="deprecated-flag", limit=1, use_server_time=True)
    assert config.time_source is TimeSource.SERVER


def test_use_server_time_conflicts_with_client_source():
    with pytest.raises(ValueError):
        SemaphoreConfig(
            name="conflict",
            limit=1,
            use_server_time=True,
            time_source=TimeSource.CLIENT,
        )


def test_server_time_acquire_cycle(redis_client):
    """Full cycle on a real Redis with in-script server time."""
    config = SemaphoreConfig(name="server-time-cycle", limit=1, lock_timeout=10.0)
    sem = Semaphore(redis_client, config)

    result = sem.acquire()
    assert result.success
    # Server clock and test clock agree within seconds on the same host.
    assert result.expires_at is not None
    assert abs(result.expires_at - (time.time() + 10.0)) < 5.0

    status = sem.status()
    assert status.is_owner
    assert status.used_slots == 1

    assert sem.refresh() is True
    assert sem.release() is True


def test_client_time_acquire_cycle(redis_client):
    config = SemaphoreConfig(
        name="client-time-cycle", limit=1, lock_timeout=10.0, time_source=TimeSource.CLIENT
    )
    sem = Semaphore(redis_client, config)

    result = sem.acquire()
    assert result.success
    assert sem.refresh() is True
    assert sem.release() is True


@pytest.mark.asyncio
async def test_server_time_acquire_cycle_async(async_redis_client):
    config = SemaphoreConfig(name="server-time-async", limit=1, lock_timeout=10.0)
    sem = Semaphore(async_redis_client, config)

    result = await sem.aacquire()
    assert result.success
    assert (await sem.astatus()).is_owner
    assert await sem.arefresh() is True
    assert await sem.arelease() is True
