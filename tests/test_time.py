"""Tests for server time usage."""

import pytest

from redis_semaphore import Semaphore, SemaphoreConfig


def test_server_time_sync():
    class DummyClient:
        def time(self):
            return (10, 500_000)

    config = SemaphoreConfig(name="server-time-sync", limit=1, use_server_time=True)
    sem = Semaphore(DummyClient(), config)

    assert sem._get_time_ms() == 10_500


@pytest.mark.asyncio
async def test_server_time_async():
    class DummyClient:
        async def time(self):
            return (10, 500_000)

    config = SemaphoreConfig(name="server-time-async", limit=1, use_server_time=True)
    sem = Semaphore(DummyClient(), config)

    assert await sem._get_time_ms_async() == 10_500
