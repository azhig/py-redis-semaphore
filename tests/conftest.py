"""Pytest fixtures for redis-semaphore tests."""

import os

import pytest
import redis
import redis.asyncio as aioredis


def _redis_port() -> int:
    return int(os.environ.get("REDIS_PORT", "6379"))


@pytest.fixture
def redis_client():
    """Create a synchronous Redis client."""
    client = redis.Redis(host="localhost", port=_redis_port(), db=15)
    client.flushdb()
    yield client
    client.flushdb()
    client.close()


@pytest.fixture
async def async_redis_client():
    """Create an asynchronous Redis client."""
    client = aioredis.Redis(host="localhost", port=_redis_port(), db=15)
    await client.flushdb()
    yield client
    await client.flushdb()
    await client.aclose()
