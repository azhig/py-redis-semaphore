"""Tests for RedisConnectionFactory."""

import os

import pytest

from redis_semaphore import (
    RedisConfig,
    RedisConnectionFactory,
    SentinelConfig,
)


def _redis_port() -> int:
    return int(os.environ.get("REDIS_PORT", "6379"))


def _parse_sentinel_hosts(raw_hosts: str) -> list[tuple[str, int]]:
    hosts = []
    for item in raw_hosts.split(","):
        item = item.strip()
        if not item:
            continue
        if ":" not in item:
            raise ValueError(f"Invalid sentinel host '{item}'")
        host, port = item.rsplit(":", 1)
        hosts.append((host, int(port)))
    return hosts


def _sentinel_config() -> SentinelConfig:
    raw_hosts = os.environ.get("REDIS_SENTINEL_HOSTS")
    if not raw_hosts:
        pytest.skip("REDIS_SENTINEL_HOSTS not set")
    service_name = os.environ.get("REDIS_SENTINEL_SERVICE", "mymaster")
    password = os.environ.get("REDIS_SENTINEL_PASSWORD")
    return SentinelConfig(
        sentinels=_parse_sentinel_hosts(raw_hosts),
        service_name=service_name,
        password=password,
    )


def test_create_sync_direct():
    """Factory creates a direct sync client."""
    config = RedisConfig(host="localhost", port=_redis_port(), db=15)
    client = RedisConnectionFactory.create_sync(config)
    assert client.ping() is True
    client.close()


@pytest.mark.asyncio
async def test_create_async_direct():
    """Factory creates a direct async client."""
    config = RedisConfig(host="localhost", port=_redis_port(), db=15)
    client = RedisConnectionFactory.create_async(config)
    assert await client.ping() is True
    await client.aclose()


def test_create_sync_sentinel():
    """Factory creates a sentinel sync client."""
    config = _sentinel_config()
    client = RedisConnectionFactory.create_sync(config)
    assert client.ping() is True
    client.close()


@pytest.mark.asyncio
async def test_create_async_sentinel():
    """Factory creates a sentinel async client."""
    config = _sentinel_config()
    client = RedisConnectionFactory.create_async(config)
    assert await client.ping() is True
    await client.aclose()
