"""Tests for Redis Sentinel connection factory."""

import os

import pytest

from redis_semaphore import RedisConnectionFactory, SentinelConfig


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


def test_sentinel_connection():
    """Connects to Sentinel when configured via environment variables."""
    raw_hosts = os.environ.get("REDIS_SENTINEL_HOSTS")
    if not raw_hosts:
        pytest.skip("REDIS_SENTINEL_HOSTS not set")

    service_name = os.environ.get("REDIS_SENTINEL_SERVICE", "mymaster")
    password = os.environ.get("REDIS_SENTINEL_PASSWORD")

    config = SentinelConfig(
        sentinels=_parse_sentinel_hosts(raw_hosts),
        service_name=service_name,
        password=password,
    )

    client = RedisConnectionFactory.create_sync(config)
    assert client.ping() is True
