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


def test_redis_config_repr_hides_password():
    """Passwords must not leak into repr()/logs."""
    rendered = repr(RedisConfig(host="h", port=6380, password="super-secret"))
    assert "super-secret" not in rendered
    assert "password" not in rendered


def test_sentinel_config_repr_hides_passwords():
    """Both Sentinel passwords must be excluded from repr()."""
    rendered = repr(
        SentinelConfig(
            sentinels=[("h", 26379)],
            service_name="mymaster",
            password="data-secret",
            sentinel_password="sentinel-secret",
        )
    )
    assert "data-secret" not in rendered
    assert "sentinel-secret" not in rendered


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


def test_client_kwargs_optional_params():
    """Optional production params reach the client's connection kwargs."""
    config = RedisConfig(
        host="localhost",
        port=_redis_port(),
        client_name="sem-test",
        health_check_interval=10.0,
        socket_connect_timeout=2.0,
    )
    client = RedisConnectionFactory.create_sync(config)
    kwargs = client.get_connection_kwargs()
    assert kwargs["client_name"] == "sem-test"
    assert kwargs["health_check_interval"] == 10.0
    assert kwargs["socket_connect_timeout"] == 2.0
    client.close()


def test_client_kwargs_ssl():
    """ssl=True switches the pool to an SSL connection class (no connect)."""
    ca_path = "/etc/ssl/certs/test-ca.pem"  # never opened: the client is lazy
    config = RedisConfig(ssl=True, ssl_ca_certs=ca_path, ssl_cert_reqs="none")
    client = RedisConnectionFactory.create_sync(config)
    assert "SSL" in client.connection_pool.connection_class.__name__
    assert client.get_connection_kwargs()["ssl_ca_certs"] == ca_path
    client.close()


def test_client_extra_kwargs_override():
    """extra_kwargs is an escape hatch that wins over the named fields."""
    config = RedisConfig(socket_timeout=5.0, extra_kwargs={"socket_timeout": 1.5})
    client = RedisConnectionFactory.create_sync(config)
    assert client.get_connection_kwargs()["socket_timeout"] == 1.5
    client.close()


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
