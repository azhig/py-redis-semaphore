"""Redis connection factory with Sentinel support."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import redis
import redis.asyncio as aioredis
from redis.asyncio.sentinel import Sentinel as AsyncSentinel
from redis.sentinel import Sentinel


@dataclass
class RedisConfig:
    """Configuration for direct Redis connection.

    Attributes:
        socket_timeout: Socket read timeout in seconds. Keep it smaller than
            lock_timeout (timely heartbeat escalation); BLPOP waits are
            automatically capped below it.
        socket_connect_timeout: TCP connect timeout (None = socket_timeout).
        ssl: Enable TLS. The ssl_* fields below are only applied when True.
        health_check_interval: Ping connections idle for this many seconds
            before reuse (0 = disabled). Helps survive silent connection drops.
        retry_on_timeout: Retry a command once after a socket timeout.
        client_name: CLIENT SETNAME value, visible in CLIENT LIST.
        extra_kwargs: Escape hatch - passed verbatim to the redis.Redis
            constructor, overriding any of the above.
    """

    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: str | None = field(default=None, repr=False)
    socket_timeout: float = 5.0
    socket_connect_timeout: float | None = None
    ssl: bool = False
    ssl_ca_certs: str | None = None
    ssl_certfile: str | None = None
    ssl_keyfile: str | None = None
    ssl_cert_reqs: str = "required"
    health_check_interval: float = 0.0
    retry_on_timeout: bool = False
    client_name: str | None = None
    extra_kwargs: dict[str, Any] = field(default_factory=dict)

    def _client_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "host": self.host,
            "port": self.port,
            "db": self.db,
            "password": self.password,
            "socket_timeout": self.socket_timeout,
            "health_check_interval": self.health_check_interval,
        }
        # Optional params are passed only when set: redis-py 6 deprecates
        # retry_on_timeout, so an always-passed default would warn.
        if self.retry_on_timeout:
            kwargs["retry_on_timeout"] = True
        if self.client_name is not None:
            kwargs["client_name"] = self.client_name
        if self.socket_connect_timeout is not None:
            kwargs["socket_connect_timeout"] = self.socket_connect_timeout
        if self.ssl:
            kwargs.update(
                ssl=True,
                ssl_ca_certs=self.ssl_ca_certs,
                ssl_certfile=self.ssl_certfile,
                ssl_keyfile=self.ssl_keyfile,
                ssl_cert_reqs=self.ssl_cert_reqs,
            )
        kwargs.update(self.extra_kwargs)
        return kwargs


@dataclass
class SentinelConfig:
    """Configuration for Redis Sentinel connection.

    Attributes:
        health_check_interval / retry_on_timeout / client_name: Applied to the
            master client returned by master_for (see RedisConfig).
        extra_kwargs: Passed verbatim to master_for, overriding the above.
        sentinel_extra_kwargs: Passed verbatim to the Sentinel constructor
            itself (e.g. TLS options for the sentinel connections).
    """

    sentinels: list[tuple[str, int]]
    service_name: str
    password: str | None = field(default=None, repr=False)
    sentinel_password: str | None = field(default=None, repr=False)
    db: int = 0
    socket_timeout: float = 5.0
    min_other_sentinels: int = 0
    health_check_interval: float = 0.0
    retry_on_timeout: bool = False
    client_name: str | None = None
    extra_kwargs: dict[str, Any] = field(default_factory=dict)
    sentinel_extra_kwargs: dict[str, Any] = field(default_factory=dict)

    def _master_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "password": self.password,
            "db": self.db,
            "socket_timeout": self.socket_timeout,
            "health_check_interval": self.health_check_interval,
        }
        if self.retry_on_timeout:
            kwargs["retry_on_timeout"] = True
        if self.client_name is not None:
            kwargs["client_name"] = self.client_name
        kwargs.update(self.extra_kwargs)
        return kwargs


ConnectionConfig = RedisConfig | SentinelConfig


class RedisConnectionFactory:
    """Factory for creating Redis connections."""

    @staticmethod
    def create_sync(config: ConnectionConfig) -> redis.Redis:
        """Create a synchronous Redis client."""
        if isinstance(config, SentinelConfig):
            sentinel = Sentinel(
                config.sentinels,
                socket_timeout=config.socket_timeout,
                password=config.sentinel_password,
                min_other_sentinels=config.min_other_sentinels,
                **config.sentinel_extra_kwargs,
            )
            return sentinel.master_for(config.service_name, **config._master_kwargs())
        else:
            return redis.Redis(**config._client_kwargs())

    @staticmethod
    def create_async(config: ConnectionConfig) -> aioredis.Redis:
        """Create an asynchronous Redis client."""
        if isinstance(config, SentinelConfig):
            sentinel = AsyncSentinel(
                config.sentinels,
                socket_timeout=config.socket_timeout,
                password=config.sentinel_password,
                min_other_sentinels=config.min_other_sentinels,
                **config.sentinel_extra_kwargs,
            )
            return sentinel.master_for(config.service_name, **config._master_kwargs())
        else:
            return aioredis.Redis(**config._client_kwargs())
