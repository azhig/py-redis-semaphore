"""Redis connection factory with Sentinel support."""

from __future__ import annotations

from dataclasses import dataclass

import redis
import redis.asyncio as aioredis
from redis.asyncio.sentinel import Sentinel as AsyncSentinel
from redis.sentinel import Sentinel


@dataclass
class RedisConfig:
    """Configuration for direct Redis connection."""

    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: str | None = None
    socket_timeout: float = 5.0


@dataclass
class SentinelConfig:
    """Configuration for Redis Sentinel connection."""

    sentinels: list[tuple[str, int]]
    service_name: str
    password: str | None = None
    sentinel_password: str | None = None
    db: int = 0
    socket_timeout: float = 5.0
    min_other_sentinels: int = 0


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
            )
            return sentinel.master_for(
                config.service_name,
                password=config.password,
                db=config.db,
                socket_timeout=config.socket_timeout,
            )
        else:
            return redis.Redis(
                host=config.host,
                port=config.port,
                db=config.db,
                password=config.password,
                socket_timeout=config.socket_timeout,
            )

    @staticmethod
    def create_async(config: ConnectionConfig) -> aioredis.Redis:
        """Create an asynchronous Redis client."""
        if isinstance(config, SentinelConfig):
            sentinel = AsyncSentinel(
                config.sentinels,
                socket_timeout=config.socket_timeout,
                password=config.sentinel_password,
                min_other_sentinels=config.min_other_sentinels,
            )
            return sentinel.master_for(
                config.service_name,
                password=config.password,
                db=config.db,
                socket_timeout=config.socket_timeout,
            )
        else:
            return aioredis.Redis(
                host=config.host,
                port=config.port,
                db=config.db,
                password=config.password,
                socket_timeout=config.socket_timeout,
            )
