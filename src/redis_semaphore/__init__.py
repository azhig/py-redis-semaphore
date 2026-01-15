"""redis-semaphore: Distributed semaphore and mutex with Redis support.

Sync usage:
    from redis_semaphore import Semaphore, Mutex, SemaphoreConfig
    import redis

    client = redis.Redis()

    # Counting semaphore (up to 5 concurrent accesses)
    config = SemaphoreConfig(name="my-resource", limit=5)
    with Semaphore(client, config) as sem:
        print(f"Got slot with fencing token: {sem.fencing_token}")

    # Mutex (exclusive lock)
    with Mutex(client, "my-lock") as lock:
        print("Exclusive access")

Async usage:
    import redis.asyncio as aioredis
    from redis_semaphore import Mutex

    client = aioredis.Redis()

    async with Mutex(client, "my-lock") as lock:
        print("Async exclusive access")

Sentinel support:
    from redis_semaphore import SentinelConfig, RedisConnectionFactory

    config = SentinelConfig(
        sentinels=[("localhost", 26379)],
        service_name="mymaster",
    )
    client = RedisConnectionFactory.create_sync(config)

Important notes:
    - Clock synchronization: This library uses client-side timestamps for TTL
      calculations. Ensure all clients are NTP-synchronized to avoid issues
      with lock expiration. A clock skew of more than a few seconds between
      clients can cause unexpected lock behavior.

    - Do not mix sync/async APIs: Use acquire()/release() OR aacquire()/arelease()
      on the same Semaphore instance, but not both. Mixing will raise MixedModeError.

    - Fencing tokens: Each acquire() returns a monotonically increasing fencing
      token that can be used to detect stale operations. Pass this token to
      downstream services to reject out-of-order writes.
"""

from .connection import (
    RedisConfig,
    RedisConnectionFactory,
    SentinelConfig,
)
from .errors import (
    AcquireError,
    AcquireTimeoutError,
    LockLostError,
    MixedModeError,
    NotAcquiredError,
    RedisConnectionError,
    RedisSemaphoreError,
    RefreshError,
    ReleaseError,
)
from .logger import logger, set_logger
from .metrics import PrometheusMetrics, set_metrics
from .semaphore import LockLostCallback, Mutex, Semaphore
from .types import (
    AcquireMode,
    AcquireResult,
    LockState,
    SemaphoreConfig,
)

__version__ = "0.1.0"

__all__ = [
    # Config types
    "AcquireMode",
    "AcquireResult",
    "LockState",
    "RedisConfig",
    "SemaphoreConfig",
    "SentinelConfig",
    # Connection
    "RedisConnectionFactory",
    # Primitives
    "Mutex",
    "Semaphore",
    # Type aliases
    "LockLostCallback",
    # Errors
    "AcquireError",
    "AcquireTimeoutError",
    "LockLostError",
    "MixedModeError",
    "NotAcquiredError",
    "RedisConnectionError",
    "RedisSemaphoreError",
    "RefreshError",
    "ReleaseError",
    # Logging & Metrics
    "PrometheusMetrics",
    "logger",
    "set_logger",
    "set_metrics",
]
