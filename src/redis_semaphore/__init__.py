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

    - Lock loss on connection failure: The heartbeat tolerates transient Redis
      connection errors and keeps retrying at refresh_retry_interval. If the
      lock cannot be refreshed for lock_timeout seconds (the server-side TTL),
      it is treated as lost: on_lock_lost fires and, in strict_mode, the next
      operation raises LockLostError. To guarantee timely detection, keep the
      client's socket_timeout (or connection timeout) smaller than lock_timeout,
      otherwise a single hung refresh attempt can delay escalation past the
      deadline.
"""

from .connection import (
    RedisConfig,
    RedisConnectionFactory,
    SentinelConfig,
)
from .errors import (
    AcquireError,
    AcquireTimeoutError,
    BackendError,
    CommandDeniedError,
    LockLostError,
    MixedModeError,
    NotAcquiredError,
    PermanentBackendError,
    RedisConnectionError,
    RedisSemaphoreError,
    ReleaseError,
    TransientBackendError,
)
from .logger import logger, set_logger
from .metrics import PrometheusMetrics, set_metrics
from .semaphore import LockLostCallback, Mutex, Semaphore
from .types import (
    AcquireMode,
    AcquireResult,
    LockState,
    SemaphoreConfig,
    SemaphoreStatus,
)

__version__ = "0.1.1"

__all__ = [
    # Config types
    "AcquireMode",
    "AcquireResult",
    "LockState",
    "RedisConfig",
    "SemaphoreConfig",
    "SemaphoreStatus",
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
    "BackendError",
    "CommandDeniedError",
    "LockLostError",
    "MixedModeError",
    "NotAcquiredError",
    "PermanentBackendError",
    "RedisConnectionError",
    "RedisSemaphoreError",
    "ReleaseError",
    "TransientBackendError",
    # Logging & Metrics
    "PrometheusMetrics",
    "logger",
    "set_logger",
    "set_metrics",
]
