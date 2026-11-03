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
    - Clock synchronization: By default (time_source=TimeSource.SERVER) all
      timestamps are read on the Redis server inside the Lua scripts, so
      clients do NOT need synchronized clocks. Only with
      time_source=TimeSource.CLIENT (needed for Redis servers older than 3.2)
      must all clients be NTP-synchronized: a clock skew of more than a few
      seconds between clients breaks lock expiration.

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
from .dispatch import DispatchQueue, DispatchWorker, Job, JobHandler, default_worker_id
from .errors import (
    AcquireError,
    AcquireTimeoutError,
    AlreadyAcquiredError,
    BackendError,
    CommandDeniedError,
    DispatchError,
    JobError,
    JobFailedError,
    JobTimeoutError,
    LockLostError,
    MixedModeError,
    NotAcquiredError,
    PermanentBackendError,
    QueueFullError,
    RateLimitTimeoutError,
    RedisConnectionError,
    RedisSemaphoreError,
    ReleaseError,
    RequeueJob,
    TransientBackendError,
)
from .logger import logger, set_logger
from .metrics import PrometheusMetrics, set_metrics
from .rate_limiter import RateLimiter
from .semaphore import LockLostCallback, Mutex, Semaphore
from .types import (
    AcquireMode,
    AcquireResult,
    DispatchConfig,
    LockState,
    QueueStatus,
    RateLimit,
    RateLimiterConfig,
    RateLimiterStatus,
    RateLimitUsage,
    RatePermit,
    ReacquirePolicy,
    SemaphoreConfig,
    SemaphoreStatus,
    TimeSource,
    WorkerHealth,
)

__version__ = "0.2.0"

__all__ = [
    # Config types
    "AcquireMode",
    "AcquireResult",
    "DispatchConfig",
    "LockState",
    "QueueStatus",
    "RateLimit",
    "RateLimitUsage",
    "RateLimiterConfig",
    "RateLimiterStatus",
    "RatePermit",
    "ReacquirePolicy",
    "RedisConfig",
    "SemaphoreConfig",
    "SemaphoreStatus",
    "SentinelConfig",
    "TimeSource",
    "WorkerHealth",
    # Connection
    "RedisConnectionFactory",
    # Primitives
    "DispatchQueue",
    "DispatchWorker",
    "Job",
    "Mutex",
    "RateLimiter",
    "Semaphore",
    # Type aliases
    "JobHandler",
    "LockLostCallback",
    # Errors
    "AcquireError",
    "AcquireTimeoutError",
    "AlreadyAcquiredError",
    "BackendError",
    "CommandDeniedError",
    "DispatchError",
    "JobError",
    "JobFailedError",
    "JobTimeoutError",
    "LockLostError",
    "MixedModeError",
    "NotAcquiredError",
    "PermanentBackendError",
    "QueueFullError",
    "RateLimitTimeoutError",
    "RedisConnectionError",
    "RedisSemaphoreError",
    "ReleaseError",
    "RequeueJob",
    "TransientBackendError",
    # Helpers
    "default_worker_id",
    # Logging & Metrics
    "PrometheusMetrics",
    "logger",
    "set_logger",
    "set_metrics",
]
