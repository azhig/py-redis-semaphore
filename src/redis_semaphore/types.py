"""Types and configuration for redis-semaphore."""

import warnings
from dataclasses import dataclass, field
from enum import Enum


class LockState(Enum):
    """State of a lock/semaphore."""

    RELEASED = "released"
    ACQUIRED = "acquired"
    LOST = "lost"


class AcquireMode(str, Enum):
    """Strategy for waiting when semaphore is full.

    POLLING: Retry at fixed/backoff intervals (simple, higher Redis load).
    BLPOP (default): Block on Redis list for notifications (efficient,
        approximately FIFO - a freshly arriving client can still overtake a
        woken waiter in the race for the freed slot).
    """

    POLLING = "polling"
    BLPOP = "blpop"


class TimeSource(str, Enum):
    """Where "now" comes from for TTL math.

    SERVER (default): Time is read via ``TIME`` *inside* the Lua scripts, so
        every client shares the Redis server clock. No NTP synchronization
        between clients is required and no extra round-trip is paid.
        Requires Redis >= 3.2 (scripts call ``redis.replicate_commands()``;
        on Redis 7+ effect replication is always on).
    CLIENT: Timestamps are taken from the client clock and passed to the
        scripts. All clients must be NTP-synchronized; skew of a few seconds
        breaks lock expiration. Only needed for Redis servers older than 3.2.
    """

    SERVER = "server"
    CLIENT = "client"


class ReacquirePolicy(str, Enum):
    """What acquire() does when this instance already holds a slot.

    RENEW (default): The slot TTL is renewed and a NEW fencing token is
        issued (monotonicity is preserved, but a token you previously handed
        to downstream systems becomes stale). There is no hold count: a
        single release() frees the slot regardless of how many times it was
        re-acquired.
    FORBID: acquire()/aacquire() on an instance that already holds a slot
        raises AlreadyAcquiredError. Choose this to surface accidental
        double-acquire bugs instead of silently renewing.
    """

    RENEW = "renew"
    FORBID = "forbid"


@dataclass(frozen=True, slots=True)
class AcquireResult:
    """Result of an acquire operation.

    Attributes:
        used_slots: Slots occupied as observed atomically by the acquire call
            (includes the caller on success). Useful for observability.
    """

    success: bool
    identifier: str | None
    fencing_token: int | None
    expires_at: float | None
    used_slots: int | None = None


@dataclass(frozen=True, slots=True)
class SemaphoreStatus:
    """Point-in-time view of a semaphore, observed without acquiring.

    Reading the status atomically removes expired owner entries first, so
    ``used_slots`` reflects live holders only.

    Attributes:
        name: Semaphore name.
        limit: Maximum number of concurrent holders.
        used_slots: Live (non-expired) holders at observation time.
        available: ``max(0, limit - used_slots)`` free slots.
        is_owner: Whether the calling instance currently holds a slot.
        expires_at: Epoch seconds when the caller's slot expires, or None if
            the caller is not an owner. With ``TimeSource.SERVER`` this is
            the Redis server's epoch, which may differ slightly from the
            client clock.
    """

    name: str
    limit: int
    used_slots: int
    available: int
    is_owner: bool
    expires_at: float | None = None


@dataclass(slots=True)
class SemaphoreConfig:
    """Configuration for a semaphore.

    Attributes:
        name: Unique name for the semaphore.
        limit: Maximum number of concurrent holders.
        lock_timeout: TTL for the lock in seconds.
        acquire_timeout: Maximum time to wait for acquire (None = infinite).
        retry_interval: Time between acquire retries (polling mode).
        refresh_interval: Heartbeat interval (default: lock_timeout / 3, so
            roughly two thirds of the TTL remains as retry budget when Redis
            is briefly unreachable).
        namespace: Redis key prefix.
        strict_mode: If True, raises LockLostError immediately when lock is lost.
            Recommended for mission-critical systems to prevent zombie processes.
        use_server_time: Deprecated alias for ``time_source=TimeSource.SERVER``
            (which is now the default). Passing True emits a DeprecationWarning;
            combining it with ``time_source=TimeSource.CLIENT`` is an error.
        acquire_mode: Wait strategy - BLPOP (blocking, default) or POLLING
            (retry loop).
        retry_interval_max: Max retry interval for exponential backoff (None = no backoff).
        retry_backoff_multiplier: Multiplier for exponential backoff (default: 2.0).
            The exponent is the number of completed wait rounds, so intervals
            grow as base, base*m, base*m^2, ... up to retry_interval_max.
        retry_jitter: Random jitter as fraction of interval, 0.0-1.0
            (default: 0.1 - avoids synchronized wakeups of many waiters).
        blpop_timeout: Timeout for BLPOP before fallback retry (default: 5.0).
            At runtime the effective per-wait timeout is additionally capped
            below the client's socket_timeout to avoid spurious read timeouts.
        refresh_retry_interval: Step between heartbeat refresh retries after a
            connection error, in seconds (None = min(refresh_interval, 1.0)).
            When the heartbeat cannot refresh the lock (e.g. Redis is
            unreachable) it retries at this shorter interval until either the
            connection recovers or lock_timeout elapses since the last
            successful refresh, at which point the lock is treated as lost.
        time_source: Clock used for TTL math (default: TimeSource.SERVER -
            time is taken inside the Lua scripts via ``TIME``, so clients do
            not need synchronized clocks). Set TimeSource.CLIENT to restore
            the pre-0.2 behavior (client clocks + NTP requirement), e.g. for
            Redis servers older than 3.2.
        cluster_hash_tags: If True, keys are formatted as
            ``{namespace:name}:owners`` etc., with a literal hash-tag brace
            block, so all keys of one semaphore map to the same Redis Cluster
            slot. Default False. WARNING: changing this on an existing
            deployment changes key names - in-flight holders under the old
            keys will not be seen by clients using the new keys.
        reacquire_policy: Behavior of acquire() when this instance already
            holds a slot (default: ReacquirePolicy.RENEW - renew TTL and
            issue a new fencing token; see ReacquirePolicy for caveats).
    """

    name: str
    limit: int
    lock_timeout: float = 30.0
    acquire_timeout: float | None = None
    retry_interval: float = 0.1
    refresh_interval: float | None = None
    namespace: str = "semaphore"
    strict_mode: bool = False
    use_server_time: bool = False
    acquire_mode: AcquireMode = AcquireMode.BLPOP
    retry_interval_max: float | None = None
    retry_backoff_multiplier: float = 2.0
    retry_jitter: float = 0.1
    blpop_timeout: float = 5.0
    refresh_retry_interval: float | None = None
    time_source: TimeSource = TimeSource.SERVER
    cluster_hash_tags: bool = False
    reacquire_policy: ReacquirePolicy = field(default=ReacquirePolicy.RENEW)

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("name must not be empty")
        if self.limit < 1:
            raise ValueError("limit must be >= 1")
        if self.lock_timeout <= 0:
            raise ValueError("lock_timeout must be > 0")
        if self.acquire_timeout is not None and self.acquire_timeout <= 0:
            raise ValueError("acquire_timeout must be > 0 or None")
        if self.retry_interval <= 0:
            raise ValueError("retry_interval must be > 0")
        if self.refresh_interval is not None and self.refresh_interval <= 0:
            raise ValueError("refresh_interval must be > 0 or None")
        if self.retry_interval_max is not None and self.retry_interval_max < self.retry_interval:
            raise ValueError("retry_interval_max must be >= retry_interval")
        if self.retry_backoff_multiplier < 1.0:
            raise ValueError("retry_backoff_multiplier must be >= 1.0")
        if not 0.0 <= self.retry_jitter <= 1.0:
            raise ValueError("retry_jitter must be between 0.0 and 1.0")
        if self.blpop_timeout <= 0:
            raise ValueError("blpop_timeout must be > 0")
        if self.refresh_retry_interval is not None and self.refresh_retry_interval <= 0:
            raise ValueError("refresh_retry_interval must be > 0 or None")
        if self.use_server_time:
            if self.time_source is TimeSource.CLIENT:
                raise ValueError(
                    "use_server_time=True contradicts time_source=TimeSource.CLIENT; "
                    "drop the deprecated use_server_time flag"
                )
            warnings.warn(
                "use_server_time is deprecated: server time is now the default "
                "(time_source=TimeSource.SERVER, computed inside Lua scripts). "
                "Remove the flag, or set time_source explicitly.",
                DeprecationWarning,
                stacklevel=3,
            )
