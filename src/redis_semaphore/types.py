"""Types and configuration for redis-semaphore."""

import warnings
from collections.abc import Sequence
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


@dataclass(frozen=True, slots=True)
class RateLimit:
    """One sliding-window limit of a rate limiter.

    Attributes:
        name: Limit name, used as the cost key and in Redis key names
            (e.g. "requests", "tokens"). Must not contain ':' - unlike the
            limiter name, which may be any string.
        limit: Maximum number of units consumed within any ``window``.
        window: Sliding window length in seconds (60.0 for "per minute").
    """

    name: str
    limit: int
    window: float = 60.0

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("RateLimit name must not be empty")
        if ":" in self.name:
            # Keys are "<namespace>:<limiter name>:<limit name>:log". Limiter
            # names may contain ':' freely, so the limit name must not -
            # otherwise "a" + "b:x" and "a:b" + "x" would share Redis keys.
            raise ValueError(f"RateLimit name must not contain ':' (got {self.name!r})")
        if self.limit < 1:
            raise ValueError("RateLimit limit must be >= 1")
        if self.window < 0.001:
            raise ValueError("RateLimit window must be >= 0.001 (1 ms)")


@dataclass(slots=True)
class RateLimiterConfig:
    """Configuration for a rate limiter.

    Attributes:
        name: Unique name of the rate-limited resource (e.g. a certificate id).
        limits: Limits checked atomically on every acquire; a request is
            admitted only if it fits all of them.
        acquire_timeout: Maximum time a blocking acquire may wait (None =
            infinite). Fails fast: if the earliest admission time computed by
            Redis lies beyond the remaining timeout, the acquire raises right
            away instead of sleeping until the deadline.
        wait_jitter: Maximum random extra delay in seconds added to each wait
            (default 0.05), so waiters woken by the same expiry do not retry
            in lockstep.
        namespace: Redis key prefix.
        time_source: Clock used for window math (default: TimeSource.SERVER;
            see TimeSource).
        cluster_hash_tags: If True, keys are formatted as
            ``{namespace:name}:...`` so all keys of one limiter map to the
            same Redis Cluster slot. Changes key names.
    """

    name: str
    limits: Sequence[RateLimit]
    acquire_timeout: float | None = None
    wait_jitter: float = 0.05
    namespace: str = "ratelimit"
    time_source: TimeSource = TimeSource.SERVER
    cluster_hash_tags: bool = False

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("name must not be empty")
        self.limits = tuple(self.limits)
        if not self.limits:
            raise ValueError("limits must not be empty")
        names = [limit.name for limit in self.limits]
        if len(set(names)) != len(names):
            raise ValueError("limit names must be unique")
        if self.acquire_timeout is not None and self.acquire_timeout <= 0:
            raise ValueError("acquire_timeout must be > 0 or None")
        if self.wait_jitter < 0:
            raise ValueError("wait_jitter must be >= 0")


@dataclass(slots=True)
class RatePermit:
    """Result of a rate limiter acquire.

    On success the consumed units are recorded in Redis under ``identifier``;
    pass the permit to ``adjust()`` once the real cost is known (e.g. tokens
    actually used by an LLM response). ``adjust()`` updates ``costs`` in place.

    Attributes:
        success: Whether the request was admitted.
        identifier: Unique id of the recorded consumption (None on failure).
        costs: Units charged per limit name.
        acquired_at: Epoch seconds of admission, or None on failure. With
            ``TimeSource.SERVER`` this is the Redis server's clock.
        retry_after: On failure, seconds until the request would fit
            (computed atomically by Redis); None on success.
        used: Units consumed per limit within its window, as observed by the
            call (including this request on success).
        limiter: Identity (``namespace:name``) of the limiter that issued the
            permit; ``adjust()`` refuses permits of other limiters.
    """

    success: bool
    identifier: str | None
    costs: dict[str, int]
    acquired_at: float | None
    retry_after: float | None
    used: dict[str, int] = field(default_factory=dict)
    limiter: str | None = None


@dataclass(frozen=True, slots=True)
class RateLimitUsage:
    """Point-in-time consumption of one limit."""

    name: str
    limit: int
    window: float
    used: int

    @property
    def available(self) -> int:
        """Units that still fit into the current window."""
        return max(0, self.limit - self.used)


@dataclass(slots=True)
class DispatchConfig:
    """Configuration of a shared dispatch queue (DispatchQueue/DispatchWorker).

    Producers and workers of the same queues must use the same namespace,
    group and cluster_hash_tags.

    Attributes:
        namespace: Redis key prefix.
        group: Consumer group name of the queue streams.
        max_queue_size: Max jobs per queue (waiting + in progress); submit
            raises QueueFullError beyond it. None = unbounded.
        leader_ttl: TTL of a queue's leadership Mutex. A dead worker's
            queues get a new leader after at most this long.
        election_interval: Mean pause between a worker's attempts to take
            one more free queue (None = leader_ttl / 3). Every attempt on a
            busy queue is a failed non-blocking Mutex acquire - it shows up
            in the semaphore metrics as result="busy" - and probing much
            faster than leader_ttl buys nothing, so keep this coarse.
        max_admitted: Max admitted jobs of one queue running concurrently on
            its leader (None = unlimited). At the cap the leader waits for a
            handler to finish before starting the next job (FIFO is kept).
        reclaim_idle: A job taken by a worker that has not been touched for
            this long is presumed abandoned (its worker died) and is re-run
            by the queue's current leader. Live workers touch their jobs every
            reclaim_idle / 3.
        reclaim_interval: How often a leader looks for abandoned jobs.
        reply_ttl: Seconds a result waits for its submitter.
        read_timeout: Max block of one stream read; also how quickly an idle
            leader notices stop(). Capped below the client socket_timeout.
        max_queues_per_worker: Max queues one worker leads (None = no cap).
            With a cap, make sure workers x cap >= queues, or some queues
            stay without a leader.
        cluster_hash_tags: If True, keys of one queue share a Redis Cluster
            hash tag (``{namespace:queue}:...``). Changes key names.
    """

    namespace: str = "dispatch"
    group: str = "dispatchers"
    max_queue_size: int | None = None
    leader_ttl: float = 10.0
    election_interval: float | None = None
    reclaim_idle: float = 30.0
    reclaim_interval: float = 5.0
    reply_ttl: float = 60.0
    read_timeout: float = 1.0
    max_queues_per_worker: int | None = None
    max_admitted: int | None = None
    cluster_hash_tags: bool = False

    def __post_init__(self) -> None:
        if not self.namespace:
            raise ValueError("namespace must not be empty")
        if not self.group:
            raise ValueError("group must not be empty")
        if self.max_queue_size is not None and self.max_queue_size < 1:
            raise ValueError("max_queue_size must be >= 1 or None")
        if self.max_queues_per_worker is not None and self.max_queues_per_worker < 1:
            raise ValueError("max_queues_per_worker must be >= 1 or None")
        if self.max_admitted is not None and self.max_admitted < 1:
            raise ValueError("max_admitted must be >= 1 or None")
        if self.election_interval is not None and self.election_interval <= 0:
            raise ValueError("election_interval must be > 0 or None")
        for name in (
            "leader_ttl",
            "reclaim_idle",
            "reclaim_interval",
            "reply_ttl",
            "read_timeout",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be > 0")


@dataclass(frozen=True, slots=True)
class QueueStatus:
    """Point-in-time view of a dispatch queue (the same from every process).

    Attributes:
        name: Queue name.
        length: Jobs in the queue: waiting + in progress.
        in_progress: Jobs taken by a worker and not finished yet - the one
            waiting for admission plus the admitted ones still running.
        waiting: Jobs nobody has taken yet.
        oldest_age: Seconds since the oldest job in the queue was submitted
            (Redis server clock), or None if the queue is empty. Growing
            steadily means the queue is stuck or starved.
        has_leader: Whether some worker currently holds the queue's
            leadership. False with a non-empty queue means nobody serves it.
    """

    name: str
    length: int
    in_progress: int
    waiting: int
    oldest_age: float | None
    has_leader: bool


@dataclass(frozen=True, slots=True)
class WorkerHealth:
    """Local health of one DispatchWorker (no Redis round-trip).

    Attributes:
        worker_id: The worker id (also its consumer name in the streams).
        running: run() is active.
        stopping: stop() was requested and shutdown is in progress.
        leading: Queues this worker currently leads.
        jobs_in_progress: Handlers running in this worker (waiting for
            admission or admitted).
        last_loop_age: Seconds since the worker's main loop last ran, or None
            if it is not running. Stays below ~1.5 x election_interval while
            the worker is alive.
        last_error: The last Redis error seen by elections/leaders, or None.
            Cleared by the next successful election round.
        last_error_age: Seconds since last_error, or None.
        healthy: running, not stopping, the main loop is not stalled and no
            Redis error in the last stall limit (5 x election_interval).
            Suits a readiness probe; for a liveness probe check only
            ``running`` and ``last_loop_age`` - restarting pods does not fix a
            Redis outage.
    """

    worker_id: str
    running: bool
    stopping: bool
    leading: tuple[str, ...]
    jobs_in_progress: int
    last_loop_age: float | None
    last_error: str | None
    last_error_age: float | None
    healthy: bool


@dataclass(frozen=True, slots=True)
class RateLimiterStatus:
    """Point-in-time view of a rate limiter, observed without consuming.

    Attributes:
        name: Rate limiter name.
        usage: Consumption per limit name.
    """

    name: str
    usage: dict[str, RateLimitUsage]
