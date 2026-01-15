"""Types and configuration for redis-semaphore."""

from dataclasses import dataclass
from enum import Enum


class LockState(Enum):
    """State of a lock/semaphore."""

    RELEASED = "released"
    ACQUIRED = "acquired"
    LOST = "lost"


class AcquireMode(str, Enum):
    """Strategy for waiting when semaphore is full.

    POLLING: Retry at fixed/backoff intervals (simple, higher Redis load).
    BLPOP: Block on Redis list for notifications (efficient, FIFO ordering).
    """

    POLLING = "polling"
    BLPOP = "blpop"


@dataclass(frozen=True, slots=True)
class AcquireResult:
    """Result of an acquire operation."""

    success: bool
    identifier: str | None
    fencing_token: int | None
    expires_at: float | None


@dataclass(slots=True)
class SemaphoreConfig:
    """Configuration for a semaphore.

    Attributes:
        name: Unique name for the semaphore.
        limit: Maximum number of concurrent holders.
        lock_timeout: TTL for the lock in seconds.
        acquire_timeout: Maximum time to wait for acquire (None = infinite).
        retry_interval: Time between acquire retries (polling mode).
        refresh_interval: Heartbeat interval (default: 80% of lock_timeout).
        namespace: Redis key prefix.
        strict_mode: If True, raises LockLostError immediately when lock is lost.
            Recommended for mission-critical systems to prevent zombie processes.
        use_server_time: If True, uses Redis server time instead of client time.
            Helps avoid clock skew issues but adds one extra RTT per operation.
        acquire_mode: Wait strategy - POLLING (retry loop) or BLPOP (blocking).
        retry_interval_max: Max retry interval for exponential backoff (None = no backoff).
        retry_backoff_multiplier: Multiplier for exponential backoff (default: 2.0).
        retry_jitter: Random jitter as fraction of interval, 0.0-1.0 (default: 0.0).
        blpop_timeout: Timeout for BLPOP before fallback retry (default: 5.0).
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
    retry_jitter: float = 0.0
    blpop_timeout: float = 5.0

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
