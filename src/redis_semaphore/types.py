"""Types and configuration for redis-semaphore."""

from dataclasses import dataclass
from enum import Enum


class LockState(Enum):
    """State of a lock/semaphore."""

    RELEASED = "released"
    ACQUIRED = "acquired"
    LOST = "lost"


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
        retry_interval: Time between acquire retries.
        refresh_interval: Heartbeat interval (default: 80% of lock_timeout).
        namespace: Redis key prefix.
        strict_mode: If True, raises LockLostError immediately when lock is lost.
            Recommended for mission-critical systems to prevent zombie processes.
        use_server_time: If True, uses Redis server time instead of client time.
            Helps avoid clock skew issues but adds one extra RTT per operation.
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
