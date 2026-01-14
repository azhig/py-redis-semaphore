"""Exceptions for redis-semaphore."""


class RedisSemaphoreError(Exception):
    """Base exception for redis-semaphore."""


class AcquireError(RedisSemaphoreError):
    """Failed to acquire lock."""


class AcquireTimeoutError(AcquireError):
    """Timeout while trying to acquire lock."""


class ReleaseError(RedisSemaphoreError):
    """Error while releasing lock."""


class NotAcquiredError(ReleaseError):
    """Attempted to release a lock that was not acquired."""


class LockLostError(RedisSemaphoreError):
    """Lock was lost (TTL expired)."""

    def __init__(self, identifier: str, message: str = "Lock was lost"):
        self.identifier = identifier
        super().__init__(message)


class RefreshError(RedisSemaphoreError):
    """Error while refreshing lock TTL."""


class RedisConnectionError(RedisSemaphoreError):
    """Error connecting to Redis."""


class MixedModeError(RedisSemaphoreError):
    """Raised when mixing sync and async operations on the same semaphore."""
