"""Exceptions for redis-semaphore."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import redis.exceptions as _rexc


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


class MixedModeError(RedisSemaphoreError):
    """Raised when mixing sync and async operations on the same semaphore."""


class BackendError(RedisSemaphoreError):
    """Error while talking to the Redis backend.

    Carries context about the failed operation. Use the ``is_transient``
    marker - or catch :class:`TransientBackendError` /
    :class:`PermanentBackendError` - to decide whether retrying makes sense.

    Attributes:
        operation: Logical operation that failed ("acquire", "release", ...).
        name: Semaphore name, if known.
        original: The underlying redis-py exception (also chained via ``__cause__``).
    """

    is_transient: bool = False

    def __init__(
        self,
        message: str,
        *,
        operation: str | None = None,
        name: str | None = None,
        original: BaseException | None = None,
    ) -> None:
        self.operation = operation
        self.name = name
        self.original = original
        super().__init__(message)


class TransientBackendError(BackendError):
    """A backend error that may succeed on retry (connection blips, failover)."""

    is_transient = True


class PermanentBackendError(BackendError):
    """A backend error that will not succeed on retry (config/ACL/bad command)."""

    is_transient = False


class RedisConnectionError(TransientBackendError):
    """Redis is unreachable: connection refused/reset, timeout, loading, readonly."""


class CommandDeniedError(PermanentBackendError):
    """A Redis command was rejected: ACL NOPERM, or unknown/renamed command."""


def map_backend_error(
    exc: BaseException,
    *,
    operation: str | None = None,
    name: str | None = None,
) -> BackendError:
    """Translate a redis-py exception into the redis-semaphore hierarchy.

    Connection-level failures (and replica/failover ``ReadOnlyError``) become
    :class:`RedisConnectionError` (transient). Command rejections - ACL
    ``NoPermissionError`` and any other ``ResponseError`` such as an unknown or
    renamed command - become :class:`CommandDeniedError` (permanent). Anything
    else is treated conservatively as a permanent backend error.
    """
    message = str(exc) or exc.__class__.__name__

    # Permanent: ACL rejection (subclass of ResponseError - check first).
    if isinstance(exc, _rexc.NoPermissionError):
        return CommandDeniedError(message, operation=operation, name=name, original=exc)

    # Transient: replica/failover write rejection (also a ResponseError).
    if isinstance(exc, _rexc.ReadOnlyError):
        return RedisConnectionError(message, operation=operation, name=name, original=exc)

    # Transient: connectivity. BusyLoadingError subclasses ConnectionError.
    if isinstance(exc, _rexc.ConnectionError | _rexc.TimeoutError | OSError):
        return RedisConnectionError(message, operation=operation, name=name, original=exc)

    # Permanent: unknown/renamed command, wrong arity, other server-side errors.
    if isinstance(exc, _rexc.ResponseError):
        return CommandDeniedError(message, operation=operation, name=name, original=exc)

    # Unknown redis error - be conservative and treat as permanent.
    return PermanentBackendError(message, operation=operation, name=name, original=exc)


@contextmanager
def backend_errors(operation: str, name: str | None = None) -> Iterator[None]:
    """Map raw redis-py exceptions raised in the block into the typed hierarchy.

    Exceptions that are already :class:`RedisSemaphoreError` instances pass
    through unchanged (no double-wrapping). Works for both sync and async call
    sites, since the awaited exception propagates synchronously into the block.
    """
    try:
        yield
    except RedisSemaphoreError:
        raise
    except (_rexc.RedisError, OSError) as exc:
        raise map_backend_error(exc, operation=operation, name=name) from exc
