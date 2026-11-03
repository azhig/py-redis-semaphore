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


class RateLimitTimeoutError(AcquireTimeoutError):
    """A rate-limited request could not be admitted within acquire_timeout.

    Attributes:
        retry_after: Seconds until the request would fit, as computed by Redis
            on the last attempt.
    """

    def __init__(self, message: str, retry_after: float):
        self.retry_after = retry_after
        super().__init__(message)


class AlreadyAcquiredError(AcquireError):
    """acquire() called on an instance that already holds a slot.

    Raised only with ``reacquire_policy=ReacquirePolicy.FORBID``.
    """


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


class DispatchError(RedisSemaphoreError):
    """Base class for shared dispatch queue errors."""


class QueueFullError(DispatchError):
    """The queue already holds max_queue_size jobs."""


class JobTimeoutError(DispatchError):
    """No result arrived within the submit timeout.

    The job may still be executed later if it was admitted before the
    deadline; jobs still waiting in the queue past it are dropped.
    """


class JobFailedError(DispatchError):
    """The job handler raised an exception.

    Attributes:
        error_type: Class name of the exception raised by the handler.
        data: Structured details from a :class:`JobError` (empty otherwise).
    """

    def __init__(self, error_type: str, message: str, data: dict | None = None):
        self.error_type = error_type
        self.data = data or {}
        super().__init__(f"{error_type}: {message}")


class JobError(Exception):
    """Raise from a job handler to fail the job with JSON-serializable details.

    The submitter receives a :class:`JobFailedError` whose ``data`` is
    ``data`` (other exceptions only transfer their class name and message).
    """

    def __init__(self, message: str, data: dict | None = None):
        self.data = data or {}
        super().__init__(message)


class RequeueJob(Exception):
    """Raise from a job handler to put the job back into its queue.

    The submitter gets no reply yet and keeps waiting. The queue's leader runs
    the job again before any job that has not been started yet - e.g. after an
    upstream 429, wait for the quota again *before* ``job.admit()`` so the
    job holds the head of the queue while it waits.

    Args:
        delay: Seconds to wait before the job may run again. Raised before
            ``admit()``, the queue waits too (the job keeps its place);
            raised after it, other jobs go on meanwhile.
    """

    def __init__(self, delay: float = 0.0):
        if delay < 0:
            raise ValueError("delay must be >= 0")
        self.delay = delay
        super().__init__(f"requeue after {delay}s")


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
