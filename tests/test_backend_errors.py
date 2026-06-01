"""Tests for typed backend-error classification and normalization."""

import pytest
import redis.exceptions as rexc

from redis_semaphore import (
    CommandDeniedError,
    PermanentBackendError,
    RedisConnectionError,
    SemaphoreConfig,
    TransientBackendError,
)
from redis_semaphore.errors import map_backend_error
from redis_semaphore.semaphore import Semaphore


class _FakeClient:
    """Minimal sync Redis stand-in that fails evalsha with a chosen error."""

    def __init__(self, exc: BaseException) -> None:
        self._exc = exc

    def script_load(self, script: str) -> str:
        return "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"

    def evalsha(self, sha: str, numkeys: int, *args: object) -> object:
        raise self._exc


# --------------------------------------------------------------------------- #
# Unit tests for the mapper
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "exc",
    [
        rexc.ConnectionError("refused"),
        rexc.TimeoutError("timed out"),
        rexc.ReadOnlyError("read only replica"),
        OSError("socket boom"),
    ],
)
def test_map_transient_errors(exc):
    mapped = map_backend_error(exc, operation="acquire", name="r")
    assert isinstance(mapped, RedisConnectionError)
    assert isinstance(mapped, TransientBackendError)
    assert mapped.is_transient is True
    assert mapped.operation == "acquire"
    assert mapped.name == "r"
    assert mapped.original is exc


@pytest.mark.parametrize(
    "exc",
    [
        rexc.NoPermissionError("NOPERM no permissions to run 'zrem'"),
        rexc.ResponseError("ERR unknown command 'EVALSHA'"),
    ],
)
def test_map_permanent_errors(exc):
    mapped = map_backend_error(exc, operation="release", name="r")
    assert isinstance(mapped, CommandDeniedError)
    assert isinstance(mapped, PermanentBackendError)
    assert mapped.is_transient is False
    assert mapped.operation == "release"
    assert mapped.original is exc


def test_map_unknown_error_is_permanent():
    exc = rexc.RedisError("something weird")
    mapped = map_backend_error(exc)
    assert isinstance(mapped, PermanentBackendError)
    assert mapped.is_transient is False


# --------------------------------------------------------------------------- #
# Integration: errors surface through the public Semaphore API, typed
# --------------------------------------------------------------------------- #


def test_acquire_maps_noperm_to_command_denied():
    exc = rexc.NoPermissionError("NOPERM this user has no permissions to run 'evalsha'")
    sem = Semaphore(_FakeClient(exc), SemaphoreConfig(name="acl-test", limit=1))

    with pytest.raises(CommandDeniedError) as ei:
        sem.acquire(blocking=False)

    assert ei.value.operation == "acquire"
    assert ei.value.is_transient is False
    assert ei.value.original is exc
    # The original NOPERM text is preserved (no longer masked as "connection lost").
    assert "NOPERM" in str(ei.value)


def test_acquire_maps_connection_error_to_transient():
    exc = rexc.ConnectionError("connection refused")
    sem = Semaphore(_FakeClient(exc), SemaphoreConfig(name="conn-test", limit=1))

    with pytest.raises(RedisConnectionError) as ei:
        sem.acquire(blocking=False)

    assert ei.value.operation == "acquire"
    assert ei.value.is_transient is True


def test_refresh_error_symbol_removed():
    """RefreshError was dead code and is no longer exported."""
    import redis_semaphore

    assert not hasattr(redis_semaphore, "RefreshError")
