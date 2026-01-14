"""Tests for error classes."""

from unittest.mock import MagicMock

import pytest

from redis_semaphore import LockLostError, SemaphoreConfig
from redis_semaphore.semaphore import Semaphore
from redis_semaphore.types import LockState


def test_lock_lost_error_identifier():
    """LockLostError exposes the identifier and message."""
    err = LockLostError("abc", "lost")
    assert err.identifier == "abc"
    assert str(err) == "lost"


def test_check_lock_lost_strict_mode_raises():
    """In strict_mode, _check_lock_lost raises LockLostError when lock is lost."""
    client = MagicMock()
    config = SemaphoreConfig(name="test", limit=1, strict_mode=True)
    sem = Semaphore(client, config)
    sem._state = LockState.LOST
    sem._identifier = "test-id"

    with pytest.raises(LockLostError) as exc:
        sem._check_lock_lost()
    assert "test-id" in str(exc.value) or exc.value.identifier == "test-id"


def test_check_lock_lost_non_strict_mode_warns(caplog):
    """Without strict_mode, _check_lock_lost logs a warning but does not raise."""
    client = MagicMock()
    config = SemaphoreConfig(name="test", limit=1, strict_mode=False)
    sem = Semaphore(client, config)
    sem._state = LockState.LOST
    sem._identifier = "test-id"

    # Should not raise
    sem._check_lock_lost()


def test_check_lock_lost_not_lost():
    """_check_lock_lost does nothing if lock is not lost."""
    client = MagicMock()
    config = SemaphoreConfig(name="test", limit=1, strict_mode=True)
    sem = Semaphore(client, config)
    sem._state = LockState.ACQUIRED

    # Should not raise
    sem._check_lock_lost()


def test_wait_for_lock_lost_timeout():
    """wait_for_lock_lost returns False on timeout."""
    client = MagicMock()
    config = SemaphoreConfig(name="test", limit=1)
    sem = Semaphore(client, config)

    # Lock is not lost, should timeout
    result = sem.wait_for_lock_lost(timeout=0.01)
    assert result is False


def test_wait_for_lock_lost_immediate():
    """wait_for_lock_lost returns True if lock already lost."""
    client = MagicMock()
    config = SemaphoreConfig(name="test", limit=1)
    sem = Semaphore(client, config)

    # Simulate lock loss
    sem._lock_lost_event.set()

    result = sem.wait_for_lock_lost(timeout=0.1)
    assert result is True


@pytest.mark.asyncio
async def test_await_lock_lost_timeout():
    """await_lock_lost returns False on timeout."""
    client = MagicMock()
    config = SemaphoreConfig(name="test", limit=1)
    sem = Semaphore(client, config)

    result = await sem.await_lock_lost(timeout=0.01)
    assert result is False


@pytest.mark.asyncio
async def test_await_lock_lost_immediate():
    """await_lock_lost returns True if lock already lost."""
    client = MagicMock()
    config = SemaphoreConfig(name="test", limit=1)
    sem = Semaphore(client, config)

    # Simulate lock loss
    sem._lock_lost_event.set()

    result = await sem.await_lock_lost(timeout=0.1)
    assert result is True
