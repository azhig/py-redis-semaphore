"""Tests for heartbeat behavior."""

import threading
import time

import pytest

from redis_semaphore import LockLostError, LockState, Semaphore, SemaphoreConfig


def test_sync_heartbeat_keeps_lock(redis_client):
    """Heartbeat should keep the lock from expiring."""
    config = SemaphoreConfig(
        name="test-heartbeat-sync",
        limit=1,
        lock_timeout=0.2,
        refresh_interval=0.05,
    )

    sem1 = Semaphore(redis_client, config)
    sem1.acquire(blocking=False)

    time.sleep(0.4)

    sem2 = Semaphore(redis_client, config)
    result = sem2.acquire(blocking=False)
    assert result.success is False

    sem1.release()
    result = sem2.acquire(blocking=False)
    assert result.success is True
    sem2.release()


def test_sync_heartbeat_lock_lost_callback(redis_client):
    """Lock lost callback fires when lock is removed."""
    event = threading.Event()

    def on_lock_lost(identifier: str) -> None:
        event.set()

    config = SemaphoreConfig(
        name="test-heartbeat-lost",
        limit=1,
        lock_timeout=0.5,
        refresh_interval=0.05,
    )

    sem = Semaphore(redis_client, config, on_lock_lost=on_lock_lost)
    sem.acquire(blocking=False)

    redis_client.zrem(sem.owners_key, sem.identifier)

    assert event.wait(timeout=1.0) is True
    assert sem.is_lost is True


def test_sync_heartbeat_lock_lost_callback_strict_mode(redis_client):
    """on_lock_lost must still fire when the heartbeat detects loss in strict_mode.

    Regression: previously the heartbeat's refresh raised LockLostError in
    strict_mode, which the loop mistook for a transient error, so the callback
    (and the lock-lost metric) never fired.
    """
    event = threading.Event()

    def on_lock_lost(identifier: str) -> None:
        event.set()

    config = SemaphoreConfig(
        name="test-heartbeat-lost-strict",
        limit=1,
        lock_timeout=0.5,
        refresh_interval=0.05,
        strict_mode=True,
    )

    sem = Semaphore(redis_client, config, on_lock_lost=on_lock_lost)
    sem.acquire(blocking=False)

    redis_client.zrem(sem.owners_key, sem.identifier)

    assert event.wait(timeout=1.0) is True
    assert sem.is_lost is True

    # strict_mode contract is still honored: the next operation raises.
    with pytest.raises(LockLostError):
        sem.acquire(blocking=False)


def test_heartbeat_on_lost_ignores_stale_signal_after_release(redis_client):
    """A late/orphaned heartbeat must not mark a cleanly released slot as lost."""
    fired = []
    config = SemaphoreConfig(
        name="hb-stale-guard",
        limit=1,
        lock_timeout=10.0,
        refresh_interval=5.0,  # heartbeat won't tick during the test
    )

    sem = Semaphore(redis_client, config, on_lock_lost=lambda identifier: fired.append(identifier))
    sem.acquire(blocking=False)
    on_lost = sem._heartbeat._on_lock_lost  # the closure installed by _start_heartbeat

    # Simulate that a concurrent release already reset the state.
    sem._state = LockState.RELEASED
    on_lost(sem.identifier or "stale")

    assert fired == []  # stale signal suppressed
    assert sem._state == LockState.RELEASED  # not flipped to LOST

    sem.release()
