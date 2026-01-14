"""Tests for heartbeat behavior."""

import threading
import time

from redis_semaphore import Semaphore, SemaphoreConfig


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
