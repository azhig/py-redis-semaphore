"""Tests for synchronous Mutex."""

import pytest

from redis_semaphore import Mutex, NotAcquiredError


class TestMutexBasic:
    """Basic mutex tests."""

    def test_mutex_exclusive(self, redis_client):
        """Mutex allows only a single owner."""
        mutex1 = Mutex(redis_client, "test-mutex")
        result1 = mutex1.acquire(blocking=False)
        assert result1.success is True

        mutex2 = Mutex(redis_client, "test-mutex")
        result2 = mutex2.acquire(blocking=False)
        assert result2.success is False

        mutex1.release()

        result3 = mutex2.acquire(blocking=False)
        assert result3.success is True
        mutex2.release()

    def test_mutex_context_manager(self, redis_client):
        """Mutex works as a context manager."""
        with Mutex(redis_client, "test-mutex-context") as lock:
            assert lock.is_acquired is True
            assert lock.fencing_token is not None

        assert lock.is_acquired is False

    def test_mutex_reentrant(self, redis_client):
        """Re-entrant acquire returns the same fencing token."""
        mutex = Mutex(redis_client, "test-mutex-reentrant")
        first = mutex.acquire(blocking=False)
        second = mutex.acquire(blocking=False)

        assert first.success is True
        assert second.success is True
        assert second.fencing_token is not None
        assert first.fencing_token is not None
        assert second.fencing_token > first.fencing_token

        mutex.release()

    def test_release_without_acquire(self, redis_client):
        """Release without acquire raises an error."""
        mutex = Mutex(redis_client, "test-mutex-release")

        with pytest.raises(NotAcquiredError):
            mutex.release()
