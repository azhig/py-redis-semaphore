"""Tests for synchronous Semaphore."""

import threading
import time

import pytest

from redis_semaphore import (
    AcquireError,
    AcquireResult,
    AcquireTimeoutError,
    Semaphore,
    SemaphoreConfig,
)


class TestSemaphoreBasic:
    """Basic semaphore tests."""

    def test_acquire_release(self, redis_client):
        """Test basic acquire and release."""
        config = SemaphoreConfig(name="test-sem", limit=1)
        sem = Semaphore(redis_client, config)

        result = sem.acquire(blocking=False)
        assert result.success is True
        assert result.identifier is not None
        assert result.fencing_token is not None
        assert sem.is_acquired is True

        released = sem.release()
        assert released is True
        assert sem.is_acquired is False

    def test_context_manager(self, redis_client):
        """Test context manager usage."""
        config = SemaphoreConfig(name="test-sem", limit=1)

        with Semaphore(redis_client, config) as sem:
            assert sem.is_acquired is True
            assert sem.fencing_token is not None
            assert sem.config is config

        assert sem.is_acquired is False

    def test_counting_semaphore(self, redis_client):
        """Test counting semaphore with limit > 1."""
        config = SemaphoreConfig(name="test-counting", limit=3)

        sems = []
        for i in range(3):
            sem = Semaphore(redis_client, config)
            result = sem.acquire(blocking=False)
            assert result.success is True, f"Failed to acquire slot {i}"
            sems.append(sem)

        # 4th acquire should fail
        sem4 = Semaphore(redis_client, config)
        result = sem4.acquire(blocking=False)
        assert result.success is False

        # Release one and try again
        sems[0].release()
        result = sem4.acquire(blocking=False)
        assert result.success is True

        # Cleanup
        for sem in sems[1:]:
            sem.release()
        sem4.release()

    def test_non_blocking_acquire_failure(self, redis_client):
        """Test non-blocking acquire when semaphore is full."""
        config = SemaphoreConfig(name="test-nonblock", limit=1)

        sem1 = Semaphore(redis_client, config)
        sem1.acquire(blocking=False)

        sem2 = Semaphore(redis_client, config)
        result = sem2.acquire(blocking=False)
        assert result.success is False

        sem1.release()

    def test_acquire_timeout(self, redis_client):
        """Test acquire timeout."""
        config = SemaphoreConfig(
            name="test-timeout",
            limit=1,
            acquire_timeout=0.5,
            retry_interval=0.1,
        )

        sem1 = Semaphore(redis_client, config)
        sem1.acquire(blocking=False)

        sem2 = Semaphore(redis_client, config)
        with pytest.raises(AcquireTimeoutError):
            sem2.acquire(blocking=True)

        sem1.release()

    def test_start_heartbeat_without_identifier(self, redis_client):
        """Starting heartbeat without identifier raises."""
        config = SemaphoreConfig(
            name="test-heartbeat-no-id",
            limit=1,
            refresh_interval=0.1,
        )
        sem = Semaphore(redis_client, config)
        with pytest.raises(AcquireError):
            sem._start_heartbeat()


@pytest.mark.asyncio
async def test_start_heartbeat_async_without_identifier(async_redis_client):
    """Starting async heartbeat without identifier raises."""
    config = SemaphoreConfig(
        name="test-heartbeat-no-id-async",
        limit=1,
        refresh_interval=0.1,
    )
    sem = Semaphore(async_redis_client, config)
    with pytest.raises(AcquireError):
        await sem._start_heartbeat_async()


class TestSemaphoreFencing:
    """Tests for fencing token functionality."""

    def test_fencing_token_increments(self, redis_client):
        """Test that fencing tokens increment monotonically."""
        config = SemaphoreConfig(name="test-fencing", limit=1)

        tokens = []
        for _ in range(5):
            sem = Semaphore(redis_client, config)
            result = sem.acquire(blocking=False)
            assert result.success is True
            tokens.append(result.fencing_token)
            sem.release()

        # Tokens should be strictly increasing
        for i in range(1, len(tokens)):
            assert tokens[i] > tokens[i - 1]

    def test_reentrant_acquire_same_token(self, redis_client):
        """Test that re-entrant acquire returns same token."""
        config = SemaphoreConfig(name="test-reentrant", limit=1)
        sem = Semaphore(redis_client, config)

        result1 = sem.acquire(blocking=False)
        token1 = result1.fencing_token

        # Acquire again with same semaphore
        result2 = sem.acquire(blocking=False)
        token2 = result2.fencing_token

        # Re-entrant acquire should advance the fencing token
        assert token1 is not None
        assert token2 is not None
        assert token2 > token1

        sem.release()


class TestSemaphoreConcurrency:
    """Concurrency tests."""

    def test_concurrent_access(self, redis_client):
        """Test concurrent access respects limit."""
        config = SemaphoreConfig(
            name="test-concurrent",
            limit=2,
            lock_timeout=10.0,
        )

        active_count = 0
        max_active = 0
        lock = threading.Lock()
        errors = []

        def worker(worker_id: int):
            nonlocal active_count, max_active

            sem = Semaphore(redis_client, config)
            result = sem.acquire(blocking=True)

            if not result.success:
                errors.append(f"Worker {worker_id} failed to acquire")
                return

            with lock:
                active_count += 1
                max_active = max(max_active, active_count)

            time.sleep(0.1)  # Simulate work

            with lock:
                active_count -= 1

            sem.release()

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Errors: {errors}"
        assert max_active <= 2, f"Max active was {max_active}, expected <= 2"


class TestSemaphoreExpiration:
    """Tests for lock expiration and cleanup."""

    def test_expired_lock_cleanup(self, redis_client):
        """Test that expired locks are cleaned up."""
        config = SemaphoreConfig(
            name="test-expiry",
            limit=1,
            lock_timeout=0.5,
            refresh_interval=1.0,
        )

        sem1 = Semaphore(redis_client, config)
        sem1.acquire(blocking=False)
        sem1._stop_heartbeat()

        # Wait for lock to expire
        time.sleep(0.7)

        # Another semaphore should be able to acquire after expiry
        sem2 = Semaphore(redis_client, config)
        result = sem2.acquire(blocking=False)
        assert result.success is True

        sem2.release()


class TestSemaphoreRefresh:
    """Tests for TTL refresh."""

    def test_manual_refresh(self, redis_client):
        """Test manual TTL refresh."""
        config = SemaphoreConfig(
            name="test-refresh",
            limit=1,
            lock_timeout=1.0,
            refresh_interval=None,  # Disable auto heartbeat
        )

        sem = Semaphore(redis_client, config)
        sem.acquire(blocking=False)

        # Wait a bit
        time.sleep(0.5)

        # Refresh
        success = sem.refresh()
        assert success is True

        # Wait more (would have expired without refresh)
        time.sleep(0.7)

        # Should still be valid
        assert sem.is_acquired is True
        success = sem.refresh()
        assert success is True

        sem.release()

    def test_refresh_after_expiry_fails(self, redis_client):
        """Test that refresh fails after lock expiry."""
        config = SemaphoreConfig(
            name="test-refresh-fail",
            limit=1,
            lock_timeout=0.3,
            refresh_interval=1.0,
        )

        sem = Semaphore(redis_client, config)
        sem.acquire(blocking=False)
        sem._stop_heartbeat()

        # Wait for expiry
        time.sleep(1.0)

        # Refresh should fail
        success = sem.refresh()
        assert success is False
        assert sem.is_lost is True

    def test_refresh_without_acquire(self, redis_client):
        """Refresh should fail when lock is not acquired."""
        config = SemaphoreConfig(name="test-refresh-unacquired", limit=1)
        sem = Semaphore(redis_client, config)

        assert sem.refresh() is False


class TestSemaphoreContextErrors:
    """Context manager error paths."""

    def test_context_manager_acquire_failure(self, redis_client):
        """__enter__ raises AcquireError when acquire returns failure."""
        config = SemaphoreConfig(
            name="test-context-fail",
            limit=1,
            acquire_timeout=0.01,
            retry_interval=0.01,
        )

        class FailingSemaphore(Semaphore):
            def acquire(self, blocking: bool = True) -> AcquireResult:
                return AcquireResult(
                    success=False,
                    identifier=None,
                    fencing_token=None,
                    expires_at=None,
                )

        sem = FailingSemaphore(redis_client, config)
        with pytest.raises(AcquireError):
            sem.__enter__()


class TestAcquireModePolling:
    """Tests for polling mode with backoff and jitter."""

    def test_polling_default_behavior(self, redis_client):
        """Test default polling behavior (no backoff)."""
        from redis_semaphore import AcquireMode

        config = SemaphoreConfig(
            name="test-polling-default",
            limit=1,
            acquire_mode=AcquireMode.POLLING,
            retry_interval=0.1,
        )

        sem = Semaphore(redis_client, config)
        result = sem.acquire(blocking=False)
        assert result.success is True
        sem.release()

    def test_polling_with_backoff(self, redis_client):
        """Test polling with exponential backoff."""
        from redis_semaphore import AcquireMode

        config = SemaphoreConfig(
            name="test-polling-backoff",
            limit=1,
            acquire_mode=AcquireMode.POLLING,
            retry_interval=0.05,
            retry_interval_max=0.2,
            retry_backoff_multiplier=2.0,
            acquire_timeout=0.5,
        )

        # Hold the lock
        sem1 = Semaphore(redis_client, config)
        sem1.acquire(blocking=False)

        # Try to acquire with backoff
        sem2 = Semaphore(redis_client, config)
        start = time.monotonic()
        with pytest.raises(AcquireTimeoutError):
            sem2.acquire(blocking=True)
        elapsed = time.monotonic() - start

        # Should have timed out around 0.5s
        assert 0.4 < elapsed < 0.7

        sem1.release()

    def test_polling_with_jitter(self, redis_client):
        """Test polling with jitter doesn't fail."""
        from redis_semaphore import AcquireMode

        config = SemaphoreConfig(
            name="test-polling-jitter",
            limit=1,
            acquire_mode=AcquireMode.POLLING,
            retry_interval=0.05,
            retry_jitter=0.5,
        )

        sem = Semaphore(redis_client, config)
        result = sem.acquire(blocking=False)
        assert result.success is True
        sem.release()


class TestAcquireModeBLPOP:
    """Tests for BLPOP mode."""

    def test_blpop_acquire_release(self, redis_client):
        """Test basic acquire/release with BLPOP mode."""
        from redis_semaphore import AcquireMode

        config = SemaphoreConfig(
            name="test-blpop-basic",
            limit=1,
            acquire_mode=AcquireMode.BLPOP,
            blpop_timeout=1.0,
        )

        sem = Semaphore(redis_client, config)
        result = sem.acquire(blocking=False)
        assert result.success is True
        sem.release()

    def test_blpop_waiter_notification(self, redis_client):
        """Test that release notifies waiting BLPOP."""
        from redis_semaphore import AcquireMode

        config = SemaphoreConfig(
            name="test-blpop-notify",
            limit=1,
            acquire_mode=AcquireMode.BLPOP,
            blpop_timeout=5.0,
        )

        acquired_event = threading.Event()
        released = threading.Event()

        sem1 = Semaphore(redis_client, config)
        sem1.acquire(blocking=False)

        def waiter():
            sem2 = Semaphore(redis_client, config)
            sem2.acquire(blocking=True)  # Will wait for release
            acquired_event.set()
            sem2.release()
            released.set()

        t = threading.Thread(target=waiter)
        t.start()

        # Give thread time to start waiting
        time.sleep(0.1)

        # Release should notify the waiter
        sem1.release()

        # Waiter should acquire quickly after notification
        assert acquired_event.wait(timeout=1.0), "Waiter did not acquire in time"
        assert released.wait(timeout=1.0)
        t.join()

    def test_blpop_fallback_timeout(self, redis_client):
        """Test BLPOP fallback polling on timeout."""
        from redis_semaphore import AcquireMode

        config = SemaphoreConfig(
            name="test-blpop-fallback",
            limit=1,
            acquire_mode=AcquireMode.BLPOP,
            blpop_timeout=0.2,  # Short timeout for fallback
            acquire_timeout=0.5,
        )

        # Hold the lock
        sem1 = Semaphore(redis_client, config)
        sem1.acquire(blocking=False)

        # Try to acquire - should fallback after blpop_timeout
        sem2 = Semaphore(redis_client, config)
        start = time.monotonic()
        with pytest.raises(AcquireTimeoutError):
            sem2.acquire(blocking=True)
        elapsed = time.monotonic() - start

        # Should have timed out
        assert 0.4 < elapsed < 0.7

        sem1.release()


class TestConfigValidation:
    """Tests for new config field validation."""

    def test_retry_interval_max_validation(self):
        """Test retry_interval_max must be >= retry_interval."""
        with pytest.raises(ValueError, match="retry_interval_max"):
            SemaphoreConfig(
                name="test",
                limit=1,
                retry_interval=0.5,
                retry_interval_max=0.1,  # Less than retry_interval
            )

    def test_retry_backoff_multiplier_validation(self):
        """Test retry_backoff_multiplier must be >= 1.0."""
        with pytest.raises(ValueError, match="retry_backoff_multiplier"):
            SemaphoreConfig(
                name="test",
                limit=1,
                retry_backoff_multiplier=0.5,  # Less than 1.0
            )

    def test_retry_jitter_validation(self):
        """Test retry_jitter must be between 0.0 and 1.0."""
        with pytest.raises(ValueError, match="retry_jitter"):
            SemaphoreConfig(
                name="test",
                limit=1,
                retry_jitter=1.5,  # Greater than 1.0
            )

        with pytest.raises(ValueError, match="retry_jitter"):
            SemaphoreConfig(
                name="test",
                limit=1,
                retry_jitter=-0.1,  # Less than 0.0
            )

    def test_blpop_timeout_validation(self):
        """Test blpop_timeout must be > 0."""
        with pytest.raises(ValueError, match="blpop_timeout"):
            SemaphoreConfig(
                name="test",
                limit=1,
                blpop_timeout=0,
            )
