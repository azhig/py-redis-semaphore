"""Semaphore and mutex implementations with sync/async APIs."""

from __future__ import annotations

import asyncio
import atexit
import logging
import random
import sys
import threading
import time
import weakref
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

import redis
import redis.asyncio as aioredis

from .base import BaseSemaphoreCommon, _AcquireMode
from .errors import (
    AcquireError,
    AcquireTimeoutError,
    LockLostError,
    MixedModeError,
    NotAcquiredError,
)
from .heartbeat import Heartbeat
from .logger import logger
from .lua_scripts import LuaScriptRunner, ScriptClientAdapter
from .metrics import get_metrics
from .types import AcquireMode, AcquireResult, LockState, SemaphoreConfig

# Type alias for lock lost callback
# Supports both sync and async callbacks
LockLostCallback = Callable[[str], None] | Callable[[str], Coroutine[Any, Any, None]]

# Global registry for cleanup on shutdown
_active_semaphores: weakref.WeakSet[Semaphore] = weakref.WeakSet()
_atexit_registered = False


def _cleanup_all_semaphores() -> None:
    """Stop all heartbeats on process exit."""
    for sem in list(_active_semaphores):
        try:
            sem._stop_heartbeat()
        except Exception:
            logger.debug("Failed to stop heartbeat during cleanup", exc_info=True)


@dataclass
class _AcquireState:
    """Internal state for acquire loop."""

    start_time: float
    wait_start: float | None = None
    waiting_registered: bool = False


class Semaphore(BaseSemaphoreCommon[redis.Redis | aioredis.Redis]):
    """Counting Semaphore with support for up to N concurrent owners.

    Usage:
        # As context manager
        with Semaphore(redis_client, config) as sem:
            # work with protected resource
            print(f"Fencing token: {sem.fencing_token}")

        # Manual management
        sem = Semaphore(redis_client, config)
        if sem.acquire(blocking=False).success:
            try:
                # work
            finally:
                sem.release()

        # Mission-critical mode (recommended for production)
        config = SemaphoreConfig(
            name="critical-resource",
            limit=1,
            strict_mode=True,      # Raises LockLostError immediately
            use_server_time=True,  # Avoids clock skew issues
        )

    Warning:
        Do not mix sync and async APIs on the same instance. Use acquire()/release()
        OR aacquire()/arelease(), but not both. Mixing will raise MixedModeError.

    Note:
        - By default, uses client-side time. Enable use_server_time=True for
          clock skew protection (adds ~1ms latency per operation).
        - Enable strict_mode=True for mission-critical systems to immediately
          raise LockLostError when lock is lost, preventing zombie processes.
        - Context manager (with/async with) always uses blocking=True and will
          wait until a slot is acquired or acquire_timeout is exceeded.
    """

    __slots__ = (
        "__weakref__",  # Required for WeakSet support
        "_async_heartbeat",
        "_async_scripts_lock",
        "_heartbeat",
        "_lock_lost_event",
        "_metrics",
        "_on_lock_lost",
        "_runner",
        "_script_client",
        "_scripts_loaded",
        "_scripts_lock",
        "_wait_lock",
        "_waiting",
    )

    def __init__(
        self,
        client: redis.Redis | aioredis.Redis,
        config: SemaphoreConfig,
        *,
        on_lock_lost: LockLostCallback | None = None,
    ) -> None:
        global _atexit_registered
        super().__init__(client, config)
        self._on_lock_lost: LockLostCallback | None = on_lock_lost
        self._heartbeat: Heartbeat | None = None
        self._async_heartbeat: Heartbeat | None = None
        self._scripts_loaded = False
        self._scripts_lock = threading.Lock()
        self._async_scripts_lock: asyncio.Lock | None = None
        self._runner = LuaScriptRunner(self._scripts)
        self._script_client = ScriptClientAdapter(client, self._scripts)
        self._wait_lock = threading.Lock()
        self._waiting = 0
        self._metrics = get_metrics()
        self._lock_lost_event = threading.Event()

        # Register for cleanup on process exit
        _active_semaphores.add(self)
        if not _atexit_registered:
            atexit.register(_cleanup_all_semaphores)
            _atexit_registered = True

    def _check_mode(self, required: _AcquireMode) -> None:
        """Check that we're not mixing sync/async modes."""
        if self._acquire_mode == _AcquireMode.NONE:
            return  # Not acquired yet, any mode is fine
        if self._acquire_mode != required:
            release_method = (
                "arelease()" if self._acquire_mode == _AcquireMode.ASYNC else "release()"
            )
            raise MixedModeError(
                f"Cannot use {'async' if required == _AcquireMode.ASYNC else 'sync'} "
                f"API: semaphore was acquired with "
                f"{'async' if self._acquire_mode == _AcquireMode.ASYNC else 'sync'} API. "
                f"Use {release_method} instead."
            )

    def _check_lock_lost(self) -> None:
        """Check if lock was lost and raise in strict_mode.

        In mission-critical systems (strict_mode=True), this prevents
        zombie processes from continuing work after losing the lock.

        Raises:
            LockLostError: If lock was lost and strict_mode is enabled.
        """
        if self._state == LockState.LOST:
            if self._config.strict_mode:
                raise LockLostError(
                    self._identifier or "unknown",
                    f"Lock '{self._config.name}' was lost. In strict_mode, "
                    "operations are not allowed after lock loss.",
                )
            logger.warning(
                "Lock '%s' was lost (identifier=%s). "
                "Consider enabling strict_mode for mission-critical systems.",
                self._config.name,
                self._identifier,
            )

    def _get_time_ms(self) -> int:
        """Get current time in milliseconds, using server time if configured."""
        if self._config.use_server_time:
            return self._get_server_time_ms()
        return self._get_current_time_ms()

    async def _get_time_ms_async(self) -> int:
        """Get current time in milliseconds (async), using server time if configured."""
        if self._config.use_server_time:
            return await self._get_server_time_ms_async()
        return self._get_current_time_ms()

    def _ensure_scripts(self) -> None:
        """Ensure scripts are loaded (thread-safe)."""
        if self._scripts_loaded:
            return
        with self._scripts_lock:
            if not self._scripts_loaded:
                self._scripts.load_all(self._script_client)
                self._scripts_loaded = True

    async def _ensure_scripts_async(self) -> None:
        """Ensure scripts are loaded (async, coroutine-safe)."""
        if self._scripts_loaded:
            return
        if self._async_scripts_lock is None:
            self._async_scripts_lock = asyncio.Lock()
        async with self._async_scripts_lock:
            if not self._scripts_loaded:
                await self._scripts.load_all_async(self._script_client)
                self._scripts_loaded = True

    def _can_log_debug(self) -> bool:
        checker = getattr(logger, "isEnabledFor", None)
        if checker is None:
            return True
        return bool(checker(logging.DEBUG))

    def _increment_waiting(self) -> None:
        with self._wait_lock:
            self._waiting += 1
            if self._metrics.enabled:
                self._metrics.set_waiting(
                    self._config.name,
                    self._config.namespace,
                    self._waiting,
                )

    def _decrement_waiting(self) -> None:
        with self._wait_lock:
            if self._waiting > 0:
                self._waiting -= 1
                if self._metrics.enabled:
                    self._metrics.set_waiting(
                        self._config.name,
                        self._config.namespace,
                        self._waiting,
                    )

    def _waiting_count(self) -> int:
        with self._wait_lock:
            return self._waiting

    def _log_and_record_status(self, current_count: int) -> None:
        """Log and record metrics for current semaphore status."""
        if self._metrics.enabled:
            self._metrics.set_slots_used(
                self._config.name,
                self._config.namespace,
                current_count,
                self._config.limit,
            )
        if self._can_log_debug():
            logger.debug(
                "Semaphore '%s' usage %s/%s",
                self._config.name,
                current_count,
                self._config.limit,
            )

    def _log_waiting_status(self, current_count: int) -> None:
        """Log status when waiting for a slot."""
        if self._metrics.enabled:
            self._metrics.set_slots_used(
                self._config.name,
                self._config.namespace,
                current_count,
                self._config.limit,
            )
        if self._can_log_debug():
            logger.debug(
                "Semaphore '%s' full %s/%s; waiting=%s",
                self._config.name,
                current_count,
                self._config.limit,
                self._waiting_count(),
            )

    def _finalize_acquire(
        self,
        fencing_token: int | None,
        expires_at: int | None,
        state: _AcquireState,
        mode: _AcquireMode,
    ) -> AcquireResult:
        """Common logic for finalizing acquire - update state and metrics."""
        self._fencing_token = int(fencing_token) if fencing_token else None
        self._expires_at = float(expires_at) / 1000 if expires_at else None
        self._state = LockState.ACQUIRED
        self._acquire_mode = mode

        if state.waiting_registered:
            self._decrement_waiting()
            if self._metrics.enabled and state.wait_start is not None:
                self._metrics.observe_wait_seconds(
                    self._config.name,
                    self._config.namespace,
                    time.monotonic() - state.wait_start,
                    "success",
                )
        if self._metrics.enabled:
            self._metrics.inc_acquire(
                self._config.name,
                self._config.namespace,
                "success",
            )

        return AcquireResult(
            success=True,
            identifier=self._identifier,
            fencing_token=self._fencing_token,
            expires_at=self._expires_at,
        )

    def _handle_acquire_success(
        self,
        fencing_token: int | None,
        expires_at: int | None,
        state: _AcquireState,
    ) -> AcquireResult:
        """Process successful sync acquire."""
        result = self._finalize_acquire(fencing_token, expires_at, state, _AcquireMode.SYNC)
        if self._refresh_interval > 0:
            self._start_heartbeat()
        return result

    async def _handle_acquire_success_async(
        self,
        fencing_token: int | None,
        expires_at: int | None,
        state: _AcquireState,
    ) -> AcquireResult:
        """Process successful async acquire."""
        result = self._finalize_acquire(fencing_token, expires_at, state, _AcquireMode.ASYNC)
        if self._refresh_interval > 0:
            await self._start_heartbeat_async()
        return result

    def _handle_non_blocking_failure(self) -> AcquireResult:
        """Handle non-blocking acquire failure."""
        if self._metrics.enabled:
            self._metrics.inc_acquire(
                self._config.name,
                self._config.namespace,
                "busy",
            )
        return AcquireResult(
            success=False,
            identifier=None,
            fencing_token=None,
            expires_at=None,
        )

    def _handle_timeout(self, state: _AcquireState) -> None:
        """Handle acquire timeout - record metrics and raise exception."""
        if state.waiting_registered:
            self._decrement_waiting()
            if self._metrics.enabled and state.wait_start is not None:
                self._metrics.observe_wait_seconds(
                    self._config.name,
                    self._config.namespace,
                    time.monotonic() - state.wait_start,
                    "timeout",
                )
        if self._metrics.enabled:
            self._metrics.inc_acquire(
                self._config.name,
                self._config.namespace,
                "timeout",
            )
        logger.error(
            "Semaphore '%s' acquire timeout after %ss",
            self._config.name,
            self._config.acquire_timeout,
        )
        raise AcquireTimeoutError(
            f"Failed to acquire semaphore '{self._config.name}' "
            f"within {self._config.acquire_timeout}s"
        )

    def _check_timeout(self, state: _AcquireState) -> bool:
        """Check if acquire timeout exceeded. Returns True if timed out."""
        if self._config.acquire_timeout is None:
            return False
        elapsed = time.monotonic() - state.start_time
        return elapsed >= self._config.acquire_timeout

    def _calculate_retry_interval(self, state: _AcquireState) -> float:
        """Calculate retry interval with optional exponential backoff and jitter.

        Args:
            state: Current acquire state with timing information.

        Returns:
            Sleep interval in seconds.
        """
        base = self._config.retry_interval

        # Apply exponential backoff if max interval is configured
        if self._config.retry_interval_max is not None:
            elapsed = time.monotonic() - state.start_time
            # Calculate number of retry cycles elapsed
            cycles = elapsed / base
            multiplier = self._config.retry_backoff_multiplier**cycles
            interval = min(base * multiplier, self._config.retry_interval_max)
        else:
            interval = base

        # Apply jitter as a fraction of the current interval
        if self._config.retry_jitter > 0:
            jitter = random.uniform(0, self._config.retry_jitter * interval)
            interval += jitter

        return interval

    def _wait_polling(self, state: _AcquireState) -> None:
        """Wait using polling strategy with optional backoff/jitter."""
        interval = self._calculate_retry_interval(state)
        time.sleep(interval)

    def _wait_blpop(self) -> None:
        """Wait using BLPOP for notification with fallback timeout."""
        # BLPOP returns None on timeout, (key, value) on success
        # We don't care about the value, just the wakeup signal
        self._client.blpop([self.queue_key], timeout=self._config.blpop_timeout)

    async def _wait_polling_async(self, state: _AcquireState) -> None:
        """Async wait using polling strategy with optional backoff/jitter."""
        interval = self._calculate_retry_interval(state)
        await asyncio.sleep(interval)

    async def _wait_blpop_async(self) -> None:
        """Async wait using BLPOP for notification with fallback timeout."""
        await self._client.blpop([self.queue_key], timeout=self._config.blpop_timeout)  # type: ignore[misc]

    def _wait_for_slot(self, state: _AcquireState) -> None:
        """Wait for a slot using the configured strategy."""
        if self._config.acquire_mode == AcquireMode.BLPOP:
            self._wait_blpop()
        else:
            self._wait_polling(state)

    async def _wait_for_slot_async(self, state: _AcquireState) -> None:
        """Async wait for a slot using the configured strategy."""
        if self._config.acquire_mode == AcquireMode.BLPOP:
            await self._wait_blpop_async()
        else:
            await self._wait_polling_async(state)

    def _notify_waiters(self) -> None:
        """Notify one waiter that a slot is available via LPUSH."""
        try:
            self._client.lpush(self.queue_key, "1")
        except Exception:
            # Non-critical: if notify fails, waiters will retry on timeout
            logger.debug("Failed to notify waiters", exc_info=True)

    async def _notify_waiters_async(self) -> None:
        """Async notify one waiter that a slot is available via LPUSH."""
        try:
            await self._client.lpush(self.queue_key, "1")  # type: ignore[misc]
        except Exception:
            logger.debug("Failed to notify waiters", exc_info=True)

    def acquire(self, blocking: bool = True) -> AcquireResult:
        """Acquire a semaphore slot.

        Args:
            blocking: If True, wait for a slot to become available.
                     If False, return immediately.
                     Note: acquire_timeout=None means wait forever,
                     but only when blocking=True. Context manager
                     (__enter__/with) always uses blocking=True.

        Returns:
            AcquireResult with acquisition information.

        Raises:
            AcquireTimeoutError: If blocking=True and acquire_timeout is exceeded.
            MixedModeError: If semaphore was acquired with async API.
            LockLostError: If lock was previously lost and strict_mode is enabled.
        """
        self._check_mode(_AcquireMode.SYNC)
        self._check_lock_lost()
        self._ensure_scripts()

        if self._identifier is None:
            self._identifier = self._generate_identifier()

        state = _AcquireState(start_time=time.monotonic())
        lock_timeout_ms = int(self._config.lock_timeout * 1000)

        while True:
            now_ms = self._get_time_ms()

            if self._metrics.enabled or self._can_log_debug():
                current_count, _, _ = self._runner.status(
                    self._script_client,
                    self.owners_key,
                    now_ms,
                    self._identifier,
                )
                self._log_and_record_status(current_count)

            success, fencing_token, expires_at = self._runner.acquire(
                self._script_client,
                self.owners_key,
                self.fencing_key,
                self._identifier,
                self._config.limit,
                lock_timeout_ms,
                now_ms,
            )

            if success:
                return self._handle_acquire_success(fencing_token, expires_at, state)

            if not blocking:
                return self._handle_non_blocking_failure()

            if not state.waiting_registered:
                state.wait_start = time.monotonic()
                self._increment_waiting()
                state.waiting_registered = True

            if self._metrics.enabled or self._can_log_debug():
                current_count, _, _ = self._runner.status(
                    self._script_client,
                    self.owners_key,
                    now_ms,
                    self._identifier,
                )
                self._log_waiting_status(current_count)

            if self._check_timeout(state):
                self._handle_timeout(state)

            self._wait_for_slot(state)

    async def aacquire(self, blocking: bool = True) -> AcquireResult:
        """Asynchronously acquire a semaphore slot.

        Note:
            acquire_timeout=None means wait forever, but only when
            blocking=True. Async context manager (__aenter__/async with)
            always uses blocking=True.

        Raises:
            AcquireTimeoutError: If blocking=True and acquire_timeout is exceeded.
            MixedModeError: If semaphore was acquired with sync API.
            LockLostError: If lock was previously lost and strict_mode is enabled.
        """
        self._check_mode(_AcquireMode.ASYNC)
        self._check_lock_lost()
        await self._ensure_scripts_async()

        if self._identifier is None:
            self._identifier = self._generate_identifier()

        state = _AcquireState(start_time=time.monotonic())
        lock_timeout_ms = int(self._config.lock_timeout * 1000)

        while True:
            now_ms = await self._get_time_ms_async()

            if self._metrics.enabled or self._can_log_debug():
                current_count, _, _ = await self._runner.status_async(
                    self._script_client,
                    self.owners_key,
                    now_ms,
                    self._identifier,
                )
                self._log_and_record_status(current_count)

            success, fencing_token, expires_at = await self._runner.acquire_async(
                self._script_client,
                self.owners_key,
                self.fencing_key,
                self._identifier,
                self._config.limit,
                lock_timeout_ms,
                now_ms,
            )

            if success:
                return await self._handle_acquire_success_async(fencing_token, expires_at, state)

            if not blocking:
                return self._handle_non_blocking_failure()

            if not state.waiting_registered:
                state.wait_start = time.monotonic()
                self._increment_waiting()
                state.waiting_registered = True

            if self._metrics.enabled or self._can_log_debug():
                current_count, _, _ = await self._runner.status_async(
                    self._script_client,
                    self.owners_key,
                    now_ms,
                    self._identifier,
                )
                self._log_waiting_status(current_count)

            if self._check_timeout(state):
                self._handle_timeout(state)

            await self._wait_for_slot_async(state)

    def release(self) -> bool:
        """Release the semaphore slot.

        Returns:
            True if successfully released, False if not owned.

        Raises:
            NotAcquiredError: If attempting to release an unacquired semaphore.
            MixedModeError: If semaphore was acquired with async API.
        """
        if self._identifier is None:
            logger.error(
                "Release called on unacquired semaphore '%s'",
                self._config.name,
            )
            raise NotAcquiredError("Cannot release: semaphore was never acquired")

        self._check_mode(_AcquireMode.SYNC)
        self._stop_heartbeat()
        self._ensure_scripts()

        released = self._runner.release(
            self._script_client,
            self.owners_key,
            self._identifier,
        )

        if released:
            self._notify_waiters()
            self._reset_state()

        return released

    async def arelease(self) -> bool:
        """Asynchronously release the semaphore slot.

        Raises:
            NotAcquiredError: If attempting to release an unacquired semaphore.
            MixedModeError: If semaphore was acquired with sync API.
        """
        if self._identifier is None:
            logger.error(
                "Release called on unacquired semaphore '%s'",
                self._config.name,
            )
            raise NotAcquiredError("Cannot release: semaphore was never acquired")

        self._check_mode(_AcquireMode.ASYNC)
        await self._stop_heartbeat_async()
        await self._ensure_scripts_async()

        released = await self._runner.release_async(
            self._script_client,
            self.owners_key,
            self._identifier,
        )

        if released:
            await self._notify_waiters_async()
            self._reset_state()

        return released

    def _reset_state(self) -> None:
        """Reset internal state after release."""
        self._state = LockState.RELEASED
        self._identifier = None
        self._fencing_token = None
        self._expires_at = None
        self._acquire_mode = _AcquireMode.NONE
        self._lock_lost_event.clear()

    def wait_for_lock_lost(self, timeout: float | None = None) -> bool:
        """Wait for the lock to be lost.

        This is useful in mission-critical systems where you want to
        explicitly wait for and handle lock loss events.

        Args:
            timeout: Maximum time to wait in seconds. None means wait forever.

        Returns:
            True if lock was lost, False if timeout occurred.

        Example:
            # In a background thread monitoring lock health
            if sem.wait_for_lock_lost(timeout=5.0):
                logger.critical("Lock lost! Initiating graceful shutdown...")
                shutdown()
        """
        return self._lock_lost_event.wait(timeout=timeout)

    async def await_lock_lost(self, timeout: float | None = None) -> bool:
        """Async version of wait_for_lock_lost.

        Args:
            timeout: Maximum time to wait in seconds. None means wait forever.

        Returns:
            True if lock was lost, False if timeout occurred.
        """
        poll_interval = 0.05  # 50ms polling
        elapsed = 0.0

        while True:
            if self._lock_lost_event.is_set():
                return True

            if timeout is not None and elapsed >= timeout:
                return False

            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

    def refresh(self) -> bool:
        """Refresh the lock TTL.

        Returns:
            True if successfully refreshed, False if lock was lost.

        Raises:
            LockLostError: If lock was lost and strict_mode is enabled.
        """
        if self._identifier is None or self._state != LockState.ACQUIRED:
            return False

        self._ensure_scripts()

        lock_timeout_ms = int(self._config.lock_timeout * 1000)
        now_ms = self._get_time_ms()

        success = self._runner.refresh(
            self._script_client,
            self.owners_key,
            self._identifier,
            lock_timeout_ms,
            now_ms,
        )

        if success:
            self._expires_at = (now_ms + lock_timeout_ms) / 1000
        else:
            self._state = LockState.LOST
            self._lock_lost_event.set()
            self._stop_heartbeat()
            self._check_lock_lost()

        return success

    async def arefresh(self) -> bool:
        """Asynchronously refresh the lock TTL.

        Raises:
            LockLostError: If lock was lost and strict_mode is enabled.
        """
        if self._identifier is None or self._state != LockState.ACQUIRED:
            return False

        await self._ensure_scripts_async()

        lock_timeout_ms = int(self._config.lock_timeout * 1000)
        now_ms = await self._get_time_ms_async()

        success = await self._runner.refresh_async(
            self._script_client,
            self.owners_key,
            self._identifier,
            lock_timeout_ms,
            now_ms,
        )

        if success:
            self._expires_at = (now_ms + lock_timeout_ms) / 1000
        else:
            self._state = LockState.LOST
            self._lock_lost_event.set()
            await self._stop_heartbeat_async()
            self._check_lock_lost()

        return success

    def _start_heartbeat(self) -> None:
        """Start the heartbeat thread."""
        if self._heartbeat is not None:
            return

        def on_lost(identifier: str) -> None:
            self._state = LockState.LOST
            self._lock_lost_event.set()
            if self._on_lock_lost:
                self._on_lock_lost(identifier)
            if self._metrics.enabled:
                self._metrics.inc_lock_lost(self._config.name, self._config.namespace)

        if self._identifier is None:
            raise AcquireError("Cannot start heartbeat without identifier")
        self._heartbeat = Heartbeat(
            refresh_fn=self.refresh,
            interval=self._refresh_interval,
            identifier=self._identifier,
            on_lock_lost=on_lost,
        )
        self._heartbeat.start()

    def _stop_heartbeat(self) -> None:
        """Stop the heartbeat thread."""
        if self._heartbeat is not None:
            self._heartbeat.stop()
            self._heartbeat = None

    async def _start_heartbeat_async(self) -> None:
        """Start the async heartbeat task."""
        if self._async_heartbeat is not None:
            return

        async def on_lost(identifier: str) -> None:
            self._state = LockState.LOST
            self._lock_lost_event.set()
            if self._on_lock_lost:
                result = self._on_lock_lost(identifier)
                if asyncio.iscoroutine(result):
                    await result
            if self._metrics.enabled:
                self._metrics.inc_lock_lost(self._config.name, self._config.namespace)

        if self._identifier is None:
            raise AcquireError("Cannot start heartbeat without identifier")

        self._async_heartbeat = Heartbeat(
            refresh_fn=self.arefresh,
            interval=self._refresh_interval,
            identifier=self._identifier,
            on_lock_lost=on_lost,
        )
        await self._async_heartbeat.astart()

    async def _stop_heartbeat_async(self) -> None:
        """Stop the async heartbeat task."""
        if self._async_heartbeat is not None:
            await self._async_heartbeat.astop()
            self._async_heartbeat = None

    # Context manager protocol
    def __enter__(self) -> Self:
        result = self.acquire(blocking=True)
        if not result.success:
            raise AcquireError(f"Failed to acquire semaphore '{self._config.name}'")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()

    async def __aenter__(self) -> Self:
        result = await self.aacquire(blocking=True)
        if not result.success:
            raise AcquireError(f"Failed to acquire semaphore '{self._config.name}'")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.arelease()

    def __del__(self) -> None:
        self._stop_heartbeat()


class Mutex(Semaphore):
    """Mutex (binary semaphore) - exclusive lock.

    Equivalent to Semaphore with limit=1.

    Usage:
        # Basic usage
        with Mutex(client, "my-lock") as lock:
            # exclusive access
            pass

        # Mission-critical mode (recommended for production)
        with Mutex(
            client,
            "critical-lock",
            strict_mode=True,      # Raises LockLostError immediately
            use_server_time=True,  # Avoids clock skew issues
        ) as lock:
            # critical work
            pass
    """

    def __init__(
        self,
        client: redis.Redis | aioredis.Redis,
        name: str,
        *,
        lock_timeout: float = 30.0,
        acquire_timeout: float | None = None,
        retry_interval: float = 0.1,
        refresh_interval: float | None = None,
        namespace: str = "mutex",
        strict_mode: bool = False,
        use_server_time: bool = False,
        on_lock_lost: LockLostCallback | None = None,
    ) -> None:
        config = SemaphoreConfig(
            name=name,
            limit=1,
            lock_timeout=lock_timeout,
            acquire_timeout=acquire_timeout,
            retry_interval=retry_interval,
            refresh_interval=refresh_interval,
            namespace=namespace,
            strict_mode=strict_mode,
            use_server_time=use_server_time,
        )
        super().__init__(client, config, on_lock_lost=on_lock_lost)
