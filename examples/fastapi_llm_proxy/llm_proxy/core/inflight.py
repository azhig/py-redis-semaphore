"""Inflight tracking for fallback mode."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

from redis_semaphore.errors import AcquireTimeoutError


@dataclass
class _InflightState:
    cond: asyncio.Condition
    fallback_count: int = 0
    redis_inflight_count: int = 0  # Requests holding Redis semaphore slots
    waiters: int = 0
    migrating_to_redis: bool = False  # True during Redis migration


class InflightTracker:
    def __init__(self) -> None:
        self._states: dict[str, _InflightState] = {}
        self._states_lock = asyncio.Lock()
        self._redis_available = True

    async def _get_state(self, key: str) -> _InflightState:
        async with self._states_lock:
            state = self._states.get(key)
            if state is None:
                state = _InflightState(cond=asyncio.Condition())
                self._states[key] = state
            return state

    async def acquire_fallback(self, key: str, limit: int, timeout: float | None) -> bool:
        """Acquire a fallback slot if Redis is unavailable.

        The effective limit is: max(0, limit - redis_inflight_count)
        This ensures total concurrent requests never exceeds the configured limit.
        """
        if self._redis_available:
            return False
        state = await self._get_state(key)
        start = time.monotonic()
        async with state.cond:
            while True:
                # Dynamic limit: reduce by number of Redis in-flight requests
                effective_limit = max(0, limit - state.redis_inflight_count)

                # Check if we can acquire
                can_acquire = (
                    state.fallback_count < effective_limit and not state.migrating_to_redis
                )
                if can_acquire:
                    if not self._redis_available:
                        state.fallback_count += 1
                        return True
                    else:
                        # Redis came back while we were checking
                        return False

                # Need to wait
                if self._redis_available and not state.migrating_to_redis:
                    return False

                state.waiters += 1
                try:
                    if timeout is None:
                        await state.cond.wait()
                    else:
                        remaining = timeout - (time.monotonic() - start)
                        if remaining <= 0:
                            raise AcquireTimeoutError("Fallback acquire timeout")
                        await asyncio.wait_for(state.cond.wait(), timeout=remaining)
                except asyncio.TimeoutError as exc:
                    raise AcquireTimeoutError("Fallback acquire timeout") from exc
                finally:
                    state.waiters = max(0, state.waiters - 1)

    async def release_fallback(self, key: str) -> None:
        state = await self._get_state(key)
        async with state.cond:
            if state.fallback_count > 0:
                state.fallback_count -= 1
            state.cond.notify_all()
        await self._maybe_cleanup_state(key)

    async def increment_redis_inflight(self, key: str) -> None:
        """Increment count of requests holding Redis semaphore slots.

        Call this AFTER successful Redis semaphore acquire.
        """
        state = await self._get_state(key)
        async with state.cond:
            state.redis_inflight_count += 1

    async def decrement_redis_inflight(self, key: str) -> None:
        """Decrement count of requests holding Redis semaphore slots.

        Call this in finally block when request completes,
        REGARDLESS of whether Redis is available for release.
        """
        state = await self._get_state(key)
        async with state.cond:
            if state.redis_inflight_count > 0:
                state.redis_inflight_count -= 1
            # Notify waiters - a fallback slot may now be available
            state.cond.notify_all()
        await self._maybe_cleanup_state(key)

    async def _maybe_cleanup_state(self, key: str) -> None:
        """Remove state entry if no active requests or waiters."""
        async with self._states_lock:
            state = self._states.get(key)
            if state is None:
                return
            if state.fallback_count == 0 and state.redis_inflight_count == 0 and state.waiters == 0:
                self._states.pop(key, None)

    async def snapshot_fallback_counts(self) -> dict[str, int]:
        async with self._states_lock:
            return {key: state.fallback_count for key, state in self._states.items()}

    async def snapshot_redis_inflight_counts(self) -> dict[str, int]:
        """Return count of requests holding Redis semaphore slots per key."""
        async with self._states_lock:
            return {key: state.redis_inflight_count for key, state in self._states.items()}

    async def snapshot_waiters(self) -> dict[str, int]:
        """Return count of waiting (not active) requests per key."""
        async with self._states_lock:
            return {key: state.waiters for key, state in self._states.items()}

    async def start_migration(self, key: str) -> None:
        """Mark a key as migrating to Redis - waiters stay in fallback queue."""
        state = await self._get_state(key)
        async with state.cond:
            state.migrating_to_redis = True

    async def finish_migration(self, key: str) -> None:
        """Mark migration complete - release waiters from fallback queue."""
        state = await self._get_state(key)
        async with state.cond:
            state.migrating_to_redis = False
            state.cond.notify_all()  # Now waiters can exit fallback

    async def set_redis_available(self, is_available: bool) -> None:
        if self._redis_available == is_available:
            return
        self._redis_available = is_available
        # Notify all waiters so they can re-check redis availability
        # and exit fallback mode if Redis is back
        async with self._states_lock:
            for state in self._states.values():
                async with state.cond:
                    state.cond.notify_all()
