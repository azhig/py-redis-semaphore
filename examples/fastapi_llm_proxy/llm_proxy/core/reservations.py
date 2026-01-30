"""Redis reservation manager for fallback inflight requests."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from llm_proxy.core.semaphore_pool import SemaphorePool


@dataclass
class _ReservationState:
    lock: asyncio.Lock
    cond: asyncio.Condition
    holders: list
    ready: bool = True


class ReservationManager:
    def __init__(self, pool: SemaphorePool, inflight_tracker=None) -> None:
        self._pool = pool
        self._inflight_tracker = inflight_tracker
        self._states: dict[str, _ReservationState] = {}
        self._states_lock = asyncio.Lock()

    async def _get_state(self, key: str) -> _ReservationState:
        async with self._states_lock:
            state = self._states.get(key)
            if state is None:
                lock = asyncio.Lock()
                state = _ReservationState(lock=lock, cond=asyncio.Condition(), holders=[])
                self._states[key] = state
            return state

    async def reserve_for_fallback(self, counts: dict[str, int]) -> None:
        """Reserve Redis slots for active fallback requests and migrate waiters."""
        # Step 1: Mark all keys as migrating (prevents waiters from exiting fallback)
        if self._inflight_tracker:
            for key in counts:
                await self._inflight_tracker.start_migration(key)

        # Step 2: Reserve slots for active fallback requests
        for key, count in counts.items():
            state = await self._get_state(key)
            async with state.lock:
                state.ready = False
            await self._ensure_reserved(key, count)
            async with state.cond:
                state.ready = True
                state.cond.notify_all()

        # Step 3: Mark migration complete - waiters can now exit fallback and use Redis
        if self._inflight_tracker:
            for key in counts:
                await self._inflight_tracker.finish_migration(key)

    async def _ensure_reserved(self, key: str, target: int) -> None:
        state = await self._get_state(key)
        async with state.lock:
            while len(state.holders) < target:
                sem = await self._pool.get_semaphore_from_key(key)
                await sem.aacquire(blocking=True)
                state.holders.append(sem)
            while len(state.holders) > target:
                sem = state.holders.pop()
                await sem.arelease()

    async def wait_ready(self, key: str) -> None:
        state = await self._get_state(key)
        async with state.cond:
            while not state.ready:
                await state.cond.wait()

    async def release_one(self, key: str) -> None:
        state = await self._get_state(key)
        async with state.lock:
            if state.holders:
                sem = state.holders.pop()
                try:
                    await sem.arelease()
                except Exception:
                    return

    async def reset(self) -> None:
        async with self._states_lock:
            self._states = {}
