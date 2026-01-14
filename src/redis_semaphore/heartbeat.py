"""Heartbeat mechanism for automatic TTL refresh."""

from __future__ import annotations

import asyncio
import threading
from contextlib import suppress

from .logger import logger


class Heartbeat:
    """Heartbeat for automatic TTL refresh (sync + async)."""

    def __init__(
        self,
        refresh_fn,
        interval: float,
        identifier: str,
        on_lock_lost=None,
    ) -> None:
        self._refresh_fn = refresh_fn
        self._interval = interval
        self._identifier = identifier
        self._on_lock_lost = on_lock_lost
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._task: asyncio.Task[None] | None = None
        self._lock_lost = False

    def start(self) -> None:
        """Start the heartbeat thread."""
        if self._thread is not None:
            return

        self._stop_event.clear()
        self._lock_lost = False
        self._thread = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,
            name=f"heartbeat-{self._identifier[:8]}",
        )
        self._thread.start()

    def stop(self) -> None:
        """Stop the heartbeat thread."""
        self._stop_event.set()
        if self._thread is not None:
            if threading.current_thread() is not self._thread:
                self._thread.join(timeout=self._interval * 2)
            self._thread = None

    async def astart(self) -> None:
        """Start the heartbeat task."""
        if self._task is not None:
            return

        self._lock_lost = False
        self._task = asyncio.create_task(
            self._heartbeat_loop_async(),
            name=f"heartbeat-{self._identifier[:8]}",
        )

    async def astop(self) -> None:
        """Stop the heartbeat task."""
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    def _heartbeat_loop(self) -> None:
        """Main heartbeat loop."""
        while not self._stop_event.wait(timeout=self._interval):
            try:
                success = self._refresh_fn()
                if not success:
                    self._lock_lost = True
                    if self._on_lock_lost:
                        self._on_lock_lost(self._identifier)
                    break
            except Exception:
                logger.exception("Heartbeat refresh failed (sync) for %s", self._identifier)

    async def _heartbeat_loop_async(self) -> None:
        """Main async heartbeat loop."""
        while True:
            await asyncio.sleep(self._interval)
            try:
                success = await self._refresh_fn()
                if not success:
                    self._lock_lost = True
                    if self._on_lock_lost:
                        result = self._on_lock_lost(self._identifier)
                        if asyncio.iscoroutine(result):
                            await result
                    break
            except asyncio.CancelledError:
                raise  # pragma: no cover
            except Exception:
                logger.exception("Heartbeat refresh failed (async) for %s", self._identifier)

    @property
    def is_lock_lost(self) -> bool:
        """Check if the lock was lost."""
        return self._lock_lost
