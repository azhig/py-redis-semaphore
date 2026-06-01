"""Heartbeat mechanism for automatic TTL refresh."""

from __future__ import annotations

import asyncio
import threading
import time
from contextlib import suppress

from .logger import logger


class Heartbeat:
    """Heartbeat for automatic TTL refresh (sync + async).

    When ``lock_timeout`` is provided, refresh failures caused by connection
    errors are tolerated for up to ``lock_timeout`` seconds (measured from the
    start of the last successful refresh attempt, i.e. conservatively). During
    that window the heartbeat retries at the shorter ``retry_step`` interval.
    If the lock cannot be refreshed before the deadline, it is treated as lost
    (``on_lock_lost`` fires) — this matches the moment the server-side entry
    expires, so a stale holder stops believing it owns the lock.

    When ``lock_timeout`` is None, deadline-based escalation is disabled and
    connection errors are simply logged and retried forever (legacy behavior).

    ``is_fatal_error`` lets the caller classify a raised refresh error as
    permanent (e.g. the command is denied by ACL): such errors will never
    succeed on retry, so the lock is treated as lost immediately instead of
    waiting out the whole deadline.
    """

    def __init__(
        self,
        refresh_fn,
        interval: float,
        identifier: str,
        on_lock_lost=None,
        lock_timeout: float | None = None,
        retry_step: float | None = None,
        is_fatal_error=None,
    ) -> None:
        self._refresh_fn = refresh_fn
        self._interval = interval
        self._identifier = identifier
        self._on_lock_lost = on_lock_lost
        self._lock_timeout = lock_timeout
        self._retry_step = retry_step if retry_step is not None else interval
        self._is_fatal_error = is_fatal_error
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

    def cancel_async(self) -> None:
        """Best-effort cancel of the async task from a sync context (no await).

        Used by process-exit cleanup and __del__, where awaiting is impossible.
        """
        task = self._task
        if task is not None and not task.done():
            with suppress(Exception):
                task.cancel()
        self._task = None

    def _mark_lost_sync(self) -> None:
        """Mark the lock as lost and fire the callback (sync)."""
        self._lock_lost = True
        if self._on_lock_lost:
            self._on_lock_lost(self._identifier)

    async def _mark_lost_async(self) -> None:
        """Mark the lock as lost and fire the callback (async)."""
        self._lock_lost = True
        if self._on_lock_lost:
            result = self._on_lock_lost(self._identifier)
            if asyncio.iscoroutine(result):
                await result

    def _heartbeat_loop(self) -> None:
        """Main heartbeat loop."""
        lock_timeout = self._lock_timeout
        deadline = time.monotonic() + lock_timeout if lock_timeout is not None else None
        step = self._interval
        while not self._stop_event.wait(timeout=step):
            attempt_start = time.monotonic()
            try:
                success = self._refresh_fn()
            except Exception as exc:
                logger.exception("Heartbeat refresh failed (sync) for %s", self._identifier)
                if self._is_fatal_error is not None and self._is_fatal_error(exc):
                    # Permanent failure (e.g. ACL denial) - retrying is futile.
                    self._mark_lost_sync()
                    break
                if deadline is not None:
                    now = time.monotonic()
                    if now >= deadline:
                        self._mark_lost_sync()
                        break
                    step = min(self._retry_step, deadline - now)
                continue
            if not success:
                self._mark_lost_sync()
                break
            # Successful refresh: reset deadline conservatively from attempt start.
            if lock_timeout is not None:
                deadline = attempt_start + lock_timeout
                step = self._interval

    async def _heartbeat_loop_async(self) -> None:
        """Main async heartbeat loop."""
        lock_timeout = self._lock_timeout
        deadline = time.monotonic() + lock_timeout if lock_timeout is not None else None
        step = self._interval
        while True:
            await asyncio.sleep(step)
            attempt_start = time.monotonic()
            try:
                success = await self._refresh_fn()
            except asyncio.CancelledError:
                raise  # pragma: no cover
            except Exception as exc:
                logger.exception("Heartbeat refresh failed (async) for %s", self._identifier)
                if self._is_fatal_error is not None and self._is_fatal_error(exc):
                    # Permanent failure (e.g. ACL denial) - retrying is futile.
                    await self._mark_lost_async()
                    break
                if deadline is not None:
                    now = time.monotonic()
                    if now >= deadline:
                        await self._mark_lost_async()
                        break
                    step = min(self._retry_step, deadline - now)
                continue
            if not success:
                await self._mark_lost_async()
                break
            # Successful refresh: reset deadline conservatively from attempt start.
            if lock_timeout is not None:
                deadline = attempt_start + lock_timeout
                step = self._interval

    @property
    def is_lock_lost(self) -> bool:
        """Check if the lock was lost."""
        return self._lock_lost
