"""Tests for cleanup handler behavior."""

import weakref

from redis_semaphore import semaphore as semaphore_module


def test_cleanup_all_semaphores_handles_errors(redis_client, monkeypatch):
    monkeypatch.setattr(semaphore_module, "_active_semaphores", weakref.WeakSet())

    class Dummy:
        def _stop_heartbeat(self):
            raise RuntimeError("boom")

    dummy = Dummy()
    semaphore_module._active_semaphores.add(dummy)

    semaphore_module._cleanup_all_semaphores()
