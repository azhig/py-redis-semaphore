"""Redis connection and health monitoring."""

from __future__ import annotations

import asyncio

import redis.asyncio as aioredis
from fastapi import FastAPI

from llm_proxy.core import InflightTracker, SemaphorePool
from llm_proxy.logging_setup import logger
from llm_proxy.metrics import set_redis_available


async def close_redis(client: aioredis.Redis) -> None:
    """Close Redis client and connection pool."""
    close_result = client.close()
    if asyncio.iscoroutine(close_result):
        await close_result
    await client.connection_pool.disconnect()


async def mark_redis_unavailable(app: FastAPI) -> None:
    """Mark Redis as unavailable and notify inflight tracker."""
    app.state.redis_available = False
    set_redis_available(False)
    await app.state.inflight_tracker.set_redis_available(False)
    await app.state.reservation_manager.reset()


async def cleanup_expired_semaphores(
    pool: SemaphorePool, redis_client: aioredis.Redis, inflight_tracker: InflightTracker
) -> None:
    """Clean up stale semaphore entries from Redis.

    When Redis reconnects after being unavailable, we know:
    - redis_inflight_count = active requests still running
    - Entries in Redis = active + dead (couldn't release)

    Strategy: Delete ALL entries. Active requests (with redis_inflight > 0)
    are still executing and will re-acquire through their next heartbeat.
    """
    namespace = pool._namespace

    # Get all semaphore keys (only owners sets, not fencing/queue)
    pattern = f"{namespace}:*:owners"
    try:
        keys = await redis_client.keys(pattern)
        if not keys:
            logger.info("No semaphore keys to clean up")
            return

        # Get active redis_inflight counts
        redis_inflight_counts = await inflight_tracker.snapshot_redis_inflight_counts()

        logger.info(
            f"Force-cleaning {len(keys)} semaphore keys. "
            f"Active redis_inflight: {redis_inflight_counts}"
        )

        for key in keys:
            # Force delete ALL entries
            # Requests with redis_inflight > 0 will re-acquire via heartbeat
            count_before = await redis_client.zcard(key)
            await redis_client.delete(key)
            key_str = key.decode()
            logger.info(f"Cleared {count_before} entries from {key_str}")

            # Extract semaphore name from key (format: namespace:name:owners)
            parts = key_str.split(":")
            if len(parts) >= 3:
                sem_name = ":".join(parts[1:-1])  # Everything between namespace and :owners
                active = redis_inflight_counts.get(sem_name, 0)
                if active > 0:
                    logger.info(f"  → {active} active requests will re-acquire via heartbeat")
    except Exception as exc:
        logger.warning(f"Failed to cleanup semaphores: {exc}")


async def redis_watchdog(app: FastAPI) -> None:
    """Watchdog that monitors Redis availability."""
    try:
        while True:
            try:
                await app.state.redis.ping()
            except Exception:
                is_available = False
            else:
                is_available = True

            if is_available != app.state.redis_available:
                if is_available:
                    # Redis reconnected - use distributed lock to coordinate cleanup
                    lock_key = f"{app.state.pool._namespace}:reconnect_lock"
                    lock_acquired = False

                    try:
                        # Try to acquire reconnect lock (expires in 30s)
                        lock_acquired = await app.state.redis.set(lock_key, "1", nx=True, ex=30)

                        if lock_acquired:
                            logger.info("Acquired reconnect lock - performing cleanup")
                            # Clean up semaphore entries from Redis
                            await cleanup_expired_semaphores(
                                app.state.pool, app.state.redis, app.state.inflight_tracker
                            )
                            # Small delay to let other workers detect Redis is back
                            await asyncio.sleep(0.5)
                        else:
                            logger.info("Another worker is handling reconnect - waiting")
                            # Wait for the lock to be released (cleanup finished)
                            for _ in range(60):  # Wait up to 30 seconds
                                if not await app.state.redis.exists(lock_key):
                                    break
                                await asyncio.sleep(0.5)

                        # Each worker reserves slots for its own fallback requests
                        counts = await app.state.inflight_tracker.snapshot_fallback_counts()
                        if counts:
                            logger.info(f"Redis reconnected, fallback counts: {counts}")
                            await app.state.reservation_manager.reserve_for_fallback(counts)

                        # Now mark Redis as available
                        app.state.redis_available = is_available
                        set_redis_available(is_available)
                        await app.state.inflight_tracker.set_redis_available(is_available)

                        logger.info("Redis reconnected - migration complete")

                    finally:
                        # Release lock if we acquired it
                        if lock_acquired:
                            await app.state.redis.delete(lock_key)
                else:
                    # Redis disconnected
                    app.state.redis_available = is_available
                    set_redis_available(is_available)
                    await app.state.inflight_tracker.set_redis_available(is_available)
                    await app.state.reservation_manager.reset()
                    logger.warning("Redis connection lost")

            await asyncio.sleep(app.state.redis_check_interval)
    except asyncio.CancelledError:
        return


def redis_is_available(app: FastAPI) -> bool:
    """Return current Redis availability status.

    The watchdog handles all availability checks and state transitions.
    """
    return app.state.redis_available
