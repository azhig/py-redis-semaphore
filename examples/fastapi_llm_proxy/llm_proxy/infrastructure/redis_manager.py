"""Redis connection and availability handling."""

from __future__ import annotations

import asyncio

import redis.asyncio as aioredis
from fastapi import FastAPI

from llm_proxy.logging_setup import logger


async def close_redis(client: aioredis.Redis) -> None:
    """Close Redis client and connection pool."""
    close_result = client.close()
    if asyncio.iscoroutine(close_result):
        await close_result
    await client.connection_pool.disconnect()


async def mark_redis_unavailable(app: FastAPI) -> None:
    """Mark Redis as unavailable and clear readiness event."""
    if app.state.redis_available:
        logger.warning("Redis connection lost - marking unavailable")
    app.state.redis_available = False
    app.state.redis_ready.clear()


async def wait_for_redis(app: FastAPI) -> None:
    """Block until Redis responds to PING.

    Ensures only one waiter performs the polling loop.
    """
    if app.state.redis_ready.is_set():
        return

    async with app.state.redis_recover_lock:
        if app.state.redis_ready.is_set():
            return

        while True:
            try:
                await app.state.redis.ping()
            except Exception:
                await asyncio.sleep(app.state.redis_check_interval)
                continue
            app.state.redis_available = True
            app.state.redis_ready.set()
            logger.info("Redis recovered")
            return


def redis_is_available(app: FastAPI) -> bool:
    """Return current Redis availability status."""
    return app.state.redis_available
