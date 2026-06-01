"""Health check and monitoring endpoints."""

from __future__ import annotations

from collections.abc import Awaitable
from typing import cast

import redis.asyncio as aioredis
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response

from llm_proxy.core import SemaphorePool
from llm_proxy.responses import service_unavailable

router = APIRouter()


@router.get("/health")
async def health() -> Response:
    """Liveness: the process is up and serving. Never touches Redis.

    Use this for the container/orchestrator liveness probe — a transient Redis
    outage must not cause the process to be killed and restarted.
    """
    return JSONResponse(status_code=200, content={"status": "ok"})


@router.get("/ready")
async def ready(request: Request) -> Response:
    """Readiness: Redis is actually reachable right now (live PING).

    Use this for the readiness probe / load-balancer gate — while Redis is
    down the instance should be pulled from rotation, not restarted. The PING
    is bounded by the client's socket_timeout.
    """
    redis_client: aioredis.Redis = request.app.state.redis
    try:
        # redis.asyncio types ping() as bool | Awaitable[bool]; it is always
        # awaitable at runtime for the async client.
        await cast(Awaitable[object], redis_client.ping())
    except Exception:
        return service_unavailable("Redis unavailable", "redis_unavailable")
    return JSONResponse(status_code=200, content={"status": "ready"})


@router.get("/semaphore/status")
async def semaphore_status(request: Request) -> Response:
    """Get semaphore pool status (debug endpoint)."""
    pool: SemaphorePool = request.app.state.pool
    snapshot = await pool.snapshot()
    return JSONResponse({"semaphores": snapshot, "pool_size": pool.pool_size()})
