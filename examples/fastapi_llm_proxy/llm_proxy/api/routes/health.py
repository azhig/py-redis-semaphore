"""Health check and monitoring endpoints."""

from __future__ import annotations

import redis.asyncio as aioredis
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response

from llm_proxy.core import SemaphorePool
from llm_proxy.responses import service_unavailable

router = APIRouter()


@router.get("/health")
async def health(request: Request) -> Response:
    """Check Redis connectivity."""
    redis_client: aioredis.Redis = request.app.state.redis
    try:
        await redis_client.ping()
    except Exception:
        return service_unavailable("Redis unavailable", "redis_unavailable")
    return JSONResponse(status_code=200, content={"status": "ok"})


@router.get("/semaphore/status")
async def semaphore_status(request: Request) -> Response:
    """Get semaphore pool status (debug endpoint)."""
    pool: SemaphorePool = request.app.state.pool
    snapshot = await pool.snapshot()
    return JSONResponse({"semaphores": snapshot, "pool_size": pool.pool_size()})
