"""FastAPI LLM proxy example with per-department/model semaphores."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from llm_proxy.api.routes import chat, health, proxy
from llm_proxy.config import load_settings, settings_dict
from llm_proxy.core import InflightTracker, ReservationManager, SemaphorePool
from llm_proxy.infrastructure import close_redis, redis_watchdog
from llm_proxy.logging_setup import configure_logging, logger
from llm_proxy.metrics import (
    set_pool_size,
    set_redis_available,
    setup_http_metrics,
    setup_semaphore_metrics,
)
from llm_proxy.responses import rate_limit_response
from redis_semaphore.errors import AcquireTimeoutError

try:
    import httpx
except ImportError as err:
    raise ImportError(
        "httpx is required for this example. Install with: pip install httpx"
    ) from err


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    settings = load_settings()
    configure_logging(settings.log_level)
    logger.bind(settings=settings_dict(settings)).info("Starting LLM proxy")
    setup_semaphore_metrics()

    redis_client = aioredis.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        health_check_interval=10,
        socket_keepalive=True,
        socket_connect_timeout=5,
        retry_on_timeout=True,
    )
    http_client = httpx.AsyncClient(timeout=settings.upstream_timeout)
    pool = SemaphorePool(redis_client, settings)
    inflight_tracker = InflightTracker()
    reservation_manager = ReservationManager(pool, inflight_tracker)

    app.state.settings = settings
    app.state.redis = redis_client
    app.state.http = http_client
    app.state.pool = pool
    app.state.inflight_tracker = inflight_tracker
    app.state.reservation_manager = reservation_manager
    app.state.redis_available = True
    app.state.redis_check_interval = settings.redis_check_interval

    try:
        await redis_client.ping()
    except Exception:
        app.state.redis_available = False
        set_redis_available(False)
        await inflight_tracker.set_redis_available(False)
    else:
        app.state.redis_available = True
        set_redis_available(True)
        await inflight_tracker.set_redis_available(True)

    app.state.redis_watchdog_task = asyncio.create_task(redis_watchdog(app))

    set_pool_size(0)

    try:
        yield
    finally:
        app.state.redis_watchdog_task.cancel()
        await http_client.aclose()
        await close_redis(redis_client)


app = FastAPI(title="LLM Proxy with Redis Semaphores", lifespan=lifespan)
setup_http_metrics(app)


@app.exception_handler(AcquireTimeoutError)
async def acquire_timeout_handler(request: Request, exc: AcquireTimeoutError) -> JSONResponse:
    """Handle semaphore acquire timeout errors."""
    logger.bind(
        department=getattr(request.state, "department", None),
        model=getattr(request.state, "model", None),
    ).warning("Queue wait timeout")
    return rate_limit_response()


# Register routers
app.include_router(chat.router, tags=["chat"])
app.include_router(health.router, tags=["health"])
app.include_router(proxy.router, tags=["proxy"])
