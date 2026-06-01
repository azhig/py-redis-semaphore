"""FastAPI LLM proxy example with per-client/model semaphores."""

from __future__ import annotations

import asyncio
import socket
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from redis_semaphore.errors import AcquireTimeoutError

from llm_proxy.api.routes import chat, embeddings, health, proxy
from llm_proxy.client_model_overrides import load_client_model_overrides
from llm_proxy.config import load_settings, settings_dict
from llm_proxy.core import SemaphorePool
from llm_proxy.infrastructure import close_redis
from llm_proxy.logging_setup import configure_logging, logger
from llm_proxy.metrics import setup_http_metrics, setup_semaphore_metrics
from llm_proxy.responses import rate_limit_response

try:
    import httpx
except ImportError as err:
    raise ImportError(
        "httpx is required for this example. Install with: pip install httpx"
    ) from err


def _tcp_keepalive_options() -> dict[int, int]:
    """Aggressive TCP keepalive so the OS reaps a dead socket in seconds.

    Without tuning, ``socket_keepalive=True`` uses kernel defaults (idle ~2h on
    Linux), so a half-open connection lingers far too long. Built portably:
    each option is included only if the platform exposes it (Linux uses
    TCP_KEEPIDLE; macOS uses TCP_KEEPALIVE for the idle timer).
    """
    options: dict[int, int] = {}
    idle = getattr(socket, "TCP_KEEPIDLE", None) or getattr(socket, "TCP_KEEPALIVE", None)
    if idle is not None:
        options[idle] = 30  # start probing after 30s idle
    if hasattr(socket, "TCP_KEEPINTVL"):
        options[socket.TCP_KEEPINTVL] = 10  # probe every 10s
    if hasattr(socket, "TCP_KEEPCNT"):
        options[socket.TCP_KEEPCNT] = 3  # drop after 3 failed probes
    return options


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    settings = load_settings()
    configure_logging(settings.log_level, settings.log_file)
    logger.bind(settings=settings_dict(settings)).info("Starting LLM proxy")
    setup_semaphore_metrics()

    overrides = load_client_model_overrides(settings.client_model_config_path)
    # Bounded blocking pool: excess callers wait for a free connection (up to
    # socket_timeout) instead of the pool growing without limit under BLPOP load.
    redis_pool = aioredis.BlockingConnectionPool(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        max_connections=settings.redis_max_connections,
        timeout=settings.redis_socket_timeout,
        health_check_interval=10,
        socket_keepalive=True,
        socket_keepalive_options=_tcp_keepalive_options(),
        # Read deadline: without it a half-open connection blocks forever
        # (until OS keepalive reaps it, ~2h with kernel defaults), freezing
        # every Redis-touching endpoint. Must stay < semaphore_lock_timeout.
        socket_timeout=settings.redis_socket_timeout,
        socket_connect_timeout=settings.redis_socket_connect_timeout,
        retry_on_timeout=True,
    )
    redis_client = aioredis.Redis(connection_pool=redis_pool)
    http_client = httpx.AsyncClient(timeout=settings.upstream_timeout)
    pool = SemaphorePool(
        redis_client,
        settings,
        capacity_overrides=overrides.semaphore_capacities,
    )

    app.state.settings = settings
    app.state.redis = redis_client
    app.state.http = http_client
    app.state.pool = pool
    app.state.client_model_overrides = overrides
    app.state.redis_available = True
    app.state.redis_check_interval = settings.redis_check_interval
    app.state.redis_ready = asyncio.Event()
    app.state.redis_recover_lock = asyncio.Lock()

    try:
        ping_result = redis_client.ping()
        if asyncio.iscoroutine(ping_result):
            await ping_result
    except Exception:
        app.state.redis_available = False
        app.state.redis_ready.clear()
    else:
        app.state.redis_available = True
        app.state.redis_ready.set()

    try:
        yield
    finally:
        await http_client.aclose()
        await close_redis(redis_client)


app = FastAPI(title="LLM Proxy with Redis Semaphores", lifespan=lifespan)
setup_http_metrics(app)


@app.exception_handler(AcquireTimeoutError)
async def acquire_timeout_handler(request: Request, exc: AcquireTimeoutError) -> JSONResponse:
    """Handle semaphore acquire timeout errors."""
    logger.bind(
        client_id=getattr(request.state, "client_id", None),
        model=getattr(request.state, "model", None),
    ).warning("Queue wait timeout")
    timeout = getattr(request.app.state.settings, "semaphore_acquire_timeout", None)
    return rate_limit_response(timeout)


# Register routers
app.include_router(chat.router, tags=["chat"])
app.include_router(embeddings.router, tags=["embeddings"])
app.include_router(health.router, tags=["health"])
app.include_router(proxy.router, tags=["proxy"])
