"""FastAPI LLM proxy example with per-client/model semaphores."""

from __future__ import annotations

import asyncio
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    settings = load_settings()
    configure_logging(settings.log_level, settings.log_file)
    logger.bind(settings=settings_dict(settings)).info("Starting LLM proxy")
    setup_semaphore_metrics()

    overrides = load_client_model_overrides(settings.client_model_config_path)
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
    return rate_limit_response()


# Register routers
app.include_router(chat.router, tags=["chat"])
app.include_router(embeddings.router, tags=["embeddings"])
app.include_router(health.router, tags=["health"])
app.include_router(proxy.router, tags=["proxy"])
