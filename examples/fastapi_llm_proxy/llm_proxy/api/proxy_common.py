"""Shared request pipeline for the rate-limited proxy endpoints.

`chat.py` and `embeddings.py` differ only in how they stream the upstream
response; everything else — header parsing, validation, the Redis-recovery
acquire loop, and structured logging — lives here so it is written once.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from fastapi import Request
from redis.exceptions import RedisError
from redis_semaphore import Semaphore

from llm_proxy.api.dependencies import parse_client_id
from llm_proxy.api.request_logging import header_value, log_proxy_request, utc_now
from llm_proxy.core import SemaphorePool
from llm_proxy.infrastructure import (
    build_upstream_headers,
    build_upstream_url,
    mark_redis_unavailable,
    redis_is_available,
    wait_for_redis,
)


class ProxyBadRequest(Exception):
    """A client error detected before proxying (maps to HTTP 400)."""

    def __init__(self, detail: str, code: str) -> None:
        super().__init__(detail)
        self.detail = detail
        self.code = code


class BackendUnavailable(Exception):
    """Redis stayed unreachable past the wait budget (maps to HTTP 503).

    Distinct from ``AcquireTimeoutError`` (slots full -> 429): here the backend
    itself is down, so there is no point making the client wait indefinitely.
    """


@dataclass
class RequestContext:
    """Per-request logging/timing context.

    Collects the fields that every ``log_proxy_request`` call needs once, so
    individual log points only pass what actually varies (status, tokens,
    timings) instead of repeating ~15 keyword arguments.
    """

    endpoint: str
    stream: bool = False
    start_time: datetime = field(default_factory=utc_now)
    queue_start: float = field(default_factory=time.perf_counter)
    client_id: str | None = None
    model: str | None = None
    session_id: str | None = None
    run_id: str | None = None

    def log(
        self,
        *,
        response_status_code: int,
        response_status_detail: str | None = None,
        queue_wait_seconds: float | None = None,
        processing_seconds: float | None = None,
        input_tokens: int | None = None,
        output_tokens: int | None = None,
        upstream_status_code: int | None = None,
        upstream_status_detail: str | None = None,
        error: str | None = None,
    ) -> None:
        """Emit one structured proxy_request log line for this request."""
        log_proxy_request(
            endpoint=self.endpoint,
            stream=self.stream,
            start_time=self.start_time,
            end_time=utc_now(),
            queue_wait_seconds=queue_wait_seconds,
            processing_seconds=processing_seconds,
            client_id=self.client_id,
            model=self.model,
            session_id=self.session_id,
            run_id=self.run_id,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            upstream_status_code=upstream_status_code,
            upstream_status_detail=upstream_status_detail,
            response_status_code=response_status_code,
            response_status_detail=response_status_detail,
            error=error,
        )


def make_context(request: Request) -> RequestContext:
    """Build a request context, capturing session/run ids and start time."""
    return RequestContext(
        endpoint=request.url.path,
        session_id=header_value(request.headers, "session_id", "x-session-id"),
        run_id=header_value(request.headers, "run_id", "x-run-id"),
    )


async def parse_proxy_request(
    request: Request, ctx: RequestContext
) -> tuple[str, str, dict[str, Any]]:
    """Validate headers/body and return ``(client_id, model, payload)``.

    Also mutates ``ctx`` with the resolved client_id/model so any subsequent
    log line (including error paths) carries them. Raises ``ProxyBadRequest``
    on any validation failure.
    """
    client_id = parse_client_id(request.headers)
    if client_id is None:
        raise ProxyBadRequest("Missing x-client-id header", "missing_client_id")
    if not client_id:
        raise ProxyBadRequest("x-client-id must be a non-empty string", "invalid_client_id")
    ctx.client_id = client_id

    try:
        payload = await request.json()
    except Exception as exc:
        raise ProxyBadRequest("Request body must be valid JSON", "invalid_json") from exc

    model = payload.get("model")
    if not isinstance(model, str) or not model.strip():
        raise ProxyBadRequest("Request JSON must include non-empty 'model'", "missing_model")

    model = model.strip()
    ctx.model = model
    request.state.client_id = client_id
    request.state.model = model
    return client_id, model, payload


def resolve_upstream(request: Request, ctx: RequestContext) -> tuple[str, dict[str, str]]:
    """Resolve the upstream URL and forwarded headers for this request.

    Honors per-client/model ``upstream_base_url`` overrides, falling back to
    the global setting. Requires ``ctx.client_id`` / ``ctx.model`` to be set.
    """
    settings = request.app.state.settings
    overrides = request.app.state.client_model_overrides
    base_url = overrides.upstream_base_url(ctx.client_id, ctx.model) or settings.upstream_base_url
    upstream_url = build_upstream_url(base_url, request)
    upstream_headers = build_upstream_headers(dict(request.headers))
    return upstream_url, upstream_headers


async def acquire_slot(request: Request, client_id: str, model: str) -> Semaphore:
    """Acquire a semaphore slot, tolerating transient Redis outages.

    Two distinct failure modes are bounded:
      * slots full -> ``aacquire`` raises ``AcquireTimeoutError`` after
        ``acquire_timeout`` (handled by the caller as HTTP 429);
      * Redis unreachable -> waiting for recovery is capped by the same
        ``acquire_timeout`` budget, after which ``BackendUnavailable`` is
        raised (HTTP 503) instead of blocking the request forever.
    """
    pool: SemaphorePool = request.app.state.pool
    budget = request.app.state.settings.semaphore_acquire_timeout
    deadline = time.monotonic() + budget
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise BackendUnavailable("Redis unavailable: wait budget exceeded")
        if not redis_is_available(request.app):
            await _wait_for_redis_bounded(request, remaining)
            continue
        try:
            semaphore = await pool.get_semaphore(client_id, model)
            await semaphore.aacquire(blocking=True)
            return semaphore
        except (RedisError, OSError, ConnectionError):
            await mark_redis_unavailable(request.app)
            await _wait_for_redis_bounded(request, deadline - time.monotonic())


async def _wait_for_redis_bounded(request: Request, remaining: float) -> None:
    """Wait for Redis recovery, but no longer than ``remaining`` seconds."""
    if remaining <= 0:
        raise BackendUnavailable("Redis unavailable: wait budget exceeded")
    try:
        await asyncio.wait_for(wait_for_redis(request.app), timeout=remaining)
    except asyncio.TimeoutError as exc:
        raise BackendUnavailable("Redis unavailable: wait budget exceeded") from exc
