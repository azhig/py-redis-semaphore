"""Embeddings endpoint with semaphore-based rate limiting."""

from __future__ import annotations

import contextlib
import time

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import Response
from redis_semaphore.errors import AcquireTimeoutError, LockLostError

from llm_proxy.api.proxy_common import (
    BackendUnavailable,
    ProxyBadRequest,
    acquire_slot,
    make_context,
    parse_proxy_request,
    resolve_upstream,
)
from llm_proxy.api.request_logging import extract_usage_from_body, status_phrase
from llm_proxy.infrastructure import filter_response_headers
from llm_proxy.logging_setup import logger
from llm_proxy.responses import bad_request, service_unavailable, upstream_error

router = APIRouter()


@router.post("/v1/embeddings")
async def proxy_embeddings(request: Request) -> Response:
    """Proxy embeddings requests with per-client/model rate limiting."""
    settings = request.app.state.settings
    http_client: httpx.AsyncClient = request.app.state.http
    ctx = make_context(request)

    try:
        client_id, model, payload = await parse_proxy_request(request, ctx)
    except ProxyBadRequest as exc:
        ctx.log(response_status_code=400, response_status_detail=exc.detail)
        return bad_request(exc.detail, exc.code)

    upstream_url, upstream_headers = resolve_upstream(request, ctx)

    try:
        semaphore = await acquire_slot(request, client_id, model)
    except AcquireTimeoutError as exc:
        timeout = settings.semaphore_acquire_timeout
        ctx.log(
            response_status_code=429,
            response_status_detail=f"Queue wait timeout exceeded ({timeout:g}s)",
            queue_wait_seconds=time.perf_counter() - ctx.queue_start,
            error=str(exc),
        )
        raise
    except BackendUnavailable as exc:
        ctx.log(
            response_status_code=503,
            response_status_detail="Redis unavailable",
            queue_wait_seconds=time.perf_counter() - ctx.queue_start,
            error=str(exc),
        )
        return service_unavailable("Redis unavailable", "redis_unavailable")

    queue_wait_seconds = time.perf_counter() - ctx.queue_start
    processing_start = time.perf_counter()

    input_tokens = None
    upstream_status_code = None
    upstream_status_detail = None
    response_status_code = 500
    response_status_detail = None
    error = None
    try:
        response = await http_client.post(upstream_url, json=payload, headers=upstream_headers)
        upstream_status_code = response.status_code
        response_status_code = response.status_code
        input_tokens, _ = extract_usage_from_body(response.content)
        if response.status_code != 200:
            upstream_status_detail = (
                response.text or response.reason_phrase or status_phrase(response.status_code)
            )
            response_status_detail = upstream_status_detail
        headers = filter_response_headers(response.headers)
        return Response(
            content=response.content,
            status_code=response.status_code,
            headers=headers,
            media_type=response.headers.get("content-type"),
        )
    except LockLostError:
        response_status_code = 503
        response_status_detail = "Semaphore lock lost"
        return service_unavailable("Semaphore lock lost", "lock_lost")
    except httpx.HTTPError as exc:
        logger.bind(error=str(exc)).exception("Upstream request failed")
        response_status_code = 502
        response_status_detail = "Upstream request failed"
        upstream_status_detail = str(exc)
        error = str(exc)
        return upstream_error("Upstream request failed")
    finally:
        ctx.log(
            response_status_code=response_status_code,
            response_status_detail=response_status_detail,
            queue_wait_seconds=queue_wait_seconds,
            processing_seconds=time.perf_counter() - processing_start,
            input_tokens=input_tokens,
            upstream_status_code=upstream_status_code,
            upstream_status_detail=upstream_status_detail,
            error=error,
        )
        with contextlib.suppress(Exception):
            await semaphore.arelease()
