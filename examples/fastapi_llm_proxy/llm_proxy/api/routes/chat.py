"""Chat completions endpoint with semaphore-based rate limiting."""

from __future__ import annotations

import contextlib
import time
from typing import Any

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import Response, StreamingResponse
from redis_semaphore.errors import AcquireTimeoutError, LockLostError

from llm_proxy.api.proxy_common import (
    BackendUnavailable,
    ProxyBadRequest,
    RequestContext,
    acquire_slot,
    make_context,
    parse_proxy_request,
    resolve_upstream,
)
from llm_proxy.api.request_logging import (
    extract_usage_from_body,
    status_phrase,
    update_usage_from_sse_line,
)
from llm_proxy.infrastructure import filter_response_headers
from llm_proxy.logging_setup import logger
from llm_proxy.responses import bad_request, service_unavailable, upstream_error

router = APIRouter()


@router.post("/v1/chat/completions")
async def proxy_chat_completions(request: Request) -> Response:
    """Proxy chat completion requests with per-client/model rate limiting."""
    settings = request.app.state.settings
    http_client: httpx.AsyncClient = request.app.state.http
    ctx = make_context(request)

    try:
        client_id, model, payload = await parse_proxy_request(request, ctx)
    except ProxyBadRequest as exc:
        ctx.log(response_status_code=400, response_status_detail=exc.detail)
        return bad_request(exc.detail, exc.code)

    ctx.stream = payload.get("stream") is True
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

    handler = _handle_streaming if ctx.stream else _handle_non_streaming
    return await handler(
        ctx=ctx,
        http_client=http_client,
        semaphore=semaphore,
        queue_wait_seconds=queue_wait_seconds,
        processing_start=processing_start,
        upstream_url=upstream_url,
        upstream_headers=upstream_headers,
        payload=payload,
    )


async def _handle_non_streaming(
    *,
    ctx: RequestContext,
    http_client: httpx.AsyncClient,
    semaphore,
    queue_wait_seconds: float,
    processing_start: float,
    upstream_url: str,
    upstream_headers: dict[str, str],
    payload: dict[str, Any],
) -> Response:
    """Handle non-streaming chat completion request."""
    input_tokens = None
    output_tokens = None
    upstream_status_code = None
    upstream_status_detail = None
    response_status_code = 500
    response_status_detail = None
    error = None
    try:
        response = await http_client.post(upstream_url, json=payload, headers=upstream_headers)
        upstream_status_code = response.status_code
        response_status_code = response.status_code
        input_tokens, output_tokens = extract_usage_from_body(response.content)
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
            output_tokens=output_tokens,
            upstream_status_code=upstream_status_code,
            upstream_status_detail=upstream_status_detail,
            error=error,
        )
        with contextlib.suppress(Exception):
            await semaphore.arelease()


async def _handle_streaming(
    *,
    ctx: RequestContext,
    http_client: httpx.AsyncClient,
    semaphore,
    queue_wait_seconds: float,
    processing_start: float,
    upstream_url: str,
    upstream_headers: dict[str, str],
    payload: dict[str, Any],
) -> Response:
    """Handle streaming chat completion request."""
    try:
        upstream_request = http_client.build_request(
            "POST", upstream_url, json=payload, headers=upstream_headers
        )
        upstream_response = await http_client.send(upstream_request, stream=True)
    except httpx.HTTPError as exc:
        logger.bind(error=str(exc)).exception("Upstream request failed")
        ctx.log(
            response_status_code=502,
            response_status_detail="Upstream request failed",
            queue_wait_seconds=queue_wait_seconds,
            processing_seconds=time.perf_counter() - processing_start,
            upstream_status_detail=str(exc),
            error=str(exc),
        )
        with contextlib.suppress(Exception):
            await semaphore.arelease()
        return upstream_error("Upstream request failed")

    upstream_status_code = upstream_response.status_code
    headers = filter_response_headers(upstream_response.headers)

    if upstream_response.status_code >= 400:
        body = await upstream_response.aread()
        await upstream_response.aclose()
        body_text = body.decode("utf-8", errors="replace")
        detail = body_text or upstream_response.reason_phrase or status_phrase(upstream_status_code)
        ctx.log(
            response_status_code=upstream_status_code,
            response_status_detail=detail,
            queue_wait_seconds=queue_wait_seconds,
            processing_seconds=time.perf_counter() - processing_start,
            upstream_status_code=upstream_status_code,
            upstream_status_detail=detail,
        )
        with contextlib.suppress(Exception):
            await semaphore.arelease()
        return Response(
            content=body,
            status_code=upstream_response.status_code,
            headers=headers,
            media_type=upstream_response.headers.get("content-type"),
        )

    usage_state: dict[str, int | None] = {"prompt_tokens": None, "completion_tokens": None}
    buffer = b""

    async def stream_body():
        nonlocal buffer
        lock_lost = False
        try:
            async for chunk in upstream_response.aiter_bytes():
                # The heartbeat may declare the slot lost mid-stream (e.g. Redis
                # blip longer than lock_timeout). We can't change an already-sent
                # status, but we stop early so a slot we no longer own doesn't
                # keep streaming as if rate-limited.
                if semaphore.is_lost:
                    lock_lost = True
                    logger.bind(client_id=ctx.client_id, model=ctx.model).warning(
                        "Semaphore lock lost mid-stream; aborting"
                    )
                    break
                buffer += chunk
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    update_usage_from_sse_line(line, usage_state)
                yield chunk
        finally:
            if buffer:
                update_usage_from_sse_line(buffer, usage_state)
            await upstream_response.aclose()
            ctx.log(
                response_status_code=upstream_status_code or 200,
                response_status_detail="Semaphore lock lost mid-stream" if lock_lost else None,
                queue_wait_seconds=queue_wait_seconds,
                processing_seconds=time.perf_counter() - processing_start,
                input_tokens=usage_state["prompt_tokens"],
                output_tokens=usage_state["completion_tokens"],
                upstream_status_code=upstream_status_code,
                error="lock_lost" if lock_lost else None,
            )
            with contextlib.suppress(Exception):
                await semaphore.arelease()

    return StreamingResponse(
        stream_body(),
        status_code=upstream_response.status_code,
        headers=headers,
        media_type=upstream_response.headers.get("content-type", "text/event-stream"),
    )
