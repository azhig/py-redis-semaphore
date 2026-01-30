"""Chat completions endpoint with semaphore-based rate limiting."""

from __future__ import annotations

import time
from typing import Any

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import Response, StreamingResponse

from llm_proxy.api.dependencies import (
    acquire_semaphore,
    cleanup_semaphore,
    parse_department,
)
from llm_proxy.core import InflightTracker, ReservationManager
from llm_proxy.infrastructure import (
    build_upstream_headers,
    build_upstream_url,
    filter_response_headers,
)
from llm_proxy.logging_setup import logger
from llm_proxy.responses import bad_request, service_unavailable, upstream_error
from redis_semaphore.errors import LockLostError

router = APIRouter()


@router.post("/v1/chat/completions")
async def proxy_chat_completions(request: Request) -> Response:
    """Proxy chat completion requests with per-department/model rate limiting."""
    settings = request.app.state.settings
    http_client: httpx.AsyncClient = request.app.state.http
    inflight_tracker: InflightTracker = request.app.state.inflight_tracker
    reservation_manager: ReservationManager = request.app.state.reservation_manager

    start_time = time.perf_counter()

    department = parse_department(request.headers)
    if department is None:
        return bad_request("Missing direction header", "missing_direction")
    if department < 1 or department > 20:
        return bad_request("direction must be an integer between 1 and 20", "invalid_direction")

    api_key = request.headers.get("x-api-key")

    try:
        payload = await request.json()
    except Exception:
        return bad_request("Request body must be valid JSON", "invalid_json")

    model = payload.get("model")
    if not isinstance(model, str) or not model.strip():
        return bad_request("Request JSON must include non-empty 'model'", "missing_model")

    model = model.strip()
    request.state.department = department
    request.state.model = model
    inflight_key = f"dept_{department}:{model}"

    upstream_url = build_upstream_url(settings.upstream_base_url, request)
    upstream_headers = build_upstream_headers(dict(request.headers), api_key)

    semaphore, used_fallback, used_redis_semaphore = await acquire_semaphore(
        request, department, model, start_time
    )

    if payload.get("stream") is True:
        return await _handle_streaming(
            app=request.app,
            http_client=http_client,
            semaphore=semaphore,
            inflight_tracker=inflight_tracker,
            reservation_manager=reservation_manager,
            inflight_key=inflight_key,
            used_fallback=used_fallback,
            used_redis_semaphore=used_redis_semaphore,
            department=department,
            model=model,
            start_time=start_time,
            upstream_url=upstream_url,
            upstream_headers=upstream_headers,
            payload=payload,
        )

    return await _handle_non_streaming(
        app=request.app,
        http_client=http_client,
        semaphore=semaphore,
        inflight_tracker=inflight_tracker,
        reservation_manager=reservation_manager,
        inflight_key=inflight_key,
        used_fallback=used_fallback,
        used_redis_semaphore=used_redis_semaphore,
        department=department,
        model=model,
        start_time=start_time,
        upstream_url=upstream_url,
        upstream_headers=upstream_headers,
        payload=payload,
    )


async def _handle_non_streaming(
    *,
    app,
    http_client: httpx.AsyncClient,
    semaphore,
    inflight_tracker: InflightTracker,
    reservation_manager: ReservationManager,
    inflight_key: str,
    used_fallback: bool,
    used_redis_semaphore: bool,
    department: int,
    model: str,
    start_time: float,
    upstream_url: str,
    upstream_headers: dict[str, str],
    payload: dict[str, Any],
) -> Response:
    """Handle non-streaming chat completion request."""
    status = "500"
    try:
        response = await http_client.post(
            upstream_url,
            json=payload,
            headers=upstream_headers,
        )
        status = str(response.status_code)
        headers = filter_response_headers(response.headers)
        return Response(
            content=response.content,
            status_code=response.status_code,
            headers=headers,
            media_type=response.headers.get("content-type"),
        )
    except LockLostError:
        status = "503"
        return service_unavailable("Semaphore lock lost", "lock_lost")
    except httpx.HTTPError as exc:
        logger.bind(error=str(exc)).exception("Upstream request failed")
        status = "502"
        return upstream_error("Upstream request failed")
    finally:
        duration = time.perf_counter() - start_time
        await cleanup_semaphore(
            app=app,
            semaphore=semaphore,
            inflight_tracker=inflight_tracker,
            inflight_key=inflight_key,
            used_fallback=used_fallback,
            used_redis_semaphore=used_redis_semaphore,
            department=department,
            model=model,
            status=status,
            duration=duration,
        )


async def _handle_streaming(
    *,
    app,
    http_client: httpx.AsyncClient,
    semaphore,
    inflight_tracker: InflightTracker,
    reservation_manager: ReservationManager,
    inflight_key: str,
    used_fallback: bool,
    used_redis_semaphore: bool,
    department: int,
    model: str,
    start_time: float,
    upstream_url: str,
    upstream_headers: dict[str, str],
    payload: dict[str, Any],
) -> Response:
    """Handle streaming chat completion request."""
    status = "500"
    try:
        upstream_request = http_client.build_request(
            "POST",
            upstream_url,
            json=payload,
            headers=upstream_headers,
        )
        upstream_response = await http_client.send(upstream_request, stream=True)
    except httpx.HTTPError as exc:
        status = "502"
        logger.bind(error=str(exc)).exception("Upstream request failed")
        duration = time.perf_counter() - start_time
        await cleanup_semaphore(
            app=app,
            semaphore=semaphore,
            inflight_tracker=inflight_tracker,
            inflight_key=inflight_key,
            used_fallback=used_fallback,
            used_redis_semaphore=used_redis_semaphore,
            department=department,
            model=model,
            status=status,
            duration=duration,
        )
        return upstream_error("Upstream request failed")

    status = str(upstream_response.status_code)
    headers = filter_response_headers(upstream_response.headers)

    if upstream_response.status_code >= 400:
        body = await upstream_response.aread()
        await upstream_response.aclose()
        duration = time.perf_counter() - start_time
        await cleanup_semaphore(
            app=app,
            semaphore=semaphore,
            inflight_tracker=inflight_tracker,
            inflight_key=inflight_key,
            used_fallback=used_fallback,
            used_redis_semaphore=used_redis_semaphore,
            department=department,
            model=model,
            status=status,
            duration=duration,
        )
        return Response(
            content=body,
            status_code=upstream_response.status_code,
            headers=headers,
            media_type=upstream_response.headers.get("content-type"),
        )

    async def stream_body():
        nonlocal status
        try:
            async for chunk in upstream_response.aiter_bytes():
                yield chunk
        finally:
            await upstream_response.aclose()
            duration = time.perf_counter() - start_time
            await cleanup_semaphore(
                app=app,
                semaphore=semaphore,
                inflight_tracker=inflight_tracker,
                inflight_key=inflight_key,
                used_fallback=used_fallback,
                used_redis_semaphore=used_redis_semaphore,
                department=department,
                model=model,
                status=status,
                duration=duration,
            )

    return StreamingResponse(
        stream_body(),
        status_code=upstream_response.status_code,
        headers=headers,
        media_type=upstream_response.headers.get("content-type", "text/event-stream"),
    )
