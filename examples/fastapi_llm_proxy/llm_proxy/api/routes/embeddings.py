"""Embeddings endpoint with semaphore-based rate limiting."""

from __future__ import annotations

import contextlib
import time

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import Response
from redis.exceptions import RedisError
from redis_semaphore.errors import AcquireTimeoutError, LockLostError

from llm_proxy.api.dependencies import parse_client_id
from llm_proxy.api.request_logging import (
    extract_usage_from_body,
    header_value,
    log_proxy_request,
    status_phrase,
    utc_now,
)
from llm_proxy.infrastructure import (
    build_upstream_headers,
    build_upstream_url,
    filter_response_headers,
    mark_redis_unavailable,
    redis_is_available,
    wait_for_redis,
)
from llm_proxy.logging_setup import logger
from llm_proxy.responses import bad_request, service_unavailable, upstream_error

router = APIRouter()


@router.post("/v1/embeddings")
async def proxy_embeddings(request: Request) -> Response:
    """Proxy embeddings requests with per-client/model rate limiting."""
    settings = request.app.state.settings
    http_client: httpx.AsyncClient = request.app.state.http
    start_time = utc_now()
    start_perf = time.perf_counter()
    queue_start = start_perf
    endpoint = request.url.path
    stream = False
    session_id = header_value(request.headers, "session_id", "x-session-id")
    run_id = header_value(request.headers, "run_id", "x-run-id")

    client_id = parse_client_id(request.headers)
    if client_id is None:
        log_proxy_request(
            endpoint=endpoint,
            stream=stream,
            start_time=start_time,
            end_time=utc_now(),
            queue_wait_seconds=None,
            processing_seconds=None,
            client_id=None,
            model=None,
            session_id=session_id,
            run_id=run_id,
            input_tokens=None,
            output_tokens=None,
            upstream_status_code=None,
            upstream_status_detail=None,
            response_status_code=400,
            response_status_detail="Missing x-client-id header",
        )
        return bad_request("Missing x-client-id header", "missing_client_id")
    if not client_id:
        log_proxy_request(
            endpoint=endpoint,
            stream=stream,
            start_time=start_time,
            end_time=utc_now(),
            queue_wait_seconds=None,
            processing_seconds=None,
            client_id=None,
            model=None,
            session_id=session_id,
            run_id=run_id,
            input_tokens=None,
            output_tokens=None,
            upstream_status_code=None,
            upstream_status_detail=None,
            response_status_code=400,
            response_status_detail="x-client-id must be a non-empty string",
        )
        return bad_request("x-client-id must be a non-empty string", "invalid_client_id")

    try:
        payload = await request.json()
    except Exception:
        log_proxy_request(
            endpoint=endpoint,
            stream=stream,
            start_time=start_time,
            end_time=utc_now(),
            queue_wait_seconds=None,
            processing_seconds=None,
            client_id=client_id,
            model=None,
            session_id=session_id,
            run_id=run_id,
            input_tokens=None,
            output_tokens=None,
            upstream_status_code=None,
            upstream_status_detail=None,
            response_status_code=400,
            response_status_detail="Request body must be valid JSON",
        )
        return bad_request("Request body must be valid JSON", "invalid_json")

    model = payload.get("model")
    if not isinstance(model, str) or not model.strip():
        log_proxy_request(
            endpoint=endpoint,
            stream=stream,
            start_time=start_time,
            end_time=utc_now(),
            queue_wait_seconds=None,
            processing_seconds=None,
            client_id=client_id,
            model=None,
            session_id=session_id,
            run_id=run_id,
            input_tokens=None,
            output_tokens=None,
            upstream_status_code=None,
            upstream_status_detail=None,
            response_status_code=400,
            response_status_detail="Request JSON must include non-empty 'model'",
        )
        return bad_request("Request JSON must include non-empty 'model'", "missing_model")

    model = model.strip()
    request.state.client_id = client_id
    request.state.model = model

    overrides = request.app.state.client_model_overrides
    upstream_base_url = overrides.upstream_base_url(client_id, model) or settings.upstream_base_url
    upstream_url = build_upstream_url(upstream_base_url, request)
    upstream_headers = build_upstream_headers(dict(request.headers))

    pool = request.app.state.pool
    try:
        while True:
            if not redis_is_available(request.app):
                await wait_for_redis(request.app)
                continue
            try:
                semaphore = await pool.get_semaphore(client_id, model)
                await semaphore.aacquire(blocking=True)
                break
            except (RedisError, OSError, ConnectionError):
                await mark_redis_unavailable(request.app)
                await wait_for_redis(request.app)
    except AcquireTimeoutError as exc:
        queue_wait_seconds = time.perf_counter() - queue_start
        log_proxy_request(
            endpoint=endpoint,
            stream=stream,
            start_time=start_time,
            end_time=utc_now(),
            queue_wait_seconds=queue_wait_seconds,
            processing_seconds=None,
            client_id=client_id,
            model=model,
            session_id=session_id,
            run_id=run_id,
            input_tokens=None,
            output_tokens=None,
            upstream_status_code=None,
            upstream_status_detail=None,
            response_status_code=429,
            response_status_detail="Queue wait timeout exceeded (60 seconds)",
            error=str(exc),
        )
        raise

    queue_wait_seconds = time.perf_counter() - queue_start
    processing_start = time.perf_counter()

    input_tokens = None
    output_tokens = None
    upstream_status_code = None
    upstream_status_detail = None
    response_status_code = 500
    response_status_detail = None
    error = None
    try:
        response = await http_client.post(
            upstream_url,
            json=payload,
            headers=upstream_headers,
        )
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
        processing_end = time.perf_counter()
        log_proxy_request(
            endpoint=endpoint,
            stream=stream,
            start_time=start_time,
            end_time=utc_now(),
            queue_wait_seconds=queue_wait_seconds,
            processing_seconds=processing_end - processing_start,
            client_id=client_id,
            model=model,
            session_id=session_id,
            run_id=run_id,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            upstream_status_code=upstream_status_code,
            upstream_status_detail=upstream_status_detail,
            response_status_code=response_status_code,
            response_status_detail=response_status_detail,
            error=error,
        )
        with contextlib.suppress(Exception):
            await semaphore.arelease()
