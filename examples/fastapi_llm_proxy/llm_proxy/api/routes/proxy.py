"""Catch-all proxy for non-chat endpoints."""

from __future__ import annotations

from typing import Any

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import Response, StreamingResponse

from llm_proxy.infrastructure import (
    build_upstream_headers,
    build_upstream_url,
    filter_response_headers,
)
from llm_proxy.logging_setup import logger
from llm_proxy.responses import upstream_error

router = APIRouter()


@router.api_route(
    "/{full_path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
)
async def proxy_passthrough(request: Request, full_path: str) -> Response:
    """Proxy all other endpoints without rate limiting."""
    settings = request.app.state.settings
    http_client: httpx.AsyncClient = request.app.state.http

    upstream_url = build_upstream_url(settings.upstream_base_url, request)
    api_key = request.headers.get("x-api-key")
    upstream_headers = build_upstream_headers(dict(request.headers), api_key)

    body = await request.body()
    request_kwargs: dict[str, Any] = {"headers": upstream_headers}
    if body:
        request_kwargs["content"] = body

    try:
        upstream_request = http_client.build_request(request.method, upstream_url, **request_kwargs)
        upstream_response = await http_client.send(upstream_request, stream=True)
    except httpx.HTTPError as exc:
        logger.bind(error=str(exc)).exception("Upstream request failed")
        return upstream_error("Upstream request failed")

    headers = filter_response_headers(upstream_response.headers)

    if upstream_response.status_code >= 400:
        body_bytes = await upstream_response.aread()
        await upstream_response.aclose()
        return Response(
            content=body_bytes,
            status_code=upstream_response.status_code,
            headers=headers,
            media_type=upstream_response.headers.get("content-type"),
        )

    async def stream_body():
        try:
            async for chunk in upstream_response.aiter_bytes():
                yield chunk
        finally:
            await upstream_response.aclose()

    return StreamingResponse(
        stream_body(),
        status_code=upstream_response.status_code,
        headers=headers,
        media_type=upstream_response.headers.get("content-type"),
    )
