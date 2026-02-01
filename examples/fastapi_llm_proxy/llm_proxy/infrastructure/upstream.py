"""HTTP helper utilities for proxying."""

from __future__ import annotations

from urllib.parse import urlsplit, urlunsplit

import httpx
from fastapi import Request


def build_upstream_url(base_url: str, request: Request) -> str:
    base = base_url.rstrip("/")
    request_path = request.url.path

    split_base = urlsplit(base)
    base_path = split_base.path.rstrip("/")

    if base_path and request_path.startswith(base_path):
        # Avoid double /v1 when upstream_base_url already includes it.
        suffix = request_path[len(base_path) :]
        combined_path = f"{base_path}{suffix}"
    else:
        combined_path = f"{base_path}{request_path}"

    combined_path = combined_path or "/"
    return urlunsplit(
        (
            split_base.scheme,
            split_base.netloc,
            combined_path,
            request.url.query,
            "",
        )
    )


def build_upstream_headers(headers: dict[str, str]) -> dict[str, str]:
    hop_by_hop = {
        "host",
        "content-length",
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
    }
    return {key: value for key, value in headers.items() if key.lower() not in hop_by_hop}


def filter_response_headers(headers: httpx.Headers) -> dict[str, str]:
    hop_by_hop = {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
        "content-length",
    }
    return {key: value for key, value in headers.items() if key.lower() not in hop_by_hop}
