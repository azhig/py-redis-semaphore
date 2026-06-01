"""HTTP helper utilities for proxying."""

from __future__ import annotations

from collections.abc import Iterable
from urllib.parse import urlsplit, urlunsplit

import httpx
from fastapi import Request

# RFC 7230 hop-by-hop headers: never forwarded through a proxy.
_HOP_BY_HOP = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
    }
)
# content-length is recomputed by the HTTP client on both legs; host is rewritten.
_REQUEST_STRIP = _HOP_BY_HOP | {"host", "content-length"}
_RESPONSE_STRIP = _HOP_BY_HOP | {"content-length"}


def _path_overlaps(request_path: str, base_path: str) -> bool:
    """True only if request_path is base_path or a path-segment below it.

    Segment-aware so a base of ``/v1`` does not swallow ``/v1beta/...``.
    """
    return request_path == base_path or request_path.startswith(base_path + "/")


def build_upstream_url(base_url: str, request: Request) -> str:
    request_path = request.url.path
    split_base = urlsplit(base_url.rstrip("/"))
    base_path = split_base.path.rstrip("/")

    if base_path and _path_overlaps(request_path, base_path):
        # Avoid double prefix when upstream_base_url already includes it (e.g. /v1).
        combined_path = request_path
    else:
        combined_path = f"{base_path}{request_path}"

    combined_path = combined_path or "/"
    return urlunsplit((split_base.scheme, split_base.netloc, combined_path, request.url.query, ""))


def _strip_headers(headers: Iterable[tuple[str, str]], drop: frozenset[str]) -> dict[str, str]:
    return {key: value for key, value in headers if key.lower() not in drop}


def build_upstream_headers(headers: dict[str, str]) -> dict[str, str]:
    return _strip_headers(headers.items(), _REQUEST_STRIP)


def filter_response_headers(headers: httpx.Headers) -> dict[str, str]:
    return _strip_headers(headers.items(), _RESPONSE_STRIP)
