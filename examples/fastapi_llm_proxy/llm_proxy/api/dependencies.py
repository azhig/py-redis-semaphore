"""FastAPI dependencies and utilities."""

from __future__ import annotations

from collections.abc import Mapping


def parse_client_id(headers: Mapping[str, str]) -> str | None:
    """Parse client id from header."""
    raw = headers.get("x-client-id")
    if raw is None:
        return None
    value = raw.strip()
    if not value or len(value) > 128:
        return ""
    return value
