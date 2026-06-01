"""Response helpers for proxy errors."""

from __future__ import annotations

from typing import Any

from fastapi.responses import JSONResponse


def _error_payload(message: str, error_type: str, code: str) -> dict[str, Any]:
    return {
        "error": {
            "message": message,
            "type": error_type,
            "code": code,
        }
    }


def bad_request(message: str, code: str) -> JSONResponse:
    return JSONResponse(
        status_code=400,
        content=_error_payload(message, "invalid_request_error", code),
    )


def rate_limit_response(timeout_seconds: float | None = None) -> JSONResponse:
    if timeout_seconds is None:
        message = "Queue wait timeout exceeded"
    else:
        message = f"Queue wait timeout exceeded ({timeout_seconds:g}s)"
    return JSONResponse(
        status_code=429,
        content={
            "error": {
                "message": message,
                "type": "rate_limit_error",
                "code": "queue_timeout",
            }
        },
    )


def service_unavailable(message: str, code: str) -> JSONResponse:
    return JSONResponse(
        status_code=503,
        content=_error_payload(message, "service_unavailable", code),
    )


def upstream_error(message: str) -> JSONResponse:
    return JSONResponse(
        status_code=502,
        content=_error_payload(message, "upstream_error", "bad_gateway"),
    )
