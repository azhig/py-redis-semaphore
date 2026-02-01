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


def rate_limit_response() -> JSONResponse:
    return JSONResponse(
        status_code=429,
        content={
            "error": {
                "message": "Queue wait timeout exceeded (60 seconds)",
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
