"""Helpers for structured request logging."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime, timezone
from http import HTTPStatus

from llm_proxy.logging_setup import logger


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def header_value(headers: Mapping[str, str], *names: str) -> str | None:
    for name in names:
        raw = headers.get(name)
        if raw is None:
            continue
        value = raw.strip()
        if value:
            return value
    return None


def status_phrase(code: int | None) -> str | None:
    if code is None:
        return None
    try:
        return HTTPStatus(code).phrase
    except ValueError:
        return None


def extract_usage_from_body(body: bytes) -> tuple[int | None, int | None]:
    try:
        payload = json.loads(body)
    except Exception:
        return None, None
    return extract_usage_from_payload(payload)


def extract_usage_from_payload(payload: object) -> tuple[int | None, int | None]:
    if not isinstance(payload, dict):
        return None, None
    usage = payload.get("usage")
    if not isinstance(usage, dict):
        return None, None
    prompt_tokens = usage.get("prompt_tokens")
    completion_tokens = usage.get("completion_tokens")
    prompt = prompt_tokens if isinstance(prompt_tokens, int) else None
    completion = completion_tokens if isinstance(completion_tokens, int) else None
    return prompt, completion


def update_usage_from_sse_line(line: bytes, usage_state: dict[str, int | None]) -> None:
    data = line.strip()
    if not data or not data.startswith(b"data:"):
        return
    payload = data[5:].strip()
    if payload == b"[DONE]" or not payload:
        return
    try:
        parsed = json.loads(payload)
    except Exception:
        return
    prompt, completion = extract_usage_from_payload(parsed)
    if prompt is not None:
        usage_state["prompt_tokens"] = prompt
    if completion is not None:
        usage_state["completion_tokens"] = completion


def log_proxy_request(
    *,
    endpoint: str,
    stream: bool,
    start_time: datetime,
    end_time: datetime,
    queue_wait_seconds: float | None,
    processing_seconds: float | None,
    client_id: str | None,
    model: str | None,
    session_id: str | None,
    run_id: str | None,
    input_tokens: int | None,
    output_tokens: int | None,
    upstream_status_code: int | None,
    upstream_status_detail: str | None,
    response_status_code: int,
    response_status_detail: str | None,
    error: str | None = None,
) -> None:
    logger.info(
        "proxy_request",
        endpoint=endpoint,
        stream=stream,
        request_started_at=start_time.isoformat(),
        request_finished_at=end_time.isoformat(),
        queue_wait_seconds=queue_wait_seconds,
        processing_seconds=processing_seconds,
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
