"""Mock upstream LLM server for testing the proxy."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Iterable
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

app = FastAPI(title="Mock LLM Upstream")
MODEL_LIMIT = 5
_model_inflight: dict[str, int] = {}
_model_lock = asyncio.Lock()


async def _try_enter_model(model: str) -> bool:
    async with _model_lock:
        current = _model_inflight.get(model, 0)
        if current >= MODEL_LIMIT:
            return False
        _model_inflight[model] = current + 1
        return True


async def _exit_model(model: str) -> None:
    async with _model_lock:
        current = _model_inflight.get(model, 0)
        if current <= 1:
            _model_inflight.pop(model, None)
        else:
            _model_inflight[model] = current - 1


def _count_tokens(text: str) -> int:
    parts = [part for part in text.strip().split() if part]
    return len(parts)


def _collect_prompt_text(body: dict[str, Any]) -> str:
    messages = body.get("messages", [])
    if isinstance(messages, Iterable) and not isinstance(messages, (str, bytes, dict)):
        parts: list[str] = []
        for message in messages:
            if not isinstance(message, dict):
                continue
            content = message.get("content")
            if isinstance(content, str):
                parts.append(content)
        if parts:
            return " ".join(parts)
    prompt = body.get("prompt")
    if isinstance(prompt, str):
        return prompt
    return ""


def _build_usage(prompt_text: str, completion_text: str) -> dict[str, int]:
    prompt_tokens = _count_tokens(prompt_text)
    completion_tokens = _count_tokens(completion_text)
    return {
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": prompt_tokens + completion_tokens,
    }


@app.get("/v1/models")
async def list_models() -> JSONResponse:
    return JSONResponse(
        {
            "object": "list",
            "data": [
                {"id": "mock-1", "object": "model"},
                {"id": "mock-2", "object": "model"},
            ],
        }
    )


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    body: dict[str, Any] = await request.json()
    model = body.get("model", "mock")
    stream = bool(body.get("stream"))
    sleep = float(body.get("sleep", 5))
    prompt_text = _collect_prompt_text(body)

    if not await _try_enter_model(model):
        return JSONResponse(
            status_code=429,
            content={
                "error": {
                    "message": "Mock upstream rate limit exceeded",
                    "type": "rate_limit_error",
                    "code": "mock_rate_limit",
                }
            },
        )

    if stream:
        completion_text = "Hello, world!"
        usage = _build_usage(prompt_text, completion_text)

        async def stream_body():
            try:
                if sleep > 0:
                    await asyncio.sleep(sleep)
                chunks = [
                    {
                        "object": "chat.completion.chunk",
                        "choices": [{"delta": {"content": "Hello"}}],
                    },
                    {
                        "object": "chat.completion.chunk",
                        "choices": [{"delta": {"content": ", "}}],
                    },
                    {
                        "object": "chat.completion.chunk",
                        "choices": [{"delta": {"content": "world!"}}],
                    },
                    {
                        "object": "chat.completion.chunk",
                        "choices": [{"delta": {}, "finish_reason": "stop"}],
                        "usage": usage,
                    },
                ]
                for chunk in chunks:
                    data = f"data: {json.dumps(chunk)}\n\n"
                    yield data.encode("utf-8")
                    await asyncio.sleep(0.1)
                yield b"data: [DONE]\n\n"
            finally:
                await _exit_model(model)

        return StreamingResponse(stream_body(), media_type="text/event-stream")

    if sleep > 0:
        await asyncio.sleep(sleep)

    completion_text = "Hello from mock upstream"
    usage = _build_usage(prompt_text, completion_text)
    response = {
        "id": f"mock-{int(time.time() * 1000)}",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": completion_text},
                "finish_reason": "stop",
            }
        ],
        "usage": usage,
    }
    try:
        return JSONResponse(response)
    finally:
        await _exit_model(model)
