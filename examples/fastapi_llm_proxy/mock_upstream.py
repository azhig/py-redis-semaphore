"""Mock upstream LLM server for testing the proxy."""

from __future__ import annotations

import asyncio
import time
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

        async def stream_body():
            try:
                if sleep > 0:
                    await asyncio.sleep(sleep)
                chunks = [
                    {"choices": [{"delta": {"content": "Hello"}}]},
                    {"choices": [{"delta": {"content": ", "}}]},
                    {"choices": [{"delta": {"content": "world!"}}]},
                ]
                for chunk in chunks:
                    data = f"data: {chunk}\n\n"
                    yield data.encode("utf-8")
                    await asyncio.sleep(0.1)
                yield b"data: [DONE]\n\n"
            finally:
                await _exit_model(model)

        return StreamingResponse(stream_body(), media_type="text/event-stream")

    if sleep > 0:
        await asyncio.sleep(sleep)

    response = {
        "id": f"mock-{int(time.time() * 1000)}",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": "Hello from mock upstream"},
                "finish_reason": "stop",
            }
        ],
    }
    try:
        return JSONResponse(response)
    finally:
        await _exit_model(model)
