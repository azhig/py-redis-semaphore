"""Small async client to load test the proxy."""

from __future__ import annotations

import argparse
import asyncio
import statistics
import time
from dataclasses import dataclass
from typing import Any

import httpx


@dataclass
class Result:
    status: str
    duration: float
    error: str | None = None


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    values_sorted = sorted(values)
    k = int((len(values_sorted) - 1) * pct)
    return values_sorted[k]


async def _run_one(
    client: httpx.AsyncClient,
    url: str,
    headers: dict[str, str],
    payload: dict[str, Any],
    stream: bool,
    timeout: float,
) -> Result:
    start = time.perf_counter()
    try:
        response = await client.post(url, json=payload, headers=headers, timeout=timeout)
        if stream:
            async for _ in response.aiter_bytes():
                pass
        else:
            await response.aread()
        duration = time.perf_counter() - start
        return Result(status=str(response.status_code), duration=duration)
    except Exception as exc:
        duration = time.perf_counter() - start
        return Result(status="error", duration=duration, error=str(exc))


async def run_test(args: argparse.Namespace) -> None:
    url = args.url.rstrip("/")
    headers = {"direction": str(args.direction)}
    if args.api_key:
        headers["x-api-key"] = args.api_key

    payload = {
        "model": args.model,
        "messages": [{"role": "user", "content": "ping"}],
        "stream": args.stream,
        "sleep": args.sleep,
    }

    results: list[Result] = []

    async with httpx.AsyncClient() as client:

        async def runner(index: int) -> None:
            result = await _run_one(
                client=client,
                url=f"{url}/v1/chat/completions",
                headers=headers,
                payload=payload,
                stream=args.stream,
                timeout=args.timeout,
            )
            results.append(result)

        await asyncio.gather(*(runner(i) for i in range(args.requests)))

    durations = [r.duration for r in results]
    status_counts: dict[str, int] = {}
    for result in results:
        status_counts[result.status] = status_counts.get(result.status, 0) + 1

    print("Results:")
    print(f"  group: direction={args.direction}, model={args.model}")
    for status, count in sorted(status_counts.items()):
        print(f"  {status}: {count}")

    if durations:
        print(
            "Timings (seconds):",
            f"min={min(durations):.3f}",
            f"avg={statistics.mean(durations):.3f}",
            f"p95={_percentile(durations, 0.95):.3f}",
            f"max={max(durations):.3f}",
        )

    errors = [r for r in results if r.error]
    if errors:
        print(f"Errors: {len(errors)} (first: {errors[0].error})")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load test the LLM proxy")
    parser.add_argument("--url", default="http://localhost:8000", help="Proxy base URL")
    parser.add_argument("--direction", type=int, default=1, help="department header")
    parser.add_argument("--model", default="mock-1", help="model name")
    parser.add_argument("--requests", type=int, default=10, help="total requests")
    parser.add_argument("--sleep", type=float, default=0.0, help="upstream sleep seconds")
    parser.add_argument("--stream", action="store_true", help="use streaming")
    parser.add_argument("--timeout", type=float, default=120.0, help="per-request timeout")
    parser.add_argument("--api-key", default="", help="optional x-api-key")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(run_test(args))


if __name__ == "__main__":
    main()
