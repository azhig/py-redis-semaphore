"""Agents sharing per-certificate RPM/TPM quotas through RateLimiter.

Each agent owns three certificates; every certificate has its own
requests-per-minute and tokens-per-minute quota. An agent processes its own
queue in order: for each job it picks the first certificate with room for the
estimated cost, or sleeps until the soonest one frees up. After the (simulated)
HTTP call the reservation is settled with the real token count.

Quotas live in Redis, so they hold across processes and machines.
"""

import asyncio
import random

import redis.asyncio as aioredis

from redis_semaphore import RateLimit, RateLimiter, RateLimiterConfig, RatePermit


def make_limiter(client: aioredis.Redis, cert_id: str) -> RateLimiter:
    config = RateLimiterConfig(
        name=cert_id,
        limits=[
            RateLimit("requests", limit=5, window=60.0),  # RPM
            RateLimit("tokens", limit=4_000, window=60.0),  # TPM
        ],
    )
    return RateLimiter(client, config)


async def acquire_any(limiters: list[RateLimiter], tokens: int) -> tuple[RateLimiter, RatePermit]:
    """Reserve the estimated cost on the first certificate that has room."""
    while True:
        denied = []
        for limiter in limiters:
            permit = await limiter.aacquire({"requests": 1, "tokens": tokens}, blocking=False)
            if permit.success:
                return limiter, permit
            denied.append(permit.retry_after or 0.0)
        await asyncio.sleep(min(denied) + random.uniform(0, 0.05))


async def call_api(cert_id: str, prompt: str) -> int:
    """Stand-in for the HTTP request; returns the tokens actually used."""
    await asyncio.sleep(0.1)
    return random.randint(300, 900)


async def agent(name: str, limiters: list[RateLimiter], queue: asyncio.Queue[str]) -> None:
    while True:
        prompt = await queue.get()
        try:
            estimate = 800
            limiter, permit = await acquire_any(limiters, estimate)
            try:
                used = await call_api(limiter.config.name, prompt)
            except Exception:
                # The request may have reached the server: keep it counted,
                # but release the reserved tokens.
                await limiter.aadjust(permit, {"tokens": 0})
                raise
            await limiter.aadjust(permit, {"tokens": used})
            print(f"{name}: {prompt!r} via {limiter.config.name}, {used} tokens")
        finally:
            queue.task_done()


async def main() -> None:
    client = aioredis.Redis()
    agents = {
        f"agent-{a}": [make_limiter(client, f"agent-{a}:cert-{c}") for c in range(3)]
        for a in range(2)
    }

    workers = []
    queues = []
    for name, limiters in agents.items():
        queue: asyncio.Queue[str] = asyncio.Queue()
        for i in range(8):
            queue.put_nowait(f"job-{i}")
        queues.append(queue)
        workers.append(asyncio.create_task(agent(name, limiters, queue)))

    for queue in queues:
        await queue.join()
    for worker in workers:
        worker.cancel()
    await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
