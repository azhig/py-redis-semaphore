"""Basic usage examples for redis-semaphore."""

import asyncio

import redis
import redis.asyncio as aioredis

from redis_semaphore import Mutex, Semaphore, SemaphoreConfig


def sync_example() -> None:
    client = redis.Redis()

    config = SemaphoreConfig(name="api", limit=3, lock_timeout=10.0)
    with Semaphore(client, config) as sem:
        print(f"Semaphore acquired. Token={sem.fencing_token}")

    with Mutex(client, "critical-section") as lock:
        print(f"Mutex acquired. Token={lock.fencing_token}")

    client.close()


async def async_example() -> None:
    client = aioredis.Redis()

    async with Mutex(client, "async-critical") as lock:
        print(f"Async mutex acquired. Token={lock.fencing_token}")

    await client.aclose()


if __name__ == "__main__":
    sync_example()
    asyncio.run(async_example())
