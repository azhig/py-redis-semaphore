"""Semaphore pool manager for per-department and per-model limits."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

from redis_semaphore import Semaphore, SemaphoreConfig
from redis_semaphore.types import AcquireMode


@dataclass(frozen=True)
class SemaphoreKey:
    department: int
    model: str

    @property
    def name(self) -> str:
        return f"dept_{self.department}:{self.model}"


class SemaphorePool:
    """Cache of semaphore configs keyed by department+model."""

    def __init__(self, redis_client, settings) -> None:
        self._redis = redis_client
        self._capacity = settings.semaphore_capacity
        self._lock_timeout = settings.semaphore_lock_timeout
        self._acquire_timeout = settings.semaphore_acquire_timeout
        self._namespace = settings.semaphore_namespace
        self._configs: dict[str, SemaphoreConfig] = {}
        self._lock = asyncio.Lock()

    def pool_size(self) -> int:
        return len(self._configs)

    async def get_semaphore(self, department: int, model: str) -> Semaphore:
        key = SemaphoreKey(department=department, model=model).name
        if key not in self._configs:
            async with self._lock:
                if key not in self._configs:
                    self._configs[key] = SemaphoreConfig(
                        name=key,
                        limit=self._capacity,
                        lock_timeout=self._lock_timeout,
                        acquire_timeout=self._acquire_timeout,
                        namespace=self._namespace,
                        acquire_mode=AcquireMode.BLPOP,
                    )
        config = self._configs[key]
        return Semaphore(self._redis, config)

    async def get_semaphore_from_key(self, key: str) -> Semaphore:
        if key not in self._configs:
            async with self._lock:
                if key not in self._configs:
                    self._configs[key] = SemaphoreConfig(
                        name=key,
                        limit=self._capacity,
                        lock_timeout=self._lock_timeout,
                        acquire_timeout=self._acquire_timeout,
                        namespace=self._namespace,
                        acquire_mode=AcquireMode.BLPOP,
                    )
        config = self._configs[key]
        return Semaphore(self._redis, config)

    async def snapshot(self) -> list[dict[str, object]]:
        async with self._lock:
            items = list(self._configs.items())

        snapshot: list[dict[str, object]] = []
        for name, config in items:
            used = await self._get_used_slots(name)
            snapshot.append(
                {
                    "name": name,
                    "limit": config.limit,
                    "lock_timeout": config.lock_timeout,
                    "acquire_timeout": config.acquire_timeout,
                    "used_slots": used,
                }
            )
        return snapshot

    async def _get_used_slots(self, name: str) -> int:
        try:
            now_ms = int(time.time() * 1000)
            owners_key = f"{self._namespace}:{name}:owners"
            await self._redis.zremrangebyscore(owners_key, 0, now_ms)
            return int(await self._redis.zcard(owners_key))
        except Exception:
            return -1
