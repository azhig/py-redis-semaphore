"""FastAPI dependencies and utilities."""

from __future__ import annotations

import time
from typing import Any

from fastapi import Request
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError

from llm_proxy.core import InflightTracker, ReservationManager, SemaphorePool
from llm_proxy.infrastructure import mark_redis_unavailable, redis_is_available
from llm_proxy.logging_setup import logger
from llm_proxy.metrics import (
    fallback_inflight_dec,
    fallback_inflight_inc,
    in_progress_dec,
    in_progress_inc,
    queue_dec,
    queue_inc,
    rate_limit_hit,
    record_request,
    redis_inflight_dec,
    redis_inflight_inc,
    set_pool_size,
)
from redis_semaphore.errors import AcquireTimeoutError


def parse_department(headers: dict[str, str]) -> int | None:
    """Parse department from direction header."""
    raw = headers.get("direction")
    if raw is None:
        return None
    try:
        value = int(raw)
    except ValueError:
        return -1
    return value


async def acquire_semaphore(
    request: Request,
    department: int,
    model: str,
    start_time: float,
) -> tuple[Any | None, bool, bool]:
    """Acquire semaphore (Redis or fallback).

    Returns:
        (semaphore, used_fallback, used_redis_semaphore)
    """
    pool: SemaphorePool = request.app.state.pool
    inflight_tracker: InflightTracker = request.app.state.inflight_tracker
    reservation_manager: ReservationManager = request.app.state.reservation_manager
    settings = request.app.state.settings

    inflight_key = f"dept_{department}:{model}"
    queue_inc(department, model)
    used_fallback = False
    used_redis_semaphore = False

    while True:
        use_redis = redis_is_available(request.app)
        if use_redis:
            await reservation_manager.wait_ready(inflight_key)
            try:
                semaphore = await pool.get_semaphore(department, model)
                set_pool_size(pool.pool_size())
                await semaphore.aacquire(blocking=True)
                await inflight_tracker.increment_redis_inflight(inflight_key)
                redis_inflight_inc(department, model)
                used_redis_semaphore = True
                break
            except AcquireTimeoutError:
                queue_dec(department, model)
                duration = time.perf_counter() - start_time
                rate_limit_hit(department, model)
                record_request(department, model, "429", duration)
                raise
            except (RedisError, OSError, ConnectionError) as exc:
                await mark_redis_unavailable(request.app)
                logger.bind(error=str(exc)).warning(
                    "Redis unavailable, switching to local semaphore"
                )
            except Exception as exc:
                await mark_redis_unavailable(request.app)
                logger.bind(error=str(exc)).exception("Semaphore acquire failed")

        try:
            acquired_fallback = await inflight_tracker.acquire_fallback(
                inflight_key,
                settings.fallback_semaphore_capacity,
                settings.semaphore_acquire_timeout,
            )
        except AcquireTimeoutError:
            queue_dec(department, model)
            duration = time.perf_counter() - start_time
            rate_limit_hit(department, model)
            record_request(department, model, "429", duration)
            raise

        if acquired_fallback:
            semaphore = None
            used_fallback = True
            fallback_inflight_inc(department, model)
            break

    queue_dec(department, model)
    in_progress_inc(department, model)

    return semaphore, used_fallback, used_redis_semaphore


async def safe_release(semaphore) -> None:
    """Safely release a semaphore."""
    from redis_semaphore.errors import NotAcquiredError

    try:
        is_acquired = getattr(semaphore, "is_acquired", None)
        if is_acquired is not None and not is_acquired:
            logger.debug("Skipping release for unacquired semaphore")
            return
        await semaphore.arelease()
    except NotAcquiredError:
        logger.debug("Release skipped: semaphore not acquired")
    except RedisConnectionError:
        from llm_proxy.metrics import record_release_failure

        record_release_failure("connection")
        logger.warning("Release failed: redis connection error")
    except Exception:
        from llm_proxy.metrics import record_release_failure

        record_release_failure("other")
        logger.exception("Failed to release semaphore")


async def cleanup_semaphore(
    app: Any,
    semaphore: Any | None,
    inflight_tracker: InflightTracker,
    inflight_key: str,
    used_fallback: bool,
    used_redis_semaphore: bool,
    department: int,
    model: str,
    status: str,
    duration: float,
) -> None:
    """Cleanup semaphore and tracking after request completion."""
    record_request(department, model, status, duration)
    in_progress_dec(department, model)
    # Decrement Redis in-flight FIRST (before release attempt)
    if used_redis_semaphore:
        await inflight_tracker.decrement_redis_inflight(inflight_key)
        redis_inflight_dec(department, model)
    if semaphore is not None:
        await safe_release(semaphore)
    if used_fallback:
        await inflight_tracker.release_fallback(inflight_key)
        fallback_inflight_dec(department, model)
        if app.state.redis_available:
            reservation_manager: ReservationManager = app.state.reservation_manager
            await reservation_manager.release_one(inflight_key)
