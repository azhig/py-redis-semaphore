"""Prometheus metrics helpers for the FastAPI LLM proxy example.

HTTP request metrics are handled via `prometheus_fastapi_instrumentator`.
This module keeps custom LLM/business metrics plus redis-semaphore metrics.
"""

from __future__ import annotations

from prometheus_client import REGISTRY, Counter, Gauge, Histogram
from prometheus_fastapi_instrumentator import Instrumentator

from redis_semaphore.metrics import PrometheusMetrics
from redis_semaphore.metrics import set_metrics as set_semaphore_metrics

REQUESTS_TOTAL = Counter(
    "llm_requests_total",
    "Total LLM proxy requests",
    ["department", "model", "status"],
)

REQUESTS_IN_PROGRESS = Gauge(
    "llm_requests_in_progress",
    "In-progress LLM proxy requests",
    ["department", "model"],
)

REQUEST_DURATION_SECONDS = Histogram(
    "llm_request_duration_seconds",
    "LLM proxy request duration in seconds",
    ["department", "model", "status"],
)

RATE_LIMIT_HITS_TOTAL = Counter(
    "llm_rate_limit_hits_total",
    "Queue wait timeout (429) responses",
    ["department", "model"],
)

SEMAPHORE_QUEUE_DEPTH = Gauge(
    "llm_semaphore_queue_depth",
    "Requests waiting to acquire a semaphore slot (per process)",
    ["department", "model"],
)

SEMAPHORE_POOL_SIZE = Gauge(
    "llm_semaphore_pool_size",
    "Number of unique department+model semaphores in the pool",
)

REDIS_AVAILABLE = Gauge(
    "llm_redis_available",
    "Redis availability for the proxy (1=up, 0=down)",
)

REDIS_RELEASE_FAILURES_TOTAL = Counter(
    "llm_redis_release_failures_total",
    "Redis semaphore release failures",
    ["type"],
)

REDIS_INFLIGHT = Gauge(
    "llm_redis_inflight",
    "Requests currently holding Redis semaphore slots (per process)",
    ["department", "model"],
)

FALLBACK_INFLIGHT = Gauge(
    "llm_fallback_inflight",
    "Requests currently using fallback semaphore (per process)",
    ["department", "model"],
)


def setup_semaphore_metrics() -> None:
    set_semaphore_metrics(PrometheusMetrics(registry=REGISTRY))


def setup_http_metrics(app) -> None:
    """Attach prometheus-fastapi-instrumentator to the app."""
    Instrumentator().instrument(app).expose(app, include_in_schema=False)


def record_request(department: int, model: str, status: str, duration: float) -> None:
    REQUESTS_TOTAL.labels(
        department=str(department),
        model=model,
        status=status,
    ).inc()
    REQUEST_DURATION_SECONDS.labels(
        department=str(department),
        model=model,
        status=status,
    ).observe(duration)


def in_progress_inc(department: int, model: str) -> None:
    REQUESTS_IN_PROGRESS.labels(department=str(department), model=model).inc()


def in_progress_dec(department: int, model: str) -> None:
    REQUESTS_IN_PROGRESS.labels(department=str(department), model=model).dec()


def queue_inc(department: int, model: str) -> None:
    SEMAPHORE_QUEUE_DEPTH.labels(department=str(department), model=model).inc()


def queue_dec(department: int, model: str) -> None:
    SEMAPHORE_QUEUE_DEPTH.labels(department=str(department), model=model).dec()


def rate_limit_hit(department: int, model: str) -> None:
    RATE_LIMIT_HITS_TOTAL.labels(department=str(department), model=model).inc()


def set_pool_size(size: int) -> None:
    SEMAPHORE_POOL_SIZE.set(size)


def set_redis_available(is_available: bool) -> None:
    REDIS_AVAILABLE.set(1 if is_available else 0)


def record_release_failure(reason: str) -> None:
    REDIS_RELEASE_FAILURES_TOTAL.labels(type=reason).inc()


def redis_inflight_inc(department: int, model: str) -> None:
    REDIS_INFLIGHT.labels(department=str(department), model=model).inc()


def redis_inflight_dec(department: int, model: str) -> None:
    REDIS_INFLIGHT.labels(department=str(department), model=model).dec()


def fallback_inflight_inc(department: int, model: str) -> None:
    FALLBACK_INFLIGHT.labels(department=str(department), model=model).inc()


def fallback_inflight_dec(department: int, model: str) -> None:
    FALLBACK_INFLIGHT.labels(department=str(department), model=model).dec()
