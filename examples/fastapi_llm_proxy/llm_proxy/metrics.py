"""Prometheus metrics helpers for the FastAPI LLM proxy example.

HTTP request metrics are handled via `prometheus_fastapi_instrumentator`.
This module wires redis-semaphore metrics into the default registry.
"""

from __future__ import annotations

from prometheus_client import REGISTRY
from prometheus_fastapi_instrumentator import Instrumentator
from redis_semaphore.metrics import PrometheusMetrics
from redis_semaphore.metrics import set_metrics as set_semaphore_metrics


def setup_semaphore_metrics() -> None:
    set_semaphore_metrics(PrometheusMetrics(registry=REGISTRY))


def setup_http_metrics(app) -> None:
    """Attach prometheus-fastapi-instrumentator to the app."""
    Instrumentator().instrument(app).expose(app, include_in_schema=False)
