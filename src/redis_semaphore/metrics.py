"""Metrics hooks for redis-semaphore."""

from __future__ import annotations

from typing import Protocol


class MetricsCollector(Protocol):
    @property
    def enabled(self) -> bool: ...

    def set_slots_used(self, name: str, namespace: str, used: int, limit: int) -> None: ...

    def set_waiting(self, name: str, namespace: str, waiting: int) -> None: ...

    def observe_wait_seconds(
        self, name: str, namespace: str, wait_seconds: float, result: str
    ) -> None: ...

    def inc_acquire(self, name: str, namespace: str, result: str) -> None: ...

    def inc_queue_total(self, name: str, namespace: str) -> None: ...

    def inc_lock_lost(self, name: str, namespace: str) -> None: ...


class _NoopMetrics:
    @property
    def enabled(self) -> bool:
        return False

    def set_slots_used(self, name: str, namespace: str, used: int, limit: int) -> None:
        return None

    def set_waiting(self, name: str, namespace: str, waiting: int) -> None:
        return None

    def observe_wait_seconds(
        self, name: str, namespace: str, wait_seconds: float, result: str
    ) -> None:
        return None

    def inc_acquire(self, name: str, namespace: str, result: str) -> None:
        return None

    def inc_queue_total(self, name: str, namespace: str) -> None:
        return None

    def inc_lock_lost(self, name: str, namespace: str) -> None:
        return None


_metrics: MetricsCollector = _NoopMetrics()


def set_metrics(metrics: MetricsCollector) -> None:
    """Override the metrics collector."""
    global _metrics
    _metrics = metrics


def get_metrics() -> MetricsCollector:
    return _metrics


class PrometheusMetrics:
    """Prometheus metrics collector."""

    def __init__(self, registry=None, buckets=None) -> None:
        from prometheus_client import Counter, Gauge, Histogram

        if buckets is None:
            buckets = (
                0.001,
                0.005,
                0.01,
                0.025,
                0.05,
                0.1,
                0.25,
                0.5,
                1.0,
                2.5,
                5.0,
                10.0,
            )

        self._slots_used = Gauge(
            "redis_semaphore_slots_used",
            "Number of currently acquired slots",
            ["name", "namespace"],
            registry=registry,
        )
        self._waiting = Gauge(
            "redis_semaphore_waiting",
            "Number of waiters on acquire",
            ["name", "namespace"],
            registry=registry,
        )
        self._acquire_total = Counter(
            "redis_semaphore_acquire_total",
            "Acquire attempts by result",
            ["name", "namespace", "result"],
            registry=registry,
        )
        self._queue_total = Counter(
            "redis_semaphore_queue_total",
            "Total number of candidates that entered the wait queue",
            ["name", "namespace"],
            registry=registry,
        )
        self._wait_seconds = Histogram(
            "redis_semaphore_queue_wait_seconds",
            "Time spent waiting in the acquire queue",
            ["name", "namespace", "result"],
            buckets=buckets,
            registry=registry,
        )
        self._lock_lost = Counter(
            "redis_semaphore_lock_lost_total",
            "Number of lost locks",
            ["name", "namespace"],
            registry=registry,
        )

    @property
    def enabled(self) -> bool:
        return True

    def set_slots_used(self, name: str, namespace: str, used: int, limit: int) -> None:
        self._slots_used.labels(name=name, namespace=namespace).set(used)

    def set_waiting(self, name: str, namespace: str, waiting: int) -> None:
        self._waiting.labels(name=name, namespace=namespace).set(waiting)

    def observe_wait_seconds(
        self, name: str, namespace: str, wait_seconds: float, result: str
    ) -> None:
        self._wait_seconds.labels(name=name, namespace=namespace, result=result).observe(
            wait_seconds
        )

    def inc_acquire(self, name: str, namespace: str, result: str) -> None:
        self._acquire_total.labels(name=name, namespace=namespace, result=result).inc()

    def inc_queue_total(self, name: str, namespace: str) -> None:
        self._queue_total.labels(name=name, namespace=namespace).inc()

    def inc_lock_lost(self, name: str, namespace: str) -> None:
        self._lock_lost.labels(name=name, namespace=namespace).inc()
