"""Tests for the attempt-based retry backoff."""

import time

import redis

from redis_semaphore import Semaphore, SemaphoreConfig
from redis_semaphore.semaphore import _AcquireState


def _sem(**config_kwargs) -> Semaphore:
    return Semaphore(redis.Redis(), SemaphoreConfig(name="backoff", limit=1, **config_kwargs))


def test_backoff_grows_by_attempts_not_elapsed():
    sem = _sem(
        retry_interval=0.1,
        retry_interval_max=1.0,
        retry_backoff_multiplier=2.0,
        retry_jitter=0.0,
    )
    # An old start_time must NOT inflate the interval (the old formula
    # exponentiated elapsed/base and hit the cap within a second).
    state = _AcquireState(start_time=time.monotonic() - 100.0)
    seq = [sem._next_retry_interval(state) for _ in range(6)]
    assert seq == [0.1, 0.2, 0.4, 0.8, 1.0, 1.0]
    assert state.attempts == 6


def test_backoff_constant_without_max():
    sem = _sem(retry_interval=0.25, retry_jitter=0.0)
    state = _AcquireState(start_time=time.monotonic())
    assert [sem._next_retry_interval(state) for _ in range(4)] == [0.25] * 4


def test_jitter_bounds():
    sem = _sem(retry_interval=0.1, retry_jitter=0.5)
    state = _AcquireState(start_time=time.monotonic())
    for _ in range(50):
        state.attempts = 0  # keep the base interval fixed
        interval = sem._next_retry_interval(state)
        assert 0.1 <= interval <= 0.1 * 1.5 + 1e-9


def test_jitter_default_is_nonzero():
    config = SemaphoreConfig(name="jitter-default", limit=1)
    assert config.retry_jitter == 0.1
