"""Tests for logger override."""

import importlib

import pytest

from redis_semaphore import AcquireTimeoutError, Semaphore, SemaphoreConfig
from redis_semaphore.logger import set_logger


class DummyLogger:
    def __init__(self) -> None:
        self.debug_calls = 0

    def debug(self, msg: str, *args, **kwargs) -> None:
        self.debug_calls += 1

    def info(self, msg: str, *args, **kwargs) -> None:
        return None

    def warning(self, msg: str, *args, **kwargs) -> None:
        return None

    def error(self, msg: str, *args, **kwargs) -> None:
        return None

    def exception(self, msg: str, *args, **kwargs) -> None:
        return None


def test_set_logger():
    logger_module = importlib.import_module("redis_semaphore.logger")
    old = logger_module.get_logger()
    dummy = DummyLogger()
    set_logger(dummy)

    assert logger_module.get_logger() is dummy
    set_logger(old)


def test_debug_logging_enabled(redis_client):
    class DebugLogger(DummyLogger):
        def isEnabledFor(self, level: int) -> bool:
            return True

    logger_module = importlib.import_module("redis_semaphore.logger")
    old = logger_module.get_logger()
    debug_logger = DebugLogger()
    set_logger(debug_logger)

    config = SemaphoreConfig(name="debug-logging", limit=1)
    sem = Semaphore(redis_client, config)
    sem.acquire(blocking=False)
    sem.release()

    assert debug_logger.debug_calls > 0
    set_logger(old)


def test_can_log_debug_no_checker(redis_client):
    class MinimalLogger:
        def debug(self, msg: str, *args, **kwargs) -> None:
            return None

        def info(self, msg: str, *args, **kwargs) -> None:
            return None

        def warning(self, msg: str, *args, **kwargs) -> None:
            return None

        def error(self, msg: str, *args, **kwargs) -> None:
            return None

        def exception(self, msg: str, *args, **kwargs) -> None:
            return None

    logger_module = importlib.import_module("redis_semaphore.logger")
    old = logger_module.get_logger()
    set_logger(MinimalLogger())

    sem = Semaphore(redis_client, SemaphoreConfig(name="no-checker", limit=1))
    assert sem._can_log_debug() is True

    set_logger(old)


def test_debug_logging_waiting(redis_client):
    class DebugLogger(DummyLogger):
        def isEnabledFor(self, level: int) -> bool:
            return True

    logger_module = importlib.import_module("redis_semaphore.logger")
    old = logger_module.get_logger()
    debug_logger = DebugLogger()
    set_logger(debug_logger)

    config = SemaphoreConfig(
        name="debug-waiting",
        limit=1,
        acquire_timeout=0.1,
        retry_interval=0.01,
    )
    sem1 = Semaphore(redis_client, config)
    sem2 = Semaphore(redis_client, config)

    assert sem1.acquire(blocking=False).success is True
    with pytest.raises(AcquireTimeoutError):
        sem2.acquire(blocking=True)
    sem1.release()

    assert debug_logger.debug_calls > 0
    set_logger(old)
