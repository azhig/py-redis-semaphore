"""Logging helpers for redis-semaphore."""

from __future__ import annotations

import logging
from typing import Protocol


class LoggerProtocol(Protocol):
    def debug(self, msg: str, *args, **kwargs) -> None: ...
    def info(self, msg: str, *args, **kwargs) -> None: ...
    def warning(self, msg: str, *args, **kwargs) -> None: ...
    def error(self, msg: str, *args, **kwargs) -> None: ...
    def exception(self, msg: str, *args, **kwargs) -> None: ...


class _LoggerProxy:
    def __init__(self, initial: LoggerProtocol) -> None:
        self._logger = initial

    def set(self, custom_logger: LoggerProtocol) -> None:
        self._logger = custom_logger

    def get(self) -> LoggerProtocol:
        return self._logger

    def __getattr__(self, name: str):
        return getattr(self._logger, name)


logger = _LoggerProxy(logging.getLogger("redis_semaphore"))


def set_logger(custom_logger: LoggerProtocol) -> None:
    """Override the internal logger (e.g. loguru/structlog adapter)."""
    logger.set(custom_logger)


def get_logger() -> LoggerProtocol:
    """Return the current logger implementation."""
    return logger.get()
