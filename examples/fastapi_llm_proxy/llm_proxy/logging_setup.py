"""Loguru setup and stdlib logging interception."""

from __future__ import annotations

import logging
import sys

from loguru import logger as loguru_logger

from redis_semaphore.logger import set_logger as set_semaphore_logger


class _InterceptHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = loguru_logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        frame = logging.currentframe()
        depth = 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        loguru_logger.bind(source=record.name).opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


class LoguruAdapter:
    def __init__(self, logger_instance, source: str | None = None) -> None:
        if source is None:
            self._logger = logger_instance
        else:
            self._logger = logger_instance.bind(source=source)

    def _log(self, level: str, msg: str, *args, **kwargs) -> None:
        kwargs.pop("extra", None)
        exc_info = kwargs.pop("exc_info", None)
        if args:
            msg = msg % args
        if exc_info:
            self._logger.opt(exception=True).log(level, msg)
        else:
            self._logger.log(level, msg)

    def debug(self, msg: str, *args, **kwargs) -> None:
        self._log("DEBUG", msg, *args, **kwargs)

    def info(self, msg: str, *args, **kwargs) -> None:
        self._log("INFO", msg, *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs) -> None:
        self._log("WARNING", msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs) -> None:
        self._log("ERROR", msg, *args, **kwargs)

    def exception(self, msg: str, *args, **kwargs) -> None:
        self._log("ERROR", msg, *args, **kwargs, exc_info=True)


logger = loguru_logger.bind(source="app")


def configure_logging(level: str) -> None:
    loguru_logger.configure(extra={"source": "app"})
    loguru_logger.remove()
    loguru_logger.add(
        sys.stdout,
        level=level,
        backtrace=True,
        diagnose=False,
        colorize=True,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level}</level> | "
        "<cyan>{extra[source]}</cyan> | "
        "<level>{message}</level>",
    )

    logging.basicConfig(handlers=[_InterceptHandler()], level=level, force=True)
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logging.getLogger(name).handlers = [_InterceptHandler()]
        logging.getLogger(name).propagate = False

    set_semaphore_logger(LoguruAdapter(loguru_logger, source="redis_semaphore"))
