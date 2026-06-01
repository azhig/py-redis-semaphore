"""Configuration settings for the FastAPI LLM proxy example."""

from __future__ import annotations

from typing import Any

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Redis
    redis_host: str = Field(default="localhost")
    redis_port: int = Field(default=6379)
    redis_db: int = Field(default=0)
    # Read/connect deadlines. Invariant: semaphore_blpop_timeout <
    # redis_socket_timeout < semaphore_lock_timeout.
    #  - Below lock_timeout: a stalled/half-open connection fails fast instead
    #    of blocking until the OS TCP keepalive reaps the socket (~2h with
    #    kernel defaults), which would freeze every Redis-touching endpoint
    #    (incl. /semaphore/status).
    #  - Above blpop_timeout: otherwise a normal BLPOP wait hits the socket
    #    read deadline and raises a spurious TimeoutError instead of returning.
    redis_socket_timeout: float = Field(default=10.0)
    redis_socket_connect_timeout: float = Field(default=5.0)
    # Upper bound on pooled connections. In BLPOP mode each waiting acquire
    # holds a connection for up to blpop_timeout, so size this above expected
    # peak concurrency. A blocking pool makes excess callers wait for a free
    # connection (up to redis_socket_timeout) instead of growing without limit.
    redis_max_connections: int = Field(default=64)

    # Semaphore
    semaphore_capacity: int = Field(default=5)
    semaphore_lock_timeout: float = Field(default=120.0)
    semaphore_acquire_timeout: float = Field(default=200.0)
    semaphore_blpop_timeout: float = Field(default=3.0)
    semaphore_namespace: str = Field(default="llm_proxy")

    # Redis availability check
    redis_check_interval: float = Field(default=5.0)

    # Upstream LLM API
    upstream_base_url: str = Field(default="https://api.openai.com/v1")
    upstream_timeout: float = Field(default=120.0)
    client_model_config_path: str | None = Field(default=None)

    # Logging
    log_level: str = Field(default="INFO")
    log_file: str | None = Field(default="logs/llm_proxy.jsonl")

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


def load_settings() -> Settings:
    """Load settings and optionally parse .env files."""

    return Settings()


def settings_dict(settings: Settings) -> dict[str, Any]:
    """Convert settings to a plain dict for logging."""

    return settings.model_dump()
