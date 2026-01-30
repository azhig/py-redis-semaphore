"""Configuration settings for the FastAPI LLM proxy example."""

from __future__ import annotations

import os
from dataclasses import asdict, dataclass
from typing import Any

try:  # Pydantic v2 (preferred when available)
    from pydantic import Field
    from pydantic_settings import BaseSettings, SettingsConfigDict

    _HAS_PYDANTIC_SETTINGS = True
except ImportError:  # Fall back to manual env parsing
    _HAS_PYDANTIC_SETTINGS = False


def _maybe_load_dotenv() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return

    load_dotenv()


if _HAS_PYDANTIC_SETTINGS:

    class Settings(BaseSettings):
        """Application settings loaded from environment variables."""

        # Redis
        redis_host: str = Field(default="localhost")
        redis_port: int = Field(default=6379)
        redis_db: int = Field(default=0)

        # Semaphore
        semaphore_capacity: int = Field(default=5)
        semaphore_lock_timeout: float = Field(default=120.0)
        semaphore_acquire_timeout: float = Field(default=200.0)
        semaphore_namespace: str = Field(default="llm_proxy")
        fallback_semaphore_capacity: int = Field(default=1)

        # Redis availability check
        redis_check_interval: float = Field(default=5.0)

        # Upstream LLM API
        upstream_base_url: str = Field(default="https://api.openai.com/v1")
        upstream_timeout: float = Field(default=120.0)

        # Logging
        log_level: str = Field(default="INFO")

        model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    def load_settings() -> Settings:
        """Load settings and optionally parse .env files."""

        _maybe_load_dotenv()
        return Settings()

    def settings_dict(settings: Settings) -> dict[str, Any]:
        """Convert settings to a plain dict for logging."""

        return settings.model_dump()

else:

    @dataclass
    class Settings:
        """Settings loaded from environment variables (manual fallback)."""

        redis_host: str = "localhost"
        redis_port: int = 6379
        redis_db: int = 0
        semaphore_capacity: int = 5
        semaphore_lock_timeout: float = 120.0
        semaphore_acquire_timeout: float = 60.0
        semaphore_namespace: str = "llm_proxy"
        fallback_semaphore_capacity: int = 10
        redis_check_interval: float = 5.0
        upstream_base_url: str = "https://api.openai.com/v1"
        upstream_timeout: float = 120.0
        log_level: str = "INFO"

    def _get_env(name: str, default: Any, cast: type) -> Any:
        raw = os.getenv(name)
        if raw is None or raw == "":
            return default
        try:
            return cast(raw)
        except (TypeError, ValueError) as exc:  # pragma: no cover - config errors
            raise ValueError(f"Invalid value for {name}: {raw}") from exc

    def load_settings() -> Settings:
        """Load settings and optionally parse .env files."""

        _maybe_load_dotenv()
        return Settings(
            redis_host=os.getenv("REDIS_HOST", "localhost"),
            redis_port=_get_env("REDIS_PORT", 6379, int),
            redis_db=_get_env("REDIS_DB", 0, int),
            semaphore_capacity=_get_env("SEMAPHORE_CAPACITY", 5, int),
            semaphore_lock_timeout=_get_env("SEMAPHORE_LOCK_TIMEOUT", 120.0, float),
            semaphore_acquire_timeout=_get_env("SEMAPHORE_ACQUIRE_TIMEOUT", 60.0, float),
            semaphore_namespace=os.getenv("SEMAPHORE_NAMESPACE", "llm_proxy"),
            fallback_semaphore_capacity=_get_env("FALLBACK_SEMAPHORE_CAPACITY", 10, int),
            redis_check_interval=_get_env("REDIS_CHECK_INTERVAL", 5.0, float),
            upstream_base_url=os.getenv("UPSTREAM_BASE_URL", "https://api.openai.com/v1"),
            upstream_timeout=_get_env("UPSTREAM_TIMEOUT", 120.0, float),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )

    def settings_dict(settings: Settings) -> dict[str, Any]:
        """Convert settings to a plain dict for logging."""

        return asdict(settings)
