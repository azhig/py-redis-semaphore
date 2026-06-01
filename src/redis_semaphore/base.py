"""Base class for semaphore implementations."""

from __future__ import annotations

import time
import uuid
from enum import Enum
from typing import Generic, TypeVar

from .lua_scripts import LuaScriptRegistry
from .types import LockState, SemaphoreConfig

ClientT = TypeVar("ClientT")


class _AcquireMode(Enum):
    """Tracks whether semaphore was acquired via sync or async API."""

    NONE = "none"
    SYNC = "sync"
    ASYNC = "async"


class BaseSemaphoreCommon(Generic[ClientT]):
    """Shared base for semaphore implementations."""

    __slots__ = (
        "_acquire_mode",
        "_client",
        "_config",
        "_expires_at",
        "_fencing_token",
        "_identifier",
        "_refresh_interval",
        "_refresh_retry_interval",
        "_scripts",
        "_state",
    )

    def __init__(
        self,
        client: ClientT,
        config: SemaphoreConfig,
    ) -> None:
        self._client = client
        self._config = config
        self._scripts = LuaScriptRegistry()

        # State
        self._identifier: str | None = None
        self._fencing_token: int | None = None
        self._expires_at: float | None = None
        self._state = LockState.RELEASED
        self._acquire_mode = _AcquireMode.NONE

        # Calculate refresh_interval
        if config.refresh_interval is None:
            self._refresh_interval = config.lock_timeout * 0.8
        else:
            self._refresh_interval = config.refresh_interval

        # Step between heartbeat refresh retries after a connection error.
        if config.refresh_retry_interval is None:
            self._refresh_retry_interval = min(self._refresh_interval, 1.0)
        else:
            self._refresh_retry_interval = config.refresh_retry_interval

    @property
    def owners_key(self) -> str:
        """Redis key for the owners sorted set."""
        return f"{self._config.namespace}:{self._config.name}:owners"

    @property
    def fencing_key(self) -> str:
        """Redis key for the fencing token."""
        return f"{self._config.namespace}:{self._config.name}:fencing"

    @property
    def queue_key(self) -> str:
        """Redis key for the notification queue (used in BLPOP mode)."""
        return f"{self._config.namespace}:{self._config.name}:queue"

    @property
    def identifier(self) -> str | None:
        """Unique identifier of the current owner."""
        return self._identifier

    @property
    def fencing_token(self) -> int | None:
        """Fencing token for preventing race conditions."""
        return self._fencing_token

    @property
    def is_acquired(self) -> bool:
        """Check if the semaphore is acquired."""
        return self._state == LockState.ACQUIRED

    @property
    def is_lost(self) -> bool:
        """Check if the lock was lost."""
        return self._state == LockState.LOST

    @property
    def config(self) -> SemaphoreConfig:
        """Get the semaphore configuration."""
        return self._config

    def _generate_identifier(self) -> str:
        """Generate a unique identifier."""
        return f"{uuid.uuid4().hex}:{time.time_ns()}"

    def _get_current_time_ms(self) -> int:
        """Get current time in milliseconds (client-side)."""
        return int(time.time() * 1000)

    def _get_server_time_ms(self) -> int:
        """Get Redis server time in milliseconds.

        Uses TIME command which returns [seconds, microseconds].
        """
        result = self._client.time()  # type: ignore[attr-defined]
        seconds, microseconds = result
        return int(seconds) * 1000 + int(microseconds) // 1000

    async def _get_server_time_ms_async(self) -> int:
        """Get Redis server time in milliseconds (async)."""
        result = await self._client.time()  # type: ignore[attr-defined]
        seconds, microseconds = result
        return int(seconds) * 1000 + int(microseconds) // 1000
