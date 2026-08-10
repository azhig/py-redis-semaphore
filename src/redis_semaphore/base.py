"""Base class for semaphore implementations."""

from __future__ import annotations

import time
import uuid
from enum import Enum
from typing import Generic, TypeVar

from .lua_scripts import LuaScriptRegistry
from .types import LockState, SemaphoreConfig, TimeSource

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

        # Calculate refresh_interval. One third of the TTL leaves roughly two
        # thirds of lock_timeout as retry budget when Redis is briefly
        # unreachable (at 80% a single failed refresh nearly exhausted it).
        if config.refresh_interval is None:
            self._refresh_interval = config.lock_timeout / 3
        else:
            self._refresh_interval = config.refresh_interval

        # Step between heartbeat refresh retries after a connection error.
        if config.refresh_retry_interval is None:
            self._refresh_retry_interval = min(self._refresh_interval, 1.0)
        else:
            self._refresh_retry_interval = config.refresh_retry_interval

    @property
    def _key_prefix(self) -> str:
        """Common key prefix; with cluster_hash_tags the whole prefix becomes
        a hash tag so all keys of one semaphore map to the same Cluster slot
        (multi-key scripts require this)."""
        prefix = f"{self._config.namespace}:{self._config.name}"
        if self._config.cluster_hash_tags:
            return "{" + prefix + "}"
        return prefix

    @property
    def owners_key(self) -> str:
        """Redis key for the owners sorted set."""
        return f"{self._key_prefix}:owners"

    @property
    def fencing_key(self) -> str:
        """Redis key for the fencing token."""
        return f"{self._key_prefix}:fencing"

    @property
    def queue_key(self) -> str:
        """Redis key for the notification queue (used in BLPOP mode)."""
        return f"{self._key_prefix}:queue"

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

    def _now_ms_arg(self) -> int | str:
        """Time argument for the Lua scripts.

        Client milliseconds in TimeSource.CLIENT mode; the empty string in
        TimeSource.SERVER mode, which tells the script to read TIME on the
        server. Never needs a network round-trip, so the same sync method
        serves both the sync and async paths.
        """
        if self._config.time_source is TimeSource.SERVER:
            return ""
        return self._get_current_time_ms()
