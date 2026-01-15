"""Lua scripts for atomic semaphore operations.

Redis data structures:
- ZSET: {namespace}:{name}:owners
  - member: identifier (unique owner ID)
  - score: expiration timestamp in milliseconds

- STRING: {namespace}:{name}:fencing
  - Monotonically increasing counter for fencing tokens
"""

from __future__ import annotations

import hashlib
from collections.abc import Awaitable
from typing import ClassVar, Protocol, TypeAlias

import redis
import redis.asyncio as aioredis

# =============================================================================
# ACQUIRE - Acquire a semaphore slot
# =============================================================================
# KEYS[1] = owners sorted set key
# KEYS[2] = fencing token key
# ARGV[1] = identifier (unique owner id)
# ARGV[2] = limit (max concurrent holders)
# ARGV[3] = lock_timeout_ms (TTL in milliseconds)
# ARGV[4] = current_time_ms (current timestamp in milliseconds)
#
# Returns: [success (0/1), fencing_token or nil, expires_at_ms or nil]
ACQUIRE_SCRIPT = """
local owners_key = KEYS[1]
local fencing_key = KEYS[2]
local identifier = ARGV[1]
local limit = tonumber(ARGV[2])
local lock_timeout_ms = tonumber(ARGV[3])
local now_ms = tonumber(ARGV[4])

-- 1. Cleanup: remove expired entries (score < now)
redis.call('ZREMRANGEBYSCORE', owners_key, '-inf', now_ms)

-- 2. Check if we already own the lock (re-entrant acquire)
local existing_score = redis.call('ZSCORE', owners_key, identifier)
if existing_score then
    -- Already own it - update TTL and issue NEW fencing token
    -- This ensures monotonicity even for re-entrant acquires
    local new_expires = now_ms + lock_timeout_ms
    redis.call('ZADD', owners_key, new_expires, identifier)
    local fencing_token = redis.call('INCR', fencing_key)
    return {1, fencing_token, new_expires}
end

-- 3. Check current owner count
local current_count = redis.call('ZCARD', owners_key)

if current_count < limit then
    -- 4. Slot available - acquire it
    local expires_at = now_ms + lock_timeout_ms
    redis.call('ZADD', owners_key, expires_at, identifier)

    -- 5. Increment and get fencing token
    local fencing_token = redis.call('INCR', fencing_key)

    return {1, fencing_token, expires_at}
else
    -- 6. No slots available
    return {0, '', ''}
end
"""


# =============================================================================
# RELEASE - Release a semaphore slot
# =============================================================================
# KEYS[1] = owners sorted set key
# ARGV[1] = identifier
#
# Returns: 1 if released, 0 if not owned
RELEASE_SCRIPT = """
local owners_key = KEYS[1]
local identifier = ARGV[1]

-- Check ownership and remove
local removed = redis.call('ZREM', owners_key, identifier)
return removed
"""


# =============================================================================
# REFRESH - Refresh lock TTL
# =============================================================================
# KEYS[1] = owners sorted set key
# ARGV[1] = identifier
# ARGV[2] = lock_timeout_ms
# ARGV[3] = current_time_ms
#
# Returns: 1 if refreshed, 0 if not owned (lock lost)
REFRESH_SCRIPT = """
local owners_key = KEYS[1]
local identifier = ARGV[1]
local lock_timeout_ms = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])

-- Check if we own the lock
local current_score = redis.call('ZSCORE', owners_key, identifier)

if not current_score then
    -- Lock lost
    return 0
end

if tonumber(current_score) < now_ms then
    -- Expired lock should be treated as lost
    redis.call('ZREM', owners_key, identifier)
    return 0
end

-- Update TTL
local new_expires = now_ms + lock_timeout_ms
redis.call('ZADD', owners_key, 'XX', new_expires, identifier)
return 1
"""


# =============================================================================
# CLEANUP - Force cleanup of expired locks
# =============================================================================
# KEYS[1] = owners sorted set key
# ARGV[1] = current_time_ms
#
# Returns: number of removed entries
CLEANUP_SCRIPT = """
local owners_key = KEYS[1]
local now_ms = tonumber(ARGV[1])

return redis.call('ZREMRANGEBYSCORE', owners_key, '-inf', now_ms)
"""


# =============================================================================
# STATUS - Get current semaphore status
# =============================================================================
# KEYS[1] = owners sorted set key
# ARGV[1] = current_time_ms
# ARGV[2] = identifier (optional, to check ownership)
#
# Returns: [current_count, is_owner (0/1), expires_at or nil]
STATUS_SCRIPT = """
local owners_key = KEYS[1]
local now_ms = tonumber(ARGV[1])
local identifier = ARGV[2]

-- Cleanup expired
redis.call('ZREMRANGEBYSCORE', owners_key, '-inf', now_ms)

local current_count = redis.call('ZCARD', owners_key)
local is_owner = 0
local expires_at = ''

if identifier and identifier ~= '' then
    local score = redis.call('ZSCORE', owners_key, identifier)
    if score then
        is_owner = 1
        expires_at = score
    end
end

return {current_count, is_owner, expires_at}
"""


class LuaScriptRegistry:
    """Registry for loaded Lua scripts."""

    SCRIPTS: ClassVar[dict[str, str]] = {
        "acquire": ACQUIRE_SCRIPT,
        "release": RELEASE_SCRIPT,
        "refresh": REFRESH_SCRIPT,
        "cleanup": CLEANUP_SCRIPT,
        "status": STATUS_SCRIPT,
    }

    def __init__(self) -> None:
        self._sha_cache: dict[str, str] = {}

    def load_all(self, client: ScriptClient) -> None:
        """Load all scripts into Redis and cache their SHAs."""
        for name, script in self.SCRIPTS.items():
            result = client.script_load(script)
            if not isinstance(result, str):
                raise TypeError("script_load returned non-str")
            sha: str = result
            self._sha_cache[name] = sha

    async def load_all_async(self, client: ScriptClient) -> None:
        """Async version of load_all."""
        for name, script in self.SCRIPTS.items():
            result = await client.ascript_load(script)
            if not isinstance(result, str):
                raise TypeError("script_load returned non-str")
            sha: str = result
            self._sha_cache[name] = sha

    def get_sha(self, name: str) -> str:
        """Get the SHA of a loaded script."""
        if name not in self._sha_cache:
            raise ValueError(f"Script '{name}' not loaded")
        return self._sha_cache[name]

    def get_script(self, name: str) -> str:
        """Get the Lua script source by name."""
        if name not in self.SCRIPTS:
            raise ValueError(f"Unknown script '{name}'")
        return self.SCRIPTS[name]

    def invalidate(self) -> None:
        """Clear the SHA cache (call after NOSCRIPT error)."""
        self._sha_cache.clear()

    @property
    def is_loaded(self) -> bool:
        """Check if scripts have been loaded."""
        return len(self._sha_cache) == len(self.SCRIPTS)


class LuaScriptRunner:
    """Typed helpers around Lua script invocations."""

    def __init__(self, registry: LuaScriptRegistry) -> None:
        self._registry = registry

    def acquire(
        self,
        client: ScriptClient,
        owners_key: str,
        fencing_key: str,
        identifier: str,
        limit: int,
        lock_timeout_ms: int,
        now_ms: int,
    ) -> tuple[bool, int | None, int | None]:
        result: object = client.evalsha(
            self._registry.get_sha("acquire"),
            2,
            owners_key,
            fencing_key,
            identifier,
            limit,
            lock_timeout_ms,
            now_ms,
        )
        return _parse_acquire_result(result)

    def release(self, client: ScriptClient, owners_key: str, identifier: str) -> bool:
        result: object = client.evalsha(
            self._registry.get_sha("release"),
            1,
            owners_key,
            identifier,
        )
        return bool(result)

    def refresh(
        self,
        client: ScriptClient,
        owners_key: str,
        identifier: str,
        lock_timeout_ms: int,
        now_ms: int,
    ) -> bool:
        result: object = client.evalsha(
            self._registry.get_sha("refresh"),
            1,
            owners_key,
            identifier,
            lock_timeout_ms,
            now_ms,
        )
        return bool(result)

    async def acquire_async(
        self,
        client: ScriptClient,
        owners_key: str,
        fencing_key: str,
        identifier: str,
        limit: int,
        lock_timeout_ms: int,
        now_ms: int,
    ) -> tuple[bool, int | None, int | None]:
        result: object = await client.aevalsha(
            self._registry.get_sha("acquire"),
            2,
            owners_key,
            fencing_key,
            identifier,
            limit,
            lock_timeout_ms,
            now_ms,
        )
        return _parse_acquire_result(result)

    async def release_async(self, client: ScriptClient, owners_key: str, identifier: str) -> bool:
        result: object = await client.aevalsha(
            self._registry.get_sha("release"),
            1,
            owners_key,
            identifier,
        )
        return bool(result)

    async def refresh_async(
        self,
        client: ScriptClient,
        owners_key: str,
        identifier: str,
        lock_timeout_ms: int,
        now_ms: int,
    ) -> bool:
        result: object = await client.aevalsha(
            self._registry.get_sha("refresh"),
            1,
            owners_key,
            identifier,
            lock_timeout_ms,
            now_ms,
        )
        return bool(result)

    def status(
        self,
        client: ScriptClient,
        owners_key: str,
        now_ms: int,
        identifier: str | None,
    ) -> tuple[int, bool, int | None]:
        result: object = client.evalsha(
            self._registry.get_sha("status"),
            1,
            owners_key,
            now_ms,
            identifier or "",
        )
        return _parse_status_result(result)

    async def status_async(
        self,
        client: ScriptClient,
        owners_key: str,
        now_ms: int,
        identifier: str | None,
    ) -> tuple[int, bool, int | None]:
        result: object = await client.aevalsha(
            self._registry.get_sha("status"),
            1,
            owners_key,
            now_ms,
            identifier or "",
        )
        return _parse_status_result(result)


def _parse_acquire_result(result: object) -> tuple[bool, int | None, int | None]:
    if not isinstance(result, list | tuple) or len(result) != 3:
        raise ValueError("Unexpected acquire result shape")

    success_raw, fencing_raw, expires_raw = result
    success = _to_bool(success_raw)

    fencing = _to_optional_int(fencing_raw)
    expires = _to_optional_int(expires_raw)
    return success, fencing, expires


def _parse_status_result(result: object) -> tuple[int, bool, int | None]:
    if not isinstance(result, list | tuple) or len(result) != 3:
        raise ValueError("Unexpected status result shape")

    count_raw, is_owner_raw, expires_raw = result
    count = _to_int(count_raw)
    is_owner = _to_bool(is_owner_raw)
    expires = _to_optional_int(expires_raw)
    return count, is_owner, expires


def _to_optional_int(value: object) -> int | None:
    if value in (None, "", b""):
        return None
    if isinstance(value, bytes | bytearray | memoryview):
        if len(value) == 0:
            return None  # pragma: no cover - handled by empty-bytes fast path
        return int(bytes(value))
    if isinstance(value, str):
        if not value:
            return None  # pragma: no cover - handled by empty-string fast path
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    raise TypeError("Expected numeric value for int conversion")


def _to_bool(value: object) -> bool:
    if isinstance(value, bytes | bytearray | memoryview):
        if len(value) == 0:
            return False
        return bool(int(bytes(value)))
    if isinstance(value, str):
        if not value:
            return False
        return bool(int(value))
    return bool(value)


def _to_int(value: object) -> int:
    if isinstance(value, bytes | bytearray | memoryview):
        if len(value) == 0:
            return 0
        return int(bytes(value))
    if isinstance(value, str):
        if not value:
            return 0
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    raise TypeError("Expected numeric value for int conversion")


ScriptArg: TypeAlias = bytes | str | int | float | memoryview | bytearray


class ScriptClient(Protocol):
    def script_load(self, script: str) -> str: ...

    def ascript_load(self, script: str) -> Awaitable[str]: ...

    def evalsha(self, sha: str, numkeys: int, *args: ScriptArg) -> object: ...

    def aevalsha(self, sha: str, numkeys: int, *args: ScriptArg) -> Awaitable[object]: ...


class ScriptClientAdapter:
    """Adapter for Redis client with NOSCRIPT retry support."""

    def __init__(
        self,
        client: redis.Redis | aioredis.Redis,
        registry: LuaScriptRegistry | None = None,
    ) -> None:
        self._client = client
        self._is_async = isinstance(client, aioredis.Redis)
        self._registry = registry

    def set_registry(self, registry: LuaScriptRegistry) -> None:
        """Set the script registry for NOSCRIPT retry."""
        self._registry = registry

    def script_load(self, script: str) -> str:
        if self._is_async:
            raise RuntimeError("sync script_load not supported")
        result = self._client.script_load(script)
        if not isinstance(result, str):
            raise TypeError("script_load returned non-str")
        return result

    async def ascript_load(self, script: str) -> str:
        if not self._is_async:
            raise RuntimeError("async script_load not supported")
        result = self._client.script_load(script)
        if isinstance(result, str):
            return result
        resolved = await result
        if not isinstance(resolved, str):
            raise TypeError("script_load returned non-str")
        return resolved

    def evalsha(self, sha: str, numkeys: int, *args: ScriptArg) -> object:
        if self._is_async:
            raise RuntimeError("sync evalsha not supported")
        try:
            return self._client.evalsha(sha, numkeys, *args)
        except redis.exceptions.NoScriptError:
            return self._handle_noscript_sync(sha, numkeys, *args)

    def _handle_noscript_sync(self, sha: str, numkeys: int, *args: ScriptArg) -> object:
        """Handle NOSCRIPT error by reloading scripts and retrying."""
        if self._registry is None:
            raise RuntimeError("No registry set for NOSCRIPT retry")

        # Find script name by SHA and reload all scripts
        self._registry.invalidate()
        self._registry.load_all(self)

        # Retry with new SHA
        new_sha = self._registry.get_sha(self._find_script_name_by_sha(sha))
        return self._client.evalsha(new_sha, numkeys, *args)

    def _find_script_name_by_sha(self, sha: str) -> str:
        """Find script name that had this SHA (before invalidation)."""
        for name, script in LuaScriptRegistry.SCRIPTS.items():
            computed_sha = hashlib.sha1(script.encode()).hexdigest()
            if computed_sha == sha:
                return name
        raise ValueError(f"Unknown script SHA: {sha}")

    async def aevalsha(self, sha: str, numkeys: int, *args: ScriptArg) -> object:
        if not self._is_async:
            raise RuntimeError("async evalsha not supported")
        try:
            result = self._client.evalsha(sha, numkeys, *args)
            if not isinstance(result, Awaitable):
                return result
            return await result
        except redis.exceptions.NoScriptError:
            return await self._handle_noscript_async(sha, numkeys, *args)

    async def _handle_noscript_async(self, sha: str, numkeys: int, *args: ScriptArg) -> object:
        """Handle NOSCRIPT error by reloading scripts and retrying (async)."""
        if self._registry is None:
            raise RuntimeError("No registry set for NOSCRIPT retry")

        # Find script name by SHA and reload all scripts
        self._registry.invalidate()
        await self._registry.load_all_async(self)

        # Retry with new SHA
        new_sha = self._registry.get_sha(self._find_script_name_by_sha(sha))
        result = self._client.evalsha(new_sha, numkeys, *args)
        if not isinstance(result, Awaitable):
            return result
        return await result
