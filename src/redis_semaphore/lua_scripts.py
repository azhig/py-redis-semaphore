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
# Returns: [success (0/1), fencing_token or nil, expires_at_ms or nil, used_slots]
#   used_slots is the occupancy observed atomically by this call (including the
#   caller on success). It lets the client feed metrics/logs without a separate
#   status round-trip.
ACQUIRE_SCRIPT = """
local owners_key = KEYS[1]
local fencing_key = KEYS[2]
local identifier = ARGV[1]
local limit = tonumber(ARGV[2])
local lock_timeout_ms = tonumber(ARGV[3])
local now_ms = tonumber(ARGV[4])

-- 1. Cleanup: remove expired entries (score < now)
redis.call('ZREMRANGEBYSCORE', owners_key, '-inf', now_ms)

-- 2. Current occupancy, computed once and returned to the caller for metrics.
local current_count = redis.call('ZCARD', owners_key)

-- 3. Check if we already own the lock (re-entrant acquire)
local existing_score = redis.call('ZSCORE', owners_key, identifier)
if existing_score then
    -- Already own it - update TTL and issue NEW fencing token
    -- This ensures monotonicity even for re-entrant acquires
    local new_expires = now_ms + lock_timeout_ms
    redis.call('ZADD', owners_key, new_expires, identifier)
    local fencing_token = redis.call('INCR', fencing_key)
    return {1, fencing_token, new_expires, current_count}
end

if current_count < limit then
    -- 4. Slot available - acquire it
    local expires_at = now_ms + lock_timeout_ms
    redis.call('ZADD', owners_key, expires_at, identifier)

    -- 5. Increment and get fencing token
    local fencing_token = redis.call('INCR', fencing_key)

    return {1, fencing_token, expires_at, current_count + 1}
else
    -- 6. No slots available
    return {0, '', '', current_count}
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
    """Registry of Lua scripts and their locally computed SHAs.

    SHAs are derived from the script bodies with SHA1 - identical to what
    ``SCRIPT LOAD`` would return - so no network round-trip is needed to obtain
    them. Scripts are loaded into Redis lazily by the adapter (EVAL on NOSCRIPT).
    """

    SCRIPTS: ClassVar[dict[str, str]] = {
        "acquire": ACQUIRE_SCRIPT,
        "release": RELEASE_SCRIPT,
        "refresh": REFRESH_SCRIPT,
        "cleanup": CLEANUP_SCRIPT,
        "status": STATUS_SCRIPT,
    }

    SHAS: ClassVar[dict[str, str]] = {
        name: hashlib.sha1(script.encode()).hexdigest() for name, script in SCRIPTS.items()
    }

    def get_sha(self, name: str) -> str:
        """Get the locally computed SHA of a script by name."""
        try:
            return self.SHAS[name]
        except KeyError:
            raise ValueError(f"Unknown script '{name}'") from None

    def get_script(self, name: str) -> str:
        """Get the Lua script source by name."""
        try:
            return self.SCRIPTS[name]
        except KeyError:
            raise ValueError(f"Unknown script '{name}'") from None


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
    ) -> tuple[bool, int | None, int | None, int]:
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
    ) -> tuple[bool, int | None, int | None, int]:
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

    def cleanup(self, client: ScriptClient, owners_key: str, now_ms: int) -> int:
        result: object = client.evalsha(
            self._registry.get_sha("cleanup"),
            1,
            owners_key,
            now_ms,
        )
        return _to_int(result)

    async def cleanup_async(self, client: ScriptClient, owners_key: str, now_ms: int) -> int:
        result: object = await client.aevalsha(
            self._registry.get_sha("cleanup"),
            1,
            owners_key,
            now_ms,
        )
        return _to_int(result)

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


def _parse_acquire_result(result: object) -> tuple[bool, int | None, int | None, int]:
    if not isinstance(result, list | tuple) or len(result) != 4:
        raise ValueError("Unexpected acquire result shape")

    success_raw, fencing_raw, expires_raw, count_raw = result
    success = _to_bool(success_raw)

    fencing = _to_optional_int(fencing_raw)
    expires = _to_optional_int(expires_raw)
    count = _to_int(count_raw)
    return success, fencing, expires, count


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
    def evalsha(self, sha: str, numkeys: int, *args: ScriptArg) -> object: ...

    def aevalsha(self, sha: str, numkeys: int, *args: ScriptArg) -> Awaitable[object]: ...


class ScriptClientAdapter:
    """Runs cached Lua scripts via EVALSHA with a transparent EVAL fallback.

    The script SHA is computed locally, so the first call needs no SCRIPT LOAD.
    If Redis does not know the script yet (NOSCRIPT - e.g. after a restart or
    failover), the adapter falls back to EVAL with the full body, which executes
    the script and caches it server-side for subsequent EVALSHA calls.
    """

    def __init__(self, client: redis.Redis | aioredis.Redis) -> None:
        self._client = client
        self._is_async = isinstance(client, aioredis.Redis)

    def evalsha(self, sha: str, numkeys: int, *args: ScriptArg) -> object:
        if self._is_async:
            raise RuntimeError("sync evalsha not supported")
        try:
            return self._client.evalsha(sha, numkeys, *args)
        except redis.exceptions.NoScriptError:
            return self._client.eval(self._script_for_sha(sha), numkeys, *args)

    async def aevalsha(self, sha: str, numkeys: int, *args: ScriptArg) -> object:
        if not self._is_async:
            raise RuntimeError("async evalsha not supported")
        try:
            result = self._client.evalsha(sha, numkeys, *args)
            return await result if isinstance(result, Awaitable) else result
        except redis.exceptions.NoScriptError:
            result = self._client.eval(self._script_for_sha(sha), numkeys, *args)
            return await result if isinstance(result, Awaitable) else result

    @staticmethod
    def _script_for_sha(sha: str) -> str:
        """Reverse-map a SHA to its script body (for the EVAL fallback)."""
        for script in LuaScriptRegistry.SCRIPTS.values():
            if hashlib.sha1(script.encode()).hexdigest() == sha:
                return script
        raise ValueError(f"Unknown script SHA: {sha}")
