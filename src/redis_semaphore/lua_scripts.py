"""Lua scripts for atomic semaphore operations.

Redis data structures:
- ZSET: {namespace}:{name}:owners
  - member: identifier (unique owner ID)
  - score: expiration timestamp in milliseconds

- STRING: {namespace}:{name}:fencing
  - Monotonically increasing counter for fencing tokens

Rate limiter (per limit):
- ZSET: {namespace}:{name}:{limit}:log
  - member: "<cost>:<identifier>", score: admission timestamp in milliseconds
- STRING: {namespace}:{name}:{limit}:total
  - Running sum of the costs currently in the log
"""

from __future__ import annotations

import hashlib
from collections.abc import Awaitable, Sequence
from typing import ClassVar, Protocol, TypeAlias

import redis
import redis.asyncio as aioredis


def _lua_resolve_now(argv_index: int) -> str:
    """Lua fragment: take now_ms from ARGV, or read it on the server.

    An empty-string ARGV (``tonumber`` -> nil) selects server time: the script
    calls TIME so every client shares the Redis clock and no NTP sync between
    clients is needed. ``redis.replicate_commands()`` switches Redis 3.2-6.x
    to effect replication, which is required to call TIME from a script; on
    Redis 7+ it is a harmless no-op.
    """
    return f"""
local now_ms = tonumber(ARGV[{argv_index}])
if not now_ms then
    if redis.replicate_commands then redis.replicate_commands() end
    local t = redis.call('TIME')
    now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end
"""


# Lua fragment: expire the owners key together with its longest-lived member,
# so abandoned semaphores do not leave keys behind forever. Uses the top ZSET
# score (members may have different lock_timeouts) plus one lock_timeout of
# margin to absorb client/server clock difference in client-time mode.
# Requires owners_key, now_ms and lock_timeout_ms in scope.
_LUA_BUMP_OWNERS_TTL = """
    local top = redis.call('ZRANGE', owners_key, -1, -1, 'WITHSCORES')
    local max_expires = tonumber(top[2])
    if max_expires then
        redis.call('PEXPIRE', owners_key, math.floor(max_expires - now_ms + lock_timeout_ms))
    end
"""


# =============================================================================
# ACQUIRE - Acquire a semaphore slot
# =============================================================================
# KEYS[1] = owners sorted set key
# KEYS[2] = fencing token key
# ARGV[1] = identifier (unique owner id)
# ARGV[2] = limit (max concurrent holders)
# ARGV[3] = lock_timeout_ms (TTL in milliseconds)
# ARGV[4] = current_time_ms, or '' to use server time (TIME inside the script)
#
# Returns: [success (0/1), fencing_token or nil, expires_at_ms or nil, used_slots]
#   used_slots is the occupancy observed atomically by this call (including the
#   caller on success). It lets the client feed metrics/logs without a separate
#   status round-trip.
ACQUIRE_SCRIPT = (
    """
local owners_key = KEYS[1]
local fencing_key = KEYS[2]
local identifier = ARGV[1]
local limit = tonumber(ARGV[2])
local lock_timeout_ms = tonumber(ARGV[3])
"""
    + _lua_resolve_now(4)
    + """
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
"""
    + _LUA_BUMP_OWNERS_TTL
    + """
    return {1, fencing_token, new_expires, current_count}
end

if current_count < limit then
    -- 4. Slot available - acquire it
    local expires_at = now_ms + lock_timeout_ms
    redis.call('ZADD', owners_key, expires_at, identifier)

    -- 5. Increment and get fencing token
    local fencing_token = redis.call('INCR', fencing_key)
"""
    + _LUA_BUMP_OWNERS_TTL
    + """
    return {1, fencing_token, expires_at, current_count + 1}
else
    -- 6. No slots available
    return {0, '', '', current_count}
end
"""
)


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
# ARGV[3] = current_time_ms, or '' to use server time (TIME inside the script)
#
# Returns: new absolute expiry in ms if refreshed, 0 if not owned (lock lost).
#   Returning the expiry (rather than a bare 1) lets the client track it
#   without knowing "now" in server-time mode.
REFRESH_SCRIPT = (
    """
local owners_key = KEYS[1]
local identifier = ARGV[1]
local lock_timeout_ms = tonumber(ARGV[2])
"""
    + _lua_resolve_now(3)
    + """
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
"""
    + _LUA_BUMP_OWNERS_TTL
    + """
return new_expires
"""
)


# =============================================================================
# CLEANUP - Force cleanup of expired locks
# =============================================================================
# KEYS[1] = owners sorted set key
# ARGV[1] = current_time_ms, or '' to use server time (TIME inside the script)
#
# Returns: number of removed entries
CLEANUP_SCRIPT = (
    """
local owners_key = KEYS[1]
"""
    + _lua_resolve_now(1)
    + """
return redis.call('ZREMRANGEBYSCORE', owners_key, '-inf', now_ms)
"""
)


# =============================================================================
# STATUS - Get current semaphore status
# =============================================================================
# KEYS[1] = owners sorted set key
# ARGV[1] = current_time_ms, or '' to use server time (TIME inside the script)
# ARGV[2] = identifier (optional, to check ownership)
#
# Returns: [current_count, is_owner (0/1), expires_at or nil]
STATUS_SCRIPT = (
    """
local owners_key = KEYS[1]
local identifier = ARGV[2]
"""
    + _lua_resolve_now(1)
    + """
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
)


# =============================================================================
# RATE LIMITER - sliding-window log with weighted entries
# =============================================================================
# Per limit i (name = RateLimit.name):
#   KEYS[2i-1] = ZSET {namespace}:{name}:{limit}:log
#                member "<cost>:<identifier>", score = admission time in ms
#   KEYS[2i]   = STRING {namespace}:{name}:{limit}:total
#                running sum of the costs in the log, so a check costs
#                O(expired entries) instead of O(window size)
# Both keys get a TTL of two windows on every write (one window of margin for
# client-time skew), so abandoned limiters clean up.

# Lua fragment: helpers shared by the rate limiter scripts.
_LUA_RATE_HELPERS = """
local function member_cost(member)
    return tonumber(string.match(member, '^(%d+):')) or 0
end

local function touch(log_key, total_key, ttl_ms)
    redis.call('PEXPIRE', log_key, ttl_ms)
    redis.call('PEXPIRE', total_key, ttl_ms)
end

-- Rebuild the running total from the log (the total key was lost or drifted).
local function recount(log_key, total_key, ttl_ms)
    local total = 0
    for _, member in ipairs(redis.call('ZRANGE', log_key, 0, -1)) do
        total = total + member_cost(member)
    end
    redis.call('SET', total_key, total, 'PX', ttl_ms)
    local log_ttl = redis.call('PTTL', log_key)
    if log_ttl > 0 then
        -- The rebuilt total must not outlive the log it was derived from.
        redis.call('PEXPIRE', total_key, log_ttl)
    end
    return total
end

-- Drop entries at or before cutoff and return the units still in the window.
local function evict(log_key, total_key, cutoff, ttl_ms)
    local expired = redis.call('ZRANGEBYSCORE', log_key, '-inf', cutoff)
    local freed = 0
    for _, member in ipairs(expired) do
        freed = freed + member_cost(member)
    end
    if #expired > 0 then
        redis.call('ZREMRANGEBYSCORE', log_key, '-inf', cutoff)
    end
    if redis.call('ZCARD', log_key) == 0 then
        redis.call('DEL', total_key)
        return 0
    end
    local raw = redis.call('GET', total_key)
    if not raw then
        return recount(log_key, total_key, ttl_ms)
    end
    local total = tonumber(raw)
    if freed > 0 then
        total = redis.call('DECRBY', total_key, freed)
    end
    if total < 0 then
        return recount(log_key, total_key, ttl_ms)
    end
    return total
end
"""


# -----------------------------------------------------------------------------
# RATE_ACQUIRE - admit a weighted request if it fits every limit
# -----------------------------------------------------------------------------
# KEYS    = log/total pairs, see above
# ARGV[1] = identifier (unique per request)
# ARGV[2] = current_time_ms, or '' to use server time (TIME inside the script)
# ARGV[3..] = per limit: limit, window_ms, cost
#
# Returns: [success (0/1), retry_after_ms, now_ms, used_1, ..., used_n]
#   On failure nothing is recorded and retry_after_ms is the earliest moment
#   (relative to now) at which enough units leave every violated window.
RATE_ACQUIRE_SCRIPT = (
    """
local identifier = ARGV[1]
"""
    + _lua_resolve_now(2)
    + _LUA_RATE_HELPERS
    + """
local n = #KEYS / 2
local used = {}
local retry_after = 0

-- 1. Evict expired entries and check every limit (nothing is written yet).
for i = 1, n do
    local log_key, total_key = KEYS[2 * i - 1], KEYS[2 * i]
    local base = 3 + (i - 1) * 3
    local limit = tonumber(ARGV[base])
    local window_ms = tonumber(ARGV[base + 1])
    local cost = tonumber(ARGV[base + 2])
    local total = evict(log_key, total_key, now_ms - window_ms, window_ms * 2)
    used[i] = total
    if cost > 0 and total + cost > limit then
        -- Walk the log oldest-first (in chunks, stopping early) until enough
        -- units would have expired.
        local need = total + cost - limit
        local wait = window_ms
        local freed = 0
        local start = 0
        local found = false
        while not found do
            local entries = redis.call('ZRANGE', log_key, start, start + 127, 'WITHSCORES')
            if #entries == 0 then break end
            for j = 1, #entries, 2 do
                freed = freed + member_cost(entries[j])
                if freed >= need then
                    wait = tonumber(entries[j + 1]) + window_ms - now_ms
                    found = true
                    break
                end
            end
            start = start + 128
        end
        if wait < 1 then wait = 1 end
        if wait > retry_after then retry_after = wait end
    end
end

local result = {0, retry_after, now_ms}
if retry_after > 0 then
    for i = 1, n do result[3 + i] = used[i] end
    return result
end

-- 2. Fits all limits: record the request in every window it is charged to.
for i = 1, n do
    local log_key, total_key = KEYS[2 * i - 1], KEYS[2 * i]
    local base = 3 + (i - 1) * 3
    local window_ms = tonumber(ARGV[base + 1])
    local cost = ARGV[base + 2]
    if tonumber(cost) > 0 then
        -- NX + the guard keep a retried acquire (the client resent the script
        -- after losing the reply) from charging the total twice: the member
        -- is already recorded then, with its original admission time.
        if redis.call('ZADD', log_key, 'NX', now_ms, cost .. ':' .. identifier) == 1 then
            used[i] = redis.call('INCRBY', total_key, tonumber(cost))
        else
            used[i] = tonumber(redis.call('GET', total_key)) or used[i]
        end
        touch(log_key, total_key, window_ms * 2)
    end
    result[3 + i] = used[i]
end
result[1] = 1
return result
"""
)


# -----------------------------------------------------------------------------
# RATE_ADJUST - replace the recorded cost of an admitted request
# -----------------------------------------------------------------------------
# KEYS    = log/total pairs of the limits being adjusted
# ARGV[1] = identifier of the admitted request
# ARGV[2] = current_time_ms, or '' to use server time (TIME inside the script)
# ARGV[3] = admission time in ms (as returned by RATE_ACQUIRE)
# ARGV[4..] = per limit: window_ms, old_cost, new_cost
#
# Returns: [used_1, ..., used_n] after the adjustment. The new cost keeps the
#   original admission time, so it leaves the window when the request would
#   have; if the request already left the window, nothing is re-added.
#   Overshooting a limit is allowed (the debt delays later acquires).
RATE_ADJUST_SCRIPT = (
    """
local identifier = ARGV[1]
"""
    + _lua_resolve_now(2)
    + _LUA_RATE_HELPERS
    + """
local acquired_at = tonumber(ARGV[3])
local n = #KEYS / 2
local used = {}

for i = 1, n do
    local log_key, total_key = KEYS[2 * i - 1], KEYS[2 * i]
    local base = 4 + (i - 1) * 3
    local window_ms = tonumber(ARGV[base])
    local old_cost = ARGV[base + 1]
    local new_cost = ARGV[base + 2]
    local ttl_ms = window_ms * 2
    local total = evict(log_key, total_key, now_ms - window_ms, ttl_ms)
    if tonumber(old_cost) > 0
        and redis.call('ZREM', log_key, old_cost .. ':' .. identifier) == 1 then
        total = redis.call('DECRBY', total_key, tonumber(old_cost))
    end
    if tonumber(new_cost) > 0 and acquired_at > now_ms - window_ms then
        -- Only count a member that is really new: a retried adjust (reply
        -- lost after the script ran) finds it already recorded.
        if redis.call('ZADD', log_key, acquired_at, new_cost .. ':' .. identifier) == 1 then
            total = redis.call('INCRBY', total_key, tonumber(new_cost))
        end
        touch(log_key, total_key, ttl_ms)
    elseif redis.call('ZCARD', log_key) == 0 then
        redis.call('DEL', total_key)
        total = 0
    end
    used[i] = total
end
return used
"""
)


# -----------------------------------------------------------------------------
# RATE_STATUS - current consumption without admitting anything
# -----------------------------------------------------------------------------
# KEYS    = log/total pairs, see above
# ARGV[1] = current_time_ms, or '' to use server time (TIME inside the script)
# ARGV[2..] = per limit: window_ms
#
# Returns: [used_1, ..., used_n]
RATE_STATUS_SCRIPT = (
    _lua_resolve_now(1)
    + _LUA_RATE_HELPERS
    + """
local used = {}
for i = 1, #KEYS / 2 do
    local window_ms = tonumber(ARGV[1 + i])
    used[i] = evict(KEYS[2 * i - 1], KEYS[2 * i], now_ms - window_ms, window_ms * 2)
end
return used
"""
)


# =============================================================================
# DISPATCH_ENQUEUE - append a job to a queue stream unless it is full
# =============================================================================
# KEYS[1] = queue stream key
# KEYS[2] = submit dedup marker key (same hash slot as the stream)
# ARGV[1] = max queue size (0 = unbounded)
# ARGV[2] = dedup marker TTL in ms
# ARGV[3] = job timeout in ms, or '' for no deadline
# ARGV[4] = job id
# ARGV[5] = JSON payload
#
# Returns: the new entry id, or nil if the queue is full.
#   - created/deadline are stamped from the SERVER clock, like everywhere else
#     in this library, so producers and workers need no NTP sync.
#   - XADD with an auto id is not idempotent, so the entry id is remembered
#     under the marker key: a resent call (the client retried after losing the
#     reply) returns the id of the entry it already created instead of
#     appending a duplicate job.
DISPATCH_ENQUEUE_SCRIPT = """
-- XADD with an auto id and TIME are non-deterministic: Redis 5-6 need effect
-- replication (a no-op on Redis 7+).
if redis.replicate_commands then redis.replicate_commands() end
local existing = redis.call('GET', KEYS[2])
if existing then return existing end
local max_size = tonumber(ARGV[1])
if max_size > 0 and redis.call('XLEN', KEYS[1]) >= max_size then
    return false
end
local t = redis.call('TIME')
local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local deadline = ''
local timeout_ms = tonumber(ARGV[3])
if timeout_ms then deadline = tostring(now_ms + timeout_ms) end
local entry_id = redis.call('XADD', KEYS[1], '*',
    'id', ARGV[4], 'payload', ARGV[5], 'created', tostring(now_ms), 'deadline', deadline)
redis.call('SET', KEYS[2], entry_id, 'PX', math.floor(tonumber(ARGV[2])))
return entry_id
"""


# =============================================================================
# DISPATCH_STATUS - observe a queue stream without changing it
# =============================================================================
# KEYS[1] = queue stream key
# ARGV[1] = consumer group name
#
# Returns: [length, in_progress, oldest_age_ms (-1 if empty)]
#   in_progress is the group's pending count (0 if the group does not exist
#   yet); the oldest age comes from the first entry id and the server clock.
DISPATCH_STATUS_SCRIPT = """
local length = redis.call('XLEN', KEYS[1])
if length == 0 then
    return {0, 0, -1}
end
local in_progress = 0
local pending = redis.pcall('XPENDING', KEYS[1], ARGV[1])
if type(pending) == 'table' and not pending.err then
    in_progress = tonumber(pending[1]) or 0
end
local oldest = -1
local first = redis.call('XRANGE', KEYS[1], '-', '+', 'COUNT', 1)
if first[1] then
    local t = redis.call('TIME')
    local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
    local added_ms = tonumber(string.match(first[1][1], '^(%d+)-'))
    oldest = math.max(0, now_ms - added_ms)
end
return {length, in_progress, oldest}
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
        "rate_acquire": RATE_ACQUIRE_SCRIPT,
        "rate_adjust": RATE_ADJUST_SCRIPT,
        "rate_status": RATE_STATUS_SCRIPT,
        "dispatch_enqueue": DISPATCH_ENQUEUE_SCRIPT,
        "dispatch_status": DISPATCH_STATUS_SCRIPT,
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
        now_ms: int | str,
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
        now_ms: int | str,
    ) -> int:
        """Refresh the slot TTL; returns the new expiry in ms, or 0 if lost."""
        result: object = client.evalsha(
            self._registry.get_sha("refresh"),
            1,
            owners_key,
            identifier,
            lock_timeout_ms,
            now_ms,
        )
        return _to_int(result)

    async def acquire_async(
        self,
        client: ScriptClient,
        owners_key: str,
        fencing_key: str,
        identifier: str,
        limit: int,
        lock_timeout_ms: int,
        now_ms: int | str,
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
        now_ms: int | str,
    ) -> int:
        """Async variant of :meth:`refresh`; returns new expiry ms, or 0 if lost."""
        result: object = await client.aevalsha(
            self._registry.get_sha("refresh"),
            1,
            owners_key,
            identifier,
            lock_timeout_ms,
            now_ms,
        )
        return _to_int(result)

    def cleanup(self, client: ScriptClient, owners_key: str, now_ms: int | str) -> int:
        result: object = client.evalsha(
            self._registry.get_sha("cleanup"),
            1,
            owners_key,
            now_ms,
        )
        return _to_int(result)

    async def cleanup_async(self, client: ScriptClient, owners_key: str, now_ms: int | str) -> int:
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
        now_ms: int | str,
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
        now_ms: int | str,
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

    def rate_acquire(
        self,
        client: ScriptClient,
        keys: Sequence[str],
        identifier: str,
        now_ms: int | str,
        limits: Sequence[tuple[int, int, int]],
    ) -> tuple[bool, int, int, list[int]]:
        """Admit a weighted request; ``limits`` holds (limit, window_ms, cost)."""
        result: object = client.evalsha(
            self._registry.get_sha("rate_acquire"),
            len(keys),
            *keys,
            identifier,
            now_ms,
            *_flatten(limits),
        )
        return _parse_rate_acquire_result(result, len(limits))

    async def rate_acquire_async(
        self,
        client: ScriptClient,
        keys: Sequence[str],
        identifier: str,
        now_ms: int | str,
        limits: Sequence[tuple[int, int, int]],
    ) -> tuple[bool, int, int, list[int]]:
        """Async variant of :meth:`rate_acquire`."""
        result: object = await client.aevalsha(
            self._registry.get_sha("rate_acquire"),
            len(keys),
            *keys,
            identifier,
            now_ms,
            *_flatten(limits),
        )
        return _parse_rate_acquire_result(result, len(limits))

    def rate_adjust(
        self,
        client: ScriptClient,
        keys: Sequence[str],
        identifier: str,
        now_ms: int | str,
        acquired_at_ms: int,
        changes: Sequence[tuple[int, int, int]],
    ) -> list[int]:
        """Replace recorded costs; ``changes`` holds (window_ms, old, new)."""
        result: object = client.evalsha(
            self._registry.get_sha("rate_adjust"),
            len(keys),
            *keys,
            identifier,
            now_ms,
            acquired_at_ms,
            *_flatten(changes),
        )
        return _parse_int_list(result, len(changes))

    async def rate_adjust_async(
        self,
        client: ScriptClient,
        keys: Sequence[str],
        identifier: str,
        now_ms: int | str,
        acquired_at_ms: int,
        changes: Sequence[tuple[int, int, int]],
    ) -> list[int]:
        """Async variant of :meth:`rate_adjust`."""
        result: object = await client.aevalsha(
            self._registry.get_sha("rate_adjust"),
            len(keys),
            *keys,
            identifier,
            now_ms,
            acquired_at_ms,
            *_flatten(changes),
        )
        return _parse_int_list(result, len(changes))

    def rate_status(
        self,
        client: ScriptClient,
        keys: Sequence[str],
        now_ms: int | str,
        windows_ms: Sequence[int],
    ) -> list[int]:
        result: object = client.evalsha(
            self._registry.get_sha("rate_status"),
            len(keys),
            *keys,
            now_ms,
            *windows_ms,
        )
        return _parse_int_list(result, len(windows_ms))

    async def rate_status_async(
        self,
        client: ScriptClient,
        keys: Sequence[str],
        now_ms: int | str,
        windows_ms: Sequence[int],
    ) -> list[int]:
        result: object = await client.aevalsha(
            self._registry.get_sha("rate_status"),
            len(keys),
            *keys,
            now_ms,
            *windows_ms,
        )
        return _parse_int_list(result, len(windows_ms))

    def dispatch_enqueue(
        self,
        client: ScriptClient,
        stream_key: str,
        marker_key: str,
        max_size: int,
        marker_ttl_ms: int,
        job_id: str,
        payload: str,
        timeout_ms: int | str,
    ) -> str | None:
        """Append a job; returns the entry id, or None if the queue is full.

        created/deadline are stamped from the server clock inside the script
        (``timeout_ms`` is a duration; '' = no deadline). The marker key makes
        a retried call return the id it already created instead of appending
        a duplicate.
        """
        result: object = client.evalsha(
            self._registry.get_sha("dispatch_enqueue"),
            2,
            stream_key,
            marker_key,
            max_size,
            marker_ttl_ms,
            timeout_ms,
            job_id,
            payload,
        )
        return _to_optional_str(result)

    async def dispatch_enqueue_async(
        self,
        client: ScriptClient,
        stream_key: str,
        marker_key: str,
        max_size: int,
        marker_ttl_ms: int,
        job_id: str,
        payload: str,
        timeout_ms: int | str,
    ) -> str | None:
        """Async variant of :meth:`dispatch_enqueue`."""
        result: object = await client.aevalsha(
            self._registry.get_sha("dispatch_enqueue"),
            2,
            stream_key,
            marker_key,
            max_size,
            marker_ttl_ms,
            timeout_ms,
            job_id,
            payload,
        )
        return _to_optional_str(result)

    def dispatch_status(
        self, client: ScriptClient, stream_key: str, group: str
    ) -> tuple[int, int, int]:
        """Returns (length, in_progress, oldest_age_ms or -1)."""
        result: object = client.evalsha(
            self._registry.get_sha("dispatch_status"), 1, stream_key, group
        )
        values = _parse_int_list(result, 3)
        return values[0], values[1], values[2]

    async def dispatch_status_async(
        self, client: ScriptClient, stream_key: str, group: str
    ) -> tuple[int, int, int]:
        """Async variant of :meth:`dispatch_status`."""
        result: object = await client.aevalsha(
            self._registry.get_sha("dispatch_status"), 1, stream_key, group
        )
        values = _parse_int_list(result, 3)
        return values[0], values[1], values[2]


def _to_optional_str(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes | bytearray | memoryview):
        return bytes(value).decode()
    return str(value)


def _flatten(rows: Sequence[tuple[int, ...]]) -> list[int]:
    return [value for row in rows for value in row]


def _parse_rate_acquire_result(result: object, n: int) -> tuple[bool, int, int, list[int]]:
    if not isinstance(result, list | tuple) or len(result) != 3 + n:
        raise ValueError("Unexpected rate_acquire result shape")

    success = _to_bool(result[0])
    retry_after_ms = _to_int(result[1])
    now_ms = _to_int(result[2])
    used = [_to_int(value) for value in result[3:]]
    return success, retry_after_ms, now_ms, used


def _parse_int_list(result: object, n: int) -> list[int]:
    if not isinstance(result, list | tuple) or len(result) != n:
        raise ValueError("Unexpected result shape")
    return [_to_int(value) for value in result]


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
