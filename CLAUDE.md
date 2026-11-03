# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**redis-semaphore** - Python library for distributed synchronization primitives on Redis. Provides counting semaphores and mutexes with Sentinel support, sync/async API, automatic heartbeat, and fencing tokens.

## Commands

```bash
# Install dependencies
uv sync

# Run all tests (requires Redis on localhost:6379)
make test

# Run specific test file
uv run pytest tests/test_semaphore.py -v

# Run single test
uv run pytest tests/test_semaphore.py::test_name -v

# Linting and formatting
make lint          # Check with ruff
make format        # Format with ruff
make ruff-fix      # Format + fix linting issues
make typecheck     # Run mypy

# Run all checks (lint + typecheck + test)
make check

# Redis management
make redis-up      # Start Redis in Docker
make redis-down    # Stop Redis
make redis-shell   # Open redis-cli

# Sentinel (for HA tests)
make sentinel-up   # Start Sentinel cluster via docker-compose
make sentinel-down
```

## Testing Environment Variables

```bash
REDIS_PORT=6380                           # Non-default Redis port
REDIS_SENTINEL_HOSTS=host1:26379,host2:26379  # Sentinel hosts
REDIS_SENTINEL_SERVICE=mymaster           # Sentinel service name
REDIS_SENTINEL_PASSWORD=secret            # Sentinel password
```

## Architecture

### Core Components

| File | Purpose |
|------|---------|
| `semaphore.py` | Main classes: `Semaphore` (counting) and `Mutex` (binary). Supports both sync and async via same class |
| `rate_limiter.py` | `RateLimiter`: sliding-window limits with weighted costs (RPM/TPM), sync + async |
| `dispatch.py` | `DispatchQueue` (producer, sync + async) and `DispatchWorker` (async only): shared FIFO job queues with one leader per queue |
| `lua_scripts.py` | Atomic Lua scripts (acquire/release/refresh/cleanup/status) + `LuaScriptRegistry` for SHA caching |
| `heartbeat.py` | `SyncHeartbeat` (thread) and `AsyncHeartbeat` (task) for TTL refresh |
| `base.py` | `BaseSemaphore` ABC with shared logic |
| `connection.py` | `RedisConnectionFactory` for Redis and Sentinel connections |
| `types.py` | Dataclasses: `SemaphoreConfig`, `AcquireResult`, `LockState`, `AcquireMode` |
| `errors.py` | Exception hierarchy rooted at `RedisSemaphoreError` |

### Redis Data Structures

- `{namespace}:{name}:owners` - Sorted Set (member=identifier, score=expires_at_ms); acquire/refresh set a key TTL derived from the top score (+ one lock_timeout margin) so abandoned semaphores clean up
- `{namespace}:{name}:fencing` - String (monotonic counter for fencing tokens); intentionally no TTL (expiry would reset monotonicity)
- `{namespace}:{name}:queue` - List (for BLPOP notifications in BLPOP mode); capped at `limit` entries via LTRIM on each notify and given a lock_timeout TTL
- `{namespace}:{name}:{limit}:log` - Sorted Set per `RateLimit` (member=`"<cost>:<identifier>"`, score=admission ms) and `{namespace}:{name}:{limit}:total` - String with the running sum of the log (self-healed by recount if lost/negative); both get a TTL of two windows on write
- `{namespace}:{queue}:stream` - Stream per dispatch queue (consumer group `DispatchConfig.group`; fields id/payload(JSON)/created/deadline); entries are XACK+XDEL'd when finished. `{namespace}:{queue}:reply:{job_id}` - List with the JSON reply (TTL `reply_ttl`). Leadership is a `Mutex` named `{queue}:leader` in the same namespace
- With `cluster_hash_tags=True` the prefix becomes a hash tag: `{namespace:name}:owners` etc., so all keys of a semaphore map to one Redis Cluster slot (opt-in: changes key names)

### Key Concepts

**Lua Scripts**: All critical operations are atomic via Lua. Scripts are loaded once and called by SHA via `EVALSHA`.

**Time source**: By default (`time_source=TimeSource.SERVER`) "now" is read via `TIME` inside the Lua scripts (the client passes an empty string as the time ARGV) - clients need no NTP sync and pay no extra RTT. `TimeSource.CLIENT` passes client milliseconds instead (requires NTP-synced clients; for Redis servers older than 3.2). `use_server_time` is a deprecated alias for SERVER.

**Heartbeat**: Background thread/task calls an internal refresh (`_heartbeat_refresh`/`_heartbeat_refresh_async`, *not* the public `refresh()`) at `refresh_interval` (default: `lock_timeout / 3`, leaving ~2/3 of the TTL as retry budget). Transient connection errors are tolerated and retried at `refresh_retry_interval` (default: `min(refresh_interval, 1.0)`); if the lock cannot be refreshed for `lock_timeout` seconds (measured conservatively from the start of the last successful attempt), it is treated as lost. Permanent errors (e.g. ACL denial → `PermanentBackendError`) escalate immediately without waiting out the deadline. On loss the heartbeat is the single owner of lock-loss handling: it sets state `LOST`, fires `on_lock_lost`, and records the metric — this fires in `strict_mode` too (the heartbeat never raises into its own thread; `strict_mode` is enforced on the next user operation via `_check_lock_lost`). Keep the client `socket_timeout` smaller than `lock_timeout` for timely escalation.

**Fencing Token**: Monotonically increasing counter returned on acquire. Protects against race conditions during GC pauses.

**Dual API**: Same `Semaphore`/`Mutex` class works with both sync (`acquire`/`release`) and async (`aacquire`/`arelease`). Do not mix modes on same instance.

**Wait Strategies**: Two modes via `acquire_mode`:
- `BLPOP` (default): Efficient blocking wait via Redis BLPOP, approximately FIFO (a fresh arriver can barge past a woken waiter). Each wait is capped below the client's `socket_timeout`; transient errors during the wait are treated as a spurious wakeup, not surfaced
- `POLLING`: Retry loop with attempt-based exponential backoff and jitter (default jitter 0.1)

**Reacquire**: `reacquire_policy=RENEW` (default) lets `acquire()` on an already-held instance renew the TTL and issue a NEW fencing token (no hold count - one release frees the slot); `FORBID` raises `AlreadyAcquiredError` instead.

**Status & cleanup**: `status()`/`astatus()` return a `SemaphoreStatus` (used_slots, available, is_owner, expires_at) observed atomically without acquiring — both purge expired owners first. `cleanup()`/`acleanup()` force-remove expired owner entries and return the count; rarely needed since `acquire()`/`status()` purge lazily. Backed by `STATUS_SCRIPT`/`CLEANUP_SCRIPT`.

**Rate limiter**: `RateLimiter` is stateless per instance (nothing to release; units leave the window by themselves). `RATE_ACQUIRE_SCRIPT` evicts expired entries, checks all limits, and records the request only if it fits all of them; on denial it returns `retry_after_ms` by walking the log oldest-first. `adjust()` replaces a permit's cost keeping its admission timestamp (overshoot allowed; `0` = refund). Blocking acquire sleeps for `retry_after` + `wait_jitter` and fails fast with `RateLimitTimeoutError` if that exceeds the remaining `acquire_timeout`. Not FIFO.

**Dispatch queue**: `DispatchQueue.asubmit()` enqueues via `DISPATCH_ENQUEUE_SCRIPT` (atomic XLEN check + XADD) and waits on the reply list with BLPOP rounds capped below `socket_timeout`. Each `DispatchWorker` takes at most one free queue per `election_interval` round (non-blocking `Mutex.aacquire`); the leader reads one entry at a time (`XREADGROUP`, plus `XAUTOCLAIM` of entries idle > `reclaim_idle` every `reclaim_interval`) and runs the handler until `job.admit()` or completion, then moves on while the handler continues. A touch loop `XCLAIM`s the worker's unanswered entries every `reclaim_idle/3` so they are not reclaimed while alive. On lost leadership or `stop()` an unadmitted handler is cancelled and its entry is handed back (`XCLAIM ... IDLE reclaim_idle`) for immediate reclaim. Replies + XACK + XDEL go in one MULTI. At-least-once delivery. `RequeueJob` from a handler: optional delay (job stays taken/touched, cut short by `stop()`), then hand back + flag the stream in `_reclaim_now`, so the leader's next iteration does a forced XAUTOCLAIM from `0-0` before reading new entries; locally requeued ids are tracked in `_requeued` so they are not logged as abandoned. Health: `DispatchQueue.astatus()` = `DISPATCH_STATUS_SCRIPT` (XLEN, XPENDING via pcall, age of the first entry id vs server TIME) + `STATUS_SCRIPT` on the leader Mutex's owners key (`_Keys.leader_owners` must stay in sync with `BaseSemaphoreCommon.owners_key` - covered by a test); `DispatchWorker.health()` is local (main-loop tick + last Redis error, no I/O).

## File Modification Guide

| Task | Files to modify |
|------|-----------------|
| New Lua script | `lua_scripts.py` |
| Change acquire/release logic | `semaphore.py`, `lua_scripts.py` |
| New error type | `errors.py`, `__init__.py` |
| New config option | `types.py`, `__init__.py` |
| Change wait strategy | `semaphore.py` (wait methods), `types.py` (config) |
| Sentinel changes | `connection.py` |
| Metrics | `metrics.py` |
| Logging | `logger.py` |

## Limitations

- No first-class Redis Cluster support (only Sentinel); `cluster_hash_tags=True` makes the key layout Cluster-slot-compatible
- No Redlock for multi-master setups
- NTP synchronization between clients is only required with `time_source=TimeSource.CLIENT`; the default SERVER mode reads time inside the Lua scripts (needs Redis >= 3.2)
- `DispatchQueue`/`DispatchWorker` require Redis >= 6.2 (XAUTOCLAIM, BLMOVE); the rest of the library works from Redis 3.2
