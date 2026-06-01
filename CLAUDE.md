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
| `lua_scripts.py` | Atomic Lua scripts (acquire/release/refresh/cleanup/status) + `LuaScriptRegistry` for SHA caching |
| `heartbeat.py` | `SyncHeartbeat` (thread) and `AsyncHeartbeat` (task) for TTL refresh |
| `base.py` | `BaseSemaphore` ABC with shared logic |
| `connection.py` | `RedisConnectionFactory` for Redis and Sentinel connections |
| `types.py` | Dataclasses: `SemaphoreConfig`, `AcquireResult`, `LockState`, `AcquireMode` |
| `errors.py` | Exception hierarchy rooted at `RedisSemaphoreError` |

### Redis Data Structures

- `{namespace}:{name}:owners` - Sorted Set (member=identifier, score=expires_at_ms)
- `{namespace}:{name}:fencing` - String (monotonic counter for fencing tokens)
- `{namespace}:{name}:queue` - List (for BLPOP notifications in BLPOP mode); capped at `limit` entries via LTRIM on each notify to prevent unbounded growth

### Key Concepts

**Lua Scripts**: All critical operations are atomic via Lua. Scripts are loaded once and called by SHA via `EVALSHA`.

**Heartbeat**: Background thread/task calls an internal refresh (`_heartbeat_refresh`/`_heartbeat_refresh_async`, *not* the public `refresh()`) at `refresh_interval` (default: 80% of `lock_timeout`). Transient connection errors are tolerated and retried at `refresh_retry_interval` (default: `min(refresh_interval, 1.0)`); if the lock cannot be refreshed for `lock_timeout` seconds (measured conservatively from the start of the last successful attempt), it is treated as lost. Permanent errors (e.g. ACL denial → `PermanentBackendError`) escalate immediately without waiting out the deadline. On loss the heartbeat is the single owner of lock-loss handling: it sets state `LOST`, fires `on_lock_lost`, and records the metric — this fires in `strict_mode` too (the heartbeat never raises into its own thread; `strict_mode` is enforced on the next user operation via `_check_lock_lost`). Keep the client `socket_timeout` smaller than `lock_timeout` for timely escalation.

**Fencing Token**: Monotonically increasing counter returned on acquire. Protects against race conditions during GC pauses.

**Dual API**: Same `Semaphore`/`Mutex` class works with both sync (`acquire`/`release`) and async (`aacquire`/`arelease`). Do not mix modes on same instance.

**Wait Strategies**: Two modes via `acquire_mode`:
- `POLLING` (default): Retry loop with configurable exponential backoff and jitter
- `BLPOP`: Efficient blocking wait via Redis BLPOP with FIFO ordering

**Status & cleanup**: `status()`/`astatus()` return a `SemaphoreStatus` (used_slots, available, is_owner, expires_at) observed atomically without acquiring — both purge expired owners first. `cleanup()`/`acleanup()` force-remove expired owner entries and return the count; rarely needed since `acquire()`/`status()` purge lazily. Backed by `STATUS_SCRIPT`/`CLEANUP_SCRIPT`.

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

- No Redis Cluster support (only Sentinel)
- No Redlock for multi-master setups
- Requires NTP synchronization between clients (clock skew causes issues)
