# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `SemaphoreConfig.time_source` (`TimeSource.SERVER` / `TimeSource.CLIENT`).
  In the new default SERVER mode, "now" is read via `TIME` *inside* the Lua
  scripts: clients no longer need NTP-synchronized clocks and pay no extra
  round-trip. CLIENT restores the pre-0.2 behavior (client clocks + NTP
  requirement) for Redis servers older than 3.2
- `SemaphoreConfig.cluster_hash_tags` (opt-in): key layout
  `{namespace:name}:owners` etc. maps all keys of one semaphore to a single
  Redis Cluster slot. WARNING: changes key names - do not enable on a live
  deployment without draining it
- `SemaphoreConfig.reacquire_policy` (`ReacquirePolicy.RENEW` / `FORBID`):
  FORBID raises the new `AlreadyAcquiredError` when `acquire()` is called on an
  instance that already holds a slot; RENEW (default) keeps the pre-0.2
  semantics - the TTL is renewed and a NEW fencing token is issued (no hold
  count: a single `release()` frees the slot)
- Key hygiene TTLs: acquire/refresh now `PEXPIRE` the `owners` key (top score
  plus one `lock_timeout` of margin) and release sets a `lock_timeout` TTL on
  the notification `queue` key, so abandoned semaphore names no longer leak
  keys. The `fencing` counter intentionally keeps no TTL (monotonicity)
- `RedisConfig`/`SentinelConfig`: TLS options (`ssl*`), `health_check_interval`,
  `retry_on_timeout`, `client_name`, `socket_connect_timeout` and an
  `extra_kwargs` escape hatch (plus `sentinel_extra_kwargs` for the Sentinel
  constructor)

### Fixed

- BLPOP wait no longer collides with the client's `socket_timeout`: each wait
  is capped below it, and transient errors during the wait (socket read
  timeout racing the BLPOP reply, connection blips) are treated as a spurious
  wakeup instead of aborting a blocking `acquire()` with
  `RedisConnectionError`. Previously, with `socket_timeout == blpop_timeout`
  (the factory defaults), a blocking `acquire()` crashed after ~5s instead of
  honoring `acquire_timeout`
- `release()`/`arelease()` now reset local state even when the slot had
  already expired server-side (previously `is_acquired` kept reporting True)
- Exponential backoff now grows per completed wait round (base, base*m,
  base*m^2, ...). Previously the exponent was proportional to elapsed time
  divided by `retry_interval`, so the interval hit `retry_interval_max` almost
  immediately
- `await_lock_lost()` wakes via an `asyncio.Event` the moment the loss is
  signalled from async paths instead of polling every 50 ms (a 1 s fallback
  re-check still covers cross-thread signalling)

### Changed

- Server time became the default (`time_source=TimeSource.SERVER`);
  `use_server_time` is deprecated and now emits a `DeprecationWarning`
  (combining it with `time_source=TimeSource.CLIENT` raises `ValueError`)
- Default `refresh_interval` lowered from 80% to one third of `lock_timeout`,
  leaving ~2/3 of the TTL as heartbeat retry budget (was 20%). Heartbeat
  traffic per held lock grows accordingly; pass an explicit
  `refresh_interval` to keep the old cadence
- Default `retry_jitter` raised from 0.0 to 0.1 to avoid synchronized wakeups
  of many waiters
- `REFRESH` Lua script returns the new absolute expiry (ms) instead of 1/0;
  `LuaScriptRunner.refresh/refresh_async` return `int` now
- Rolling deploys: old and new library versions can share a semaphore during
  the transition (scripts are addressed by SHA). Mixed time sources during the
  rollout are no worse than the previous mixed client clocks. Do NOT flip
  `cluster_hash_tags` during a rolling deploy

## [0.1.1] - 2026-06-02

### Added

- `status()` / `astatus()` returning a `SemaphoreStatus` — observe occupancy,
  availability, ownership, and slot expiry without acquiring (expired owners are
  purged first)
- `cleanup()` / `acleanup()` to force-remove expired owner entries; returns the
  number of entries removed
- `AcquireResult.used_slots` — slot occupancy observed atomically by the acquire
  call, for observability without an extra round-trip
- `SemaphoreConfig.refresh_retry_interval` — retry cadence for the heartbeat
  after a transient connection error (defaults to `min(refresh_interval, 1.0)`)
- Backend error hierarchy: `BackendError`, `TransientBackendError`,
  `PermanentBackendError`, and `CommandDeniedError`
- BLPOP wait strategy (now the default) and POLLING with exponential backoff and
  jitter, selectable via `acquire_mode`

### Changed

- Heartbeat now tolerates transient Redis connection errors and keeps retrying
  until the lock is refreshed or `lock_timeout` elapses since the last
  successful refresh; only then is the lock treated as lost. Permanent errors
  (e.g. ACL denial → `PermanentBackendError`) escalate immediately
- `RedisConnectionError` is now a subclass of `TransientBackendError`

### Removed

- `RefreshError` (no longer part of the public API)

## [0.1.0] - 2026-01-15

### Added

- Initial release
- `Semaphore` - counting semaphore with configurable limit
- `Mutex` - exclusive lock (binary semaphore)
- Sync and async API support via same classes
- Redis Sentinel support for high availability
- Automatic heartbeat to maintain lock TTL
- Fencing tokens for distributed consistency
- `on_lock_lost` callback for lock loss detection
- Prometheus metrics (optional)
- Custom logger support (loguru, structlog compatible)

### Technical

- Atomic operations via Lua scripts
- `py.typed` marker for PEP 561 compliance
- Python 3.10+ support
