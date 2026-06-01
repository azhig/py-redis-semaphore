# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
