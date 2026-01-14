# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2025-XX-XX

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
