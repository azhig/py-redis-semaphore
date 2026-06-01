```bash
 ╔════╗
 ║ 🔴 ║   ╔═════╗ ╔══════╗╔═════╗ ╔═╗╔══════╗
 ║    ║   ║ ╔══╗╚╗║ ╔════╝║ ╔═╗ ╚╗║ ║║ ╔════╝
 ║ 🟡 ║   ║ ╚══╝╔╝║ ╚══╗  ║ ║ ╚╗ ║║ ║║ ╚════╗
 ║    ║   ║ ╔═╗ ╚╗║ ╔══╝  ║ ║ ╔╝ ║║ ║╚════╗ ║
 ║ 🟢 ║   ║ ║ ╚╗ ║║ ╚════╗║ ╚═╝ ╔╝║ ║╔════╝ ║
 ║    ║   ╚═╝  ╚═╝╚══════╝╚═════╝ ╚═╝╚══════╝
 ╚════╝
   ||     ███████ ███████ ███    ███  █████  ██████  ██   ██  ██████  ██████  ███████
  ▄▄▄▄    ██      ██      ████  ████ ██   ██ ██   ██ ██   ██ ██    ██ ██   ██ ██
  ████    ███████ █████   ██ ████ ██ ███████ ██████  ███████ ██    ██ ██████  █████
               ██ ██      ██  ██  ██ ██   ██ ██      ██   ██ ██    ██ ██   ██ ██
          ███████ ███████ ██      ██ ██   ██ ██      ██   ██  ██████  ██   ██ ███████

          🔒 Distributed Synchronization Primitives on Redis 🔒
             Counting Semaphores • Mutexes • Fencing Tokens
                   Sync/Async • Sentinel • Heartbeat

```

[![PyPI version](https://img.shields.io/pypi/v/py-redis-semaphore.svg)](https://pypi.org/project/py-redis-semaphore/)
[![Python versions](https://img.shields.io/pypi/pyversions/py-redis-semaphore.svg)](https://pypi.org/project/py-redis-semaphore/)
[![License](https://img.shields.io/pypi/l/py-redis-semaphore.svg)](https://pypi.org/project/py-redis-semaphore/)
[![CI](https://github.com/azhig/py-redis-semaphore/actions/workflows/ci.yml/badge.svg)](https://github.com/azhig/py-redis-semaphore/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/azhig/py-redis-semaphore/branch/main/graph/badge.svg)](https://codecov.io/gh/azhig/py-redis-semaphore)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

Distributed semaphore and mutex on Redis with Sentinel support, a sync/async API, and an automatic heartbeat.

## Features

- **Counting Semaphore** — limit concurrent access to a resource to at most N
- **Mutex** — exclusive lock (binary semaphore)
- **Redis Sentinel** — failover support for high availability
- **Sync and Async APIs** — works with both threading and asyncio
- **Heartbeat** — automatically extends the lock TTL
- **Fencing Tokens** — protection against race conditions during GC pauses
- **Atomic operations** — all critical operations run via Lua scripts
- **Flexible wait strategies** — polling with exponential backoff, or BLPOP for efficient blocking waits
- **Docker for Redis** — ready-to-use container for local development

## Installation

```bash
pip install py-redis-semaphore
```

Or with uv:

```bash
uv add py-redis-semaphore
```

## Quick start

### Mutex (exclusive lock)

An exclusive lock. Use it when only one process/instance may access the resource
at any point in time (e.g. database migrations, a cron job, cache recomputation).

```python
import redis
from redis_semaphore import Mutex

client = redis.Redis()

with Mutex(client, "my-resource") as lock:
    print(f"Fencing token: {lock.fencing_token}")
    # Only one process can run this code
```

### Counting Semaphore

Limits the number of concurrent owners of a resource to N. Use it when the
resource tolerates bounded concurrency (a DB pool, an external API limit,
CPU/IO task throttling).

```python
from redis_semaphore import Semaphore, SemaphoreConfig

config = SemaphoreConfig(
    name="database-pool",
    limit=5,  # Up to 5 concurrent connections
    lock_timeout=30.0,
)

with Semaphore(client, config) as sem:
    # Work with the database
    pass
```

### Async API

Async variants for asyncio. They behave like their sync counterparts but do not
block the event loop, and use `async with`, `aacquire`, and `arelease`.

```python
import asyncio
import redis.asyncio as aioredis
from redis_semaphore import Mutex, Semaphore, SemaphoreConfig

async def main():
    client = aioredis.Redis()

    async with Mutex(client, "async-lock") as lock:
        print(f"Mutex token: {lock.fencing_token}")

    sem_cfg = SemaphoreConfig(name="async-semaphore", limit=3)
    async with Semaphore(client, sem_cfg) as sem:
        print(f"Semaphore token: {sem.fencing_token}")

    await client.aclose()

asyncio.run(main())
```

### Redis Sentinel

Use Sentinel when Redis is deployed in a high-availability setup and you need
automatic failover when the master goes down.

Example of connecting through Sentinel (Mutex and Semaphore):

```python
from redis_semaphore import SentinelConfig, RedisConnectionFactory, Mutex, Semaphore, SemaphoreConfig

config = SentinelConfig(
    sentinels=[
        ("sentinel1.example.com", 26379),
        ("sentinel2.example.com", 26379),
        ("sentinel3.example.com", 26379),
    ],
    service_name="mymaster",
    password="secret",
)

client = RedisConnectionFactory.create_sync(config)

with Mutex(client, "ha-lock") as lock:
    # Automatic failover when the master goes down
    pass

sem_cfg = SemaphoreConfig(name="ha-semaphore", limit=5)
with Semaphore(client, sem_cfg) as sem:
    pass
```


## Reliability model and its limits

It is important to understand which guarantees the library provides and which it does not.

**What is guaranteed.** All operations (acquire/release/refresh/cleanup) are atomic
on the Redis side via Lua scripts. As long as the Redis node (master) is reachable
and consistent, mutual exclusion holds: there can never be more than `limit` owners
at once.

**What is NOT guaranteed — failover.** This is **not Redlock** and not a
multi-master algorithm. A lock lives on a single master (directly or via Sentinel).
Replication in Redis is asynchronous, so when the master fails and a replica is
promoted, the owner record may not have been replicated yet — and a second client
can acquire the "same" slot on the new master. In other words, **during a
failover or network split, mutual exclusion may be briefly violated.**

**How to live with this — fencing tokens.** Every successful acquire returns a
monotonically increasing `fencing_token`. Pass it to the protected resource (a DB,
a file store, an external service) and reject operations carrying a token lower
than the last one seen. This way safety is enforced on the resource side rather
than by trusting the lock itself — it is the only correct way to protect against
double ownership during failover and against GC pauses.

**Clocks.** TTLs are computed using client-side time. NTP synchronization between
clients is required; a clock skew of a few seconds breaks lock expiration.

**Timeliness of loss detection.** Keep the client `socket_timeout` smaller than
`lock_timeout`, otherwise a single stalled refresh can delay lock-loss escalation
past the deadline.

Bottom line: for task coordination, rate limiting, and pools where the resource
verifies a fencing token, the library is production-ready. For scenarios that
require strict mutual exclusion with zero tolerance for even a single violation
during failover, you need a different mechanism (e.g. a consensus store).

## What to choose and when

- **Mutex** — when you need exactly one owner: "no one but me" (migrations,
  report generation, background jobs that must not run in parallel).
- **Semaphore** — when bounded concurrency is acceptable: "no more than N"
  (connection pools, rate limiting external services, load throttling).

## Configuration and usage

### SemaphoreConfig

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | str | required | Logical resource name. Used in Redis keys, e.g. `semaphore:{name}:owners`. |
| `limit` | int | required | Maximum number of concurrent owners. Once there are `limit` owners, a new `acquire` returns `busy`. |
| `lock_timeout` | float | 30.0 | Slot TTL in seconds. Stored as the `score` in the owners ZSET and extended by the heartbeat. |
| `acquire_timeout` | float | None | Maximum wait time when `blocking=True`. `None` means wait forever. |
| `retry_interval` | float | 0.1 | Pause between repeated `acquire` attempts while waiting. |
| `refresh_interval` | float | None | Heartbeat interval. If not set, 80% of `lock_timeout` is used. |
| `namespace` | str | "semaphore" | Redis key prefix for isolating environments/services. |
| `strict_mode` | bool | False | If `True`, raises `LockLostError` immediately when the slot is lost. |
| `use_server_time` | bool | False | If `True`, time is taken from Redis (`TIME`). Useful when clocks drift between machines: TTLs and expired-slot cleanup stay consistent. The downside is one extra network RTT per time operation. Enable `use_server_time` if you run several instances on different hosts with no guaranteed clock sync, or if you observe premature timeouts or "stuck" slots caused by clock skew. |
| `acquire_mode` | AcquireMode | BLPOP | Wait strategy: `POLLING` (retry loop) or `BLPOP` (efficient blocking wait via Redis). |
| `retry_interval_max` | float | None | Maximum interval for exponential backoff. `None` means no backoff. |
| `retry_backoff_multiplier` | float | 2.0 | Multiplier for exponential backoff. |
| `retry_jitter` | float | 0.0 | Random jitter as a fraction of the interval (0.0–1.0). Helps avoid a thundering herd. |
| `blpop_timeout` | float | 5.0 | BLPOP timeout before a fallback retry (only for `BLPOP` mode). |



### Example configuration and how it works

```python
from redis_semaphore import Semaphore, SemaphoreConfig

config = SemaphoreConfig(
    name="payments",
    limit=5, # at most 5 concurrent owners
    lock_timeout=30.0, # a slot lives for 30s without a refresh
    acquire_timeout=None, # wait forever
    retry_interval=0.5, # check for a free slot every 0.5s
    refresh_interval=24.0, # heartbeat extends the TTL ahead of time
    strict_mode=False, # don't crash when the slot is lost
)

# acquire will wait for a slot, checking every 0.5s.
# Once a slot is acquired, the heartbeat keeps the TTL alive
# so the record does not expire in Redis.
with Semaphore(client, config) as sem:
    do_work()
```

### Manual management (without a context manager)

```python
from redis_semaphore import Semaphore, SemaphoreConfig

config = SemaphoreConfig(name="jobs", limit=3)
sem = Semaphore(client, config)

result = sem.acquire(blocking=True)
if result.success:
    try:
        sem.refresh()  # manually extend the TTL if needed
        do_work()
    finally:
        sem.release()
else:
    print("Resource busy")
```

- `acquire()` returns the result of the acquisition attempt.
- `refresh()` extends the slot TTL and returns `True`/`False`.
- `release()` frees the slot; it is important to call it in `finally`
  so you don't leave a slot held if the worker code raises.

### How acquire() works

- `blocking=True` enables waiting for a slot, polling at `retry_interval`.
- `acquire_timeout=None` means wait forever (only when `blocking=True`).
- `blocking=False` makes a single attempt and returns `success=False` immediately.
- `with Semaphore(...)` and `with Mutex(...)` always use `blocking=True`.

#### Example: waiting for a slot (blocking)

```python
from redis_semaphore import Semaphore, SemaphoreConfig

config = SemaphoreConfig(
    name="jobs",
    limit=2,
    retry_interval=0.5,
    acquire_timeout=5.0,
)
sem = Semaphore(client, config)

result = sem.acquire(blocking=True)
if result.success:
    try:
        do_work()
    finally:
        sem.release()
else:
    # we only reach here if blocking=False
    print("Resource busy")
```

The client will wait for a slot for up to 5 seconds, checking every 0.5 seconds.
If a slot is acquired, `do_work()` runs and the slot is released in `finally`.
Had we used `blocking=False`, the code would have gone straight to the `else` branch.

#### Example: a single quick attempt (non-blocking)

```python
result = mutex.acquire(blocking=False)
if result.success:
    try:
        do_work()
    finally:
        mutex.release()
else:
    print("Resource busy")
```

`acquire` makes a single attempt and returns immediately.
If the slot is taken, the code does not wait and goes straight to the `else` branch.


### Wait strategies (AcquireMode)

When `blocking=True`, the semaphore waits for a slot to free up. There are two strategies:

#### BLPOP (default)

Uses Redis `BLPOP` to block until notified. On `release()`, a signal is published
that wakes a waiting client.

```python
from redis_semaphore import Semaphore, SemaphoreConfig

# BLPOP is used by default
config = SemaphoreConfig(
    name="jobs",
    limit=5,
    blpop_timeout=5.0,  # fallback polling every 5 seconds
)
```

**Advantages of BLPOP:**
- Minimal load on Redis (no constant polling)
- Instant wake-up when a slot is freed
- The wait queue is not stored explicitly — BLPOP serves only as a wake-up signal

#### POLLING

A simple retry loop with a `retry_interval` pause between attempts.

```python
from redis_semaphore import Semaphore, SemaphoreConfig, AcquireMode

config = SemaphoreConfig(
    name="jobs",
    limit=5,
    acquire_mode=AcquireMode.POLLING,
    retry_interval=0.1,  # check every 100ms
)
```

**With exponential backoff and jitter:**

```python
config = SemaphoreConfig(
    name="jobs",
    limit=5,
    acquire_mode=AcquireMode.POLLING,
    retry_interval=0.1,         # initial interval
    retry_interval_max=2.0,     # maximum interval
    retry_backoff_multiplier=2.0,  # double every cycle
    retry_jitter=0.1,           # ±10% random jitter
)
```

Backoff is useful for reducing load on Redis during long waits.
Jitter helps avoid a thundering herd when many clients wait at the same time.

**When to choose POLLING:**
- Simple cases with short waits
- Compatibility with older Redis versions


### Handling lock loss

```python
def on_lock_lost(identifier: str):
    print(f"Lock {identifier} was lost!")
    # Graceful shutdown

# Mutex example
mutex = Mutex(client, "critical", on_lock_lost=on_lock_lost)

# semaphore example
sem_cfg = SemaphoreConfig(name="critical-pool", limit=2)
semaphore = Semaphore(client, sem_cfg, on_lock_lost=on_lock_lost)
```

What happens in this example:
- When the slot is lost (e.g. the TTL expired), `on_lock_lost` is invoked.
- In the callback you can trigger a graceful shutdown.

### Async methods

Async methods use the `a` prefix:

- `aacquire` / `arelease` / `arefresh`
- `__aenter__` / `__aexit__` (via `async with`)

Example:

```python
import redis.asyncio as aioredis
from redis_semaphore import Semaphore, SemaphoreConfig

async def main():
    client = aioredis.Redis()
    config = SemaphoreConfig(name="async", limit=2)

    async with Semaphore(client, config) as sem:
        print(sem.fencing_token)

    await client.aclose()
```

You can also use the explicit async methods without `async with`:

```python
import redis.asyncio as aioredis
from redis_semaphore import Mutex

async def main():
    client = aioredis.Redis()
    mutex = Mutex(client, "explicit-async")

    result = await mutex.aacquire(blocking=False)
    if result.success:
        try:
            print(mutex.fencing_token)
        finally:
            await mutex.arelease()

    await client.aclose()
```

## Logging

By default the standard `logging` module is used with the logger name `redis_semaphore`.

You can swap in your own logger (e.g. loguru or structlog):

```python
from redis_semaphore import set_logger

# loguru
from loguru import logger as loguru_logger
set_logger(loguru_logger)

# structlog
import structlog
set_logger(structlog.get_logger("redis_semaphore"))
```

## Metrics

Prometheus metrics are optional. To install:

```bash
pip install "py-redis-semaphore[prometheus]"
```

Example usage:

```python
from prometheus_client import REGISTRY
from redis_semaphore import PrometheusMetrics, set_metrics

# Register the metrics in the application's existing registry
set_metrics(PrometheusMetrics(registry=REGISTRY))
```

## Examples

See `examples/basic_usage.py` (sync/async examples) and
`examples/multiprocess_simulation.py` (a simulation of several processes).

Run them with:

```bash
python examples/basic_usage.py
python examples/multiprocess_simulation.py
```

## More about how the semaphore works

See [`ARCHITECTURE.md`](ARCHITECTURE.md) — a step-by-step walkthrough of the
algorithm and implementation details.

## Developer documentation

See [`CONTRIBUTING.md`](CONTRIBUTING.md).

## License

MIT
