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
- **Rate Limiter** — sliding-window limits with weighted costs (e.g. RPM + TPM)
- **Shared dispatch queue** — FIFO job queues shared by many pods, one leader per queue, failover
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

### Rate limiter (RPM / TPM)

A semaphore bounds *concurrency*; `RateLimiter` bounds *throughput*: "at most
N units in any sliding window of W seconds". Several limits are checked
atomically, and a request is admitted only if it fits all of them — e.g. an
LLM API with both requests-per-minute and tokens-per-minute quotas.

```python
from redis_semaphore import RateLimit, RateLimiter, RateLimiterConfig

limiter = RateLimiter(client, RateLimiterConfig(
    name="cert-1",
    limits=[
        RateLimit("requests", limit=100, window=60.0),  # RPM
        RateLimit("tokens", limit=50_000, window=60.0),  # TPM
    ],
    acquire_timeout=30.0,
))

# Reserve the estimated cost; blocks until it fits all limits
permit = limiter.acquire({"requests": 1, "tokens": 1_500})
response = call_api()
# Settle the real cost (keeps the admission time; may overshoot the limit)
limiter.adjust(permit, {"tokens": response.total_tokens})
# ...or refund a request that was never sent: limiter.adjust(permit, 0)

# Non-blocking: Redis returns the exact time until the request fits
permit = limiter.acquire({"tokens": 1_500}, blocking=False)
if not permit.success:
    print(f"retry in {permit.retry_after:.2f}s")

print(limiter.status().usage["tokens"].available)
```

How it works:

- `cost` is an int charged to every limit, or a mapping `{limit_name: units}`;
  limits missing from the mapping are charged 1 (so `{"tokens": n}` also
  counts one request). A cost larger than its limit raises `ValueError`.
- Each limit is a sliding-window log in Redis (`{namespace}:{name}:{limit}:log`
  plus a running `:total`); acquire/adjust/status are single Lua scripts using
  the Redis server clock, so all clients share one view and no NTP is needed.
- A blocking `acquire()` sleeps exactly until the window frees up (plus
  `wait_jitter`). With `acquire_timeout` it fails fast: if Redis says the
  request cannot fit before the deadline, `RateLimitTimeoutError` (with
  `retry_after`) is raised immediately.
- There is nothing to release — units leave the window on their own; keys
  carry a TTL of two windows.
- Admission is not FIFO: a small request may overtake a waiting large one.
  Keep per-caller ordering in your own queue.

The same instance supports `aacquire()` / `aadjust()` / `astatus()` with an
async client. See `examples/rate_limited_agents.py` for agents with
per-agent queues spreading load over several certificates.

### Shared dispatch queue (many pods, strict FIFO)

`RateLimiter` decides *whether* a request may go now; it does not queue
requests. When many processes or pods share a quota and requests must get it
in arrival order, put them into a shared queue: `DispatchQueue` (producers)
and `DispatchWorker` (consumers) keep one FIFO queue per name in Redis.
Requires Redis >= 6.2 (the rest of the library only needs >= 3.2).

```python
from redis_semaphore import DispatchConfig, DispatchQueue, DispatchWorker, Job

config = DispatchConfig(namespace="llm", max_queue_size=10_000)

# Worker side - run in every pod, as many workers as you like
async def handler(job: Job) -> dict:
    permit = await limiter.aacquire({"tokens": job.payload["tokens"]})  # FIFO across pods
    job.admit()                                 # the next job of the queue may start
    response = await http.post("/v1/chat", json=job.payload)  # concurrent with it
    await limiter.aadjust(permit, {"tokens": response.json()["usage"]["total_tokens"]})
    return response.json()

worker = DispatchWorker(async_client, ["cert-1", "cert-2"], handler, config)
await worker.run()          # until worker.stop() / cancellation

# Producer side - from any pod
queue = DispatchQueue(async_client, config)
result = await queue.asubmit("cert-1", {"tokens": 1500, ...}, timeout=60)
```

How it works:

- Every queue is a Redis Stream. Exactly one worker *leads* a queue at a time
  (it holds the queue's `Mutex`); workers of all pods compete for queues, so
  queues spread over them (`max_queues_per_worker` caps it).
- The leader runs the handler for one job at a time, in stream order, until
  the handler calls `job.admit()` (or returns). Everything before `admit()` is
  strictly serialized per queue across all pods - wait for the scarce resource
  there; everything after it runs concurrently with the following jobs.
- The submitter gets the handler's return value (JSON), or `JobFailedError`
  (`error_type`, `data` - raise `JobError(msg, data=...)` for structured
  details), or `JobTimeoutError`. A job still queued past its `timeout` is
  dropped instead of run; `job.deadline` / `job.remaining` let the handler
  give up early. `QueueFullError` above `max_queue_size`.
- `raise RequeueJob(delay=0)` from the handler puts the job back into its
  queue instead of failing it (e.g. after an upstream 429): the submitter
  keeps waiting, and the leader runs the job again before any job not
  started yet. Wait for the resource again *before* `admit()` on the re-run
  (`job.reclaimed` is True), so the job holds the head of the queue while it
  waits. The job's `timeout` still bounds the whole thing.
- Pod crash: its Mutexes expire after `leader_ttl` and other workers take
  over. Jobs it had taken stay pending; live workers touch their jobs, so
  after `reclaim_idle` without that the new leader re-runs them
  (`job.reclaimed=True`). **Delivery is at-least-once** - use `job.id` as an
  idempotency key if a repeat is harmful.
- Graceful shutdown (`worker.stop()`, e.g. on SIGTERM): jobs not admitted yet
  are handed back to the queue at once, admitted ones run to completion.
- Deadlines use the Redis server clock (stamped inside the enqueue script),
  like everything else in this library - no NTP sync between pods needed.
  Each waiting `asubmit()` holds one Redis connection (BLMOVE) - size the
  pool for the expected number of concurrent submissions.

#### Health checks

Two views, for different questions:

```python
# Global state of a queue - the same from every pod (one Lua call + a leader check)
status = await queue.astatus("cert-1")
# QueueStatus(name='cert-1', length=12, in_progress=3, waiting=9,
#             oldest_age=4.2, has_leader=True)
statuses = await queue.astatus_many(["cert-1", "cert-2"])   # dict by name

# Local health of a worker in this pod - no Redis round-trip
health = worker.health()
# WorkerHealth(worker_id=..., running=True, stopping=False, leading=('cert-1',),
#              jobs_in_progress=1, last_loop_age=0.4, last_error=None,
#              last_error_age=None, healthy=True)
```

- Alarm on `has_leader=False` with a non-empty queue (nobody serves it) or a
  steadily growing `oldest_age` (the queue is stuck or starved).
- `health.healthy` = running, not stopping, main loop not stalled and no Redis
  error within `5 x election_interval`: suits a Kubernetes **readiness**
  probe. For a **liveness** probe check only `running` and `last_loop_age` -
  restarting pods does not fix a Redis outage.
- Both are dataclasses: `dataclasses.asdict()` gives JSON-ready dicts.

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

**Clocks.** By default (`time_source=TimeSource.SERVER`) all timestamps are read
on the Redis server *inside* the Lua scripts, so clients do **not** need
synchronized clocks and no extra round-trip is paid. Only with
`time_source=TimeSource.CLIENT` (needed for Redis servers older than 3.2) are
TTLs computed from client-side time — then NTP synchronization between clients
is required, and a clock skew of a few seconds breaks lock expiration.

**Timeliness of loss detection.** Keep the client `socket_timeout` smaller than
`lock_timeout`, otherwise a single stalled refresh can delay lock-loss escalation
past the deadline. (BLPOP waits are automatically capped below `socket_timeout`,
so a blocking `acquire` never trips over the socket read timeout.)

Bottom line: for task coordination, rate limiting, and pools where the resource
verifies a fencing token, the library is production-ready. For scenarios that
require strict mutual exclusion with zero tolerance for even a single violation
during failover, you need a different mechanism (e.g. a consensus store).

## What to choose and when

- **Mutex** — when you need exactly one owner: "no one but me" (migrations,
  report generation, background jobs that must not run in parallel).
- **Semaphore** — when bounded concurrency is acceptable: "no more than N"
  (connection pools, rate limiting external services, load throttling).
- **RateLimiter** — when the quota is per time window rather than per
  concurrent holder: "no more than N per minute" (API requests/tokens per
  minute). Combine with a Semaphore if both kinds of limit apply.

## Configuration and usage

### Names and keys

Names of semaphores, mutexes, rate limiters and dispatch queues are opaque
strings: any non-empty string works - one word, `agent:division:model`,
`a:b:c:d`, spaces, unicode. The library never splits them; it only adds the
namespace in front and a fixed suffix behind (`<namespace>:<name>:owners`,
`<namespace>:<queue>:stream`, ...), so different names never share keys.

The one exception is the name of a `RateLimit` inside a limiter
(`"requests"`, `"tokens"`): it must not contain `:`, because it is appended
after the free-form limiter name (`<namespace>:<limiter>:<limit>:log`).

Use different namespaces for different kinds of primitives (the defaults -
`semaphore`, `mutex`, `ratelimit`, `dispatch` - already differ): a `Mutex` is
a `Semaphore` with `limit=1`, so the same name in the same namespace is the
same lock.

### SemaphoreConfig

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | str | required | Logical resource name. Used in Redis keys, e.g. `semaphore:{name}:owners`. |
| `limit` | int | required | Maximum number of concurrent owners. Once there are `limit` owners, a new `acquire` returns `busy`. |
| `lock_timeout` | float | 30.0 | Slot TTL in seconds. Stored as the `score` in the owners ZSET and extended by the heartbeat. |
| `acquire_timeout` | float | None | Maximum wait time when `blocking=True`. `None` means wait forever. |
| `retry_interval` | float | 0.1 | Pause between repeated `acquire` attempts while waiting. |
| `refresh_interval` | float | None | Heartbeat interval. If not set, `lock_timeout / 3` is used, leaving ~2/3 of the TTL as retry budget when Redis is briefly unreachable. |
| `namespace` | str | "semaphore" | Redis key prefix for isolating environments/services. |
| `strict_mode` | bool | False | If `True`, raises `LockLostError` immediately when the slot is lost. |
| `time_source` | TimeSource | SERVER | Where "now" comes from. `SERVER` (default): read via `TIME` inside the Lua scripts — clients share the Redis clock, no NTP sync needed, no extra RTT. `CLIENT`: client-side timestamps (requires NTP-synchronized clients); only needed for Redis servers older than 3.2. |
| `use_server_time` | bool | False | **Deprecated** alias for `time_source=TimeSource.SERVER` (which is now the default). Emits a `DeprecationWarning`. |
| `acquire_mode` | AcquireMode | BLPOP | Wait strategy: `POLLING` (retry loop) or `BLPOP` (efficient blocking wait via Redis). |
| `retry_interval_max` | float | None | Maximum interval for exponential backoff. `None` means no backoff. |
| `retry_backoff_multiplier` | float | 2.0 | Multiplier for exponential backoff; the exponent is the number of completed wait rounds. |
| `retry_jitter` | float | 0.1 | Random jitter as a fraction of the interval (0.0–1.0). Helps avoid a thundering herd. |
| `blpop_timeout` | float | 5.0 | BLPOP timeout before a fallback retry (only for `BLPOP` mode). At runtime each wait is additionally capped below the client's `socket_timeout` to avoid spurious read timeouts. |
| `refresh_retry_interval` | float | None | Heartbeat retry cadence after a transient connection error. If not set, `min(refresh_interval, 1.0)`. |
| `cluster_hash_tags` | bool | False | Format keys as `{namespace:name}:owners` etc. so all keys of one semaphore map to a single Redis Cluster slot. **Changes key names** — do not flip on an existing deployment without draining it first. |
| `reacquire_policy` | ReacquirePolicy | RENEW | What `acquire()` does when this instance already holds a slot. `RENEW`: refresh the TTL and issue a **new** fencing token (the previously returned token becomes stale; there is no hold count — one `release()` frees the slot). `FORBID`: raise `AlreadyAcquiredError` instead. |



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

Ordering is **approximately FIFO**: blocked clients are woken in BLPOP order,
but a freshly arriving client that calls `acquire()` at the right moment can
still win the race for the freed slot against a woken waiter.

The wait phase tolerates transient backend errors (a socket read timeout, a
connection blip): they are treated as a spurious wake-up and the wait resumes.
A real outage still surfaces as a `BackendError` from the next acquire attempt.

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

See `examples/basic_usage.py` (sync/async examples),
`examples/multiprocess_simulation.py` (a simulation of several processes) and
`examples/rate_limited_agents.py` (agents sharing RPM/TPM quotas per certificate).

Run them with:

```bash
python examples/basic_usage.py
python examples/multiprocess_simulation.py
python examples/rate_limited_agents.py
```

## More about how the semaphore works

See [`ARCHITECTURE.md`](ARCHITECTURE.md) — a step-by-step walkthrough of the
algorithm and implementation details.

## Developer documentation

See [`CONTRIBUTING.md`](CONTRIBUTING.md).

## License

MIT
