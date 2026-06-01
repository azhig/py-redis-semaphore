# How the semaphore works

A step-by-step description of the algorithm, in words and examples, without diagrams.

Scenario: there is a shared Redis and several applications that need to limit
concurrent access to a resource.

---

## 1) What is stored in Redis

For a semaphore named `payments`, two keys are created (the `namespace` prefix
defaults to `semaphore`):

1) `semaphore:payments:owners` (ZSET)
   - member: the unique identifier of an owner (process/instance)
   - score: the expiration time of that owner (milliseconds)

2) `semaphore:payments:fencing` (STRING)
   - an integer that grows via INCR
   - this is the fencing token

---

## 2) What acquire does (obtain a slot)

Suppose `limit=2` and there are 3 processes: A, B, C.

### What happens in Redis

1) Expired owners are removed (ZREMRANGEBYSCORE).
2) If the current identifier is already an owner:
   - its TTL is updated
   - the fencing token is incremented
   - success is returned
3) The current number of owners is counted (ZCARD).
4) If there are fewer owners than `limit`:
   - a new owner is added to the ZSET with a fresh expires_at
   - the fencing token is incremented
   - success is returned
5) If there are already `limit` owners:
   - "busy" is returned

### Blocking vs non-blocking

- `blocking=True` (default) — a wait loop stepping at `retry_interval`
  until a slot is acquired or `acquire_timeout` elapses.
- `acquire_timeout=None` — wait forever.
- `blocking=False` — a single attempt and an immediate `busy` response, no waiting.

Note: `with Semaphore(...)` always uses `blocking=True` and will wait for a
slot; if `acquire_timeout` is set, an `AcquireTimeoutError` is raised when it
elapses.

### Example

- A calls acquire -> gets a slot (token=1)
- B calls acquire -> gets a slot (token=2)
- C calls acquire:
  - if blocking=False -> immediately "busy"
  - if blocking=True -> waits and retries

---

## 3) How waiting works (blocking)

There are two wait strategies, selected via `acquire_mode`:

### BLPOP (default)

Uses Redis `BLPOP` for a blocking wait:

```
try acquire
if busy:
    BLPOP semaphore:{name}:queue timeout
    try again
```

On `release()`, a signal is pushed to the queue via `LPUSH`, which wakes a
waiting client.

**Advantages of BLPOP:**
- Minimal load on Redis (no constant polling)
- Instant wake-up when a slot is freed
- The queue is not stored explicitly — it is a wake-up signal, not a strict FIFO

**Fallback:**

If the signal is lost (a race condition), a retry happens after `blpop_timeout`
seconds — this protects against waiting forever.

### Additional key for BLPOP

When using BLPOP, a third key is created:

3) `semaphore:payments:queue` (LIST)
   - used to notify waiters via BLPOP/LPUSH
   - this is not a real wait queue: the elements are just wake-up signals

### POLLING

Waiting is a local loop inside the process:

```
try acquire
if busy:
    sleep(retry_interval)
    try again
```

**With exponential backoff:**

If `retry_interval_max` is set, the interval grows exponentially:
```
attempt 1: sleep(0.1)
attempt 2: sleep(0.2)
attempt 3: sleep(0.4)
...
attempt N: sleep(min(calculated, retry_interval_max))
```

**With jitter:**

If `retry_jitter` is set (e.g. 0.1), a random amount of up to 10% of the current
interval is added. This helps avoid a thundering herd, when many clients wake up
at the same time.

---

**Note:** In DEBUG logs, `waiting=1` is a **local counter** within this process
only, not the global number of waiters across the whole system.

---

## 4) Release (free a slot)

When a process is done, it calls `release()`:

1) The heartbeat is stopped.
2) Its identifier is removed from owners in Redis.
3) Waiting clients are notified (LPUSH into queue).
4) Local state is reset.

The notification (step 3) always happens, regardless of `acquire_mode` —
this lets different clients use different wait strategies.

Calling `release()` without a successful `acquire()` raises an error.

---

## 5) Refresh and Heartbeat (extending the TTL)

### Refresh

`refresh()` extends the owner's TTL:
1) Redis checks that the identifier still exists and has not expired.
2) If everything is fine -> it extends the TTL.
3) If it expired or is missing -> it returns False (lock lost).

### Heartbeat

Once a slot is acquired, a background heartbeat starts and periodically calls
`refresh()` to extend the TTL.

If the heartbeat stops working (the process dies, the network drops), the TTL
expires and the slot frees itself.

---

## 6) Lock lost

Scenario:

- A process believes it holds a slot.
- But its TTL expired or the record was removed.
- refresh returns False.

What happens next:
- If strict_mode=False: a warning is logged, but the code keeps running.
- If strict_mode=True: a LockLostError is raised immediately.

This is useful for critical systems where you must not continue after losing the lock.

---

## 7) Fencing token (protection against races)

A fencing token is a number that grows on every successful acquire.
Even on a repeated acquire by the same owner, the token increases.

### Why this is needed

Imagine:
- Process A obtained token=10 but then stalled and never finished its work.
- Process B obtained token=11 and continued.

If the resource (e.g. a database) only accepts the highest token, then stale
operations carrying token=10 can be safely ignored.

---

## 8) Example of a full cycle

limit=2, processes A, B, C:

1) A acquire -> success, token=1
2) B acquire -> success, token=2
3) C acquire -> busy, waits
4) A release -> a slot frees up
5) C acquire -> success, token=3

---

## 9) Time: client-side or server-side

By default, client-side time (`time.time()`) is used.
You can enable `use_server_time=True`, and time will be taken from Redis (`TIME`).

Why this is needed:
- If clocks on different machines drift, server-side time is more reliable.
- The downside: an extra network RTT.

---

## 10) Sync vs Async

The same `Semaphore` object cannot be used in both sync and async modes
at the same time.

Example of incorrect usage:
```
sem.acquire()   # sync
await sem.aacquire()  # async -> error
```

This protects against races inside a single object.

---

## 11) Metrics and logging

### Metrics

Metrics are counted **within the process**, not globally.
If there are many processes, Prometheus should aggregate them itself.

### DEBUG logs

Logs like:
```
Semaphore 'payments' usage 2/2
Semaphore 'payments' full 2/2; waiting=1
```

- `usage 2/2` — total slots taken in Redis
- `waiting=1` — local to the current process
