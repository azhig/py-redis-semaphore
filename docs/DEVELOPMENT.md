# Developer Notes

This file contains development and maintenance information for the project.

## Local Redis via Docker

```bash
make redis-up
```

If port 6379 is busy:

```bash
make redis-up REDIS_PORT=6380
```

Stop container:

```bash
make redis-down
```

Redis CLI:

```bash
make redis-shell
```

Default address is localhost:6379.

## Testing

Start Redis and run tests:

```bash
make redis-up
pytest
```

If Redis is on another port:

```bash
REDIS_PORT=6380 pytest
```

Sentinel tests require env vars:

- REDIS_SENTINEL_HOSTS (example: host1:26379,host2:26379)
- REDIS_SENTINEL_SERVICE (optional, default: mymaster)
- REDIS_SENTINEL_PASSWORD (optional)

Run full test suite against Sentinel and a specific Redis port:

```bash
REDIS_PORT=6381 \
REDIS_SENTINEL_HOSTS=127.0.0.1:26379 \
REDIS_SENTINEL_SERVICE=mymaster \
uv run pytest
```

Run only Sentinel tests:

```bash
REDIS_SENTINEL_HOSTS=127.0.0.1:26379 \
REDIS_SENTINEL_SERVICE=mymaster \
uv run pytest tests/test_sentinel.py
```

Sentinel local setup with docker compose:

```bash
make sentinel-up
```

Stop it:

```bash
make sentinel-down
```

## Architecture

Redis data structures:

- Sorted Set stores owners with expiration timestamps.
- String stores the monotonic fencing token counter.

Lua scripts:

- acquire: acquire with cleanup of expired owners
- release: release ownership
- refresh: extend TTL
- cleanup: force cleanup
- status: read current state

## Guarantees and limitations

- Atomicity is ensured by Lua scripts.
- Lock loss is tracked via heartbeat and on_lock_lost callbacks.
- If a process dies, a slot is released after lock_timeout.

## Requirements

- Python >= 3.13
- Redis >= 6.0
- redis-py >= 5.0.0
