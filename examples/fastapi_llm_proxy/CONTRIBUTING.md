# Contributing Guide

Quick reference for contributing to the FastAPI LLM Proxy example.

## Project Structure Quick Reference

```
llm_proxy/
├── core/           # Pure business logic (no FastAPI/httpx)
├── api/            # FastAPI routes and dependencies
├── infrastructure/ # Redis and upstream HTTP clients
└── *.py            # Shared utilities (config, metrics, etc.)
```

## Where to Add Code

| Change Type | Location | Example |
|------------|----------|---------|
| New endpoint | `api/routes/` | Add router, register in `main.py` |
| Semaphore logic | `core/semaphore_pool.py` | Modify pool management |
| Request tracking | `core/inflight.py` | Add new counter/tracker |
| Redis watchdog | `infrastructure/redis_manager.py` | Modify health checks |
| Upstream HTTP | `infrastructure/upstream.py` | Add header handling |
| Configuration | `config.py` | Add new settings |
| Metrics | `metrics.py` | Add Prometheus metric |
| HTTP responses | `responses.py` | Add response helper |

## Dependency Rules

**NEVER import from**:
- `core/` → `api/` or `infrastructure/` ❌
- `infrastructure/` → `api/` ❌

**OK to import**:
- `api/` → `core/`, `infrastructure/`, shared utils ✅
- `infrastructure/` → `core/`, shared utils ✅
- `main.py` → everything ✅

## Code Style

### Imports
```python
# Standard library
from __future__ import annotations
import asyncio

# Third-party
import httpx
from fastapi import APIRouter

# Local (absolute imports)
from llm_proxy.core import SemaphorePool
from llm_proxy.infrastructure import redis_is_available
```

### Type Hints
```python
async def my_function(count: int) -> bool:
    return count > 0
```

### Docstrings
```python
def my_function(param: str) -> int:
    """Brief description.

    Longer explanation if needed.
    """
    return 42
```

## Testing Checklist

Before committing:

```bash
# 1. Check syntax
python -m py_compile llm_proxy/**/*.py

# 2. Format code (if ruff is configured)
ruff format .

# 3. Run linter
ruff check .

# 4. Run type checker
mypy llm_proxy/

# 5. Run tests
pytest tests/
```

## Common Tasks

### Add New Route

1. Create router in `api/routes/my_route.py`:
```python
from fastapi import APIRouter

router = APIRouter()

@router.get("/my-endpoint")
async def my_handler():
    return {"status": "ok"}
```

2. Register in `main.py`:
```python
from llm_proxy.api.routes import my_route
app.include_router(my_route.router, tags=["my_feature"])
```

### Add Rate-Limited Endpoint

1. Use `acquire_semaphore()` from `api/dependencies.py`
2. Call `cleanup_semaphore()` in finally block

Example:
```python
from llm_proxy.api.dependencies import acquire_semaphore, cleanup_semaphore

@router.post("/my-endpoint")
async def my_handler(request: Request):
    start_time = time.perf_counter()
    sem, fallback, redis_sem = await acquire_semaphore(
        request, department, model, start_time
    )

    try:
        # Your logic here
        pass
    finally:
        await cleanup_semaphore(
            request, sem, inflight_tracker, key,
            fallback, redis_sem, dept, model, status, duration
        )
```

### Add Metric

In `metrics.py`:
```python
MY_COUNTER = Counter(
    "my_metric_total",
    "Description",
    ["label1", "label2"]
)

def increment_my_metric(label1: str, label2: str):
    MY_COUNTER.labels(label1=label1, label2=label2).inc()
```

### Add Configuration

In `config.py`:
```python
class Settings(BaseSettings):
    my_setting: str = "default"
    my_int_setting: int = 42
```

In `.env`:
```bash
MY_SETTING=custom_value
MY_INT_SETTING=100
```

## Debugging Tips

### Enable Debug Logging
```bash
LOG_LEVEL=DEBUG uvicorn llm_proxy.main:app
```

### Check Semaphore State
```bash
curl http://localhost:8000/semaphore/status | jq
```

### Monitor Metrics
```bash
curl http://localhost:8000/metrics | grep llm_
```

### Check Redis Connection
```bash
redis-cli -h localhost -p 6379 ping
```

### View Semaphore Keys in Redis
```bash
redis-cli keys "llm:*"
```

## Performance Considerations

- **Streaming**: Always handle cleanup in `finally` block within generator
- **Metrics**: Use labels sparingly (high cardinality = memory issues)
- **Redis**: Connection pool is shared, don't create new clients
- **HTTP**: Reuse `httpx.AsyncClient` from `app.state.http`

## Error Handling Patterns

### API Layer
```python
try:
    result = await upstream_call()
except httpx.HTTPError:
    return upstream_error("Request failed")
except AcquireTimeoutError:
    return rate_limit_response()
```

### Core Layer
```python
# Let exceptions propagate - API layer handles them
async def core_function():
    return await some_operation()
```

### Infrastructure Layer
```python
try:
    await redis.ping()
except Exception:
    # Log and mark unavailable
    logger.warning("Redis unavailable")
    await mark_redis_unavailable(app)
```

## Documentation

When adding features, update:
- [ ] `README.md` - User-facing usage
- [ ] `ARCHITECTURE.md` - Design decisions
- [ ] `STRUCTURE.md` - File responsibilities
- [ ] Code docstrings
- [ ] Example `.env.example` if adding config

## Questions?

- Architecture questions → see [ARCHITECTURE.md](ARCHITECTURE.md)
- File organization → see [STRUCTURE.md](STRUCTURE.md)
- Migration from old structure → see [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md)
