# Migration Guide

This guide explains the structural changes and how to migrate from the old structure.

## What Changed?

The example was reorganized from a flat structure to **Clean Architecture** layers.

### Before (Flat Structure)
```
llm_proxy/
├── config.py
├── http_utils.py
├── inflight.py
├── logging_setup.py
├── main.py              # 600+ lines
├── metrics.py
├── reservations.py
├── responses.py
└── semaphore_pool.py
```

### After (Layered Structure)
```
llm_proxy/
├── core/                # Business logic
│   ├── inflight.py
│   ├── reservations.py
│   └── semaphore_pool.py
├── api/                 # HTTP layer
│   ├── routes/
│   │   ├── chat.py
│   │   ├── health.py
│   │   └── proxy.py
│   └── dependencies.py
├── infrastructure/      # External services
│   ├── redis_manager.py
│   └── upstream.py
├── config.py
├── logging_setup.py
├── main.py             # 100 lines
├── metrics.py
└── responses.py
```

## Breaking Changes

### Import Paths

If you imported from this example, update your imports:

```python
# OLD
from llm_proxy.semaphore_pool import SemaphorePool
from llm_proxy.inflight import InflightTracker
from llm_proxy.reservations import ReservationManager
from llm_proxy.http_utils import build_upstream_headers

# NEW
from llm_proxy.core import SemaphorePool, InflightTracker, ReservationManager
from llm_proxy.infrastructure import build_upstream_headers
```

### Uvicorn Command

**No change required** - both still work:
```bash
# Option 1 (recommended)
uvicorn llm_proxy.main:app

# Option 2 (compatibility)
uvicorn app:app
```

### Docker Build

**No change required** - Dockerfile unchanged

### Environment Variables

**No changes** - all settings remain the same

## What Stayed the Same?

- ✅ All functionality identical
- ✅ API endpoints unchanged
- ✅ Configuration format unchanged
- ✅ Metrics unchanged
- ✅ Docker support unchanged
- ✅ Dependencies unchanged

## Benefits of New Structure

### 1. Smaller Files
- `main.py`: 600 lines → 100 lines
- Logic split into focused modules

### 2. Clear Responsibilities
- **core/** - Pure business logic, no FastAPI
- **api/** - HTTP handling only
- **infrastructure/** - External service integration

### 3. Easier Testing
```python
# Before: Need to mock FastAPI app
def test_old():
    app = FastAPI()
    # Complex setup...

# After: Test pure functions
def test_new():
    tracker = InflightTracker()
    assert tracker.get_count("key") == 0
```

### 4. Better Reusability
```python
# Core logic can be reused in other frameworks
from llm_proxy.core import SemaphorePool

# Works with Flask, Litestar, etc.
pool = SemaphorePool(redis_client, settings)
```

## How to Extend

### Add New Endpoint

**Before**: Add 100+ lines to `main.py`

**After**: Create new router
```python
# llm_proxy/api/routes/my_feature.py
from fastapi import APIRouter

router = APIRouter()

@router.get("/my-endpoint")
async def my_handler():
    return {"status": "ok"}

# llm_proxy/main.py
from llm_proxy.api.routes import my_feature
app.include_router(my_feature.router)
```

### Modify Semaphore Logic

**Before**: Find code in 600-line file

**After**: Go to `core/semaphore_pool.py` - single responsibility

### Change Redis Reconnection

**Before**: Scattered across `main.py`

**After**: All in `infrastructure/redis_manager.py`

## Migrating Your Fork

If you forked the old version:

1. **Update imports** in your custom code (see above)
2. **Move custom logic** to appropriate layer:
   - Business rules → `core/`
   - HTTP handlers → `api/routes/`
   - External services → `infrastructure/`
3. **Test** that everything still works

## Questions?

See also:
- [ARCHITECTURE.md](ARCHITECTURE.md) - Design decisions
- [STRUCTURE.md](STRUCTURE.md) - File organization
- [README.md](README.md) - Usage guide
