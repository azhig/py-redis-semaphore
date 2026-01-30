# Architecture

This document describes the architectural decisions and structure of the FastAPI LLM Proxy example.

## Overview

The application follows **Clean Architecture** principles with three main layers:

1. **Core (Domain)** - Business logic independent of frameworks
2. **API (Presentation)** - HTTP handling with FastAPI
3. **Infrastructure** - External service integrations (Redis, upstream LLM)

## Layer Responsibilities

### Core Layer (`llm_proxy/core/`)

Contains pure business logic with no framework dependencies:

- **`semaphore_pool.py`** - Manages semaphore instances per department+model
  - Creates and caches semaphore configs
  - Provides snapshot for monitoring

- **`inflight.py`** - Tracks in-flight requests across Redis and fallback modes
  - Maintains counters for active requests
  - Handles migration between Redis and fallback

- **`reservations.py`** - Manages Redis slot reservations during fallback→Redis migration
  - Pre-acquires slots for active fallback requests
  - Ensures smooth transition without rate limit violations

### API Layer (`llm_proxy/api/`)

Handles HTTP requests and responses:

- **`routes/chat.py`** - `/v1/chat/completions` endpoint
  - Validates department and model
  - Acquires semaphore (Redis or fallback)
  - Proxies to upstream with streaming support

- **`routes/health.py`** - Health checks and monitoring
  - `/health` - Redis connectivity check
  - `/semaphore/status` - Debug endpoint for pool state

- **`routes/proxy.py`** - Catch-all for other endpoints
  - Proxies requests without rate limiting
  - Supports all HTTP methods

- **`dependencies.py`** - Shared request handling logic
  - Department parsing
  - Semaphore acquisition with fallback
  - Cleanup after request completion

### Infrastructure Layer (`llm_proxy/infrastructure/`)

Integrates with external services:

- **`redis_manager.py`** - Redis connection management
  - Health monitoring via watchdog
  - Automatic reconnection
  - Cleanup of stale semaphore entries

- **`upstream.py`** - Upstream LLM HTTP client
  - Request/response header handling
  - URL construction

## Key Design Decisions

### 1. Router-based endpoints instead of monolithic handler

**Before**: Single 600-line `main.py` with all logic inline

**After**: Separate routers for each concern
- `/v1/chat/completions` → `api/routes/chat.py`
- `/health`, `/semaphore/status` → `api/routes/health.py`
- Catch-all proxy → `api/routes/proxy.py`

**Benefits**:
- Easier to test individual endpoints
- Clear separation of concerns
- Simpler to add new endpoints

### 2. Shared dependency functions

Request handling logic (acquire, cleanup) is extracted to `api/dependencies.py`:
- `acquire_semaphore()` - Handles Redis + fallback logic
- `cleanup_semaphore()` - Release and metrics tracking
- `parse_department()` - Header validation

**Benefits**:
- Reduces duplication between streaming/non-streaming handlers
- Easier to modify acquire logic in one place
- Testable in isolation

### 3. Redis watchdog in infrastructure layer

Watchdog runs continuously to detect Redis reconnection:
- Starts only when Redis is unavailable
- Auto-stops when Redis recovers
- Triggers cleanup and migration

**Benefits**:
- Decoupled from request handling
- No polling overhead when Redis is healthy
- Centralized reconnection logic

### 4. Core layer framework-agnostic

Business logic (semaphore pool, inflight tracking) has no FastAPI dependencies:
- Can be reused in Flask, Litestar, etc.
- Easier to test without HTTP mocking
- Clear boundary between domain and presentation

## Request Flow

### Non-streaming request

```
1. Client → POST /v1/chat/completions
2. chat.py → parse department/model
3. dependencies.py → acquire_semaphore()
   ├─ Try Redis semaphore
   └─ Fallback to local if unavailable
4. chat.py → proxy to upstream
5. dependencies.py → cleanup_semaphore()
   ├─ Release semaphore
   ├─ Update metrics
   └─ Decrement counters
6. Client ← Response
```

### Streaming request

Same as non-streaming, but cleanup happens in `stream_body()` finally block after all chunks are sent.

### Redis reconnection

```
1. Redis becomes unavailable
2. Request handler → mark_redis_unavailable()
3. infrastructure/redis_manager.py → start watchdog
4. Watchdog polls every N seconds
5. Redis recovers
6. Watchdog → cleanup_expired_semaphores()
7. Watchdog → reserve slots for active fallback requests
8. Watchdog → mark Redis available
9. Watchdog → stop itself
```

## Testing Strategy

### Unit tests
- `core/` modules with mocked Redis
- `api/dependencies.py` with mocked app state
- Header parsing, validation logic

### Integration tests
- Full request flow with test Redis
- Fallback → Redis migration
- Streaming vs non-streaming

### Load tests
- Concurrent requests exceeding semaphore capacity
- Redis disconnect/reconnect scenarios
- Department isolation (multiple keys)

## Future Enhancements

1. **Service layer** - Extract use cases from route handlers
2. **Domain events** - Publish events on acquire/release for observability
3. **Repository pattern** - Abstract Redis operations behind interface
4. **Request/Response models** - Pydantic schemas for validation
5. **Middleware** - Move department parsing to middleware
