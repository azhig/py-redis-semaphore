# Project Structure

This document describes the file organization and responsibilities.

## Directory Layout

```
examples/fastapi_llm_proxy/
├── llm_proxy/                    # Main application package
│   ├── core/                     # 📦 Business Logic (Domain Layer)
│   │   ├── __init__.py
│   │   ├── inflight.py          # In-flight request tracking
│   │   ├── reservations.py      # Redis slot reservation manager
│   │   └── semaphore_pool.py    # Semaphore pool management
│   │
│   ├── api/                      # 🌐 HTTP Layer (Presentation)
│   │   ├── __init__.py
│   │   ├── dependencies.py      # Shared request handling logic
│   │   └── routes/
│   │       ├── __init__.py
│   │       ├── chat.py          # /v1/chat/completions
│   │       ├── health.py        # /health, /semaphore/status
│   │       └── proxy.py         # Catch-all proxy
│   │
│   ├── infrastructure/           # 🔌 External Services
│   │   ├── __init__.py
│   │   ├── redis_manager.py     # Redis connection + watchdog
│   │   └── upstream.py          # Upstream LLM HTTP client
│   │
│   ├── __init__.py
│   ├── config.py                # ⚙️  Configuration (pydantic-settings)
│   ├── logging_setup.py         # 📝 Logging configuration
│   ├── main.py                  # 🚀 Application entry point
│   ├── metrics.py               # 📊 Prometheus metrics
│   └── responses.py             # HTTP response helpers
│
├── app.py                        # Compatibility shim for old uvicorn command
├── mock_upstream.py              # Mock LLM server for testing
├── test_client.py                # Load test client
├── pyproject.toml                # Dependencies
├── Dockerfile                    # Container image
├── README.md                     # User guide
├── ARCHITECTURE.md               # Architecture decisions
└── STRUCTURE.md                  # This file
```

## File Responsibilities

### Core Layer (Pure Business Logic)

#### `core/semaphore_pool.py`
- **Purpose**: Manage semaphore instances per department+model
- **Key Classes**: `SemaphorePool`, `SemaphoreKey`
- **Dependencies**: `redis_semaphore`
- **Used by**: API routes, infrastructure (watchdog)

#### `core/inflight.py`
- **Purpose**: Track active requests across Redis and fallback modes
- **Key Classes**: `InflightTracker`
- **Methods**: `increment/decrement_redis_inflight`, `acquire/release_fallback`, `snapshot_*`
- **Used by**: API dependencies, infrastructure (watchdog)

#### `core/reservations.py`
- **Purpose**: Pre-reserve Redis slots during fallback→Redis migration
- **Key Classes**: `ReservationManager`
- **Methods**: `reserve_for_fallback`, `wait_ready`, `release_one`
- **Used by**: Infrastructure (watchdog), API dependencies

### API Layer (HTTP Handling)

#### `api/routes/chat.py`
- **Purpose**: Handle `/v1/chat/completions` with rate limiting
- **Functions**: `proxy_chat_completions`, `_handle_streaming`, `_handle_non_streaming`
- **Features**: Department/model validation, semaphore acquisition, streaming support

#### `api/routes/health.py`
- **Purpose**: Health checks and monitoring
- **Endpoints**:
  - `GET /health` - Redis connectivity
  - `GET /semaphore/status` - Pool state (debug)

#### `api/routes/proxy.py`
- **Purpose**: Proxy non-chat endpoints without rate limiting
- **Endpoint**: `ANY /{path:path}` (catch-all)
- **Methods**: All HTTP methods supported

#### `api/dependencies.py`
- **Purpose**: Shared request handling logic
- **Functions**:
  - `parse_department()` - Extract department from headers
  - `acquire_semaphore()` - Try Redis, fallback to local
  - `cleanup_semaphore()` - Release + metrics
  - `safe_release()` - Error-safe semaphore release

### Infrastructure Layer (External Services)

#### `infrastructure/redis_manager.py`
- **Purpose**: Redis lifecycle management
- **Functions**:
  - `redis_watchdog()` - Detect reconnection, trigger migration
  - `cleanup_expired_semaphores()` - Remove stale entries
  - `mark_redis_unavailable()` - Switch to fallback mode
  - `close_redis()` - Clean shutdown

#### `infrastructure/upstream.py`
- **Purpose**: Upstream LLM HTTP utilities
- **Functions**:
  - `build_upstream_url()` - Construct request URL
  - `build_upstream_headers()` - Add auth headers
  - `filter_response_headers()` - Remove hop-by-hop headers

### Supporting Files

#### `config.py`
- **Purpose**: Configuration management
- **Classes**: `Settings` (pydantic-settings)
- **Source**: Environment variables + `.env` file

#### `metrics.py`
- **Purpose**: Prometheus metrics
- **Metrics**: Request counters, in-flight gauges, queue depth, Redis availability
- **Setup**: `setup_http_metrics()`, `setup_semaphore_metrics()`

#### `responses.py`
- **Purpose**: Standard HTTP responses
- **Functions**: `bad_request()`, `rate_limit_response()`, `service_unavailable()`, `upstream_error()`

#### `main.py`
- **Purpose**: Application assembly
- **Contents**:
  - `lifespan()` - Startup/shutdown logic
  - `app` - FastAPI instance
  - Router registration
  - Exception handlers

## Import Rules

### ✅ Allowed Dependencies

- **core/** → `redis_semaphore`, standard library only
- **api/** → `core/`, `infrastructure/`, `responses.py`, `metrics.py`
- **infrastructure/** → `core/`, `metrics.py`, `logging_setup.py`
- **main.py** → All modules

### ❌ Forbidden Dependencies

- **core/** ❌ → `api/`, `infrastructure/`, FastAPI
- **infrastructure/** ❌ → `api/`

## Adding New Features

### New endpoint without rate limiting
1. Add route to `api/routes/proxy.py` OR create new router
2. Register in `main.py`

### New endpoint WITH rate limiting
1. Add route to `api/routes/chat.py` OR create new router
2. Use `acquire_semaphore()` from dependencies
3. Call `cleanup_semaphore()` in finally block

### New semaphore strategy
1. Modify `core/semaphore_pool.py`
2. Update `SemaphoreConfig` creation
3. Add tests

### New fallback mode
1. Extend `core/inflight.py` with new tracking
2. Update `api/dependencies.py` acquire logic
3. Modify `infrastructure/redis_manager.py` watchdog

## Testing Organization

```
tests/
├── unit/
│   ├── test_core_semaphore_pool.py
│   ├── test_core_inflight.py
│   └── test_api_dependencies.py
├── integration/
│   ├── test_chat_endpoint.py
│   ├── test_redis_failover.py
│   └── test_streaming.py
└── load/
    └── test_concurrent_requests.py
```
