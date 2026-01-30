# Plan: FastAPI LLM Proxy with Per-Department/Model Semaphores

## Overview
Create a production-ready FastAPI example that proxies LLM requests (OpenAI/GigaChat/Ollama) with distributed rate limiting using redis-semaphore. Each department+model combination gets its own semaphore with 5 slots, ensuring fair resource allocation across different teams and models.

## Requirements Analysis

### Core Features
1. **Universal LLM Proxy**: Accept arbitrary JSON payloads - work with OpenAI, GigaChat, Ollama, etc.
2. **Per-Department Rate Limiting**: Header `direction` (1-20) determines department
3. **Per-Model Isolation**: Different models get separate semaphore pools
4. **Streaming Support**: Handle both regular and streaming LLM responses
5. **Dynamic Semaphore Creation**: Create semaphores on-demand for new department+model combinations
6. **API Key Pass-through**: Client provides API key in request (not stored server-side)

### Production Features (per user requirements)
- Error handling & structured logging
- Prometheus metrics (queue depth, request counts, rate limit hits)
- Request/response validation (department header, model name)
- Health checks

### Architecture Decisions
- **Semaphore Key Pattern**: `{department}:{model}` → e.g., `"dept_1:gpt-4"`, `"dept_2:gigachat"`
- **Semaphore Pool**: Dictionary cache of Semaphore instances (created lazily)
- **Capacity**: 5 slots per department+model combination
- **Lock Timeout**: 120 seconds (for long-running LLM requests)
- **Acquire Timeout**: 60 seconds (maximum queue wait time)
- **Wait Strategy**: BLPOP mode (efficient blocking wait via Redis LIST)

## Critical Files

### New Files to Create
1. **`examples/fastapi_llm_proxy/app.py`** - Main FastAPI application
2. **`examples/fastapi_llm_proxy/config.py`** - Configuration and settings
3. **`examples/fastapi_llm_proxy/semaphore_pool.py`** - Semaphore pool manager
4. **`examples/fastapi_llm_proxy/metrics.py`** - Prometheus metrics
5. **`examples/fastapi_llm_proxy/README.md`** - Documentation and usage examples
6. **`examples/fastapi_llm_proxy/.env.example`** - Environment variable template

### Dependencies (documented, not added to project)
- `fastapi` - Web framework
- `uvicorn` - ASGI server
- `httpx` - Async HTTP client for proxying
- `prometheus-client` - Metrics (already optional dep)
- `python-dotenv` - Environment variables (optional)

## Implementation Plan

### Phase 1: Project Structure Setup
**Files**: Directory structure, README, .env.example

1. Create `examples/fastapi_llm_proxy/` directory
2. Create README.md with:
   - Overview and architecture diagram
   - Installation instructions (manual pip install)
   - Configuration guide (Redis, env vars)
   - Usage examples for OpenAI/GigaChat/Ollama
   - Testing instructions
3. Create `.env.example` with placeholders:
   ```
   REDIS_HOST=localhost
   REDIS_PORT=6379
   SEMAPHORE_CAPACITY=5
   SEMAPHORE_LOCK_TIMEOUT=120
   SEMAPHORE_ACQUIRE_TIMEOUT=60
   LOG_LEVEL=INFO
   ```

### Phase 2: Configuration Module
**File**: `config.py`

Create Pydantic settings class:
```python
class Settings(BaseSettings):
    # Redis
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0

    # Semaphore
    semaphore_capacity: int = 5
    semaphore_lock_timeout: float = 120.0
    semaphore_acquire_timeout: float = 60.0  # Maximum queue wait time
    semaphore_namespace: str = "llm_proxy"

    # Upstream LLM API
    upstream_base_url: str = "https://api.openai.com"  # Or GigaChat URL
    upstream_timeout: float = 120.0

    # Logging
    log_level: str = "INFO"
```

### Phase 3: Semaphore Pool Manager
**File**: `semaphore_pool.py`

Create `SemaphorePool` class:
- Dictionary to cache Semaphore instances by key `{dept}:{model}`
- Thread-safe lazy initialization (use asyncio.Lock)
- `async def get_semaphore(dept: int, model: str) -> Semaphore`
- Methods to track pool size, active locks (for metrics)
- Cleanup method to remove unused semaphores (optional)

Key implementation:
```python
class SemaphorePool:
    def __init__(self, redis_client, config):
        self._pool: dict[str, Semaphore] = {}
        self._lock = asyncio.Lock()

    async def get_semaphore(self, dept: int, model: str) -> Semaphore:
        key = f"dept_{dept}:{model}"
        if key not in self._pool:
            async with self._lock:
                if key not in self._pool:  # Double-check
                    config = SemaphoreConfig(
                        name=key,
                        limit=self.capacity,
                        lock_timeout=self.lock_timeout,
                        ...
                    )
                    self._pool[key] = Semaphore(self.redis, config)
        return self._pool[key]
```

### Phase 4: Prometheus Metrics
**File**: `metrics.py`

Define metrics:
- `llm_requests_total` - Counter with labels: department, model, status
- `llm_requests_in_progress` - Gauge with labels: department, model
- `llm_request_duration_seconds` - Histogram
- `llm_rate_limit_hits_total` - Counter (429 responses)
- `llm_semaphore_queue_depth` - Gauge per department+model
- `llm_semaphore_pool_size` - Gauge (total unique semaphores)

Provide helper functions:
- `record_request(dept, model, status, duration)`
- `update_queue_depth(dept, model, depth)`

### Phase 5: Main FastAPI Application
**File**: `app.py`

#### 5.1 Application Lifespan
- Initialize Redis client (async)
- Initialize SemaphorePool
- Initialize metrics
- Setup structured logging
- Cleanup on shutdown

#### 5.2 Request Models (Pydantic)
```python
class ProxyRequest(BaseModel):
    model: str  # Required
    # All other fields are optional and passed through
    class Config:
        extra = "allow"  # Accept arbitrary fields
```

#### 5.3 Endpoints

**1. POST /v1/chat/completions** (non-streaming)
- Extract `direction` header (required, 1-20)
- Extract `x-api-key` header (required)
- Parse request body, validate `model` field exists
- Get semaphore from pool
- **Acquire semaphore** - request waits in queue up to 60 seconds (BLPOP mode)
  - If a slot becomes available - acquire and proceed
  - If 60 sec timeout expires - return 429 (queue overflow)
- Forward request to OpenAI API via httpx
- Release semaphore (notifies next request in queue)
- Return response
- Handle errors: 400 (bad request), 429 (queue timeout), 502 (upstream error)

**2. POST /v1/chat/completions** (streaming)
- Same as above but detect `stream=true` in body
- Use httpx streaming response
- Stream chunks back to client
- Ensure semaphore released even if connection drops (try/finally)

**3. GET /health**
- Check Redis connectivity
- Return 200 if healthy, 503 if unhealthy

**4. GET /metrics**
- Return Prometheus metrics in text format

**5. GET /semaphore/status** (optional debug endpoint)
- Return JSON with all active semaphores, their current usage
- Useful for debugging/monitoring

#### 5.4 Error Handling
```python
@app.exception_handler(AcquireTimeoutError)
async def rate_limit_handler(request, exc):
    # Log with dept/model context
    # Increment rate_limit_hits metric
    # IMPORTANT: 429 means "queue wait timeout", NOT "no slots available"
    return JSONResponse(
        status_code=429,
        content={
            "error": {
                "message": "Queue wait timeout exceeded (60 seconds)",
                "type": "rate_limit_error",
                "code": "queue_timeout"
            }
        }
    )
```

#### 5.5 Middleware
- Request logging (dept, model, endpoint)
- Timing middleware (for duration metrics)
- Exception catching

### Phase 6: README Documentation

Sections:
1. **Overview** - What this example demonstrates
2. **Architecture** - Diagram showing request flow through semaphores
3. **Installation**
   ```bash
   cd examples/fastapi_llm_proxy
   pip install fastapi uvicorn httpx prometheus-client python-dotenv
   pip install -e ../../  # Install redis-semaphore
   ```
4. **Configuration** - Copy .env.example, edit values
5. **Running**
   ```bash
   # Start Redis
   docker run -d -p 6379:6379 redis:7

   # Start FastAPI
   uvicorn app:app --host 0.0.0.0 --port 8000
   ```
6. **Usage Examples**
   - OpenAI curl example with direction header
   - GigaChat example (different base URL)
   - Streaming example
   - Testing rate limits (parallel requests)
7. **Monitoring**
   - Access metrics at /metrics
   - Prometheus scraping config example
   - Key metrics to watch
8. **Production Considerations**
   - Redis Sentinel for HA
   - Adjust semaphore capacity per dept/model
   - Handle Redis connection failures
   - Deploy behind load balancer

## Key Design Patterns

### 1. Universal Proxy Pattern
Accept any JSON, extract only what we need (`model` field), pass rest through:
```python
body = await request.json()
model_name = body.get("model")  # Validate this
# Forward entire body to upstream
```

### 2. Semaphore Context Manager
Always use async context manager for automatic cleanup:
```python
semaphore = await pool.get_semaphore(dept, model)
try:
    await semaphore.aacquire()
    # Make upstream request
finally:
    await semaphore.arelease()
```

### 3. Streaming with Cleanup
Ensure semaphore released even if client disconnects:
```python
async def stream_response():
    try:
        async for chunk in upstream_stream:
            yield chunk
    finally:
        await semaphore.arelease()

return StreamingResponse(stream_response(), media_type="text/event-stream")
```

### 4. Metrics Integration
Record metrics at key points:
- Before acquire: `requests_total++`
- After acquire: `in_progress++`, `queue_depth--`
- After release: `in_progress--`
- On timeout: `rate_limit_hits++`

## Error Handling Strategy

| Error Type | HTTP Status | Action |
|------------|-------------|--------|
| Missing `direction` header | 400 | Return error JSON |
| Invalid `direction` (not 1-20) | 400 | Return error JSON |
| Missing `x-api-key` header | 400 | Return error JSON |
| Missing `model` in body | 400 | Return error JSON |
| **Queue wait timeout** (60 sec) | 429 | Return "Queue wait timeout exceeded" |
| Semaphore lock lost | 503 | Return service unavailable |
| Upstream API error | 502 | Forward upstream error |
| Redis connection error | 503 | Return service unavailable |

**Important**: 429 error means "waited too long in queue", NOT "no slots available". If there are 5 active requests and a 6th arrives, it **joins the queue and waits** until one of the first 5 completes. Only if waiting exceeds 60 seconds - return 429.

## Verification Plan

### Manual Testing
1. **Basic Request**: Send single request, verify it works
2. **Request Queueing**: Send 10 parallel long requests (> 5)
   - First 5 proceed immediately (acquire slots)
   - Next 5 join the queue and **wait**
   - As the first 5 complete, the next ones are processed gradually
   - NO 429 errors, all 10 should succeed
3. **Queue Timeout**: Send 10 slow requests (70 sec each)
   - First 5 start processing
   - Requests 6-9 join the queue
   - 10th request waits > 60 sec and gets 429
4. **Department Isolation**: Send dept_1 and dept_2 requests in parallel, verify no interference
5. **Model Isolation**: Send gpt-3.5 and gpt-4 requests in parallel, verify separate semaphores
6. **Streaming**: Test streaming endpoint with `stream=true`
7. **Metrics**: Check /metrics shows correct counts
8. **Health Check**: Verify /health returns 200 when Redis is up, 503 when down

### Automated Testing (optional)
Create `test_app.py` with pytest:
- Test request validation
- Test rate limiting with httpx client
- Mock Redis for unit tests
- Integration test with real Redis

### Load Testing (optional)
Use `locust` or `wrk` to simulate realistic load:
- 100 concurrent users
- Mix of departments (1-5)
- Mix of models (gpt-3.5, gpt-4)
- Verify semaphore capacity respected

## File Tree (Final Structure)
```
examples/fastapi_llm_proxy/
├── README.md              # Complete documentation
├── .env.example           # Environment template
├── app.py                 # Main FastAPI application (~300 lines)
├── config.py              # Settings (~50 lines)
├── semaphore_pool.py      # Pool manager (~100 lines)
├── metrics.py             # Prometheus metrics (~80 lines)
└── (optional) test_app.py # Tests
```

## Timeline Estimate
*Note: Per instructions, no time estimates provided - tasks broken into implementable chunks*

## Open Questions (Resolved)
✅ Dependencies: Documented in README, not added to project
✅ Auth: API key passed in request headers
✅ Docker: No docker-compose needed
✅ Scope: Full production features (logging, metrics, validation)

## Notes
- This example will be the most comprehensive in the repo (production-ready)
- Demonstrates advanced async patterns with semaphores
- Reusable pattern for any HTTP proxy with rate limiting
- Can be adapted for GigaChat, Ollama, or any other LLM provider by changing upstream URL
