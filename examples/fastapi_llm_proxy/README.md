# FastAPI LLM Proxy with Per-Department/Model Semaphores

Production-ready FastAPI example that proxies LLM requests (OpenAI/GigaChat/Ollama) with distributed rate limiting using `redis-semaphore`. Each `department + model` combination gets its own semaphore with 5 slots, ensuring fair allocation across teams and models.

```
Client -> FastAPI Proxy -> Redis Semaphore -> Upstream LLM
```

## Architecture

The example follows **Clean Architecture** principles with clear separation of concerns:

```
llm_proxy/
├── core/                  # 📦 Business logic (domain layer)
│   ├── inflight.py       # In-flight request tracking
│   ├── reservations.py   # Reservation management for fallback
│   └── semaphore_pool.py # Semaphore pool management
├── api/                   # 🌐 HTTP layer (presentation)
│   ├── routes/
│   │   ├── chat.py       # /v1/chat/completions endpoint
│   │   ├── health.py     # Health checks and monitoring
│   │   └── proxy.py      # Catch-all proxy for other endpoints
│   └── dependencies.py   # Shared request handling logic
├── infrastructure/        # 🔌 External services
│   ├── redis_manager.py  # Redis connection and watchdog
│   └── upstream.py       # Upstream LLM HTTP client
├── config.py             # ⚙️  Configuration
├── metrics.py            # 📊 Prometheus metrics
├── responses.py          # HTTP response helpers
└── main.py               # 🚀 Application entry point
```

**Key benefits**:
- **Testable**: Core logic has no FastAPI dependencies
- **Maintainable**: Each file has a single responsibility
- **Reusable**: Business logic can work with any web framework
- **Scalable**: Easy to add new routes or features

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed design decisions and [STRUCTURE.md](STRUCTURE.md) for file-by-file documentation.

## Features
- Universal JSON proxy (OpenAI-compatible payloads)
- Per-department rate limiting via `direction` header (1-20)
- Per-model isolation via separate semaphore keys
- Streaming and non-streaming support
- All other endpoints are proxied as-is without semaphores
- Prometheus metrics
- Health checks and optional semaphore status endpoint

## Installation
```bash
cd examples/fastapi_llm_proxy
python -m venv .venv
source .venv/bin/activate
pip install fastapi uvicorn httpx prometheus-client prometheus-fastapi-instrumentator python-dotenv
pip install loguru pydantic-settings
pip install -e ../../
```

## Configuration
Copy `.env.example` to `.env` and adjust values:
```bash
cp .env.example .env
```

Key settings:
- `UPSTREAM_BASE_URL` — base URL of your LLM provider (should include `/v1`)
- `SEMAPHORE_CAPACITY` — concurrent slots per department+model
- `SEMAPHORE_ACQUIRE_TIMEOUT` — maximum queue wait time in seconds
- `FALLBACK_SEMAPHORE_CAPACITY` — local in-process limit when Redis is unavailable
- `REDIS_CHECK_INTERVAL` — seconds between Redis health probes

## Running
```bash
# Start Redis
docker run -d -p 6379:6379 redis:7

# Start FastAPI
uvicorn llm_proxy.main:app --host 0.0.0.0 --port 8000
```

## Docker
Build from the repo root so the image can install the main package and the example:
```bash
docker build -f examples/fastapi_llm_proxy/Dockerfile -t llm-proxy .
docker run --rm -p 8000:8000 --env-file examples/fastapi_llm_proxy/.env llm-proxy
```

## Usage
Header `x-api-key` is optional. If present, it is forwarded to upstream as both `Authorization: Bearer <key>` and `x-api-key`. If omitted, the proxy forwards any existing `Authorization` header from the client.

Any other paths (e.g., `/v1/models`, `/v1/embeddings`, `/v1/files`) are proxied without rate limiting.

### Graceful Redis Fallback

When Redis becomes unavailable, the proxy automatically switches to in-process fallback semaphores with the following guarantees:

**During Redis downtime:**
- Active requests holding Redis slots continue executing without interruption
- New requests acquire fallback semaphore slots (`FALLBACK_SEMAPHORE_CAPACITY` per worker)
- Dynamic limiting ensures total concurrent requests never exceed upstream capacity:
  ```
  effective_fallback_limit = max(0, fallback_capacity - redis_inflight_count)
  ```
- Example: If 3 requests still hold Redis slots, only 2 new fallback slots are available (assuming capacity=5)

**Redis reconnection (checked every `REDIS_CHECK_INTERVAL` seconds):**
1. **Coordinated cleanup**: One worker acquires distributed lock and clears stale Redis entries
2. **Slot reservation**: Each worker reserves Redis slots for its active fallback requests
3. **Smooth migration**: Only after reservation completes, Redis is marked as available
4. **Gradual transition**: Fallback requests complete naturally while new requests use Redis

**Key features:**
- Zero request failures during Redis outage
- No upstream overload during failover or recovery
- Multi-worker coordination via distributed locks
- Heartbeat-based slot recovery for long-running requests

### OpenAI (non-streaming)
```bash
curl http://localhost:8000/v1/chat/completions \
  -H "content-type: application/json" \
  -H "direction: 1" \
  -H "x-api-key: $OPENAI_API_KEY" \
  -d '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"Hello!"}]}'
```

### Streaming
```bash
curl -N http://localhost:8000/v1/chat/completions \
  -H "content-type: application/json" \
  -H "direction: 2" \
  -H "x-api-key: $OPENAI_API_KEY" \
  -d '{"model":"gpt-4o-mini","stream":true,"messages":[{"role":"user","content":"Stream this"}]}'
```

### GigaChat
Set `UPSTREAM_BASE_URL` to your GigaChat gateway base URL and send the same OpenAI-style payload.
```bash
export UPSTREAM_BASE_URL=https://gigachat.example.com/v1
```

### Ollama
If you run Ollama with an OpenAI-compatible endpoint, set its base URL.
```bash
export UPSTREAM_BASE_URL=http://localhost:11434/v1
```

## Monitoring
- Metrics: `GET /metrics`
- Health: `GET /health`
- Semaphore status (debug): `GET /semaphore/status`

`/metrics` includes FastAPI HTTP metrics (via `prometheus-fastapi-instrumentator`) and redis-semaphore internal metrics in the same registry.

### Key metrics
- `llm_requests_total` - Total requests by department, model, and status
- `llm_requests_in_progress` - Currently executing requests
- `llm_request_duration_seconds` - Request latency histogram
- `llm_rate_limit_hits_total` - Queue timeout (429) responses
- `llm_semaphore_queue_depth` - Requests waiting to acquire slots (per-process)
- `llm_semaphore_pool_size` - Number of unique department+model semaphores
- `llm_redis_available` - Redis availability (1=up, 0=down)
- `llm_redis_release_failures_total` - Failed semaphore releases
- `llm_redis_inflight` - Requests holding Redis semaphore slots (per-process)
- `llm_fallback_inflight` - Requests using fallback semaphore (per-process)
- `http_requests_total` - HTTP request counter
- `http_request_size_bytes` - Request body size
- `http_response_size_bytes` - Response body size
- `http_request_duration_seconds` - HTTP latency histogram

## Production Considerations
- Use Redis Sentinel or managed Redis for HA
- Adjust semaphore capacity per department/model
- Deploy behind a load balancer
- Handle Redis connectivity errors with retries

## Testing (manual)
1. Send 10 parallel requests with long runtime to observe queueing
2. Increase runtime beyond 60 seconds to trigger 429 queue timeout
3. Use different `direction` values to verify isolation
4. Check `/metrics` for counters and gauges

## Test App (mock upstream + load test client)
Start a mock upstream and point the proxy at it:
```bash
uvicorn mock_upstream:app --host 0.0.0.0 --port 9001
export UPSTREAM_BASE_URL=http://localhost:9001/v1
```

Run the proxy in another terminal, then launch the test client:
```bash
python test_client.py --requests 20 --sleep 2
```
