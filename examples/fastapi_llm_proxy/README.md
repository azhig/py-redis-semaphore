# FastAPI LLM Proxy with Per-Client/Model Semaphores

Production-ready FastAPI example that proxies LLM requests (OpenAI/GigaChat/Ollama) with distributed rate limiting using `redis-semaphore`. Each `client_id + model` combination gets its own semaphore with 5 slots, ensuring fair allocation across clients and models.

```
Client -> FastAPI Proxy -> Redis Semaphore -> Upstream LLM
```

## Architecture

The example follows **Clean Architecture** principles with clear separation of concerns:

```
llm_proxy/
├── core/                  # 📦 Business logic (domain layer)
│   └── semaphore_pool.py # Semaphore pool management
├── api/                   # 🌐 HTTP layer (presentation)
│   ├── routes/
│   │   ├── chat.py       # /v1/chat/completions endpoint
│   │   ├── health.py     # Health checks and monitoring
│   │   └── proxy.py      # Catch-all proxy for other endpoints
│   └── dependencies.py   # Shared request handling logic
├── infrastructure/        # 🔌 External services
│   ├── redis_manager.py  # Redis connection handling
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

## Features
- Universal JSON proxy (OpenAI-compatible payloads)
- Per-client rate limiting via `x-client-id` header
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
pip install structlog pydantic-settings
pip install -e ../../
```

## Configuration
Copy `.env.example` to `.env` and adjust values:
```bash
cp .env.example .env
```

Key settings:
- `UPSTREAM_BASE_URL` — base URL of your LLM provider (should include `/v1`)
- `SEMAPHORE_CAPACITY` — concurrent slots per client_id+model
- `SEMAPHORE_ACQUIRE_TIMEOUT` — maximum queue wait time in seconds
- `REDIS_CHECK_INTERVAL` — seconds between Redis recovery polls while down

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
All request headers are forwarded upstream as-is, except hop-by-hop headers (e.g., `Host`, `Content-Length`, `Connection`, `Transfer-Encoding`). If you need authentication, pass the upstream `Authorization` header directly.

Any other paths (e.g., `/v1/models`, `/v1/files`) are proxied without rate limiting.

### Per-client/model overrides

You can override `upstream_base_url` and `semaphore_capacity` for конкретной пары `client_id:model`
через JSON/YAML файл. Задайте путь в `CLIENT_MODEL_CONFIG_PATH`.

Example `client_model_overrides.example.json`:
```json
{
  "client-1:gpt-4o": {
    "upstream_base_url": "https://api.openai.com/v1",
    "semaphore_capacity": 3
  },
  "client-1:text-embedding-3-small": {
    "semaphore_capacity": 2
  }
}
```

Если пары нет в файле, используются значения по умолчанию из `.env`.

### Redis Downtime Handling

When Redis becomes unavailable, the proxy does not switch to local semaphores. Instead:

**During Redis downtime:**
- Active requests holding Redis slots continue executing without interruption
- New requests wait in the queue until Redis responds again

**Redis recovery (checked every `REDIS_CHECK_INTERVAL` seconds):**
1. A single waiter polls Redis
2. Once Redis responds to `PING`, the queue resumes

This keeps Redis as the single source of truth for concurrency and avoids migration complexity.

### OpenAI (non-streaming)
```bash
curl http://localhost:8000/v1/chat/completions \
  -H "content-type: application/json" \
  -H "x-client-id: client-1" \
  -H "Authorization: Bearer $OPENAI_API_KEY" \
  -d '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"Hello!"}]}'
```

### Streaming
```bash
curl -N http://localhost:8000/v1/chat/completions \
  -H "content-type: application/json" \
  -H "x-client-id: client-2" \
  -H "Authorization: Bearer $OPENAI_API_KEY" \
  -d '{"model":"gpt-4o-mini","stream":true,"messages":[{"role":"user","content":"Stream this"}]}'
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
- `redis_semaphore_slots_used` - Currently acquired slots
- `redis_semaphore_waiting` - Waiters on acquire
- `redis_semaphore_acquire_total` - Acquire attempts by result
- `redis_semaphore_queue_total` - Entries into the wait queue
- `redis_semaphore_queue_wait_seconds` - Time spent waiting in queue
- `redis_semaphore_lock_lost_total` - Lost locks
- `http_requests_total` - HTTP request counter
- `http_request_size_bytes` - Request body size
- `http_response_size_bytes` - Response body size
- `http_request_duration_seconds` - HTTP latency histogram

## Production Considerations
- Use Redis Sentinel or managed Redis for HA
- Adjust semaphore capacity per client/model
- Deploy behind a load balancer
- Handle Redis connectivity errors with retries

## Testing (manual)
1. Send 10 parallel requests with long runtime to observe queueing
2. Increase runtime beyond 60 seconds to trigger 429 queue timeout
3. Use different `x-client-id` values to verify isolation
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
