# Mock Upstream + Test Client

This directory contains a mock upstream service and a small async client you can
use to exercise the proxy.

## 1) Start the mock upstream

From the repo root:

```bash
uvicorn examples.fastapi_llm_proxy.tests.mock_upstream:app --host 0.0.0.0 --port 9000
```

The mock implements OpenAI-style `/v1/chat/completions` and returns `usage`
fields so the proxy can log token counts.

## 2) Start the proxy pointing to the mock

In a new shell:

```bash
cd examples/fastapi_llm_proxy
export UPSTREAM_BASE_URL=http://localhost:9000/v1
uvicorn llm_proxy.main:app --host 0.0.0.0 --port 8000
```

## 3) Run the test client

From the repo root:

```bash
python examples/fastapi_llm_proxy/tests/test_client.py \
  --url http://localhost:8000 \
  --client-id client-1 \
  --model mock-1 \
  --requests 10 \
  --sleep 1
```

For streaming responses:

```bash
python examples/fastapi_llm_proxy/tests/test_client.py \
  --url http://localhost:8000 \
  --client-id client-1 \
  --model mock-1 \
  --requests 5 \
  --stream
```

The client prints each request as it completes with its duration and request
number.
