FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Copy repository (build context should be repo root).
COPY . .

# Base image is assumed to have Python without pip.
RUN python -m ensurepip --upgrade \
    && python -m pip install --no-cache-dir --upgrade pip \
    && python -m pip install --no-cache-dir uv

WORKDIR /app/examples/fastapi_llm_proxy

RUN uv venv \
    && uv pip install -e .

ENV PATH="/app/examples/fastapi_llm_proxy/.venv/bin:${PATH}"

EXPOSE 8000

CMD ["uvicorn", "llm_proxy.main:app", "--host", "0.0.0.0", "--port", "8000"]
