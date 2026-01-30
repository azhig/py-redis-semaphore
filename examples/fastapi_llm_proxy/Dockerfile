FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Copy repository (build context should be repo root).
COPY . .

RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -m pip install --no-cache-dir -e . \
    && python -m pip install --no-cache-dir -e examples/fastapi_llm_proxy

WORKDIR /app/examples/fastapi_llm_proxy

EXPOSE 8000

CMD ["uvicorn", "llm_proxy.main:app", "--host", "0.0.0.0", "--port", "8000"]
