"""Load per-client/model overrides from JSON or YAML."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ClientModelOverrides:
    upstream_base_urls: dict[str, str]
    semaphore_capacities: dict[str, int]

    def upstream_base_url(self, client_id: str, model: str) -> str | None:
        return self.upstream_base_urls.get(f"{client_id}:{model}")

    def semaphore_capacity(self, client_id: str, model: str) -> int | None:
        return self.semaphore_capacities.get(f"{client_id}:{model}")


def load_client_model_overrides(path: str | None) -> ClientModelOverrides:
    if not path:
        return ClientModelOverrides(upstream_base_urls={}, semaphore_capacities={})

    payload = _load_mapping(path)
    if not isinstance(payload, dict):
        raise ValueError("Client/model overrides must be a JSON/YAML mapping")

    upstream_base_urls: dict[str, str] = {}
    semaphore_capacities: dict[str, int] = {}

    for key, value in payload.items():
        if not isinstance(key, str) or ":" not in key:
            raise ValueError("Override keys must be strings in the form 'client_id:model'")
        if not isinstance(value, dict):
            raise ValueError(f"Override for {key} must be an object")

        upstream = value.get("upstream_base_url")
        if upstream is not None:
            if not isinstance(upstream, str) or not upstream.strip():
                raise ValueError(f"Invalid upstream_base_url for {key}")
            upstream_base_urls[key] = upstream.strip()

        capacity = value.get("semaphore_capacity")
        if capacity is not None:
            if isinstance(capacity, bool) or not isinstance(capacity, int):
                raise ValueError(f"Invalid semaphore_capacity for {key}")
            if capacity < 1:
                raise ValueError(f"semaphore_capacity must be >= 1 for {key}")
            semaphore_capacities[key] = capacity

    return ClientModelOverrides(
        upstream_base_urls=upstream_base_urls,
        semaphore_capacities=semaphore_capacities,
    )


def _load_mapping(path: str) -> Any:
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"Overrides file not found: {path}")

    raw = file_path.read_text(encoding="utf-8")
    suffix = file_path.suffix.lower()
    if suffix in {".yaml", ".yml"}:
        try:
            import yaml  # type: ignore[import-untyped]
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise ImportError(
                "PyYAML is required to load YAML overrides. " "Install it or use JSON."
            ) from exc
        return yaml.safe_load(raw)

    return json.loads(raw)
