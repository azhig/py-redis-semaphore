"""Compatibility entrypoint for uvicorn.

Use `uvicorn llm_proxy.main:app` for new setups.
"""

from llm_proxy.main import app

__all__ = ["app"]
