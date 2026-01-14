"""Tests for ScriptClientAdapter behavior."""

import pytest
import redis

from redis_semaphore.lua_scripts import LuaScriptRegistry, ScriptClientAdapter


@pytest.mark.asyncio
async def test_script_client_adapter_sync_errors(redis_client):
    adapter = ScriptClientAdapter(redis_client)

    with pytest.raises(RuntimeError):
        await adapter.ascript_load("return 1")

    with pytest.raises(RuntimeError):
        await adapter.aevalsha("sha", 0)

    def bad_script_load(script: str):
        return 123

    adapter._client.script_load = bad_script_load
    with pytest.raises(TypeError):
        adapter.script_load("return 1")


@pytest.mark.asyncio
async def test_script_client_adapter_async_errors(async_redis_client):
    adapter = ScriptClientAdapter(async_redis_client)

    with pytest.raises(RuntimeError):
        adapter.script_load("return 1")

    with pytest.raises(RuntimeError):
        adapter.evalsha("sha", 0)

    async def bad_script_load(script: str):
        return 123

    adapter._client.script_load = bad_script_load
    with pytest.raises(TypeError):
        await adapter.ascript_load("return 1")


@pytest.mark.asyncio
async def test_script_client_adapter_async_shortcuts(async_redis_client, monkeypatch):
    adapter = ScriptClientAdapter(async_redis_client)

    def fake_script_load(script: str):
        return "sha"

    def fake_evalsha(*args, **kwargs):
        return "ok"

    monkeypatch.setattr(adapter._client, "script_load", fake_script_load)
    monkeypatch.setattr(adapter._client, "evalsha", fake_evalsha)

    assert await adapter.ascript_load("return 1") == "sha"
    assert await adapter.aevalsha("sha", 0) == "ok"


def test_script_client_adapter_noscript_sync():
    import hashlib

    class DummyClient:
        def __init__(self) -> None:
            self.calls = 0

        def script_load(self, script: str):
            return hashlib.sha1(script.encode()).hexdigest()

        def evalsha(self, sha: str, numkeys: int, *args):
            self.calls += 1
            if self.calls == 1:
                raise redis.exceptions.NoScriptError()
            return "ok"

    registry = LuaScriptRegistry()
    adapter = ScriptClientAdapter(DummyClient(), registry)
    adapter.set_registry(registry)
    registry.load_all(adapter)

    sha = registry.get_sha("acquire")
    assert adapter.evalsha(sha, 0) == "ok"


def test_script_client_adapter_noscript_sync_no_registry():
    class DummyClient:
        def evalsha(self, sha: str, numkeys: int, *args):
            raise redis.exceptions.NoScriptError()

    adapter = ScriptClientAdapter(DummyClient())
    with pytest.raises(RuntimeError):
        adapter.evalsha("sha", 0)


def test_script_client_adapter_find_script_name_unknown():
    adapter = ScriptClientAdapter(object())
    with pytest.raises(ValueError):
        adapter._find_script_name_by_sha("unknown")


@pytest.mark.asyncio
async def test_script_client_adapter_noscript_async(async_redis_client, monkeypatch):
    import hashlib

    registry = LuaScriptRegistry()
    adapter = ScriptClientAdapter(async_redis_client, registry)
    adapter.set_registry(registry)

    def script_load(script: str):
        return hashlib.sha1(script.encode()).hexdigest()

    calls = {"count": 0}

    async def evalsha(sha: str, numkeys: int, *args):
        calls["count"] += 1
        if calls["count"] == 1:
            raise redis.exceptions.NoScriptError()
        return "ok"

    monkeypatch.setattr(async_redis_client, "script_load", script_load)
    monkeypatch.setattr(async_redis_client, "evalsha", evalsha)

    await registry.load_all_async(adapter)

    sha = registry.get_sha("acquire")
    assert await adapter.aevalsha(sha, 0) == "ok"


@pytest.mark.asyncio
async def test_script_client_adapter_noscript_async_no_registry(async_redis_client, monkeypatch):
    adapter = ScriptClientAdapter(async_redis_client)

    def evalsha(sha: str, numkeys: int, *args):
        raise redis.exceptions.NoScriptError()

    monkeypatch.setattr(async_redis_client, "evalsha", evalsha)

    with pytest.raises(RuntimeError):
        await adapter.aevalsha("sha", 0)


@pytest.mark.asyncio
async def test_script_client_adapter_noscript_async_returns_non_awaitable(
    async_redis_client, monkeypatch
):
    import hashlib

    registry = LuaScriptRegistry()
    adapter = ScriptClientAdapter(async_redis_client, registry)
    adapter.set_registry(registry)

    def script_load(script: str):
        return hashlib.sha1(script.encode()).hexdigest()

    calls = {"count": 0}

    def evalsha(sha: str, numkeys: int, *args):
        calls["count"] += 1
        if calls["count"] == 1:
            raise redis.exceptions.NoScriptError()
        return "ok"

    monkeypatch.setattr(async_redis_client, "script_load", script_load)
    monkeypatch.setattr(async_redis_client, "evalsha", evalsha)

    await registry.load_all_async(adapter)

    sha = registry.get_sha("acquire")
    assert await adapter.aevalsha(sha, 0) == "ok"
