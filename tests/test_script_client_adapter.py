"""Tests for ScriptClientAdapter behavior (local SHA + EVAL fallback)."""

import pytest
import redis

from redis_semaphore.lua_scripts import LuaScriptRegistry, ScriptClientAdapter


@pytest.mark.asyncio
async def test_adapter_rejects_wrong_mode(redis_client, async_redis_client):
    """Sync adapter refuses async calls and vice versa."""
    sync_adapter = ScriptClientAdapter(redis_client)
    with pytest.raises(RuntimeError):
        await sync_adapter.aevalsha("sha", 0)

    async_adapter = ScriptClientAdapter(async_redis_client)
    with pytest.raises(RuntimeError):
        async_adapter.evalsha("sha", 0)


def test_adapter_noscript_falls_back_to_eval():
    """On NOSCRIPT the adapter runs EVAL with the script body."""

    class DummyClient:
        def __init__(self) -> None:
            self.eval_called_with: str | None = None

        def evalsha(self, sha: str, numkeys: int, *args):
            raise redis.exceptions.NoScriptError()

        def eval(self, script: str, numkeys: int, *args):
            self.eval_called_with = script
            return "ok"

    client = DummyClient()
    adapter = ScriptClientAdapter(client)
    sha = LuaScriptRegistry().get_sha("acquire")

    assert adapter.evalsha(sha, 0) == "ok"
    # The fallback used the real acquire script body.
    assert client.eval_called_with == LuaScriptRegistry.SCRIPTS["acquire"]


def test_adapter_noscript_unknown_sha_raises():
    """A NOSCRIPT for an unrecognized SHA cannot be served by EVAL."""

    class DummyClient:
        def evalsha(self, sha: str, numkeys: int, *args):
            raise redis.exceptions.NoScriptError()

        def eval(self, script: str, numkeys: int, *args):
            return "ok"

    adapter = ScriptClientAdapter(DummyClient())
    with pytest.raises(ValueError):
        adapter.evalsha("deadbeef", 0)


def test_adapter_script_for_sha_unknown():
    with pytest.raises(ValueError):
        ScriptClientAdapter._script_for_sha("unknown-sha")


@pytest.mark.asyncio
async def test_adapter_noscript_falls_back_to_eval_async(async_redis_client, monkeypatch):
    async def evalsha(sha: str, numkeys: int, *args):
        raise redis.exceptions.NoScriptError()

    async def eval_(script: str, numkeys: int, *args):
        return "ok"

    monkeypatch.setattr(async_redis_client, "evalsha", evalsha)
    monkeypatch.setattr(async_redis_client, "eval", eval_)

    adapter = ScriptClientAdapter(async_redis_client)
    sha = LuaScriptRegistry().get_sha("acquire")
    assert await adapter.aevalsha(sha, 0) == "ok"


@pytest.mark.asyncio
async def test_adapter_evalsha_non_awaitable_async(async_redis_client, monkeypatch):
    """aevalsha tolerates a synchronous (non-awaitable) evalsha result."""

    def evalsha(sha: str, numkeys: int, *args):
        return "ok"

    monkeypatch.setattr(async_redis_client, "evalsha", evalsha)

    adapter = ScriptClientAdapter(async_redis_client)
    assert await adapter.aevalsha("sha", 0) == "ok"
