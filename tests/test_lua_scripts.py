"""Tests for LuaScriptRegistry / LuaScriptRunner."""

import hashlib

import pytest

import redis_semaphore.lua_scripts as lua_scripts
from redis_semaphore.lua_scripts import LuaScriptRegistry, LuaScriptRunner, ScriptClientAdapter


def test_registry_sha_is_local_and_correct():
    """SHAs are computed locally and match SHA1 of the script body."""
    registry = LuaScriptRegistry()
    sha = registry.get_sha("acquire")
    assert sha == hashlib.sha1(LuaScriptRegistry.SCRIPTS["acquire"].encode()).hexdigest()


def test_registry_get_sha_unknown():
    registry = LuaScriptRegistry()
    with pytest.raises(ValueError):
        registry.get_sha("missing")


def test_registry_get_script_unknown():
    registry = LuaScriptRegistry()
    with pytest.raises(ValueError):
        registry.get_script("missing")


def test_registry_get_script():
    registry = LuaScriptRegistry()
    assert isinstance(registry.get_script("acquire"), str)


def test_runner_acquire_lazy_loads_via_eval(redis_client):
    """First EVALSHA misses (NOSCRIPT) and transparently falls back to EVAL."""
    redis_client.script_flush()  # ensure the script is NOT cached server-side
    registry = LuaScriptRegistry()
    client = ScriptClientAdapter(redis_client)
    runner = LuaScriptRunner(registry)

    success, token, expires, count = runner.acquire(
        client, "test:owners", "test:fencing", "id", 1, 1000, 0
    )
    assert success is True
    assert token is not None
    assert expires is not None
    assert count == 1  # one slot now occupied (us)


@pytest.mark.asyncio
async def test_runner_acquire_async_lazy_loads_via_eval(async_redis_client):
    await async_redis_client.script_flush()
    registry = LuaScriptRegistry()
    client = ScriptClientAdapter(async_redis_client)
    runner = LuaScriptRunner(registry)

    success, token, expires, count = await runner.acquire_async(
        client, "test:owners", "test:fencing", "id", 1, 1000, 0
    )
    assert success is True
    assert token is not None
    assert expires is not None
    assert count == 1


def test_cleanup_parsing():
    class CleanupClient:
        def evalsha(self, sha: str, numkeys: int, *args):
            return 3

    runner = LuaScriptRunner(LuaScriptRegistry())
    assert runner.cleanup(CleanupClient(), "key", 0) == 3


@pytest.mark.asyncio
async def test_cleanup_async_parsing():
    class CleanupClient:
        async def aevalsha(self, sha: str, numkeys: int, *args):
            return b"2"

    runner = LuaScriptRunner(LuaScriptRegistry())
    assert await runner.cleanup_async(CleanupClient(), "key", 0) == 2


def test_status_parsing_bytes():
    class StatusClient:
        def evalsha(self, sha: str, numkeys: int, *args):
            return [b"2", b"1", b"123"]

    runner = LuaScriptRunner(LuaScriptRegistry())
    count, is_owner, expires = runner.status(StatusClient(), "key", 0, "id")
    assert count == 2
    assert is_owner is True
    assert expires == 123


@pytest.mark.asyncio
async def test_status_parsing_async():
    class StatusClient:
        async def aevalsha(self, sha: str, numkeys: int, *args):
            return [b"1", b"0", b""]

    runner = LuaScriptRunner(LuaScriptRegistry())
    count, is_owner, expires = await runner.status_async(StatusClient(), "key", 0, None)
    assert count == 1
    assert is_owner is False
    assert expires is None


def test_status_invalid_shape():
    class BadStatusClient:
        def evalsha(self, sha: str, numkeys: int, *args):
            return [1, 2]

    runner = LuaScriptRunner(LuaScriptRegistry())
    with pytest.raises(ValueError):
        runner.status(BadStatusClient(), "key", 0, None)


def test_status_invalid_type():
    class BadStatusClient:
        def evalsha(self, sha: str, numkeys: int, *args):
            return ["nope", "1", ""]

    runner = LuaScriptRunner(LuaScriptRegistry())
    with pytest.raises(ValueError):
        runner.status(BadStatusClient(), "key", 0, None)


def test_acquire_invalid_shape():
    class BadAcquireClient:
        def evalsha(self, sha: str, numkeys: int, *args):
            return [1, 2]

    runner = LuaScriptRunner(LuaScriptRegistry())
    with pytest.raises(ValueError):
        runner.acquire(BadAcquireClient(), "owners", "fencing", "id", 1, 1, 0)


def test_conversion_helpers():
    assert lua_scripts._to_optional_int("5") == 5
    assert lua_scripts._to_optional_int(7) == 7
    assert lua_scripts._to_optional_int(3.2) == 3
    assert lua_scripts._to_optional_int(memoryview(b"")) is None
    assert lua_scripts._to_optional_int(bytearray(b"")) is None
    assert lua_scripts._to_optional_int(b"9") == 9
    assert lua_scripts._to_optional_int("") is None

    assert lua_scripts._to_bool(b"1") is True
    assert lua_scripts._to_bool("1") is True
    assert lua_scripts._to_bool(b"") is False
    assert lua_scripts._to_bool("") is False

    assert lua_scripts._to_int(memoryview(b"")) == 0
    assert lua_scripts._to_int(b"4") == 4
    assert lua_scripts._to_int("") == 0
    assert lua_scripts._to_int(2.7) == 2
    with pytest.raises(TypeError):
        lua_scripts._to_optional_int(object())
    with pytest.raises(TypeError):
        lua_scripts._to_int(object())
