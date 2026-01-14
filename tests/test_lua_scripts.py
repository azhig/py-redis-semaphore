"""Tests for LuaScriptRegistry."""

import pytest

import redis_semaphore.lua_scripts as lua_scripts
from redis_semaphore.lua_scripts import LuaScriptRegistry, LuaScriptRunner, ScriptClientAdapter


def test_registry_requires_load():
    """get_sha should fail before scripts are loaded."""
    registry = LuaScriptRegistry()
    assert registry.is_loaded is False

    with pytest.raises(ValueError):
        registry.get_sha("acquire")


def test_registry_load_all(redis_client):
    """load_all loads all scripts and makes SHAs available."""
    registry = LuaScriptRegistry()
    client = ScriptClientAdapter(redis_client)
    registry.load_all(client)

    assert registry.is_loaded is True
    sha = registry.get_sha("acquire")
    assert isinstance(sha, str) and sha
    runner = LuaScriptRunner(registry)
    success, token, expires = runner.acquire(
        client,
        "test:owners",
        "test:fencing",
        "id",
        1,
        1000,
        0,
    )
    assert success is True
    assert token is not None
    assert expires is not None


def test_registry_get_script_unknown():
    registry = LuaScriptRegistry()
    with pytest.raises(ValueError):
        registry.get_script("missing")


def test_registry_get_script():
    registry = LuaScriptRegistry()
    script = registry.get_script("acquire")
    assert isinstance(script, str) and script


def test_registry_invalidate(redis_client):
    registry = LuaScriptRegistry()
    client = ScriptClientAdapter(redis_client)
    registry.load_all(client)
    assert registry.is_loaded is True

    registry.invalidate()

    assert registry.is_loaded is False


def test_registry_load_all_type_error():
    class BadClient:
        def script_load(self, script: str):
            return 123

        def evalsha(self, sha: str, numkeys: int, *args):
            return 1

        async def ascript_load(self, script: str):
            return 123

        async def aevalsha(self, sha: str, numkeys: int, *args):
            return 1

    registry = LuaScriptRegistry()
    with pytest.raises(TypeError):
        registry.load_all(BadClient())


def test_status_parsing_bytes():
    class StatusClient:
        def evalsha(self, sha: str, numkeys: int, *args):
            return [b"2", b"1", b"123"]

    registry = LuaScriptRegistry()
    registry._sha_cache = {"status": "sha"}
    runner = LuaScriptRunner(registry)

    count, is_owner, expires = runner.status(StatusClient(), "key", 0, "id")
    assert count == 2
    assert is_owner is True
    assert expires == 123


@pytest.mark.asyncio
async def test_status_parsing_async():
    class StatusClient:
        async def aevalsha(self, sha: str, numkeys: int, *args):
            return [b"1", b"0", b""]

    registry = LuaScriptRegistry()
    registry._sha_cache = {"status": "sha"}
    runner = LuaScriptRunner(registry)

    count, is_owner, expires = await runner.status_async(StatusClient(), "key", 0, None)
    assert count == 1
    assert is_owner is False
    assert expires is None


@pytest.mark.asyncio
async def test_registry_load_all_async(async_redis_client):
    """load_all_async loads scripts for async client."""
    registry = LuaScriptRegistry()
    client = ScriptClientAdapter(async_redis_client)
    await registry.load_all_async(client)

    assert registry.is_loaded is True
    sha = registry.get_sha("release")
    assert isinstance(sha, str) and sha
    runner = LuaScriptRunner(registry)
    success, token, expires = await runner.acquire_async(
        client,
        "test:owners",
        "test:fencing",
        "id",
        1,
        1000,
        0,
    )
    assert success is True
    assert token is not None
    assert expires is not None


@pytest.mark.asyncio
async def test_registry_load_all_async_type_error():
    class BadClient:
        def script_load(self, script: str):
            return 123

        async def ascript_load(self, script: str):
            return 123

        def evalsha(self, sha: str, numkeys: int, *args):
            return 1

        async def aevalsha(self, sha: str, numkeys: int, *args):
            return 1

    registry = LuaScriptRegistry()
    with pytest.raises(TypeError):
        await registry.load_all_async(BadClient())


def test_status_invalid_shape():
    class BadStatusClient:
        def evalsha(self, sha: str, numkeys: int, *args):
            return [1, 2]

    registry = LuaScriptRegistry()
    registry._sha_cache = {"status": "sha"}
    runner = LuaScriptRunner(registry)

    with pytest.raises(ValueError):
        runner.status(BadStatusClient(), "key", 0, None)


def test_status_invalid_type():
    class BadStatusClient:
        def evalsha(self, sha: str, numkeys: int, *args):
            return ["nope", "1", ""]

    registry = LuaScriptRegistry()
    registry._sha_cache = {"status": "sha"}
    runner = LuaScriptRunner(registry)

    with pytest.raises(ValueError):
        runner.status(BadStatusClient(), "key", 0, None)


def test_acquire_invalid_shape():
    class BadAcquireClient:
        def evalsha(self, sha: str, numkeys: int, *args):
            return [1, 2]

    registry = LuaScriptRegistry()
    registry._sha_cache = {"acquire": "sha"}
    runner = LuaScriptRunner(registry)

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
