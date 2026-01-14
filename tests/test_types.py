"""Tests for configuration validation."""

import pytest

from redis_semaphore import SemaphoreConfig


def test_config_validation_name_required():
    with pytest.raises(ValueError):
        SemaphoreConfig(name="", limit=1)


def test_config_validation_limit():
    with pytest.raises(ValueError):
        SemaphoreConfig(name="bad-limit", limit=0)


def test_config_validation_lock_timeout():
    with pytest.raises(ValueError):
        SemaphoreConfig(name="bad-lock", limit=1, lock_timeout=0)


def test_config_validation_acquire_timeout():
    with pytest.raises(ValueError):
        SemaphoreConfig(name="bad-acquire", limit=1, acquire_timeout=0)


def test_config_validation_retry_interval():
    with pytest.raises(ValueError):
        SemaphoreConfig(name="bad-retry", limit=1, retry_interval=0)


def test_config_validation_refresh_interval():
    with pytest.raises(ValueError):
        SemaphoreConfig(name="bad-refresh", limit=1, refresh_interval=0)


def test_config_strict_mode():
    """Test strict_mode configuration option."""
    config = SemaphoreConfig(name="strict", limit=1, strict_mode=True)
    assert config.strict_mode is True

    config_default = SemaphoreConfig(name="default", limit=1)
    assert config_default.strict_mode is False


def test_config_use_server_time():
    """Test use_server_time configuration option."""
    config = SemaphoreConfig(name="server-time", limit=1, use_server_time=True)
    assert config.use_server_time is True

    config_default = SemaphoreConfig(name="default", limit=1)
    assert config_default.use_server_time is False


def test_config_mission_critical():
    """Test mission-critical configuration pattern."""
    config = SemaphoreConfig(
        name="critical",
        limit=1,
        strict_mode=True,
        use_server_time=True,
    )
    assert config.strict_mode is True
    assert config.use_server_time is True
