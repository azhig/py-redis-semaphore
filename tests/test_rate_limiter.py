"""Tests for the sliding-window RateLimiter."""

import threading
import time

import pytest

from redis_semaphore import (
    RateLimit,
    RateLimiter,
    RateLimiterConfig,
    RateLimitTimeoutError,
    TimeSource,
)


def _limiter(client, name, requests=3, tokens=100, window=60.0, **kwargs):
    config = RateLimiterConfig(
        name=name,
        limits=[RateLimit("requests", requests, window), RateLimit("tokens", tokens, window)],
        namespace="t-rate",
        **kwargs,
    )
    return RateLimiter(client, config)


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"name": "", "limit": 1}, "name"),
        ({"name": "x", "limit": 0}, "limit"),
        ({"name": "x", "limit": 1, "window": 0}, "window"),
    ],
)
def test_rate_limit_validation(kwargs, message):
    with pytest.raises(ValueError, match=message):
        RateLimit(**kwargs)


def test_config_validation():
    with pytest.raises(ValueError, match="name"):
        RateLimiterConfig(name="", limits=[RateLimit("r", 1)])
    with pytest.raises(ValueError, match="empty"):
        RateLimiterConfig(name="x", limits=[])
    with pytest.raises(ValueError, match="unique"):
        RateLimiterConfig(name="x", limits=[RateLimit("r", 1), RateLimit("r", 2)])
    with pytest.raises(ValueError, match="acquire_timeout"):
        RateLimiterConfig(name="x", limits=[RateLimit("r", 1)], acquire_timeout=0)
    with pytest.raises(ValueError, match="wait_jitter"):
        RateLimiterConfig(name="x", limits=[RateLimit("r", 1)], wait_jitter=-1)


def test_config_limits_stored_as_tuple():
    config = RateLimiterConfig(name="x", limits=[RateLimit("r", 1)])
    assert isinstance(config.limits, tuple)


def test_cost_validation(redis_client):
    limiter = _limiter(redis_client, "cost-validation")
    with pytest.raises(ValueError, match="Unknown limit"):
        limiter.acquire({"bogus": 1})
    with pytest.raises(ValueError, match=">= 0"):
        limiter.acquire({"tokens": -1})
    with pytest.raises(ValueError, match="never be admitted"):
        limiter.acquire({"tokens": 101})


@pytest.mark.parametrize("bad", [1.5, 1000.0, True, "10"])
def test_cost_must_be_int(redis_client, bad):
    """Non-int costs would break the Lua scripts halfway through a write."""
    limiter = _limiter(redis_client, "cost-int")
    with pytest.raises(ValueError, match="must be an int"):
        limiter.acquire({"tokens": bad})
    permit = limiter.acquire({"tokens": 1}, blocking=False)
    with pytest.raises(ValueError, match="must be an int"):
        limiter.adjust(permit, {"tokens": bad})
    assert limiter.status().usage["tokens"].used == 1


def test_key_names(redis_client):
    plain = _limiter(redis_client, "keys")
    assert plain.log_key("tokens") == "t-rate:keys:tokens:log"
    assert plain.total_key("tokens") == "t-rate:keys:tokens:total"
    tagged = _limiter(redis_client, "keys", cluster_hash_tags=True)
    assert tagged.log_key("tokens") == "{t-rate:keys}:tokens:log"


# ---------------------------------------------------------------------------
# Admission
# ---------------------------------------------------------------------------


def test_request_limit_denies_with_retry_after(redis_client):
    limiter = _limiter(redis_client, "rpm", requests=3, window=10.0)
    for i in range(3):
        permit = limiter.acquire({"tokens": 1}, blocking=False)
        assert permit.success is True
        assert permit.identifier is not None
        assert permit.acquired_at is not None
        assert permit.used["requests"] == i + 1

    denied = limiter.acquire({"tokens": 1}, blocking=False)
    assert denied.success is False
    assert denied.identifier is None
    assert denied.used == {"requests": 3, "tokens": 3}
    assert denied.retry_after is not None
    assert 9.0 < denied.retry_after <= 10.0


def test_weighted_tokens(redis_client):
    limiter = _limiter(redis_client, "tpm", requests=100, tokens=100)
    assert limiter.acquire({"tokens": 60}, blocking=False).success is True
    assert limiter.acquire({"tokens": 60}, blocking=False).success is False
    assert limiter.acquire({"tokens": 40}, blocking=False).success is True
    assert limiter.status().usage["tokens"].used == 100


def test_int_cost_applies_to_every_limit(redis_client):
    limiter = _limiter(redis_client, "int-cost", requests=5, tokens=5)
    permit = limiter.acquire(2, blocking=False)
    assert permit.costs == {"requests": 2, "tokens": 2}
    assert permit.used == {"requests": 2, "tokens": 2}


def test_denial_is_all_or_nothing(redis_client):
    """A request rejected by one limit is not recorded in the others."""
    limiter = _limiter(redis_client, "atomic", requests=10, tokens=50)
    assert limiter.acquire({"tokens": 50}, blocking=False).success is True
    assert limiter.acquire({"tokens": 10}, blocking=False).success is False
    assert limiter.status().usage["requests"].used == 1


def test_zero_cost_limit_is_not_recorded(redis_client):
    limiter = _limiter(redis_client, "zero-cost")
    permit = limiter.acquire({"requests": 1, "tokens": 0}, blocking=False)
    assert permit.success is True
    assert redis_client.exists(limiter.log_key("tokens")) == 0


def test_retry_after_waits_for_enough_units(redis_client):
    """retry_after points at the entry whose expiry frees enough units."""
    limiter = _limiter(redis_client, "retry-math", requests=100, tokens=100, window=2.0)
    limiter.acquire({"tokens": 30}, blocking=False)
    time.sleep(0.5)
    limiter.acquire({"tokens": 70}, blocking=False)
    # Needs 50 units: the first entry (30) is not enough, only the second frees them.
    denied = limiter.acquire({"tokens": 50}, blocking=False)
    assert denied.retry_after is not None
    assert 1.2 < denied.retry_after <= 2.0
    # Needs 20 units: the first entry alone frees them.
    denied = limiter.acquire({"tokens": 20}, blocking=False)
    assert denied.retry_after is not None
    assert denied.retry_after <= 1.5


def test_window_slides(redis_client):
    limiter = _limiter(redis_client, "slide", requests=1, window=0.3)
    assert limiter.acquire(blocking=False).success is True
    assert limiter.acquire(blocking=False).success is False
    time.sleep(0.35)
    assert limiter.acquire(blocking=False).success is True


def test_blocking_acquire_waits_until_window_frees(redis_client):
    limiter = _limiter(redis_client, "blocking", requests=1, window=0.4)
    limiter.acquire()
    start = time.monotonic()
    permit = limiter.acquire()
    elapsed = time.monotonic() - start
    assert permit.success is True
    assert 0.3 < elapsed < 1.0


def test_acquire_timeout_fails_fast(redis_client):
    """If the request cannot fit before the deadline, raise without sleeping."""
    limiter = _limiter(redis_client, "fail-fast", requests=1, window=30.0, acquire_timeout=5.0)
    limiter.acquire()
    start = time.monotonic()
    with pytest.raises(RateLimitTimeoutError) as exc_info:
        limiter.acquire()
    assert time.monotonic() - start < 0.5
    assert 29.0 < exc_info.value.retry_after <= 30.0


def test_acquire_timeout_allows_wait_within_deadline(redis_client):
    limiter = _limiter(redis_client, "within-deadline", requests=1, window=0.3, acquire_timeout=2.0)
    limiter.acquire()
    assert limiter.acquire().success is True


def test_concurrent_acquires_never_exceed_limit(redis_client):
    limiter = _limiter(redis_client, "concurrent", requests=20, tokens=10_000)
    admitted = []
    lock = threading.Lock()

    def worker():
        for _ in range(10):
            if limiter.acquire({"tokens": 1}, blocking=False).success:
                with lock:
                    admitted.append(1)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(admitted) == 20


def test_client_time_source(redis_client):
    limiter = _limiter(redis_client, "client-time", requests=1, time_source=TimeSource.CLIENT)
    permit = limiter.acquire(blocking=False)
    assert permit.success is True
    assert permit.acquired_at is not None
    assert abs(permit.acquired_at - time.time()) < 1.0
    assert limiter.acquire(blocking=False).success is False


# ---------------------------------------------------------------------------
# Adjust
# ---------------------------------------------------------------------------


def test_adjust_up_consumes_more(redis_client):
    limiter = _limiter(redis_client, "adjust-up", requests=10, tokens=100)
    permit = limiter.acquire({"tokens": 10}, blocking=False)
    limiter.adjust(permit, {"tokens": 90})
    assert permit.costs == {"requests": 1, "tokens": 90}
    assert permit.used["tokens"] == 90
    assert limiter.acquire({"tokens": 20}, blocking=False).success is False


def test_adjust_may_overshoot_limit(redis_client):
    """The real cost can exceed the limit; the debt blocks later acquires."""
    limiter = _limiter(redis_client, "adjust-over", requests=10, tokens=100)
    permit = limiter.acquire({"tokens": 50}, blocking=False)
    limiter.adjust(permit, {"tokens": 150})
    assert limiter.status().usage["tokens"].used == 150
    assert limiter.status().usage["tokens"].available == 0
    assert limiter.acquire({"tokens": 1}, blocking=False).success is False


def test_adjust_refund_frees_units(redis_client):
    limiter = _limiter(redis_client, "refund", requests=1, tokens=100)
    permit = limiter.acquire({"tokens": 100}, blocking=False)
    limiter.adjust(permit, 0)
    assert limiter.status().usage["requests"].used == 0
    assert redis_client.exists(limiter.total_key("tokens")) == 0
    assert limiter.acquire({"tokens": 100}, blocking=False).success is True


def test_adjust_keeps_admission_time(redis_client):
    """The adjusted cost leaves the window when the original request would."""
    limiter = _limiter(redis_client, "adjust-time", requests=10, tokens=100, window=0.5)
    permit = limiter.acquire({"tokens": 10}, blocking=False)
    time.sleep(0.3)
    limiter.adjust(permit, {"tokens": 100})
    assert limiter.acquire({"tokens": 1}, blocking=False).success is False
    time.sleep(0.25)
    assert limiter.status().usage["tokens"].used == 0


def test_adjust_after_window_is_noop(redis_client):
    limiter = _limiter(redis_client, "adjust-late", requests=10, tokens=100, window=0.2)
    permit = limiter.acquire({"tokens": 10}, blocking=False)
    time.sleep(0.25)
    limiter.adjust(permit, {"tokens": 100})
    assert limiter.status().usage["tokens"].used == 0


def test_adjust_twice_uses_latest_cost(redis_client):
    limiter = _limiter(redis_client, "adjust-twice", requests=10, tokens=100)
    permit = limiter.acquire({"tokens": 10}, blocking=False)
    limiter.adjust(permit, {"tokens": 40})
    limiter.adjust(permit, {"tokens": 25})
    assert limiter.status().usage["tokens"].used == 25
    assert redis_client.zcard(limiter.log_key("tokens")) == 1


def test_retried_adjust_does_not_double_count(redis_client):
    """An adjust retried after a lost reply must not add the cost twice."""
    limiter = _limiter(redis_client, "adjust-retry", requests=10, tokens=100)
    permit = limiter.acquire({"tokens": 10}, blocking=False)
    limiter.adjust(permit, {"tokens": 40})
    # The reply was "lost": the caller still believes the old cost and retries.
    permit.costs["tokens"] = 10
    limiter.adjust(permit, {"tokens": 40})
    assert limiter.status().usage["tokens"].used == 40
    assert int(redis_client.get(limiter.total_key("tokens"))) == 40


def test_retried_acquire_does_not_double_count(redis_client):
    """An acquire resent after a lost reply (client auto-retry) charges once.

    The cost is over half the limit on purpose: the resent call must not be
    denied against its own first attempt either (used + cost > limit).
    """
    limiter = _limiter(redis_client, "acquire-retry", requests=10, tokens=100)
    costs = {"requests": 1, "tokens": 60}
    first = limiter._try_acquire(costs, "retry-me")
    second = limiter._try_acquire(costs, "retry-me")  # the resent script call
    assert first.success is True
    assert second.success is True
    assert second.used["tokens"] == 60
    assert limiter.status().usage["tokens"].used == 60
    assert int(redis_client.get(limiter.total_key("tokens"))) == 60
    assert redis_client.zcard(limiter.log_key("tokens")) == 1


def test_retry_after_walks_past_first_chunk(redis_client):
    """retry_after is exact even when the freeing entry lies beyond 128 entries."""
    limiter = _limiter(redis_client, "retry-chunks", requests=1_000, tokens=300, window=5.0)
    for _ in range(150):
        limiter.acquire({"tokens": 1}, blocking=False)
    time.sleep(1.0)
    for _ in range(150):
        limiter.acquire({"tokens": 1}, blocking=False)
    # Needs 140 units: entry #140 is in the second chunk but the first batch,
    # so it frees ~1 s earlier than a whole window.
    denied = limiter.acquire({"tokens": 140}, blocking=False)
    assert denied.retry_after is not None
    assert 3.0 < denied.retry_after < 4.5
    # Needs 200 units: only the second batch frees them.
    denied = limiter.acquire({"tokens": 200}, blocking=False)
    assert denied.retry_after is not None
    assert denied.retry_after > 4.5


def test_adjust_unchanged_cost_skips_redis(redis_client):
    limiter = _limiter(redis_client, "adjust-same")
    permit = limiter.acquire({"tokens": 10}, blocking=False)
    redis_client.delete(limiter.log_key("tokens"), limiter.total_key("tokens"))
    limiter.adjust(permit, {"tokens": 10})
    assert redis_client.exists(limiter.log_key("tokens")) == 0


def test_adjust_rejects_invalid_permits(redis_client):
    limiter = _limiter(redis_client, "adjust-invalid", requests=1)
    permit = limiter.acquire(blocking=False)
    denied = limiter.acquire(blocking=False)
    with pytest.raises(ValueError, match="admitted"):
        limiter.adjust(denied, 0)
    with pytest.raises(ValueError, match=">= 0"):
        limiter.adjust(permit, {"tokens": -5})
    with pytest.raises(ValueError, match="Unknown limit"):
        limiter.adjust(permit, {"bogus": 1})

    other = RateLimiter(redis_client, RateLimiterConfig(name="other", limits=[RateLimit("r", 1)]))
    with pytest.raises(ValueError, match="belong"):
        other.adjust(permit, 0)


def test_adjust_rejects_permit_of_another_limiter_with_same_limits(redis_client):
    """Matching limit names ("requests", "tokens") must not fool adjust()."""
    ours = _limiter(redis_client, "cert-1")
    other = _limiter(redis_client, "cert-2")
    permit = ours.acquire({"tokens": 10}, blocking=False)
    with pytest.raises(ValueError, match="belongs"):
        other.adjust(permit, {"tokens": 0})
    # Nothing leaked into the other limiter's windows, ours kept its record.
    assert other.status().usage["tokens"].used == 0
    assert ours.status().usage["tokens"].used == 10


# ---------------------------------------------------------------------------
# Status & key hygiene
# ---------------------------------------------------------------------------


def test_status_without_consuming(redis_client):
    limiter = _limiter(redis_client, "status", requests=3, tokens=100)
    empty = limiter.status()
    assert empty.name == "status"
    assert empty.usage["requests"].used == 0
    assert empty.usage["tokens"].available == 100

    limiter.acquire({"tokens": 30}, blocking=False)
    status = limiter.status()
    assert status.usage["requests"].used == 1
    assert status.usage["requests"].available == 2
    assert status.usage["tokens"].used == 30
    assert status.usage["tokens"].window == 60.0
    assert limiter.status().usage["requests"].used == 1


def test_keys_get_ttl(redis_client):
    limiter = _limiter(redis_client, "ttl", window=10.0)
    limiter.acquire({"tokens": 5}, blocking=False)
    for key in (limiter.log_key("tokens"), limiter.total_key("tokens")):
        ttl = redis_client.pttl(key)
        assert 10_000 < ttl <= 20_000


def test_total_key_self_heals(redis_client):
    """A lost or corrupted running total is rebuilt from the log."""
    limiter = _limiter(redis_client, "self-heal", requests=10, tokens=100)
    limiter.acquire({"tokens": 30}, blocking=False)
    limiter.acquire({"tokens": 20}, blocking=False)

    redis_client.delete(limiter.total_key("tokens"))
    assert limiter.status().usage["tokens"].used == 50

    redis_client.set(limiter.total_key("tokens"), -7)
    assert limiter.status().usage["tokens"].used == 50
    assert redis_client.pttl(limiter.total_key("tokens")) > 0


def test_rebuilt_total_does_not_outlive_log(redis_client):
    limiter = _limiter(redis_client, "heal-ttl", requests=10, tokens=100, window=10.0)
    limiter.acquire({"tokens": 30}, blocking=False)
    redis_client.delete(limiter.total_key("tokens"))
    assert limiter.status().usage["tokens"].used == 30
    assert (
        redis_client.pttl(limiter.total_key("tokens"))
        <= redis_client.pttl(limiter.log_key("tokens")) + 100
    )


def test_expired_window_deletes_total(redis_client):
    limiter = _limiter(redis_client, "expired-total", window=0.2)
    limiter.acquire({"tokens": 5}, blocking=False)
    time.sleep(0.25)
    assert limiter.status().usage["tokens"].used == 0
    assert redis_client.exists(limiter.total_key("tokens")) == 0


# ---------------------------------------------------------------------------
# Async API
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_acquire_adjust_status(async_redis_client):
    limiter = _limiter(async_redis_client, "async", requests=2, tokens=100)
    permit = await limiter.aacquire({"tokens": 40}, blocking=False)
    assert permit.success is True

    await limiter.aadjust(permit, {"tokens": 90})
    status = await limiter.astatus()
    assert status.usage["tokens"].used == 90

    denied = await limiter.aacquire({"tokens": 20}, blocking=False)
    assert denied.success is False
    assert denied.retry_after is not None

    await limiter.aadjust(permit, 0)
    assert (await limiter.astatus()).usage["requests"].used == 0


@pytest.mark.asyncio
async def test_async_blocking_acquire_waits(async_redis_client):
    limiter = _limiter(async_redis_client, "async-blocking", requests=1, window=0.3)
    await limiter.aacquire()
    start = time.monotonic()
    assert (await limiter.aacquire()).success is True
    assert 0.2 < time.monotonic() - start < 1.0


@pytest.mark.asyncio
async def test_async_acquire_timeout_fails_fast(async_redis_client):
    limiter = _limiter(
        async_redis_client, "async-fail-fast", requests=1, window=30.0, acquire_timeout=1.0
    )
    await limiter.aacquire()
    with pytest.raises(RateLimitTimeoutError):
        await limiter.aacquire()
