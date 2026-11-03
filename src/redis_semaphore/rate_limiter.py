"""Distributed sliding-window rate limiter with weighted costs."""

from __future__ import annotations

import asyncio
import random
import time
import uuid
from collections.abc import Mapping

import redis
import redis.asyncio as aioredis

from .errors import RateLimitTimeoutError, backend_errors
from .logger import logger
from .lua_scripts import LuaScriptRegistry, LuaScriptRunner, ScriptClientAdapter
from .types import (
    RateLimit,
    RateLimiterConfig,
    RateLimiterStatus,
    RateLimitUsage,
    RatePermit,
    TimeSource,
)

# A cost is either one value charged to every limit, or a per-limit mapping.
Cost = int | Mapping[str, int]


def _window_ms(limit: RateLimit) -> int:
    return max(1, round(limit.window * 1000))


def _check_int_cost(name: str, value: object) -> None:
    # The Lua scripts store costs as integers (INCRBY, "<cost>:" members).
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"cost for '{name}' must be an int, got {value!r}")


class RateLimiter:
    """Rate limiter enforcing several sliding-window limits atomically.

    Every limit is a sliding window ("at most N units in any W seconds"); a
    request is admitted only if it fits all of them, and is then recorded in
    each window it is charged to. Typical use is an external API with both a
    requests-per-minute and a tokens-per-minute quota.

    Usage:
        limiter = RateLimiter(redis_client, RateLimiterConfig(
            name="cert-1",
            limits=[RateLimit("requests", 100), RateLimit("tokens", 50_000)],
        ))

        # Reserve the estimated cost; blocks until it fits (or acquire_timeout)
        permit = limiter.acquire({"requests": 1, "tokens": 1_500})
        response = call_api()
        # Settle the actual cost once known (keeps the original timestamp)
        limiter.adjust(permit, {"tokens": response.total_tokens})

        # Non-blocking: check several resources and pick the soonest one
        permit = limiter.acquire({"tokens": 1_500}, blocking=False)
        if not permit.success:
            print(f"retry in {permit.retry_after:.2f}s")

    Note:
        - Admission is not FIFO: a small request may overtake a waiting large
          one. Keep per-caller ordering in your own queue if it matters.
        - Nothing is released: consumed units leave the window on their own.
          To return an unused reservation, ``adjust(permit, 0)``.
        - The same instance works with a sync (acquire/adjust/status) or an
          async (aacquire/aadjust/astatus) client, matching the client type.
    """

    __slots__ = ("_client", "_config", "_runner", "_script_client")

    def __init__(self, client: redis.Redis | aioredis.Redis, config: RateLimiterConfig) -> None:
        self._client = client
        self._config = config
        self._runner = LuaScriptRunner(LuaScriptRegistry())
        self._script_client = ScriptClientAdapter(client)

    @property
    def config(self) -> RateLimiterConfig:
        """Get the rate limiter configuration."""
        return self._config

    @property
    def _key_prefix(self) -> str:
        """Common key prefix; a hash tag with cluster_hash_tags (see Semaphore)."""
        prefix = f"{self._config.namespace}:{self._config.name}"
        if self._config.cluster_hash_tags:
            return "{" + prefix + "}"
        return prefix

    @property
    def _scope(self) -> str:
        """Identity stamped into permits, so adjust() rejects foreign ones."""
        return f"{self._config.namespace}:{self._config.name}"

    def log_key(self, limit_name: str) -> str:
        """Redis key of the sorted-set log of one limit."""
        return f"{self._key_prefix}:{limit_name}:log"

    def total_key(self, limit_name: str) -> str:
        """Redis key of the running total of one limit."""
        return f"{self._key_prefix}:{limit_name}:total"

    def _keys(self, limits: list[RateLimit]) -> list[str]:
        keys: list[str] = []
        for limit in limits:
            keys.extend((self.log_key(limit.name), self.total_key(limit.name)))
        return keys

    def _now_ms_arg(self) -> int | str:
        """Time argument for the Lua scripts ('' = server time)."""
        if self._config.time_source is TimeSource.SERVER:
            return ""
        return int(time.time() * 1000)

    # ------------------------------------------------------------------
    # Argument handling
    # ------------------------------------------------------------------

    def _check_names(self, cost: Mapping[str, int]) -> None:
        unknown = set(cost) - {limit.name for limit in self._config.limits}
        if unknown:
            raise ValueError(
                f"Unknown limit(s) {sorted(unknown)} for rate limiter '{self._config.name}'"
            )

    def _resolve_costs(self, cost: Cost) -> dict[str, int]:
        """Per-limit costs for acquire; limits missing from a mapping cost 1."""
        if isinstance(cost, int):
            costs = {limit.name: cost for limit in self._config.limits}
        else:
            self._check_names(cost)
            costs = {limit.name: cost.get(limit.name, 1) for limit in self._config.limits}
        for limit in self._config.limits:
            value = costs[limit.name]
            _check_int_cost(limit.name, value)
            if value < 0:
                raise ValueError(f"cost for '{limit.name}' must be >= 0")
            if value > limit.limit:
                raise ValueError(
                    f"cost {value} for '{limit.name}' exceeds its limit {limit.limit}; "
                    "such a request can never be admitted"
                )
        return costs

    def _resolve_adjustment(self, permit: RatePermit, cost: Cost) -> dict[str, int]:
        """New per-limit costs for adjust; limits missing from a mapping keep theirs."""
        if not permit.success or permit.identifier is None or permit.acquired_at is None:
            raise ValueError("Only an admitted permit (success=True) can be adjusted")
        if permit.limiter != self._scope:
            # Limit names alone ("requests", "tokens") recur across limiters,
            # so they cannot prove the permit is ours - the identity can.
            raise ValueError(
                f"Permit belongs to rate limiter '{permit.limiter}', not '{self._scope}'"
            )
        if set(permit.costs) != {limit.name for limit in self._config.limits}:
            raise ValueError(
                f"Permit does not match the limits of rate limiter '{self._config.name}' "
                "(its configuration changed since the permit was issued)"
            )
        if isinstance(cost, int):
            new_costs = dict.fromkeys(permit.costs, cost)
        else:
            self._check_names(cost)
            new_costs = {**permit.costs, **cost}
        # Exceeding a limit is fine here: the real cost was what it was, and
        # the debt simply delays later acquires.
        for name, value in new_costs.items():
            _check_int_cost(name, value)
            if value < 0:
                raise ValueError(f"cost for '{name}' must be >= 0")
        return new_costs

    def _build_permit(
        self,
        costs: dict[str, int],
        identifier: str,
        success: bool,
        retry_after_ms: int,
        now_ms: int,
        used: list[int],
    ) -> RatePermit:
        used_by_name = {
            limit.name: value for limit, value in zip(self._config.limits, used, strict=True)
        }
        if success:
            return RatePermit(
                success=True,
                identifier=identifier,
                costs=costs,
                acquired_at=now_ms / 1000,
                retry_after=None,
                used=used_by_name,
                limiter=self._scope,
            )
        logger.debug(
            "Rate limiter '%s' full (used=%s, cost=%s); retry in %.3fs",
            self._config.name,
            used_by_name,
            costs,
            retry_after_ms / 1000,
        )
        return RatePermit(
            success=False,
            identifier=None,
            costs=costs,
            acquired_at=None,
            retry_after=retry_after_ms / 1000,
            used=used_by_name,
            limiter=self._scope,
        )

    def _next_wait(self, permit: RatePermit, start: float) -> float:
        """Sleep before the next attempt; fails fast past acquire_timeout."""
        retry_after = permit.retry_after or 0.0
        delay = retry_after
        if self._config.wait_jitter > 0:
            delay += random.uniform(0, self._config.wait_jitter)
        if self._config.acquire_timeout is not None:
            remaining = self._config.acquire_timeout - (time.monotonic() - start)
            if retry_after > remaining:
                logger.warning(
                    "Rate limiter '%s' cannot admit request within %ss (retry in %.3fs)",
                    self._config.name,
                    self._config.acquire_timeout,
                    retry_after,
                )
                raise RateLimitTimeoutError(
                    f"Rate limiter '{self._config.name}' cannot admit the request "
                    f"within {self._config.acquire_timeout}s (retry in {retry_after:.3f}s)",
                    retry_after=retry_after,
                )
            delay = min(delay, remaining)
        return delay

    # ------------------------------------------------------------------
    # Acquire
    # ------------------------------------------------------------------

    def acquire(self, cost: Cost = 1, *, blocking: bool = True) -> RatePermit:
        """Admit a request of the given cost, waiting until it fits.

        Args:
            cost: Units to charge - an int for every limit, or a mapping
                ``{limit_name: units}`` (limits not listed are charged 1).
            blocking: If False, return at once; a denied permit carries
                ``retry_after``.

        Returns:
            RatePermit; ``success`` is False only with ``blocking=False``.

        Raises:
            ValueError: Unknown limit name, negative cost, or a cost larger
                than its limit (it could never be admitted).
            RateLimitTimeoutError: If blocking and the request cannot be
                admitted within acquire_timeout.
            BackendError: On Redis connection failure (transient) or rejected
                command such as ACL/unknown command (permanent).
        """
        costs = self._resolve_costs(cost)
        with backend_errors("rate_acquire", self._config.name):
            identifier = uuid.uuid4().hex
            start = time.monotonic()
            while True:
                permit = self._try_acquire(costs, identifier)
                if permit.success or not blocking:
                    return permit
                time.sleep(self._next_wait(permit, start))

    async def aacquire(self, cost: Cost = 1, *, blocking: bool = True) -> RatePermit:
        """Async version of :meth:`acquire`."""
        costs = self._resolve_costs(cost)
        with backend_errors("rate_acquire", self._config.name):
            identifier = uuid.uuid4().hex
            start = time.monotonic()
            while True:
                permit = await self._try_acquire_async(costs, identifier)
                if permit.success or not blocking:
                    return permit
                await asyncio.sleep(self._next_wait(permit, start))

    def _acquire_limits(self, costs: dict[str, int]) -> list[tuple[int, int, int]]:
        return [
            (limit.limit, _window_ms(limit), costs[limit.name]) for limit in self._config.limits
        ]

    def _try_acquire(self, costs: dict[str, int], identifier: str) -> RatePermit:
        success, retry_after_ms, now_ms, used = self._runner.rate_acquire(
            self._script_client,
            self._keys(list(self._config.limits)),
            identifier,
            self._now_ms_arg(),
            self._acquire_limits(costs),
        )
        return self._build_permit(costs, identifier, success, retry_after_ms, now_ms, used)

    async def _try_acquire_async(self, costs: dict[str, int], identifier: str) -> RatePermit:
        success, retry_after_ms, now_ms, used = await self._runner.rate_acquire_async(
            self._script_client,
            self._keys(list(self._config.limits)),
            identifier,
            self._now_ms_arg(),
            self._acquire_limits(costs),
        )
        return self._build_permit(costs, identifier, success, retry_after_ms, now_ms, used)

    # ------------------------------------------------------------------
    # Adjust
    # ------------------------------------------------------------------

    def adjust(self, permit: RatePermit, cost: Cost) -> RatePermit:
        """Replace the recorded cost of an admitted request.

        Use it to settle a reservation once the real cost is known (e.g. LLM
        tokens from the response), or ``adjust(permit, 0)`` to refund a
        request that was never sent. The new cost keeps the original
        admission time; a request that already left its window is not
        re-added. Going over a limit is allowed and delays later acquires.

        Args:
            permit: A successful permit returned by acquire()/aacquire().
            cost: New units - an int for every limit, or a mapping
                ``{limit_name: units}`` (limits not listed keep their cost).

        Returns:
            The same permit, with ``costs`` and ``used`` updated in place.

        Raises:
            ValueError: Permit was not admitted / belongs to another limiter,
                unknown limit name, or negative cost.
            BackendError: On Redis failure (the permit is left unchanged).
        """
        new_costs = self._resolve_adjustment(permit, cost)
        limits, keys, changes = self._adjust_args(permit, new_costs)
        if not changes:
            return permit
        with backend_errors("rate_adjust", self._config.name):
            used = self._runner.rate_adjust(
                self._script_client,
                keys,
                permit.identifier or "",
                self._now_ms_arg(),
                round((permit.acquired_at or 0) * 1000),
                changes,
            )
        return self._apply_adjustment(permit, new_costs, limits, used)

    async def aadjust(self, permit: RatePermit, cost: Cost) -> RatePermit:
        """Async version of :meth:`adjust`."""
        new_costs = self._resolve_adjustment(permit, cost)
        limits, keys, changes = self._adjust_args(permit, new_costs)
        if not changes:
            return permit
        with backend_errors("rate_adjust", self._config.name):
            used = await self._runner.rate_adjust_async(
                self._script_client,
                keys,
                permit.identifier or "",
                self._now_ms_arg(),
                round((permit.acquired_at or 0) * 1000),
                changes,
            )
        return self._apply_adjustment(permit, new_costs, limits, used)

    def _adjust_args(
        self, permit: RatePermit, new_costs: dict[str, int]
    ) -> tuple[list[RateLimit], list[str], list[tuple[int, int, int]]]:
        """Only the limits whose cost actually changes go to Redis."""
        limits = [
            limit
            for limit in self._config.limits
            if new_costs[limit.name] != permit.costs[limit.name]
        ]
        changes = [
            (_window_ms(limit), permit.costs[limit.name], new_costs[limit.name]) for limit in limits
        ]
        return limits, self._keys(limits), changes

    @staticmethod
    def _apply_adjustment(
        permit: RatePermit,
        new_costs: dict[str, int],
        limits: list[RateLimit],
        used: list[int],
    ) -> RatePermit:
        permit.costs.update(new_costs)
        permit.used.update({limit.name: value for limit, value in zip(limits, used, strict=True)})
        return permit

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def status(self) -> RateLimiterStatus:
        """Read the current consumption of every limit without consuming.

        Raises:
            BackendError: On Redis connection failure (transient) or rejected
                command such as ACL/unknown command (permanent).
        """
        with backend_errors("rate_status", self._config.name):
            used = self._runner.rate_status(
                self._script_client,
                self._keys(list(self._config.limits)),
                self._now_ms_arg(),
                [_window_ms(limit) for limit in self._config.limits],
            )
        return self._build_status(used)

    async def astatus(self) -> RateLimiterStatus:
        """Async version of :meth:`status`."""
        with backend_errors("rate_status", self._config.name):
            used = await self._runner.rate_status_async(
                self._script_client,
                self._keys(list(self._config.limits)),
                self._now_ms_arg(),
                [_window_ms(limit) for limit in self._config.limits],
            )
        return self._build_status(used)

    def _build_status(self, used: list[int]) -> RateLimiterStatus:
        return RateLimiterStatus(
            name=self._config.name,
            usage={
                limit.name: RateLimitUsage(
                    name=limit.name, limit=limit.limit, window=limit.window, used=value
                )
                for limit, value in zip(self._config.limits, used, strict=True)
            },
        )
