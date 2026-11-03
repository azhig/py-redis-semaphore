"""Shared FIFO job queues for many processes/pods, with one leader per queue.

    any process: DispatchQueue.asubmit() --XADD--> queue stream
                                                     |
                 leader of the queue (DispatchWorker) <--+
                   handler(job): wait for a resource -> job.admit() -> work
                                                     |
    submitter <--------------- BLPOP reply <---------+

- Every queue is a Redis Stream; submitting appends a job and waits for its
  result on a per-job reply list.
- Every queue has exactly one leader at a time: the worker holding the
  queue's Mutex. Workers of all processes compete for the queues, so queues
  spread over the workers.
- The leader runs the handler for one job at a time, in stream order, until
  the handler calls ``job.admit()`` (or returns). The rest of the handler runs
  concurrently with the following jobs. Waiting for a scarce resource (e.g. a
  RateLimiter permit) before ``admit()`` therefore grants that resource in
  strict FIFO order across all processes, while the slow part (the request
  itself) runs in parallel.
- If a worker dies, its Mutexes expire after leader_ttl and other workers take
  over its queues. Jobs it had taken but not answered stay in the stream's
  pending list; a live worker touches its jobs, so after reclaim_idle without
  that the queue's leader reclaims and re-runs them (delivery is
  at-least-once; ``job.reclaimed`` tells the handler).
- Deadlines are wall-clock (time.time()): process clocks must be NTP-synced.
- Requires Redis >= 6.2 (XAUTOCLAIM); both RESP2 and RESP3 clients work.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import random
import socket
import time
import uuid
from collections.abc import Awaitable, Callable, Iterable
from typing import Any

import redis
import redis.asyncio as aioredis

from .errors import (
    BackendError,
    CommandDeniedError,
    JobError,
    JobFailedError,
    JobTimeoutError,
    QueueFullError,
    RequeueJob,
    backend_errors,
    map_backend_error,
)
from .logger import logger
from .lua_scripts import LuaScriptRegistry, LuaScriptRunner, ScriptClientAdapter
from .semaphore import _BLPOP_SOCKET_TIMEOUT_FRACTION, Mutex, _client_socket_timeout
from .types import DispatchConfig, QueueStatus, WorkerHealth

# Longest single BLPOP / XREADGROUP wait; also bounded by the socket timeout.
_MAX_BLOCK = 5.0
_MIN_BLOCK = 0.01
# How often a leader re-checks its Mutex while a handler waits for admission.
_LEADERSHIP_CHECK = 1.0
# A submitter rides out transient errors while waiting for its reply for this
# long (e.g. a Sentinel failover), pausing between attempts.
_WAIT_ERROR_BUDGET = 10.0
_WAIT_ERROR_PAUSE = 0.5


class Job:
    """A job taken from a queue, passed to the handler.

    Attributes:
        queue: Queue name.
        id: Unique job id (stable across re-runs; use it for idempotency).
        payload: The JSON payload given to submit().
        created: Epoch seconds when the job was submitted.
        deadline: Epoch seconds after which the submitter stops waiting, or
            None. A handler waiting for a resource should give up past it.
        reclaimed: True if the job runs again: after the handler raised
            RequeueJob, or taken over from a worker presumed dead (then it
            may have run partly or fully before).
    """

    __slots__ = (
        "_admitted",
        "_settled",
        "created",
        "deadline",
        "entry_id",
        "id",
        "payload",
        "queue",
        "reclaimed",
    )

    def __init__(
        self,
        queue: str,
        entry_id: str,
        id: str,
        payload: Any,
        created: float,
        deadline: float | None,
        reclaimed: bool = False,
    ) -> None:
        self.queue = queue
        self.entry_id = entry_id
        self.id = id
        self.payload = payload
        self.created = created
        self.deadline = deadline
        self.reclaimed = reclaimed
        self._admitted = False
        # Set on admit() or when the handler finishes: the queue may move on.
        self._settled = asyncio.Event()

    def admit(self) -> None:
        """Let the queue move on: the next job starts while this one continues."""
        self._admitted = True
        self._settled.set()

    @property
    def admitted(self) -> bool:
        return self._admitted

    @property
    def remaining(self) -> float | None:
        """Seconds until the deadline (may be negative), or None without one."""
        if self.deadline is None:
            return None
        return self.deadline - time.time()

    def __repr__(self) -> str:
        return f"Job(queue={self.queue!r}, id={self.id!r}, admitted={self._admitted})"


JobHandler = Callable[[Job], Awaitable[Any]]


class _Keys:
    """Redis key layout shared by producers and workers."""

    def __init__(self, config: DispatchConfig) -> None:
        self._config = config

    def _prefix(self, queue: str) -> str:
        prefix = f"{self._config.namespace}:{queue}"
        if self._config.cluster_hash_tags:
            return "{" + prefix + "}"
        return prefix

    def stream(self, queue: str) -> str:
        return f"{self._prefix(queue)}:stream"

    def reply(self, queue: str, job_id: str) -> str:
        return f"{self._prefix(queue)}:reply:{job_id}"

    @staticmethod
    def leader_name(queue: str) -> str:
        """Name of the queue's leadership Mutex (in the same namespace)."""
        return f"{queue}:leader"

    def leader_owners(self, queue: str) -> str:
        """The leadership Mutex's owners key (same layout as Semaphore.owners_key)."""
        prefix = f"{self._config.namespace}:{self.leader_name(queue)}"
        if self._config.cluster_hash_tags:
            prefix = "{" + prefix + "}"
        return f"{prefix}:owners"


def _block_cap(client: object) -> float:
    """Longest safe single blocking wait for this client."""
    socket_timeout = _client_socket_timeout(client)
    if socket_timeout is None or socket_timeout <= 0:
        return _MAX_BLOCK
    return min(_MAX_BLOCK, socket_timeout * _BLPOP_SOCKET_TIMEOUT_FRACTION)


def _tolerate_wait_error(exc: Exception, queue: str, failing_since: float | None) -> float:
    """Ride out a transient error while waiting for a reply (the job is queued).

    Surfacing a blip would lose the result of a job that still runs. Returns
    the start of the current failure streak; raises the mapped BackendError if
    the error is permanent or the streak exceeds _WAIT_ERROR_BUDGET.
    """
    mapped = map_backend_error(exc, operation="dispatch_wait", name=queue)
    now = time.monotonic()
    if failing_since is None:
        failing_since = now
    if not mapped.is_transient or now - failing_since >= _WAIT_ERROR_BUDGET:
        raise mapped from exc
    logger.warning("Waiting for a reply in queue '%s' failed, retrying: %s", queue, exc)
    return failing_since


def _wait_error_pause(give_up: float | None) -> float:
    if give_up is None:
        return _WAIT_ERROR_PAUSE
    return max(0.0, min(_WAIT_ERROR_PAUSE, give_up - time.monotonic()))


def _check_queue_name(queue: str) -> None:
    if not queue:
        raise ValueError("queue name must not be empty")


def _parse_reply(raw: bytes | str) -> Any:
    data = json.loads(raw)
    if data.get("ok"):
        return data.get("result")
    raise JobFailedError(data.get("error_type", "Error"), data.get("message", ""), data.get("data"))


# =============================================================================
# Producer side
# =============================================================================


class DispatchQueue:
    """Submits jobs to shared queues and waits for their results.

    Usage:
        queue = DispatchQueue(redis_client)
        result = await queue.asubmit("cert-1", {"prompt": "hi"}, timeout=60)

    Works with a sync client (submit) or an async client (asubmit).
    """

    __slots__ = ("_block_cap", "_client", "_config", "_keys", "_runner", "_script_client")

    def __init__(
        self, client: redis.Redis | aioredis.Redis, config: DispatchConfig | None = None
    ) -> None:
        self._client = client
        self._config = config or DispatchConfig()
        self._keys = _Keys(self._config)
        self._runner = LuaScriptRunner(LuaScriptRegistry())
        self._script_client = ScriptClientAdapter(client)
        self._block_cap = _block_cap(client)

    @property
    def config(self) -> DispatchConfig:
        return self._config

    def stream_key(self, queue: str) -> str:
        """Redis key of the queue's stream."""
        return self._keys.stream(queue)

    def _fields(self, job_id: str, payload: Any, timeout: float | None) -> list[str]:
        now = time.time()
        return [
            "id",
            job_id,
            "payload",
            json.dumps(payload),
            "created",
            repr(now),
            "deadline",
            "" if timeout is None else repr(now + timeout),
        ]

    def _check_args(self, queue: str, timeout: float | None) -> None:
        _check_queue_name(queue)
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be > 0 or None")

    def _full(self, queue: str) -> QueueFullError:
        return QueueFullError(f"Queue '{queue}' is full ({self._config.max_queue_size} jobs)")

    def _timed_out(self, queue: str, job_id: str, timeout: float | None) -> JobTimeoutError:
        return JobTimeoutError(f"No result for job {job_id} in queue '{queue}' within {timeout}s")

    async def asubmit(
        self, queue: str, payload: Any = None, *, timeout: float | None = None
    ) -> Any:
        """Put a job into the queue and wait for the handler's result.

        Args:
            queue: Queue name.
            payload: JSON-serializable job data.
            timeout: Max seconds to wait for the result (None = forever).
                Also the job's deadline: a job still queued past it is
                dropped instead of run.

        Returns:
            Whatever the handler returned (after a JSON round-trip).

        Raises:
            QueueFullError: The queue holds max_queue_size jobs.
            JobTimeoutError: No result within timeout.
            JobFailedError: The handler raised.
            BackendError: On Redis failure.
        """
        self._check_args(queue, timeout)
        job_id = uuid.uuid4().hex
        fields = self._fields(job_id, payload, timeout)
        with backend_errors("dispatch_submit", queue):
            entry_id = await self._runner.dispatch_enqueue_async(
                self._script_client,
                self._keys.stream(queue),
                self._config.max_queue_size or 0,
                fields,
            )
            if entry_id is None:
                raise self._full(queue)

            give_up = None if timeout is None else time.monotonic() + timeout
            reply_key = self._keys.reply(queue, job_id)
            failing_since: float | None = None
            while True:
                block = self._block_cap
                if give_up is not None:
                    block = min(block, give_up - time.monotonic())
                    if block <= 0:
                        raise self._timed_out(queue, job_id, timeout)
                    # Redis rejects exponent notation such as 3e-05 as a timeout.
                    block = max(block, _MIN_BLOCK)
                try:
                    reply = await self._client.blpop([reply_key], timeout=block)  # type: ignore[misc]
                except (redis.RedisError, OSError) as exc:
                    failing_since = _tolerate_wait_error(exc, queue, failing_since)
                    await asyncio.sleep(_wait_error_pause(give_up))
                    continue
                failing_since = None
                if reply is not None:
                    return _parse_reply(reply[1])

    def submit(self, queue: str, payload: Any = None, *, timeout: float | None = None) -> Any:
        """Sync version of :meth:`asubmit` (needs a sync client)."""
        self._check_args(queue, timeout)
        job_id = uuid.uuid4().hex
        fields = self._fields(job_id, payload, timeout)
        with backend_errors("dispatch_submit", queue):
            entry_id = self._runner.dispatch_enqueue(
                self._script_client,
                self._keys.stream(queue),
                self._config.max_queue_size or 0,
                fields,
            )
            if entry_id is None:
                raise self._full(queue)

            give_up = None if timeout is None else time.monotonic() + timeout
            reply_key = self._keys.reply(queue, job_id)
            failing_since: float | None = None
            while True:
                block = self._block_cap
                if give_up is not None:
                    block = min(block, give_up - time.monotonic())
                    if block <= 0:
                        raise self._timed_out(queue, job_id, timeout)
                    # Redis rejects exponent notation such as 3e-05 as a timeout.
                    block = max(block, _MIN_BLOCK)
                try:
                    reply = self._client.blpop([reply_key], timeout=block)
                except (redis.RedisError, OSError) as exc:
                    failing_since = _tolerate_wait_error(exc, queue, failing_since)
                    time.sleep(_wait_error_pause(give_up))
                    continue
                failing_since = None
                if reply is not None:
                    return _parse_reply(reply[1])  # type: ignore[index]

    async def astatus(self, queue: str) -> QueueStatus:
        """Current state of a queue, e.g. for a health check or dashboard.

        Works from any process; reflects all producers and workers.

        Raises:
            BackendError: On Redis failure.
        """
        _check_queue_name(queue)
        with backend_errors("dispatch_status", queue):
            length, in_progress, oldest_ms = await self._runner.dispatch_status_async(
                self._script_client, self._keys.stream(queue), self._config.group
            )
            leaders, _, _ = await self._runner.status_async(
                self._script_client, self._keys.leader_owners(queue), "", None
            )
        return self._build_status(queue, length, in_progress, oldest_ms, leaders)

    async def astatus_many(self, queues: Iterable[str]) -> dict[str, QueueStatus]:
        """:meth:`astatus` of several queues, queried concurrently."""
        names = list(dict.fromkeys(queues))
        statuses = await asyncio.gather(*(self.astatus(queue) for queue in names))
        return dict(zip(names, statuses, strict=True))

    def status(self, queue: str) -> QueueStatus:
        """Sync version of :meth:`astatus` (needs a sync client)."""
        _check_queue_name(queue)
        with backend_errors("dispatch_status", queue):
            length, in_progress, oldest_ms = self._runner.dispatch_status(
                self._script_client, self._keys.stream(queue), self._config.group
            )
            leaders, _, _ = self._runner.status(
                self._script_client, self._keys.leader_owners(queue), "", None
            )
        return self._build_status(queue, length, in_progress, oldest_ms, leaders)

    def status_many(self, queues: Iterable[str]) -> dict[str, QueueStatus]:
        """Sync version of :meth:`astatus_many` (queries one by one)."""
        return {queue: self.status(queue) for queue in dict.fromkeys(queues)}

    @staticmethod
    def _build_status(
        queue: str, length: int, in_progress: int, oldest_ms: int, leaders: int
    ) -> QueueStatus:
        return QueueStatus(
            name=queue,
            length=length,
            in_progress=in_progress,
            waiting=max(0, length - in_progress),
            oldest_age=oldest_ms / 1000 if oldest_ms >= 0 else None,
            has_leader=leaders > 0,
        )

    async def alength(self, queue: str) -> int:
        """Jobs in the queue (waiting + in progress)."""
        with backend_errors("dispatch_length", queue):
            return int(await self._client.xlen(self._keys.stream(queue)))

    def length(self, queue: str) -> int:
        """Sync version of :meth:`alength`."""
        with backend_errors("dispatch_length", queue):
            return int(self._client.xlen(self._keys.stream(queue)))  # type: ignore[arg-type]


# =============================================================================
# Worker side
# =============================================================================


def default_worker_id() -> str:
    """hostname (the pod name in Kubernetes) + pid + random suffix."""
    return f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"


def _text(value: bytes | str) -> str:
    return value.decode() if isinstance(value, bytes) else value


class DispatchWorker:
    """Competes for queue leadership and runs the handler for the queues it leads.

    Usage:
        async def handler(job: Job) -> dict:
            permit = await limiter.aacquire(...)   # in FIFO order across all pods
            job.admit()                            # the next job may start now
            response = await http.post(...)        # concurrent with next jobs
            return {"status": response.status_code}

        worker = DispatchWorker(async_redis, ["cert-1", "cert-2"], handler)
        await worker.run()      # until stop() or cancellation

    Run as many workers per process and as many processes as needed; each
    queue is led by exactly one of them at a time. Requires an async client.
    """

    def __init__(
        self,
        client: aioredis.Redis,
        queues: Iterable[str],
        handler: JobHandler,
        config: DispatchConfig | None = None,
        *,
        worker_id: str | None = None,
    ) -> None:
        if not isinstance(client, aioredis.Redis):
            raise TypeError("DispatchWorker requires a redis.asyncio client")
        self._queues = list(dict.fromkeys(queues))
        if not self._queues:
            raise ValueError("queues must not be empty")
        for queue in self._queues:
            _check_queue_name(queue)
        self.id = worker_id or default_worker_id()
        self._client = client
        self._handler = handler
        self._config = config or DispatchConfig()
        self._keys = _Keys(self._config)
        self._block_cap = _block_cap(client)
        self._stopping = asyncio.Event()
        self._running = False
        # Health: last main loop tick and last Redis error (monotonic time).
        self._last_loop: float | None = None
        self._last_error: tuple[float, str] | None = None
        # A permanent backend error (ACL, unsupported command): re-electing
        # would fail the same way forever, so run() stops and raises it.
        self._fatal: BackendError | None = None
        self._leading: dict[str, asyncio.Task[None]] = {}
        self._handlers: set[asyncio.Task[None]] = set()
        # stream -> entry ids taken by this worker and not answered yet
        self._taken: dict[str, set[str]] = {}
        # streams with a job just requeued: scan for it before reading new jobs
        self._reclaim_now: set[str] = set()
        # (stream, entry id) -> when this worker requeued it; such entries are
        # expected in the next reclaim and are not reported as abandoned
        self._requeued: dict[tuple[str, str], float] = {}

    @property
    def leading(self) -> list[str]:
        """Queues this worker currently leads."""
        return [queue for queue, task in self._leading.items() if not task.done()]

    def health(self) -> WorkerHealth:
        """Local health snapshot of this worker (no Redis round-trip).

        Cheap enough for every probe request; combine with
        :meth:`DispatchQueue.astatus` for the queues' global state.
        """
        now = time.monotonic()
        stall_limit = 5 * self._config.election_interval
        loop_age = None if self._last_loop is None else now - self._last_loop
        error_age = None if self._last_error is None else now - self._last_error[0]
        healthy = (
            self._running
            and not self._stopping.is_set()
            and loop_age is not None
            and loop_age < stall_limit
            and (error_age is None or error_age >= stall_limit)
        )
        return WorkerHealth(
            worker_id=self.id,
            running=self._running,
            stopping=self._running and self._stopping.is_set(),
            leading=tuple(self.leading),
            jobs_in_progress=len(self._handlers),
            last_loop_age=loop_age,
            last_error=None if self._last_error is None else self._last_error[1],
            last_error_age=error_age,
            healthy=healthy,
        )

    def _record_error(self, exc: BaseException) -> None:
        self._last_error = (time.monotonic(), f"{type(exc).__name__}: {exc}")

    def stop(self) -> None:
        """Ask run() to finish: stop taking jobs, let admitted jobs complete."""
        self._stopping.set()

    async def run(self) -> None:
        """Lead queues until stop() or cancellation, then shut down gracefully.

        On shutdown jobs not admitted yet are handed back (their handlers are
        cancelled and the next leader picks them up at once); admitted jobs
        run to completion.

        Raises:
            PermanentBackendError: Leading failed in a way a re-election would
                repeat forever (ACL denial, Redis < 6.2 without XAUTOCLAIM).
        """
        if self._running:
            raise RuntimeError("DispatchWorker.run() is already running")
        self._running = True
        # _stopping is cleared on exit (not here), so a stop() issued before
        # run() starts is honoured.
        touch = asyncio.create_task(self._touch_loop())
        try:
            while not self._stopping.is_set():
                self._last_loop = time.monotonic()
                await self._elect()
                pause = self._config.election_interval * random.uniform(0.5, 1.5)
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self._stopping.wait(), timeout=pause)
        finally:
            self._stopping.set()
            await asyncio.gather(*self._leading.values(), return_exceptions=True)
            await asyncio.gather(*self._handlers, return_exceptions=True)
            touch.cancel()
            await asyncio.gather(touch, return_exceptions=True)
            self._leading.clear()
            self._stopping.clear()
            self._running = False
            self._last_loop = None
        if self._fatal is not None:
            fatal, self._fatal = self._fatal, None
            raise fatal

    def _stop_fatally(self, error: BackendError, queue: str) -> None:
        """A permanent error would repeat on every election: stop, don't thrash."""
        logger.error(
            "Worker %s: queue '%s' failed permanently, stopping: %s", self.id, queue, error
        )
        if self._fatal is None:
            self._fatal = error
        self._stopping.set()

    async def _elect(self) -> None:
        """Try to take one more free queue (one per round spreads queues)."""
        self._leading = {q: t for q, t in self._leading.items() if not t.done()}
        cap = self._config.max_queues_per_worker
        if cap is not None and len(self._leading) >= cap:
            return
        free = [queue for queue in self._queues if queue not in self._leading]
        random.shuffle(free)
        for queue in free:
            mutex = Mutex(
                self._client,
                self._keys.leader_name(queue),
                namespace=self._config.namespace,
                lock_timeout=self._config.leader_ttl,
                cluster_hash_tags=self._config.cluster_hash_tags,
            )
            try:
                acquired = (await mutex.aacquire(blocking=False)).success
            except Exception as exc:
                self._record_error(exc)
                if isinstance(exc, BackendError) and not exc.is_transient:
                    self._stop_fatally(exc, queue)
                else:
                    logger.warning(
                        "Worker %s: election for '%s' failed", self.id, queue, exc_info=True
                    )
                return
            self._last_error = None  # Redis answered
            if acquired:
                logger.info("Worker %s leads queue '%s'", self.id, queue)
                self._leading[queue] = asyncio.create_task(self._lead(queue, mutex))
                return

    # -- leading a queue ------------------------------------------------------

    async def _lead(self, queue: str, mutex: Mutex) -> None:
        stream = self._keys.stream(queue)
        next_reclaim = 0.0
        reclaim_cursor = "0-0"
        try:
            await self._ensure_group(stream)
            while mutex.is_acquired and not self._stopping.is_set():
                entries: list[tuple[Any, Any]] = []
                reclaimed = False
                forced = stream in self._reclaim_now
                if forced or time.monotonic() >= next_reclaim:
                    # Requeued jobs and jobs of dead workers go before unread ones.
                    self._reclaim_now.discard(stream)
                    if forced:
                        reclaim_cursor = "0-0"
                    entries, reclaim_cursor = await self._reclaim(stream, reclaim_cursor)
                    reclaimed = bool(entries)
                    if reclaim_cursor == "0-0":
                        # Whole pending list scanned; otherwise continue at once.
                        next_reclaim = time.monotonic() + self._config.reclaim_interval
                if not entries:
                    entries = await self._read(stream)
                # Touch the whole batch from now on: a job waiting behind slow
                # admissions must not look abandoned and be reclaimed twice.
                taken = self._taken.setdefault(stream, set())
                taken.update(_text(e[0]) for e in entries if e[0] is not None)
                done = 0
                try:
                    for entry_id, fields in entries:
                        done += 1
                        if not await self._dispatch(
                            queue, stream, entry_id, fields, reclaimed, mutex
                        ):
                            # Lost leadership / stopping: hand back the rest at once.
                            await self._hand_back(stream, [e[0] for e in entries[done:]])
                            return
                finally:
                    # Entries never dispatched (e.g. an error mid-batch) must
                    # not be touched forever: leave them reclaimable.
                    for entry_id, _ in entries[done:]:
                        if entry_id is not None:
                            taken.discard(_text(entry_id))
        except (redis.RedisError, OSError, BackendError) as exc:
            self._record_error(exc)
            mapped = (
                exc
                if isinstance(exc, BackendError)
                else map_backend_error(exc, operation="dispatch_lead", name=queue)
            )
            if mapped.is_transient:
                logger.warning("Worker %s: leading '%s' failed", self.id, queue, exc_info=True)
            else:
                self._stop_fatally(mapped, queue)
        finally:
            if mutex.is_acquired:
                try:
                    await mutex.arelease()
                except Exception:
                    logger.debug("Worker %s: release of '%s' failed", self.id, queue, exc_info=True)
            logger.info("Worker %s stopped leading queue '%s'", self.id, queue)

    async def _ensure_group(self, stream: str) -> None:
        try:
            await self._client.xgroup_create(stream, self._config.group, id="0", mkstream=True)
        except redis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def _read(self, stream: str) -> list[tuple[Any, Any]]:
        block_ms = int(min(self._config.read_timeout, self._block_cap) * 1000)
        try:
            response = await self._client.xreadgroup(
                self._config.group, self.id, {stream: ">"}, count=1, block=max(1, block_ms)
            )
        except redis.ResponseError as exc:
            if "NOGROUP" not in str(exc):
                raise
            await self._ensure_group(stream)  # the stream was deleted meanwhile
            return []
        return self._entries(response)

    @staticmethod
    def _entries(response: Any) -> list[tuple[Any, Any]]:
        """Entries of an XREADGROUP reply: RESP2 is a list of (stream, entries)
        pairs, RESP3 (protocol=3) a dict of entry batches per stream."""
        if not response:
            return []
        if isinstance(response, dict):
            return [entry for batch in next(iter(response.values())) for entry in batch]
        return list(response[0][1])

    async def _reclaim(
        self, stream: str, start_id: str = "0-0"
    ) -> tuple[list[tuple[Any, Any]], str]:
        """Take over jobs whose worker stopped touching them, and prune dead consumers.

        Returns the reclaimed entries and the XAUTOCLAIM cursor to continue
        from ("0-0" once the whole pending list has been scanned).
        """
        try:
            reply = await self._client.xautoclaim(
                stream,
                self._config.group,
                self.id,
                min_idle_time=int(self._config.reclaim_idle * 1000),
                start_id=start_id,
                count=100,
            )
        except redis.ResponseError as exc:
            if "unknown command" in str(exc).lower():
                raise CommandDeniedError(
                    f"DispatchWorker requires Redis >= 6.2 (XAUTOCLAIM): {exc}",
                    operation="dispatch_reclaim",
                    original=exc,
                ) from exc
            raise
        cursor = _text(reply[0])
        entries = list(reply[1])
        now = time.monotonic()
        abandoned = 0
        for entry_id, _ in entries:
            key = (stream, _text(entry_id)) if entry_id is not None else None
            if key in self._requeued:
                del self._requeued[key]  # requeued by this worker on purpose
                continue
            abandoned += 1
        if abandoned:
            logger.warning(
                "Worker %s reclaimed %d abandoned job(s) from %s", self.id, abandoned, stream
            )
        # Forget requeues another worker picked up (leadership moved meanwhile).
        stale = now - 10 * self._config.reclaim_idle
        self._requeued = {k: t for k, t in self._requeued.items() if t > stale}
        for consumer in await self._client.xinfo_consumers(stream, self._config.group):
            name = _text(consumer["name"])
            idle_ms = self._config.reclaim_idle * 1000
            if name != self.id and consumer["pending"] == 0 and consumer["idle"] > idle_ms:
                await self._client.xgroup_delconsumer(stream, self._config.group, name)
        return entries, cursor

    async def _touch_loop(self) -> None:
        """Reset the idle time of our unanswered jobs, so they are not reclaimed."""
        while True:
            await asyncio.sleep(self._config.reclaim_idle / 3)
            for stream, ids in list(self._taken.items()):
                if not ids:
                    continue
                try:
                    await self._client.xclaim(
                        stream, self._config.group, self.id, 0, list(ids), justid=True
                    )
                except (redis.RedisError, OSError):
                    logger.warning("Worker %s failed to touch jobs in %s", self.id, stream)

    async def _hand_back(self, stream: str, entry_ids: list[Any]) -> None:
        """Make taken-but-unstarted jobs reclaimable by the next leader right away."""
        # Redis 6.2 XAUTOCLAIM reports entries deleted while pending as nil ids.
        entry_ids = [entry_id for entry_id in entry_ids if entry_id is not None]
        for entry_id in entry_ids:
            self._taken.get(stream, set()).discard(_text(entry_id))
        if not entry_ids:
            return
        try:
            await self._client.xclaim(
                stream,
                self._config.group,
                self.id,
                0,
                entry_ids,
                idle=int(self._config.reclaim_idle * 1000),
                justid=True,
            )
        except (redis.RedisError, OSError):
            logger.debug("Worker %s: hand back failed", self.id, exc_info=True)

    # -- one job --------------------------------------------------------------

    async def _dispatch(
        self,
        queue: str,
        stream: str,
        entry_id: Any,
        fields: Any,
        reclaimed: bool,
        mutex: Mutex,
    ) -> bool:
        """Run one job until it is admitted. Returns False if the queue must be left."""
        if entry_id is None:
            # Deleted while pending: Redis < 7 replies nil to XAUTOCLAIM, with
            # no id to acknowledge.
            return True
        entry = _text(entry_id)
        if fields is None:
            self._taken.get(stream, set()).discard(entry)
            await self._client.xack(stream, self._config.group, entry)
            return True
        try:
            job = self._decode(queue, entry, fields, reclaimed)
        except (KeyError, ValueError):
            logger.error("Worker %s: dropping malformed job %s in %s", self.id, entry, stream)
            self._taken.get(stream, set()).discard(entry)
            await self._client.xack(stream, self._config.group, entry)
            await self._client.xdel(stream, entry)
            return True
        if job.remaining is not None and job.remaining <= 0:
            # The submitter has given up already.
            logger.info("Worker %s: dropping expired job %s", self.id, job.id)
            self._taken.get(stream, set()).discard(entry)
            await self._client.xack(stream, self._config.group, entry)
            await self._client.xdel(stream, entry)
            return True

        self._taken.setdefault(stream, set()).add(entry)
        task = asyncio.create_task(self._run_job(stream, job))
        self._handlers.add(task)
        task.add_done_callback(self._handlers.discard)
        task.add_done_callback(lambda _: job._settled.set())

        try:
            while not job._settled.is_set():
                if not mutex.is_acquired or self._stopping.is_set():
                    await self._abandon(stream, job, task)
                    return False
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(job._settled.wait(), timeout=_LEADERSHIP_CHECK)
        except asyncio.CancelledError:
            if not job._settled.is_set():
                await self._abandon(stream, job, task)
            raise
        return True

    async def _abandon(self, stream: str, job: Job, task: asyncio.Task[None]) -> None:
        """Cancel a not-yet-admitted job and hand it back to the queue."""
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        if job.admitted or not task.cancelled():
            return  # it got admitted or finished meanwhile
        await self._hand_back(stream, [job.entry_id])

    @staticmethod
    def _decode(queue: str, entry_id: str, fields: dict[Any, Any], reclaimed: bool) -> Job:
        data = {_text(key): _text(value) for key, value in fields.items()}
        return Job(
            queue=queue,
            entry_id=entry_id,
            id=data["id"],
            payload=json.loads(data["payload"]),
            created=float(data["created"]),
            deadline=float(data["deadline"]) if data["deadline"] else None,
            reclaimed=reclaimed,
        )

    async def _run_job(self, stream: str, job: Job) -> None:
        try:
            result = await self._handler(job)
            reply = json.dumps({"ok": True, "result": result})
        except asyncio.CancelledError:
            raise  # handed back (not admitted) or forced shutdown: no reply
        except RequeueJob as exc:
            await self._requeue(stream, job, exc.delay)
            return
        except JobError as exc:
            reply = self._error_reply(exc, exc.data)
        except Exception as exc:
            logger.warning("Worker %s: job %s failed", self.id, job.id, exc_info=True)
            reply = self._error_reply(exc, {})
        await self._finish(stream, job, reply)

    async def _requeue(self, stream: str, job: Job, delay: float) -> None:
        """Put the job back (no reply): the leader re-runs it before unread jobs."""
        logger.info("Worker %s: requeue job %s (delay %.3fs)", self.id, job.id, delay)
        if delay > 0:
            # Still taken (and touched) meanwhile; stop() cuts the wait short.
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._stopping.wait(), timeout=delay)
        self._requeued[(stream, job.entry_id)] = time.monotonic()
        await self._hand_back(stream, [job.entry_id])
        self._reclaim_now.add(stream)

    @staticmethod
    def _error_reply(exc: Exception, data: dict) -> str:
        message = {"ok": False, "error_type": type(exc).__name__, "message": str(exc)}
        try:
            return json.dumps({**message, "data": data})
        except (TypeError, ValueError):
            return json.dumps(message)

    async def _finish(self, stream: str, job: Job, reply: str) -> None:
        """Deliver the reply and remove the job from the queue, atomically."""
        reply_key = self._keys.reply(job.queue, job.id)
        try:
            pipe = self._client.pipeline(transaction=True)
            pipe.lpush(reply_key, reply)
            pipe.pexpire(reply_key, int(self._config.reply_ttl * 1000))
            pipe.xack(stream, self._config.group, job.entry_id)
            pipe.xdel(stream, job.entry_id)
            await pipe.execute()
        except (redis.RedisError, OSError):
            # Left pending and untouched: the job is re-run after reclaim_idle.
            logger.error("Worker %s: could not finish job %s", self.id, job.id, exc_info=True)
        finally:
            self._taken.get(stream, set()).discard(job.entry_id)
