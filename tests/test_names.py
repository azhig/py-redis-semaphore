"""Names are opaque strings: any non-empty name works and never collides.

Semaphore/Mutex, RateLimiter and dispatch queue names go into Redis keys
as-is; the library never parses them (e.g. "agent:division:model" is just an
application convention).
"""

import asyncio

import pytest
from redis.crc import key_slot

from redis_semaphore import (
    DispatchConfig,
    DispatchQueue,
    DispatchWorker,
    Job,
    Mutex,
    RateLimit,
    RateLimiter,
    RateLimiterConfig,
    Semaphore,
    SemaphoreConfig,
)
from redis_semaphore.dispatch import _Keys

pytestmark = pytest.mark.timeout(30)

# One part, many parts, empty segments, edges, spaces, unicode, braces.
NAMES = [
    "q",
    "agent:mmb:max",
    "a:b:c:d:e:f",
    "a::b",
    ":leading",
    "trailing:",
    "with space",
    "агент:подразделение:модель",
    "{brace}:x",
    "a}b{c",
]
# Prefix pairs that would collide if a name were split at ':'.
NESTED = ["a", "a:b", "a:b:c"]


def test_semaphore_any_name(redis_client):
    def semaphore(name: str) -> Semaphore:
        return Semaphore(redis_client, SemaphoreConfig(name=name, limit=1, namespace="t-names"))

    for name in NAMES:
        sem = semaphore(name)
        assert sem.acquire(blocking=False).success, name
        assert not semaphore(name).acquire(blocking=False).success, name
        sem.release()

        # A Mutex is a limit=1 Semaphore: same name + namespace = same lock.
        lock = Mutex(redis_client, name, namespace="t-names-mutex")
        assert lock.acquire(blocking=False).success, name
        assert (
            not Mutex(redis_client, name, namespace="t-names-mutex").acquire(blocking=False).success
        )
        lock.release()


def test_semaphore_nested_names_are_independent(redis_client):
    sems = [
        Semaphore(redis_client, SemaphoreConfig(name=name, limit=1, namespace="t-names"))
        for name in NESTED
    ]
    assert all(sem.acquire(blocking=False).success for sem in sems)


def test_rate_limiter_any_name(redis_client):
    for name in NAMES:
        limiter = RateLimiter(
            redis_client,
            RateLimiterConfig(name=name, limits=[RateLimit("requests", 1)], namespace="t-names"),
        )
        assert limiter.acquire(blocking=False).success, name
        assert not limiter.acquire(blocking=False).success, name


def test_rate_limiter_nested_names_are_independent(redis_client):
    limiters = [
        RateLimiter(
            redis_client,
            RateLimiterConfig(name=name, limits=[RateLimit("requests", 1)], namespace="t-names"),
        )
        for name in NESTED
    ]
    assert all(limiter.acquire(blocking=False).success for limiter in limiters)
    assert len({limiter.log_key("requests") for limiter in limiters}) == len(NESTED)


def test_limit_name_must_not_contain_colon():
    """Otherwise limiter "a" + limit "b:x" would share keys with "a:b" + "x"."""
    with pytest.raises(ValueError, match="must not contain ':'"):
        RateLimit("max:requests", 1)


async def test_dispatch_any_queue_name(async_redis_client):
    async def handler(job: Job):
        return job.queue

    config = DispatchConfig(
        namespace="t-names", election_interval=0.02, read_timeout=0.1, reclaim_interval=0.1
    )
    queue = DispatchQueue(async_redis_client, config)
    worker = DispatchWorker(async_redis_client, NAMES + NESTED, handler, config)
    task = asyncio.create_task(worker.run())
    try:
        results = await asyncio.gather(
            *(queue.asubmit(name, None, timeout=10) for name in NAMES + NESTED)
        )
        assert results == NAMES + NESTED  # each job came from its own queue
        statuses = await queue.astatus_many(NAMES + NESTED)
        assert all(s.has_leader and s.length == 0 for s in statuses.values())
    finally:
        worker.stop()
        await asyncio.wait_for(task, 10)


def test_dispatch_keys_never_collide():
    keys = _Keys(DispatchConfig())
    seen: dict[str, tuple[str, str]] = {}
    for name in NAMES + NESTED + ["q:stream", "q:reply", "q:leader", "q:leader:owners"]:
        for kind, key in (
            ("stream", keys.stream(name)),
            ("reply", keys.reply(name, "0" * 32)),
            ("leader", keys.leader_owners(name)),
        ):
            assert key not in seen, (key, seen.get(key), (name, kind))
            seen[key] = (name, kind)


@pytest.mark.parametrize("name", NAMES)
def test_cluster_hash_tags_keep_entity_keys_in_one_slot(redis_client, name):
    """Multi-key scripts/MULTI need one slot per entity - even with braces in names."""
    sem = Semaphore(
        redis_client,
        SemaphoreConfig(name=name, limit=1, namespace="t", cluster_hash_tags=True),
    )
    assert len({key_slot(k.encode()) for k in (sem.owners_key, sem.fencing_key)}) == 1

    limiter = RateLimiter(
        redis_client,
        RateLimiterConfig(
            name=name,
            limits=[RateLimit("requests", 1), RateLimit("tokens", 1)],
            namespace="t",
            cluster_hash_tags=True,
        ),
    )
    limiter_keys = [
        limiter.log_key("requests"),
        limiter.total_key("requests"),
        limiter.log_key("tokens"),
        limiter.total_key("tokens"),
    ]
    assert len({key_slot(k.encode()) for k in limiter_keys}) == 1

    keys = _Keys(DispatchConfig(namespace="t", cluster_hash_tags=True))
    assert key_slot(keys.stream(name).encode()) == key_slot(keys.reply(name, "id").encode())
