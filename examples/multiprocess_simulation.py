"""Multi-process simulation of a shared distributed semaphore."""

import logging
import multiprocessing
import os
import time

import redis

from redis_semaphore import Semaphore, SemaphoreConfig


def worker(name: str, loops: int, hold_seconds: float) -> None:
    logger = logging.getLogger("redis_semaphore")
    port = int(os.environ.get("REDIS_PORT", "6379"))
    client = redis.Redis(host="localhost", port=port, db=15)
    config = SemaphoreConfig(
        name="multiprocess-demo",
        limit=2,
        acquire_timeout=None,
        retry_interval=0.1,
    )
    sem = Semaphore(client, config)

    for i in range(loops):
        sem.acquire(blocking=True)
        logger.debug("worker=%s acquired slot (%s/%s)", name, i + 1, loops)
        time.sleep(hold_seconds)
        sem.release()
        logger.debug("worker=%s released slot (%s/%s)", name, i + 1, loops)

    client.close()


def main() -> None:
    logging.basicConfig(level=logging.DEBUG, format="%(levelname)s %(name)s: %(message)s")

    port = int(os.environ.get("REDIS_PORT", "6379"))
    client = redis.Redis(host="localhost", port=port, db=15)
    client.flushdb()
    client.close()

    processes = []
    for idx in range(5):
        proc = multiprocessing.Process(
            target=worker,
            args=(f"p{idx + 1}", 3, 0.3),
            daemon=False,
        )
        proc.start()
        processes.append(proc)

    for proc in processes:
        proc.join()


if __name__ == "__main__":
    main()
