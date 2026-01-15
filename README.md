```bash
 ╔════╗
 ║ 🔴 ║   ╔═════╗ ╔══════╗╔═════╗ ╔═╗╔══════╗
 ║    ║   ║ ╔══╗╚╗║ ╔════╝║ ╔═╗ ╚╗║ ║║ ╔════╝
 ║ 🟡 ║   ║ ╚══╝╔╝║ ╚══╗  ║ ║ ╚╗ ║║ ║║ ╚════╗
 ║    ║   ║ ╔═╗ ╚╗║ ╔══╝  ║ ║ ╔╝ ║║ ║╚════╗ ║
 ║ 🟢 ║   ║ ║ ╚╗ ║║ ╚════╗║ ╚═╝ ╔╝║ ║╔════╝ ║
 ║    ║   ╚═╝  ╚═╝╚══════╝╚═════╝ ╚═╝╚══════╝
 ╚════╝
   ||     ███████ ███████ ███    ███  █████  ██████  ██   ██  ██████  ██████  ███████
  ▄▄▄▄    ██      ██      ████  ████ ██   ██ ██   ██ ██   ██ ██    ██ ██   ██ ██
  ████    ███████ █████   ██ ████ ██ ███████ ██████  ███████ ██    ██ ██████  █████
               ██ ██      ██  ██  ██ ██   ██ ██      ██   ██ ██    ██ ██   ██ ██
          ███████ ███████ ██      ██ ██   ██ ██      ██   ██  ██████  ██   ██ ███████

          🔒 Distributed Synchronization Primitives on Redis 🔒
             Counting Semaphores • Mutexes • Fencing Tokens
                   Sync/Async • Sentinel • Heartbeat

```

[![PyPI version](https://img.shields.io/pypi/v/py-redis-semaphore.svg)](https://pypi.org/project/py-redis-semaphore/)
[![Python versions](https://img.shields.io/pypi/pyversions/py-redis-semaphore.svg)](https://pypi.org/project/py-redis-semaphore/)
[![License](https://img.shields.io/pypi/l/py-redis-semaphore.svg)](https://pypi.org/project/py-redis-semaphore/)
[![CI](https://github.com/azhig/py-redis-semaphore/actions/workflows/ci.yml/badge.svg)](https://github.com/azhig/py-redis-semaphore/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/azhig/py-redis-semaphore/branch/main/graph/badge.svg)](https://codecov.io/gh/azhig/py-redis-semaphore)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

Распределённый семафор и mutex на Redis с поддержкой Sentinel, sync/async API и автоматическим heartbeat.

## Возможности

- **Counting Semaphore** - ограничение до N одновременных доступов к ресурсу
- **Mutex** - эксклюзивная блокировка (бинарный семафор)
- **Redis Sentinel** - поддержка failover для высокой доступности
- **Sync и Async API** - работа с threading и asyncio
- **Heartbeat** - автоматическое продление TTL блокировки
- **Fencing Tokens** - защита от race conditions при GC паузах
- **Атомарные операции** - все критичные операции через Lua скрипты
- **Гибкие стратегии ожидания** - polling с exponential backoff или BLPOP для эффективного ожидания
- **Docker для Redis** - готовый контейнер для локальной разработки

## Установка

```bash
pip install py-redis-semaphore
```

Или с использованием uv:

```bash
uv add py-redis-semaphore
```

## Быстрый старт

### Mutex (эксклюзивная блокировка)

Эксклюзивная блокировка. Подходит, когда к ресурсу должен иметь доступ
только один процесс/инстанс в любой момент времени (например, миграции
БД, cron-задача, пересчет кэша).

```python
import redis
from redis_semaphore import Mutex

client = redis.Redis()

with Mutex(client, "my-resource") as lock:
    print(f"Fencing token: {lock.fencing_token}")
    # Только один процесс может выполнять этот код
```

### Counting Semaphore

Ограничивает количество одновременных владельцев ресурса до N. Используйте,
когда ресурс допускает ограниченную параллельность (пул БД, лимит внешнего API,
ограничение CPU/IO задач).

```python
from redis_semaphore import Semaphore, SemaphoreConfig

config = SemaphoreConfig(
    name="database-pool",
    limit=5,  # До 5 одновременных подключений
    lock_timeout=30.0,
)

with Semaphore(client, config) as sem:
    # Работа с БД
    pass
```

### Async API

Асинхронные варианты для asyncio. Работают так же, как sync, но не блокируют
event loop и используют `async with`, `aacquire`, `arelease`.

```python
import asyncio
import redis.asyncio as aioredis
from redis_semaphore import Mutex, Semaphore, SemaphoreConfig

async def main():
    client = aioredis.Redis()

    async with Mutex(client, "async-lock") as lock:
        print(f"Mutex token: {lock.fencing_token}")

    sem_cfg = SemaphoreConfig(name="async-semaphore", limit=3)
    async with Semaphore(client, sem_cfg) as sem:
        print(f"Semaphore token: {sem.fencing_token}")

    await client.aclose()

asyncio.run(main())
```

### Redis Sentinel

Используйте Sentinel, если Redis развернут в режиме высокой доступности
и требуется автоматический failover при падении master.

Пример подключения через Sentinel (Mutex и Semaphore):

```python
from redis_semaphore import SentinelConfig, RedisConnectionFactory, Mutex, Semaphore, SemaphoreConfig

config = SentinelConfig(
    sentinels=[
        ("sentinel1.example.com", 26379),
        ("sentinel2.example.com", 26379),
        ("sentinel3.example.com", 26379),
    ],
    service_name="mymaster",
    password="secret",
)

client = RedisConnectionFactory.create_sync(config)

with Mutex(client, "ha-lock") as lock:
    # Автоматический failover при падении master
    pass

sem_cfg = SemaphoreConfig(name="ha-semaphore", limit=5)
with Semaphore(client, sem_cfg) as sem:
    pass
```


## Что выбрать и когда

- **Mutex** — если нужен ровно один владелец: “никто кроме меня” (миграции,
  генерация отчетов, фоновые задачи без параллели).
- **Semaphore** — если допустима ограниченная параллельность: “не больше N”
  (пулы подключений, rate-limit к внешним сервисам, ограничения нагрузки).

## Настройка и использование

### SemaphoreConfig

| Параметр | Тип | По умолчанию | Описание |
|----------|-----|--------------|----------|
| `name` | str | required | Логическое имя ресурса. Входит в Redis-ключи, например `semaphore:{name}:owners`. |
| `limit` | int | required | Максимум одновременных владельцев. Если владельцев `limit`, новый `acquire` вернет `busy`. |
| `lock_timeout` | float | 30.0 | TTL слота в секундах. Записывается как `score` в owners ZSET и продлевается heartbeat. |
| `acquire_timeout` | float | None | Максимальное время ожидания при `blocking=True`. `None` — ждать бесконечно. |
| `retry_interval` | float | 0.1 | Пауза между повторными попытками `acquire` при ожидании. |
| `refresh_interval` | float | None | Интервал heartbeat. Если не задан, берется 80% от `lock_timeout`. |
| `namespace` | str | "semaphore" | Префикс Redis-ключей для изоляции окружений/сервисов. |
| `strict_mode` | bool | False | Если `True`, при потере слота сразу кидает `LockLostError`. |
| `use_server_time` | bool | False | Если `True`, время берется с Redis (`TIME`). Полезно при рассинхронизации часов между машинами: TTL и очистка просроченных слотов будут согласованы. Минус — один дополнительный сетевой RTT на операции времени. Стоит включать `use_server_time`, если у вас несколько инстансов на разных хостах и нет гарантии синхронизации времени или вы видите преждевременные таймауты или "зависшие" слоты из-за скью часов.|
| `acquire_mode` | AcquireMode | BLPOP | Стратегия ожидания: `POLLING` (retry loop) или `BLPOP` (эффективное блокирующее ожидание через Redis). |
| `retry_interval_max` | float | None | Макс. интервал для exponential backoff. `None` — без backoff. |
| `retry_backoff_multiplier` | float | 2.0 | Множитель для exponential backoff. |
| `retry_jitter` | float | 0.0 | Случайный jitter как доля интервала (0.0-1.0). Помогает избежать thundering herd. |
| `blpop_timeout` | float | 5.0 | Таймаут BLPOP перед fallback retry (только для `BLPOP` режима). |



### Пример конфигурации и как это работает

```python
from redis_semaphore import Semaphore, SemaphoreConfig

config = SemaphoreConfig(
    name="payments",
    limit=5, # максимум 5 параллельных владельцев
    lock_timeout=30.0, # слот живет 30s без обновления
    acquire_timeout=None, # ждать бесконечно
    retry_interval=0.5, # проверять свободный слот каждые 0.5s
    refresh_interval=24.0, # heartbeat продлевает TTL заранее
    strict_mode=False, # при потере слота не падаем
)

# acquire будет ждать слот, проверяя каждые 0.5s.
# Когда слот получен, heartbeat поддерживает TTL,
# чтобы запись не истекла в Redis.
with Semaphore(client, config) as sem:
    do_work()
```

### Ручное управление (без контекстного менеджера)

```python
from redis_semaphore import Semaphore, SemaphoreConfig

config = SemaphoreConfig(name="jobs", limit=3)
sem = Semaphore(client, config)

result = sem.acquire(blocking=True)
if result.success:
    try:
        sem.refresh()  # продлить TTL вручную, если нужно
        do_work()
    finally:
        sem.release()
else:
    print("Resource busy")
```

- `acquire()` возвращает результат попытки захвата.
- `refresh()` продлевает TTL слота и возвращает `True/False`.
- `release()` освобождает слот; его важно вызывать в `finally`,
  чтобы не оставить занятый слот при ошибке в рабочем коде.

### Как работает acquire()

- `blocking=True` включает ожидание слота с интервалом `retry_interval`.
- `acquire_timeout=None` означает ждать бесконечно (только при `blocking=True`).
- `blocking=False` делает одну попытку и сразу возвращает `success=False`.
- `with Semaphore(...)` и `with Mutex(...)` всегда используют `blocking=True`.

#### Пример: ожидание слота (blocking)

```python
from redis_semaphore import Semaphore, SemaphoreConfig

config = SemaphoreConfig(
    name="jobs",
    limit=2,
    retry_interval=0.5,
    acquire_timeout=5.0,
)
sem = Semaphore(client, config)

result = sem.acquire(blocking=True)
if result.success:
    try:
        do_work()
    finally:
        sem.release()
else:
    # сюда попадем только если blocking=False
    print("Resource busy")
```

Клиент будет ждать слот до 5 секунд, проверяя каждые 0.5 секунды.
Если слот получен, выполняется `do_work()` и слот освобождается в `finally`.
Если бы мы использовали `blocking=False`, код сразу бы пошел в ветку `else`.

#### Пример: один быстрый запрос (non-blocking)

```python
result = mutex.acquire(blocking=False)
if result.success:
    try:
        do_work()
    finally:
        mutex.release()
else:
    print("Resource busy")
```

`acquire` делает одну попытку и сразу возвращает результат.
Если слот занят, код не ждет и сразу переходит в ветку `else`.


### Стратегии ожидания (AcquireMode)

При `blocking=True` семафор ждёт освобождения слота. Есть две стратегии:

#### BLPOP (по умолчанию)

Использует Redis `BLPOP` для блокирующего ожидания уведомления.
При `release()` публикуется сигнал, пробуждающий ожидающего клиента.

```python
from redis_semaphore import Semaphore, SemaphoreConfig

# BLPOP используется по умолчанию
config = SemaphoreConfig(
    name="jobs",
    limit=5,
    blpop_timeout=5.0,  # fallback polling каждые 5 сек
)
```

**Преимущества BLPOP:**
- Минимальная нагрузка на Redis (нет постоянных запросов)
- Мгновенное пробуждение при освобождении слота
- Честная очередь (FIFO) — кто первый начал ждать, тот первый получит

#### POLLING

Простой retry loop с паузой `retry_interval` между попытками.

```python
from redis_semaphore import Semaphore, SemaphoreConfig, AcquireMode

config = SemaphoreConfig(
    name="jobs",
    limit=5,
    acquire_mode=AcquireMode.POLLING,
    retry_interval=0.1,  # проверять каждые 100ms
)
```

**С exponential backoff и jitter:**

```python
config = SemaphoreConfig(
    name="jobs",
    limit=5,
    acquire_mode=AcquireMode.POLLING,
    retry_interval=0.1,         # начальный интервал
    retry_interval_max=2.0,     # максимальный интервал
    retry_backoff_multiplier=2.0,  # удваиваем каждый цикл
    retry_jitter=0.1,           # ±10% случайный jitter
)
```

Backoff полезен для снижения нагрузки на Redis при длительном ожидании.
Jitter помогает избежать thundering herd, когда много клиентов ждут одновременно.

**Когда выбирать POLLING:**
- Простые случаи с коротким ожиданием
- Совместимость со старыми версиями Redis


### Обработка потери блокировки

```python
def on_lock_lost(identifier: str):
    print(f"Lock {identifier} was lost!")
    # Graceful shutdown

# Mutex example
mutex = Mutex(client, "critical", on_lock_lost=on_lock_lost)

# semaphore example
sem_cfg = SemaphoreConfig(name="critical-pool", limit=2)
semaphore = Semaphore(client, sem_cfg, on_lock_lost=on_lock_lost)
```

Что происходит в этом примере:
- При потере слота (например, TTL истек) вызывается `on_lock_lost`.
- В колбэке можно инициировать корректное завершение работы.

### Async методы

Асинхронные методы используют префикс `a`:

- `aacquire` / `arelease` / `arefresh`
- `__aenter__` / `__aexit__` (через `async with`)

Пример:

```python
import redis.asyncio as aioredis
from redis_semaphore import Semaphore, SemaphoreConfig

async def main():
    client = aioredis.Redis()
    config = SemaphoreConfig(name="async", limit=2)

    async with Semaphore(client, config) as sem:
        print(sem.fencing_token)

    await client.aclose()
```

Также можно использовать явные async-методы без `async with`:

```python
import redis.asyncio as aioredis
from redis_semaphore import Mutex

async def main():
    client = aioredis.Redis()
    mutex = Mutex(client, "explicit-async")

    result = await mutex.aacquire(blocking=False)
    if result.success:
        try:
            print(mutex.fencing_token)
        finally:
            await mutex.arelease()

    await client.aclose()
```

## Логирование

По умолчанию используется стандартный `logging` с именем `redis_semaphore`.

Можно заменить логгер на свой (например loguru или structlog):

```python
from redis_semaphore import set_logger

# loguru
from loguru import logger as loguru_logger
set_logger(loguru_logger)

# structlog
import structlog
set_logger(structlog.get_logger("redis_semaphore"))
```

## Метрики

Prometheus-метрики опциональны. Установить:

```bash
pip install "py-redis-semaphore[prometheus]"
```

Пример использования:

```python
from prometheus_client import REGISTRY
from redis_semaphore import PrometheusMetrics, set_metrics

# Регистрируем метрики в существующем registry приложения
set_metrics(PrometheusMetrics(registry=REGISTRY))
```

## Примеры

Смотри `examples/basic_usage.py` (sync/async примеры) и
`examples/multiprocess_simulation.py` (имитация работы нескольких процессов).

Запуск:

```bash
python examples/basic_usage.py
python examples/multiprocess_simulation.py
```

## Подробнее о работе семафора

Смотри `docs/SEMAPHORE.md` (человеческое объяснение) и `docs/ALGORITHM.md`
(алгоритм и детали реализации).

## Документация для разработчика

Смотри `docs/DEVELOPMENT.md`.

## Лицензия

MIT
