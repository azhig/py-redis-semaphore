# План: FastAPI прокси для LLM с семафорами на каждое подразделение/модель

## Обзор
Создать production-ready FastAPI приложение, которое проксирует запросы к LLM (OpenAI/GigaChat/Ollama) с распределённым rate limiting через redis-semaphore. Каждая комбинация подразделение+модель получает свой семафор с 5 слотами, обеспечивая справедливое распределение ресурсов между разными командами и моделями.

## Анализ требований

### Основные функции
1. **Универсальный прокси для LLM**: Принимает произвольные JSON запросы - работает с OpenAI, GigaChat, Ollama и т.д.
2. **Rate limiting по подразделениям**: Заголовок `direction` (1-20) определяет подразделение
3. **Изоляция по моделям**: Разные модели получают отдельные пулы семафоров
4. **Поддержка streaming**: Обработка обычных и потоковых ответов от LLM
5. **Динамическое создание семафоров**: Семафоры создаются по требованию для новых комбинаций подразделение+модель
6. **Передача API ключа**: Клиент передаёт API ключ в запросе (не хранится на сервере)

### Production функции (по требованиям пользователя)
- Обработка ошибок и структурированное логирование
- Prometheus метрики (глубина очереди, количество запросов, rate limit срабатывания)
- Валидация запросов/ответов (заголовок подразделения, имя модели)
- Health checks

### Архитектурные решения
- **Паттерн ключей семафоров**: `{department}:{model}` → например, `"dept_1:gpt-4"`, `"dept_2:gigachat"`
- **Пул семафоров**: Кеш в виде словаря с экземплярами Semaphore (создаются лениво)
- **Вместимость**: 5 слотов на каждую комбинацию подразделение+модель
- **Lock Timeout**: 120 секунд (для долгих запросов к LLM)
- **Acquire Timeout**: 60 секунд (максимальное время ожидания в очереди)
- **Стратегия ожидания**: BLPOP mode (эффективное блокирующее ожидание через Redis LIST)

## Критические файлы

### Новые файлы для создания
1. **`examples/fastapi_llm_proxy/app.py`** - Основное FastAPI приложение
2. **`examples/fastapi_llm_proxy/config.py`** - Конфигурация и настройки
3. **`examples/fastapi_llm_proxy/semaphore_pool.py`** - Менеджер пула семафоров
4. **`examples/fastapi_llm_proxy/metrics.py`** - Prometheus метрики
5. **`examples/fastapi_llm_proxy/README.md`** - Документация и примеры использования
6. **`examples/fastapi_llm_proxy/.env.example`** - Шаблон переменных окружения

### Зависимости (документируются, не добавляются в проект)
- `fastapi` - Веб-фреймворк
- `uvicorn` - ASGI сервер
- `httpx` - Асинхронный HTTP клиент для проксирования
- `prometheus-client` - Метрики (уже есть как опциональная зависимость)
- `python-dotenv` - Переменные окружения (опционально)

## План реализации

### Фаза 1: Структура проекта
**Файлы**: Структура директорий, README, .env.example

1. Создать директорию `examples/fastapi_llm_proxy/`
2. Создать README.md с:
   - Обзор и диаграмма архитектуры
   - Инструкции по установке (ручная установка через pip)
   - Руководство по конфигурации (Redis, переменные окружения)
   - Примеры использования для OpenAI/GigaChat/Ollama
   - Инструкции по тестированию
3. Создать `.env.example` с плейсхолдерами:
   ```
   REDIS_HOST=localhost
   REDIS_PORT=6379
   SEMAPHORE_CAPACITY=5
   SEMAPHORE_LOCK_TIMEOUT=120
   SEMAPHORE_ACQUIRE_TIMEOUT=60
   LOG_LEVEL=INFO
   ```

### Фаза 2: Модуль конфигурации
**Файл**: `config.py`

Создать Pydantic settings класс:
```python
class Settings(BaseSettings):
    # Redis
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0

    # Semaphore
    semaphore_capacity: int = 5
    semaphore_lock_timeout: float = 120.0
    semaphore_acquire_timeout: float = 60.0  # Максимальное время ожидания в очереди
    semaphore_namespace: str = "llm_proxy"

    # Upstream LLM API
    upstream_base_url: str = "https://api.openai.com"  # Или GigaChat URL
    upstream_timeout: float = 120.0

    # Logging
    log_level: str = "INFO"
```

### Фаза 3: Менеджер пула семафоров
**Файл**: `semaphore_pool.py`

Создать класс `SemaphorePool`:
- Словарь для кеширования экземпляров Semaphore по ключу `{dept}:{model}`
- Потокобезопасная ленивая инициализация (использовать asyncio.Lock)
- `async def get_semaphore(dept: int, model: str) -> Semaphore`
- Методы для отслеживания размера пула, активных блокировок (для метрик)
- Метод очистки для удаления неиспользуемых семафоров (опционально)

Ключевая реализация:
```python
class SemaphorePool:
    def __init__(self, redis_client, config):
        self._pool: dict[str, Semaphore] = {}
        self._lock = asyncio.Lock()

    async def get_semaphore(self, dept: int, model: str) -> Semaphore:
        key = f"dept_{dept}:{model}"
        if key not in self._pool:
            async with self._lock:
                if key not in self._pool:  # Double-check
                    config = SemaphoreConfig(
                        name=key,
                        limit=self.capacity,
                        lock_timeout=self.lock_timeout,
                        ...
                    )
                    self._pool[key] = Semaphore(self.redis, config)
        return self._pool[key]
```

### Фаза 4: Prometheus метрики
**Файл**: `metrics.py`

Определить метрики:
- `llm_requests_total` - Counter с метками: department, model, status
- `llm_requests_in_progress` - Gauge с метками: department, model
- `llm_request_duration_seconds` - Histogram
- `llm_rate_limit_hits_total` - Counter (ответы 429)
- `llm_semaphore_queue_depth` - Gauge для каждого подразделения+модели
- `llm_semaphore_pool_size` - Gauge (всего уникальных семафоров)

Вспомогательные функции:
- `record_request(dept, model, status, duration)`
- `update_queue_depth(dept, model, depth)`

### Фаза 5: Основное FastAPI приложение
**Файл**: `app.py`

#### 5.1 Жизненный цикл приложения
- Инициализация Redis клиента (async)
- Инициализация SemaphorePool
- Инициализация метрик
- Настройка структурированного логирования
- Очистка при завершении

#### 5.2 Модели запросов (Pydantic)
```python
class ProxyRequest(BaseModel):
    model: str  # Обязательное поле
    # Все остальные поля опциональны и проксируются дальше
    class Config:
        extra = "allow"  # Принимаем произвольные поля
```

#### 5.3 Эндпоинты

**1. POST /v1/chat/completions** (без streaming)
- Извлечь заголовок `direction` (обязательный, 1-20)
- Извлечь заголовок `x-api-key` (обязательный)
- Разобрать тело запроса, проверить наличие поля `model`
- Получить семафор из пула
- **Захватить семафор** - запрос ждёт в очереди до 60 секунд (BLPOP mode)
  - Если слот освободился - захватываем и продолжаем
  - Если таймаут 60 сек истёк - возвращаем 429 (очередь переполнена)
- Переслать запрос в OpenAI API через httpx
- Освободить семафор (уведомляет следующий запрос в очереди)
- Вернуть ответ
- Обработать ошибки: 400 (неверный запрос), 429 (таймаут ожидания), 502 (ошибка upstream)

**2. POST /v1/chat/completions** (streaming)
- То же самое, но определить `stream=true` в теле запроса
- Использовать httpx streaming response
- Стримить чанки обратно клиенту
- Гарантировать освобождение семафора даже при обрыве соединения (try/finally)

**3. GET /health**
- Проверить подключение к Redis
- Вернуть 200 если здорово, 503 если нет

**4. GET /metrics**
- Вернуть Prometheus метрики в текстовом формате

**5. GET /semaphore/status** (опциональный debug эндпоинт)
- Вернуть JSON со всеми активными семафорами и их текущим использованием
- Полезно для отладки/мониторинга

#### 5.4 Обработка ошибок
```python
@app.exception_handler(AcquireTimeoutError)
async def rate_limit_handler(request, exc):
    # Логировать с контекстом dept/model
    # Увеличить метрику rate_limit_hits
    # ВАЖНО: 429 возвращается только если таймаут ожидания истёк
    # НЕ потому что "нет мест", а потому что "слишком долго ждали"
    return JSONResponse(
        status_code=429,
        content={
            "error": {
                "message": "Превышено время ожидания в очереди (60 сек)",
                "type": "rate_limit_error",
                "code": "queue_timeout"
            }
        }
    )
```

#### 5.5 Middleware
- Логирование запросов (dept, model, endpoint)
- Middleware для замера времени (для метрик длительности)
- Перехват исключений

### Фаза 6: Документация в README

Разделы:
1. **Обзор** - Что демонстрирует этот пример
2. **Архитектура** - Диаграмма потока запросов через семафоры
3. **Установка**
   ```bash
   cd examples/fastapi_llm_proxy
   pip install fastapi uvicorn httpx prometheus-client python-dotenv
   pip install -e ../../  # Установить redis-semaphore
   ```
4. **Конфигурация** - Скопировать .env.example, отредактировать значения
5. **Запуск**
   ```bash
   # Запустить Redis
   docker run -d -p 6379:6379 redis:7

   # Запустить FastAPI
   uvicorn app:app --host 0.0.0.0 --port 8000
   ```
6. **Примеры использования**
   - OpenAI curl пример с заголовком direction
   - GigaChat пример (другой base URL)
   - Streaming пример
   - Тестирование rate limits (параллельные запросы)
7. **Мониторинг**
   - Доступ к метрикам на /metrics
   - Пример конфигурации для Prometheus scraping
   - Ключевые метрики для отслеживания
8. **Production соображения**
   - Redis Sentinel для HA
   - Настройка вместимости семафоров для каждого dept/model
   - Обработка отказов подключения к Redis
   - Развёртывание за load balancer

## Ключевые паттерны проектирования

### 1. Паттерн универсального прокси
Принимаем любой JSON, извлекаем только то, что нам нужно (поле `model`), остальное передаём дальше:
```python
body = await request.json()
model_name = body.get("model")  # Валидируем это поле
# Пересылаем всё тело запроса upstream
```

### 2. Контекстный менеджер семафора
Всегда используем async context manager для автоматической очистки:
```python
semaphore = await pool.get_semaphore(dept, model)
try:
    await semaphore.aacquire()
    # Делаем запрос upstream
finally:
    await semaphore.arelease()
```

### 3. Streaming с очисткой
Гарантируем освобождение семафора даже при отключении клиента:
```python
async def stream_response():
    try:
        async for chunk in upstream_stream:
            yield chunk
    finally:
        await semaphore.arelease()

return StreamingResponse(stream_response(), media_type="text/event-stream")
```

### 4. Интеграция метрик
Записываем метрики в ключевых точках:
- Перед захватом: `requests_total++`
- После захвата: `in_progress++`, `queue_depth--`
- После освобождения: `in_progress--`
- При таймауте: `rate_limit_hits++`

## Стратегия обработки ошибок

| Тип ошибки | HTTP статус | Действие |
|------------|-------------|----------|
| Отсутствует заголовок `direction` | 400 | Вернуть JSON с ошибкой |
| Неверный `direction` (не 1-20) | 400 | Вернуть JSON с ошибкой |
| Отсутствует заголовок `x-api-key` | 400 | Вернуть JSON с ошибкой |
| Отсутствует `model` в теле | 400 | Вернуть JSON с ошибкой |
| **Таймаут ожидания в очереди** (60 сек) | 429 | Вернуть "Превышено время ожидания в очереди" |
| Потеря блокировки семафора | 503 | Вернуть service unavailable |
| Ошибка Upstream API | 502 | Переслать ошибку upstream |
| Ошибка подключения к Redis | 503 | Вернуть service unavailable |

**Важно**: 429 ошибка означает "слишком долго ждали в очереди", а НЕ "нет свободных мест". Если есть 5 активных запросов и приходит 6-й, он **встаёт в очередь и ждёт**, пока один из первых 5 не завершится. Только если ожидание превышает 60 секунд - возвращается 429.

## План проверки

### Ручное тестирование
1. **Базовый запрос**: Отправить один запрос, проверить работоспособность
2. **Очередь запросов**: Отправить 10 параллельных долгих запросов (> 5)
   - Первые 5 сразу проходят (захватывают слоты)
   - Следующие 5 встают в очередь и **ждут**
   - По мере завершения первых 5, следующие постепенно обрабатываются
   - НЕТ ошибок 429, все 10 должны успешно выполниться
3. **Таймаут очереди**: Отправить 10 медленных запросов (по 70 сек каждый)
   - Первые 5 начинают обработку
   - 6-9 запросы встают в очередь
   - 10-й запрос ждёт > 60 сек и получает 429
4. **Изоляция подразделений**: Отправить запросы от dept_1 и dept_2 параллельно, проверить отсутствие влияния
5. **Изоляция моделей**: Отправить запросы к gpt-3.5 и gpt-4 параллельно, проверить отдельные семафоры
6. **Streaming**: Протестировать streaming эндпоинт с `stream=true`
7. **Метрики**: Проверить /metrics показывает правильные счётчики
8. **Health Check**: Проверить /health возвращает 200 когда Redis работает, 503 когда нет

### Автоматизированное тестирование (опционально)
Создать `test_app.py` с pytest:
- Тест валидации запросов
- Тест rate limiting с httpx клиентом
- Mock Redis для unit тестов
- Интеграционный тест с реальным Redis

### Нагрузочное тестирование (опционально)
Использовать `locust` или `wrk` для симуляции реальной нагрузки:
- 100 одновременных пользователей
- Микс подразделений (1-5)
- Микс моделей (gpt-3.5, gpt-4)
- Проверить соблюдение вместимости семафоров

## Дерево файлов (финальная структура)
```
examples/fastapi_llm_proxy/
├── README.md              # Полная документация
├── .env.example           # Шаблон окружения
├── app.py                 # Основное FastAPI приложение (~300 строк)
├── config.py              # Настройки (~50 строк)
├── semaphore_pool.py      # Менеджер пула (~100 строк)
├── metrics.py             # Prometheus метрики (~80 строк)
└── (optional) test_app.py # Тесты
```

## Оценка времени
*Примечание: По инструкциям, временные оценки не предоставляются - задачи разбиты на реализуемые части*

## Открытые вопросы (Решены)
✅ Зависимости: Документированы в README, не добавляются в проект
✅ Авторизация: API ключ передаётся в заголовках запроса
✅ Docker: docker-compose не требуется
✅ Охват: Полные production функции (логирование, метрики, валидация)

## Заметки
- Этот пример будет самым полным в репозитории (production-ready)
- Демонстрирует продвинутые async паттерны с семафорами
- Переиспользуемый паттерн для любого HTTP прокси с rate limiting
- Может быть адаптирован для GigaChat, Ollama или любого другого LLM провайдера изменением upstream URL
