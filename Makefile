REDIS_IMAGE ?= redis-semaphore-redis
REDIS_CONTAINER ?= redis-semaphore-redis
REDIS_PORT ?= 6379
RUN ?= uv run

.PHONY: redis-build redis-up redis-down redis-logs redis-shell test lint format ruff-fix typecheck check install-hooks pre-commit
SENTINEL_COMPOSE ?= docker/sentinel/docker-compose.yml

redis-build:
	docker build -t $(REDIS_IMAGE) -f docker/redis/Dockerfile .

redis-up: redis-build
	docker run -d --rm \
		--name $(REDIS_CONTAINER) \
		-p $(REDIS_PORT):6379 \
		$(REDIS_IMAGE)

redis-down:
	-@docker stop $(REDIS_CONTAINER)

redis-logs:
	docker logs -f $(REDIS_CONTAINER)

redis-shell:
	docker exec -it $(REDIS_CONTAINER) redis-cli

test:
	$(RUN) pytest

lint:
	$(RUN) ruff check .

format:
	$(RUN) ruff format .

ruff-fix:
	$(RUN) ruff format .
	$(RUN) ruff check --fix .

typecheck:
	$(RUN) mypy src tests

check: lint typecheck test

install-hooks:
	$(RUN) pre-commit install

pre-commit:
	$(RUN) pre-commit run --all-files

.PHONY: sentinel-up sentinel-down sentinel-logs

sentinel-up:
	docker compose -f $(SENTINEL_COMPOSE) up -d

sentinel-down:
	docker compose -f $(SENTINEL_COMPOSE) down

sentinel-logs:
	docker compose -f $(SENTINEL_COMPOSE) logs -f
