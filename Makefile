PROJECT_NAME := github-repository-radar
RUN_DATE ?= $(shell date +%Y-%m-01)

.PHONY: help env build up down logs dbt-run dbt-test prefect-run lightdash lint test-python qa-python check

help:
	@echo "$(PROJECT_NAME) commands"
	@echo "make env          - create .env from .env.example"
	@echo "make build        - build local docker images"
	@echo "make up           - start all main services"
	@echo "make down         - stop all services"
	@echo "make logs         - show docker compose logs"
	@echo "make dbt-run      - run dbt models"
	@echo "make dbt-test     - run dbt tests"
	@echo "make prefect-run  - run the full Prefect flow"
	@echo "make lint         - run Python linters"
	@echo "make test-python  - run Python unit tests"
	@echo "make qa-python    - run Python lint + tests"
	@echo "make check        - run the main validation flow"
	@echo "make lightdash    - show the local Lightdash URL"

env:
	@if [ ! -f .env ]; then cp .env.example .env; fi

build:
	docker compose build dbt prefect

up:
	docker compose up -d clickhouse lightdash_db dbt lightdash prefect

down:
	docker compose down

logs:
	docker compose logs --tail=100

dbt-run:
	docker compose exec dbt dbt run

dbt-test:
	docker compose exec dbt dbt test

prefect-run:
	docker compose run --rm prefect python orchestration/prefect_flow.py --run-date $(RUN_DATE)

lint:
	docker compose run --rm prefect ruff check ingestion orchestration tests

test-python:
	docker compose run --rm prefect pytest

qa-python:
	$(MAKE) lint
	$(MAKE) test-python

check:
	$(MAKE) build
	$(MAKE) qa-python
	$(MAKE) up
	$(MAKE) prefect-run RUN_DATE=$(RUN_DATE)
	$(MAKE) dbt-run
	$(MAKE) dbt-test

lightdash:
	@echo "Open http://localhost:8080"
