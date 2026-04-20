PROJECT_NAME := github-repository-radar
RUN_DATE ?= $(shell date +%Y-%m-01)
SITE_DATA_ROOT := evidence/sources/site_data

.PHONY: help env build up down logs dbt-deps dbt-run dbt-test prefect-run export-site-data evidence-install evidence-deps evidence-dev evidence-build pages-build-local check-site clean-site-data-tmp lint test-python qa-python check

help:
	@echo "$(PROJECT_NAME) commands"
	@echo "make env          - create .env from .env.example"
	@echo "make build        - build local docker images"
	@echo "make up           - start all main services"
	@echo "make down         - stop all services"
	@echo "make logs         - show docker compose logs"
	@echo "make dbt-deps     - install dbt package dependencies"
	@echo "make dbt-run      - run dbt models"
	@echo "make dbt-test     - run dbt tests"
	@echo "make prefect-run  - run the full Prefect flow and update local public datasets"
	@echo "make export-site-data - export publish datasets into evidence/sources/site_data/current"
	@echo "make evidence-install - install Evidence dependencies"
	@echo "make evidence-deps - ensure Evidence dependencies exist in the Docker volume"
	@echo "make evidence-dev - start Evidence dev server"
	@echo "make evidence-build - build the Evidence static site"
	@echo "make pages-build-local - rebuild flat-file sources and produce a GitHub Pages-ready artifact"
	@echo "make check-site   - validate the static-site build flow"
	@echo "make clean-site-data-tmp - remove temporary export directories"
	@echo "make lint         - run Python linters"
	@echo "make test-python  - run Python unit tests"
	@echo "make qa-python    - run Python lint + tests"
	@echo "make check        - run the main validation flow including publish/static-site checks"

env:
	@if [ ! -f .env ]; then cp .env.example .env; fi

build:
	docker compose build dbt prefect

up:
	docker compose up -d clickhouse dbt prefect

down:
	docker compose down

logs:
	docker compose logs --tail=100

dbt-deps:
	docker compose run --rm dbt dbt deps

dbt-run: dbt-deps
	docker compose run --rm dbt dbt run

dbt-test:
	docker compose run --rm dbt dbt test

prefect-run:
	docker compose run --rm prefect python orchestration/prefect_flow.py --run-date $(RUN_DATE)

export-site-data:
	docker compose run --rm prefect python publish/export_site_data.py --run-date $(RUN_DATE)

evidence-install:
	docker compose run --rm evidence npm ci

evidence-deps:
	docker compose run --rm evidence sh -lc 'test -f node_modules/@evidence-dev/evidence/cli.js || npm ci'

evidence-dev:
	docker compose up evidence

evidence-build:
	$(MAKE) evidence-deps
	docker compose run --rm evidence npm run build:strict

pages-build-local:
	$(MAKE) evidence-deps
	docker compose run --rm evidence npm run build:pages

check-site:
	$(MAKE) evidence-deps
	docker compose run --rm evidence npm run sources -- --strict
	docker compose run --rm evidence npm run build:strict

clean-site-data-tmp:
	rm -rf $(SITE_DATA_ROOT)/_tmp

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
	$(MAKE) dbt-run
	$(MAKE) dbt-test
	$(MAKE) export-site-data RUN_DATE=$(RUN_DATE)
	$(MAKE) check-site
