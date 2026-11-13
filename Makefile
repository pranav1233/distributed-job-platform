.PHONY: help up down build logs ps migrate api-key smoke test unit integration cov lint fmt typecheck chaos bench tf-validate

COMPOSE ?= docker compose

help:
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | sed 's/:.*##/ -/'

up: ## Build and start the full stack (api, workers, relay, reaper, datastores, monitoring)
	$(COMPOSE) up -d --build --wait

down: ## Stop the stack and remove volumes
	$(COMPOSE) down -v

build: ## Build the application image
	$(COMPOSE) build

logs: ## Follow application logs
	$(COMPOSE) logs -f api worker relay reaper

ps: ## Show service status
	$(COMPOSE) ps

migrate: ## Apply database migrations
	$(COMPOSE) run --rm migrate

api-key: ## Create an API key: make api-key OWNER=alice
	@$(COMPOSE) run --rm --no-deps -T api python -m jobplatform.cli create-api-key --owner $(or $(OWNER),demo) 2>/dev/null

smoke: ## End-to-end curl smoke test against a running stack
	./scripts/smoke.sh

test: ## Unit and integration tests with coverage (integration uses testcontainers)
	uv run pytest --cov --cov-report=term-missing

unit: ## Unit tests only (no containers)
	uv run pytest tests/unit -q

integration: ## Integration tests only
	uv run pytest tests/integration -q

cov: ## Coverage report as HTML
	uv run pytest --cov --cov-report=html

lint: ## ruff, formatting check and mypy
	uv run ruff check src tests scripts
	uv run ruff format --check src tests scripts
	uv run mypy

fmt: ## Format code
	uv run ruff format src tests scripts
	uv run ruff check --fix src tests scripts

chaos: ## Chaos test: kill workers, restart the relay and RabbitMQ, assert no job is lost
	uv run python scripts/chaos_test.py --write-report

bench: ## Load test at 1, 2 and 4 workers, writes docs/BENCHMARKS.md
	uv run python scripts/load_test.py --workers 1 2 4 --write-report

tf-validate: ## Terraform fmt and validate
	cd infra && terraform fmt -check -recursive && terraform init -backend=false -input=false >/dev/null && terraform validate
