# Banking Data Platform — Makefile (Architecture 16.1)
# Primary: uv. Fallback: pip. Windows alternative: tasks.bat <target>

UV ?= uv
PY ?= $(UV) run python
RUF ?= $(UV) run ruff

.PHONY: help env setup hooks up down lint test test_fast ingest dbt_build dq dashboard tf_plan tf_apply seed_mongo docs clean

help:
	@echo "Banking Data Platform — make targets (see tasks.bat on Windows)"
	@echo "  make help              - this list"
	@echo "  make env               - create .env from .env.example if missing"
	@echo "  make setup             - uv sync + install hooks"
	@echo "  make hooks             - set core.hooksPath to .githooks"
	@echo "  make up PROFILE=core   - docker compose --profile <p> up -d"
	@echo "  make down              - compose down"
	@echo "  make lint              - ruff + yamllint + sqlfluff (if present)"
	@echo "  make test              - pytest -q"
	@echo "  make test_fast         - pytest -q -m 'not slow'"
	@echo "  make ingest            - (Phase 1) batch ingestion locally"
	@echo "  make dbt_build         - (Phase 2) dbt build"
	@echo "  make dq                - (Phase 3) GX checkpoints"
	@echo "  make dashboard         - (Phase 4) streamlit run"
	@echo "  make tf_plan ENV=dev   - terraform plan"
	@echo "  make tf_apply ENV=dev  - terraform apply"
	@echo "  make seed_mongo        - load sample dataset into local Mongo"
	@echo "  make docs              - dbt docs + Data Docs"
	@echo "  make clean             - remove caches"

env:
	@if [ ! -f .env ]; then cp .env.example .env; echo "created .env from .env.example — EDIT secrets"; else echo ".env exists"; fi

setup: env
	$(UV) sync --group dev || pip install -e ".[dev]"
	git config core.hooksPath .githooks
	@echo "setup done — run: make up PROFILE=core"

hooks:
	git config core.hooksPath .githooks
	@echo "hooks enabled (.githooks)"

up:
	docker compose --profile $(or $(PROFILE),core) up -d
	@echo "up --profile $(or $(PROFILE),core) done; check: docker compose ps"

down:
	docker compose down

lint:
	$(RUF) check . || ruff check .
	$(RUF) format --check . || ruff format --check .
	- yamllint . || echo "yamllint not installed (pip install yamllint)"
	- sqlfluff lint || echo "sqlfluff not installed"

test:
	$(UV) run pytest -q || pytest -q

test_fast:
	$(UV) run pytest -q -m "not slow" || pytest -q -m "not slow"

# Phase stubs — implemented in later phases
ingest:
	@echo "Phase 1: ingest_mongo_batch not yet implemented — see PROJECT_PLAN.md Phase 1"

dbt_build:
	@echo "Phase 2: dbt build not yet implemented"

dq:
	@echo "Phase 3: GX checkpoints not yet implemented"

dashboard:
	@echo "Phase 4: streamlit not yet implemented"

tf_plan:
	@echo "Phase 6: terraform plan ENV=$(or $(ENV),dev)"

tf_apply:
	@echo "Phase 6: terraform apply ENV=$(or $(ENV),dev)"

seed_mongo:
	@echo "Seeding Mongo — requires compose core up and dataset in data/ or contracts/"
	$(PY) scripts/seed_mongo.py || echo "seed_mongo.py not yet implemented (Phase 0 task)"

docs:
	@echo "dbt docs generate + GX Data Docs — Phase 2/3"

clean:
	rm -rf .pytest_cache .ruff_cache __pycache__ jobs/__pycache__ tests/__pycache__ .spark spark-warehouse logs/*.log 2>/dev/null || true
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
