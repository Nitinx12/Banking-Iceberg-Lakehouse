# Banking Data Platform - Makefile (Architecture 16.1)
# Heavy data tasks (Bronze/Silver/Gold) run Scala via sbt (Architecture 7.1: JVM shuffle for
# 2M+ row tables). Quick monitoring/ops tasks run shell wrappers in scripts/sh/.
# Windows alternative: tasks.bat <target>

UV ?= uv
PY ?= $(UV) run python
# direct (non-make) uv runs read this too - see .envrc / README
export UV_PROJECT_ENVIRONMENT
ifeq ($(UNAME),Linux)
export UV_PROJECT_ENVIRONMENT ?= .venv-linux
endif
RUF ?= $(UV) run ruff

ENV ?= dev
DBT_SELECTOR ?= all
BATCH_ID ?=
JARS_DIR ?= jars

# Per-platform venv: a venv is not portable across Windows and WSL sharing this
# checkout. Linux (WSL) gets .venv-linux; Windows keeps the default .venv.
UNAME := $(shell uname -s 2>/dev/null)
ifeq ($(UNAME),Linux)
export UV_PROJECT_ENVIRONMENT ?= .venv-linux
endif

SBT ?= sbt
SBT_PROJECT := jobs.transform.scala
# unquoted - recipe lines wrap this in their own quoting; nested quotes break /bin/sh
HEAVY_NOTE := requires sbt + jobs/transform/scala/build.sbt (Phase 7) - Python fallback: make ingest_py

.PHONY: help env setup hooks up down status health lint test test_fast \
        ingest ingest_py silver silver_scala gold publish monitor dq dbt_build \
        dashboard tf_plan tf_apply seed_mongo docs clean jars

help:
	@echo "Banking Data Platform - make targets"
	@echo ""
	@echo "JARS (stable/offline - Architecture 6.1/16.1):"
	@echo "  make jars               - download pinned Spark/Iceberg JARs to jars/ for stable offline runs (needs curl, optional sbt)"
	@echo ""
	@echo "HEAVY (Scala via sbt - Architecture 7.1):"
	@echo "  make ingest             - Bronze batch ingestion (sbt runMain jobs.ingestion.scala.BronzeIngestion)"
	@echo "  make ingest_py          - Bronze via Python fallback (jobs.ingestion.bronze)"
	@echo "  make silver             - Silver for the 3 heavy tables (Scala: customers/transactions/card_txns)"
	@echo "  make gold               - Gold star-schema build (Scala)"
	@echo ""
	@echo "QUICK MONITORING (shell - scripts/sh/):"
	@echo "  make health             - service healthcheck (mongo/postgres/minio/airflow)"
	@echo "  make status             - pipeline run status + watermark from ops tables"
	@echo "  make monitor            - dbt run_results.json report + Pushgateway metrics"
	@echo "  make dq                 - DQ checks + dbt test (fail-closed gate)"
	@echo ""
	@echo "PIPELINE (other):"
	@echo "  make ingest_py          - Bronze via Python (uv, no sbt needed)"
	@echo "  make publish            - publish Silver/Gold to Postgres serving"
	@echo "  make dbt_build          - dbt build (scripts/sh/run_dbt.sh $(DBT_SELECTOR))"
	@echo ""
	@echo "INFRA / DEV:"
	@echo "  make env                - create .env from .env.example if missing"
	@echo "  make setup              - uv sync + install hooks"
	@echo "  make hooks              - set core.hooksPath to .githooks"
	@echo "  make up PROFILE=core    - docker compose --profile <p> up -d"
	@echo "  make down               - compose down"
	@echo "  make lint               - ruff + yamllint + sqlfluff (if present)"
	@echo "  make test               - pytest -q"
	@echo "  make test_fast          - pytest -q -m 'not slow'"
	@echo "  make dashboard          - streamlit run dashboard/Home.py"
	@echo "  make tf_plan ENV=dev    - terraform plan"
	@echo "  make tf_apply ENV=dev   - terraform apply"
	@echo "  make seed_mongo         - load sample dataset into local Mongo"
	@echo "  make docs               - dbt docs + Data Docs"
	@echo "  make clean              - remove caches"

env:
	@if [ ! -f .env ]; then cp .env.example .env; echo "created .env from .env.example - EDIT secrets"; else echo ".env exists"; fi

setup: env
	$(UV) sync --group dev --group ingestion --group transform --group quality --group dashboard
	git config core.hooksPath .githooks
	@echo "setup done - run: make up PROFILE=core"

hooks:
	git config core.hooksPath .githooks
	@echo "hooks enabled (.githooks)"

up:
	@# orchestration/streaming/monitoring need core services; always include core so
	@# `make up PROFILE=orchestration` does not fail on unresolved depends_on
	docker compose --profile core --profile $(or $(PROFILE),core) up -d
	@echo "up --profile core+$(or $(PROFILE),core) done; check: make health"

down:
	docker compose down

# ---------------------------------------------------------------- quick monitoring (shell)

health:
	bash scripts/sh/healthcheck.sh

status:
	@echo "== ops.pipeline_runs (latest 5) =="
	docker exec $$(docker ps --filter name=banking_postgres -q | head -1) psql -U $${POSTGRES_USER:-postgres} -d $${POSTGRES_WAREHOUSE_DB:-banking_dw} -c \
	  "select run_id, stage, status, rows_written, finished_at from ops.pipeline_runs order by started_at desc limit 5" 2>/dev/null \
	  || echo "postgres not reachable - make up first"
	@echo "== watermarks =="
	docker exec $$(docker ps --filter name=banking_postgres -q | head -1) psql -U $${POSTGRES_USER:-postgres} -d $${POSTGRES_WAREHOUSE_DB:-banking_dw} -c \
	  "select collection, watermark_value, updated_at from ops.watermarks order by collection" 2>/dev/null || true

monitor:
	@# dbt run_results.json -> console report + Pushgateway (Architecture 12.1)
	@if [ -f dbt/banking_dbt/target/run_results.json ]; then \
	  scala-cli run dbt/monitor/dbt-report.scala -- dbt/banking_dbt/target/run_results.json; \
	else echo "no run_results.json yet - run make dbt_build first"; fi

# ---------------------------------------------------------------- heavy tasks (Scala)

ingest:
	@echo "Bronze ingestion via Scala - $(HEAVY_NOTE)"
	$(SBT) ";project $(SBT_PROJECT);runMain jobs.ingestion.scala.BronzeIngestion"

ingest_py:
	@# Python fallback - proven CE path (Spark local mode), same idempotency contract
	bash scripts/sh/run_ingestion.sh $(if $(BATCH_ID),--batch-id $(BATCH_ID),) \
	  || $(PY) -m jobs.ingestion.bronze --all

silver_py:
	@# all 10 Python Silver jobs in one Spark session (CE fallback for make silver)
	$(PY) scripts/silver_all.py

run_all:
	@# full pipeline (seed->bronze->silver->gold->publish->dq) in ONE Spark process
	@# - single JVM startup instead of five; --skip-seed to reuse current Mongo data
	$(PY) scripts/run_pipeline.py --skip-seed

run_all_seed:
	$(PY) scripts/run_pipeline.py

silver:
	@echo "Silver (3 heavy tables) via Scala - $(HEAVY_NOTE)"
	$(SBT) ";project $(SBT_PROJECT);runMain jobs.transform.scala.SilverAll"

silver_scala:
	@# single table: make silver_scala TABLE=SilverTransactions
	$(SBT) ";project $(SBT_PROJECT);runMain jobs.transform.scala.$(TABLE)"

gold:
	@echo "Gold build via Scala - $(HEAVY_NOTE)"
	$(SBT) ";project $(SBT_PROJECT);runMain jobs.transform.scala.GoldBuild"

# ---------------------------------------------------------------- other pipeline tasks

publish:
	$(PY) -m jobs.publish.serving --table fct_transactions --run-id make-publish
	$(PY) -m jobs.publish.serving --table dim_customer --run-id make-publish

dbt_build:
	bash scripts/sh/run_dbt.sh $(DBT_SELECTOR) || $(UV) run dbt build --project-dir dbt/banking_dbt --select $(DBT_SELECTOR)

dq:
	bash scripts/sh/run_dq.sh || $(PY) -m jobs.quality.checks

lint:
	$(RUF) check . || ruff check .
	$(RUF) format --check . || ruff format --check .
	- yamllint . || echo "yamllint not installed (pip install yamllint)"
	- sqlfluff lint || echo "sqlfluff not installed"

test:
	$(UV) run pytest -q || pytest -q

test_fast:
	$(UV) run pytest -q -m "not slow" || pytest -q -m "not slow"

dashboard:
	$(UV) run streamlit run dashboard/Home.py || streamlit run dashboard/Home.py

tf_plan:
	terraform -chdir=terraform/envs/$(ENV) init -backend-config=backend.hcl -reconfigure -input=false
	terraform -chdir=terraform/envs/$(ENV) plan -input=false

tf_apply:
	terraform -chdir=terraform/envs/$(ENV) init -backend-config=backend.hcl -reconfigure -input=false
	terraform -chdir=terraform/envs/$(ENV) apply -input=false

seed_mongo:
	@echo "Seeding Mongo - requires compose core up"
	$(PY) scripts/seed_mongo.py

docs:
	$(UV) run dbt docs generate --project-dir dbt/banking_dbt --target-path docs_site || dbt docs generate --project-dir dbt/banking_dbt --target-path docs_site
	@echo "dbt docs at dbt/banking_dbt/docs_site/index.html; GX Data Docs via make dq"

jars:
	bash scripts/sh/download_jars.sh

clean:
	rm -rf .pytest_cache .ruff_cache __pycache__ jobs/__pycache__ tests/__pycache__ .spark spark-warehouse logs/*.log 2>/dev/null || true
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
