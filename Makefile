# StreamFlix Lakehouse — Makefile (uv primary, pip fallback for CE notebooks)
# Usage: make install | make lint | make test | make gx | make run | make test-connection

UV ?= uv
PY ?= $(UV) run python
RUF ?= $(UV) run ruff
PYT ?= $(UV) run pytest

.PHONY: help install lint format test gx generate run bronze silver gold pipeline test-connection sql-init clean

help:
	@echo "Targets:"
	@echo "  make install         - uv sync (frozen)"
	@echo "  make lint            - ruff check"
	@echo "  make format          - ruff format + fix"
	@echo "  make test            - pytest -q (22 tests, ~2min with Spark)"
	@echo "  make test-quick      - GX suite smoke only"
	@echo "  make gx              - list GX suites"
	@echo "  make generate        - synthetic landing data (small)"
	@echo "  make run             - pipeline bronze->silver->gold (requires Spark/DBR)"
	@echo "  make bronze/silver/gold - single layer"
	@echo "  make test-connection - Databricks workspace + SQL + imports"
	@echo "  make sql-init        - show init SQL path"
	@echo "  make clean           - remove caches"

install:
	$(UV) sync --frozen

lint:
	$(RUF) check .

format:
	$(RUF) format .
	$(RUF) check --fix .

test:
	$(PYT) -q

test-quick:
	$(PYT) tests/test_gx_expectations.py -v

gx:
	$(PY) main.py gx --list
	@echo "suites in gx/expectations/:"
	@$(PY) -c "import pathlib; print('\n'.join(str(p) for p in pathlib.Path('gx/expectations').glob('*.json')))"

generate:
	$(PY) main.py generate --content 1000 --users 500 --watch 10000 --billing 2000

run: pipeline
pipeline:
	$(PY) main.py pipeline

bronze:
	$(PY) main.py bronze --what all

silver:
	$(PY) main.py silver --what all

gold:
	$(PY) main.py gold --what all

test-connection:
	$(PY) main.py test-connection

sql-init:
	@echo "Run in Databricks SQL Editor: sql/init_schema.sql  (catalog streamflix-lakehouse)"
	@head -20 sql/init_schema.sql

clean:
	rm -rf .pytest_cache .ruff_cache __pycache__ src/__pycache__ src/core/__pycache__ src/utils/__pycache__ src/jobs/__pycache__ gx/uncommitted
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
