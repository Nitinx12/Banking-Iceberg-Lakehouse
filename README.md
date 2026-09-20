# HDFC Banking Data Platform — Lakehouse

> Architecture: `Architecture.md` | Delivery plan: `PROJECT_PLAN.md` | Phase: **0 Foundations** (MVP = Phases 0-4)

MongoDB (replicaSet `rs0`) → PySpark incremental batch (watermark + overlap, `_batch_id` idempotent) → Iceberg lakehouse (Bronze raw `_doc` + lineage, Silver typed/masked, Gold star SCD2) → PostgreSQL (`banking_dw: serving/ops/rt`) → Streamlit, orchestrated by Airflow, validated by GX/dbt, observed by Prometheus/Grafana.

## Quick start (Phase 0)

```bash
# 1. env
cp .env.example .env   # then edit secrets (already generated locally)
# or
make env               # tasks.bat env on Windows

# 2. setup
make setup             # uv sync --group dev + git config core.hooksPath .githooks
# or
tasks.bat setup

# 3. up core profile (postgres + mongo rs0 + minio)
make up PROFILE=core   # tasks.bat up core
docker compose ps      # wait for healthy

# 4. seed & profile (after you add dataset to tests/data/)
make seed_mongo        # tasks.bat seed_mongo

# 5. lint / test
make lint              # tasks.bat lint
make test              # tasks.bat test
```

`.env` is gitignored. Commit with a fake secret is blocked by `.githooks/pre-commit`.

## Repo layout (Architecture 18)

```
banking_data_platform/
├── .github/workflows/   ci.yml, terraform_plan.yml
├── .githooks/           pre-commit, commit-msg, pre-push
├── airflow/dags/        daily_banking_pipeline etc. (Phase 1+)
├── jobs/                ingestion/transform/quality/publish/maintenance + common/
├── contracts/           one YAML per collection (Phase 0 task)
├── dbt/banking_dbt/     staging/silver/gold + snapshots (Phase 2)
├── gx/                  suites/checkpoints (Phase 3)
├── flink/sql/           CDC + windows (Phase 7)
├── streamlit_app/       pages/ (Phase 4)
├── terraform/           modules + envs/dev|staging|prod (Phase 6)
├── monitoring/          prometheus/grafana/alertmanager (Phase 5)
├── scripts/sh/lib.sh    shared logging helper
├── docker-compose.yml   core/orchestration/streaming/quality/monitoring/dashboard profiles
├── pyproject.toml       uv dependency groups (ingestion/transform/quality/dashboard/dev)
└── sql/init_postgres.sql serving/ops/rt + dq_results/freshness DDL
```

## What I need from you to continue

Per `PROJECT_PLAN.md:14` open questions — to close early:

1. **Sample documents** per actual Mongo collection (customers/accounts/transactions/branches/loans/cards) — or real volume/growth numbers
2. **Databricks workspace tier** (CE vs trial/paid — decides ADR 3 Iceberg vs Delta fallback)
3. **Cloud provider** for object storage + Terraform state (AWS/MinIO/GCP?)
4. **Refresh cadence** per collection (daily/hourly/streaming)
5. **Monthly cloud budget** ceiling
6. **Alert routing** — Slack webhook + email for `SLACK_WEBHOOK_URL`/`ALERT_EMAIL_TO` in `.env`

Next scaffold steps after you confirm: Spark session factory (Iceberg JDBC), Bronze DDL, `ops` DDL, batch ingestion job (Phase 1).
