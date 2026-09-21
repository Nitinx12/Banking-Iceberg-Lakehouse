# HDFC Banking Data Platform — Lakehouse

> Architecture: `Architecture.md` | Delivery plan: `PROJECT_PLAN.md` | Phase: **5 Observability — SLOs, alerts and error budget proven live (Phases 0-4 live-verified)** | Next: Phase 6 IaC/CD
>
> Phases 0-4 proven live: Contracts ×10, Bronze watermark/overlap/_batch_id idempotent (rerun = same counts), Silver ×10 (6 Python + 3 Scala heavy), Gold star SCD2 (6/9/20 counts), GX + gate (98% threshold), publish swap idempotent, backfill DAG. Iceberg-everywhere on CE (ADR 003 amended 2026-09-21) — Delta fallback not triggered.

MongoDB (replicaSet `rs0`) → PySpark incremental batch (watermark + overlap, `_batch_id` idempotent) → Iceberg lakehouse (Bronze raw `_doc` + lineage, Silver typed/masked, Gold star SCD2) → PostgreSQL (`banking_dw: serving/ops/rt`) → Streamlit, orchestrated by Airflow, validated by GX/dbt, observed by Prometheus/Grafana.

## Quick start (Phase 5 — monitoring live)

Monitoring stack: `docker compose --profile monitoring up -d` — Grafana at
http://localhost:13000 (dashboards provisioned from `monitoring/grafana/`), Prometheus
at http://localhost:9090 (SLO rules in `monitoring/prometheus/rules/`), Alertmanager
at http://localhost:9093. Set `SLACK_WEBHOOK_URL` in `.env` to route alerts to Slack.
SLO proof drills: `uv run python scripts/proof_slo.py` (forced freshness delay + forced
critical DQ failure, both reversible). CE Gold build: `uv run python scripts/build_gold.py --publish`.

**WSL2 note:** enable Docker Desktop -> Settings -> Resources -> WSL Integration for your
distro, install a JDK (`sudo apt-get install -y openjdk-17-jdk` — PySpark needs one), and
run `make setup` inside WSL so the venv carries all dependency groups (`dev` alone misses
pymongo/sqlalchemy/dbt). The `.venv` is platform-specific: switching between Windows Git
Bash and WSL requires re-running `make setup` on that side (uv rebuilds it).

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

## What remains (Phase 6+)

- Phase 5 (done): SLOs measured + proven live — forced-delay and forced-DQ-fail drills pass
  (`scripts/proof_slo.py`), 5 Grafana dashboards provisioned as code, per-alert runbooks,
  error budget policy in `docs/slo/error-budget-policy.md`, compaction proven on live tables.
- Phase 6: Terraform modules (`postgres`/`object_storage`/`monitoring` + paid `databricks`) + `cd.yml` promotion. CE verified Iceberg-everywhere; Delta fallback kept only for paid Unity Catalog.
- Phase 7: Flink CDC (stretch) — `jobs/transform/scala/build.sbt` still missing; `make ingest/silver/gold` sbt targets are wired but expected to fail until Phase 7 scaffold. Python fallback (`make ingest_py`) is the proven CE path.
- Agent page (06_Agent): data path verified (5 serving tables, grants, guard); LLM calls blocked on OpenAI credits (429 insufficient_quota) — add credits at platform.openai.com billing to enable.

## Open questions (from PROJECT_PLAN.md:14 — partially closed)

1. ~~Sample documents~~ — closed via `tests/data/*.json` + profiling `docs/profiling.md` (10 collections, watermark `created_at`)
2. ~~Databricks tier~~ — closed: CE selected, Iceberg-everywhere verified (ADR 003 amended 2026-09-21), paid workspace deferred to Phase 6
3. Cloud provider for object storage + Terraform state (AWS/MinIO/GCP?) — local MinIO proven; cloud choice still open
4. Refresh cadence per collection (daily/hourly/streaming) — daily batch proven; streaming Phase 7 stretch
5. Monthly cloud budget ceiling — still open
6. Alert routing — Slack webhook + email for `SLACK_WEBHOOK_URL`/`ALERT_EMAIL_TO` in `.env` — scaffolded in `monitoring/alertmanager/alertmanager.yml`, needs real webhook to prove
