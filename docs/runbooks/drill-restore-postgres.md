# Runbook: Rebuild Silver/Gold from Bronze + Restore Postgres (Architecture 19, Phase 8 Drill)

**Alert:** `PostgresDown` / `ServingStale` / `PipelineHealthRed` — `serving.*` unreachable, `sla_monitor` `serving.fct_transactions` probe fail, or manual drill `Rebuild Silver/Gold from Bronze` / `Restore Postgres from Backup`.
**Severity:** `critical` if `banking_dw` down (dashboard + `publish` blocked, freshness SLA burning); `high` for drill rehearsal.
**Scope:** Bronze is append-only 90d hot (Architecture 6.6) — Silver/Gold can always be rebuilt from Bronze. Postgres holds `serving`, `ops`, `rt`, and Iceberg catalog (`iceberg_catalog` DB).

## Symptoms
- Streamlit `Pipeline Health` page reports `serving.*` unreachable; `sla_monitor` fails `SELECT max(_loaded_at) FROM serving.*`
- Airflow `publish` task fails `schema 'serving' missing — run sql/init_postgres.sql first` or JDBC `psycopg2 OperationalError`
- `make health` postgres unhealthy; `docker compose ps` `banking_postgres` not healthy; `pg_isready` fail
- `ops.pipeline_runs` / `ops.freshness_metrics` not advancing
- Manual drill trigger: Phase 8 hardening rehearsal (PROJECT_PLAN.md Phase 8)

## Impact
- Dashboard down or stale (serving layer is the only Streamlit source via `streamlit_reader` per Architecture 8.2/8.4).
- `iceberg_catalog` (JDBC) down blocks Silver/Gold writes (same postgres instance in local dev) — treat as lake outage.

## Triage (5 min)
1. **Check postgres liveness:** `docker exec banking_postgres pg_isready -U postgres -d banking_dw && docker compose --profile core ps && docker logs banking_postgres --tail 50`
2. **Check which DB failing:** Local dev has 3 DBs in one instance: `banking_dw` (serving/ops), `airflow`, `iceberg_catalog` (per `sql/init_postgres.sql:7`). Test each: `psql -d banking_dw -c "SELECT 1"`, `psql -d iceberg_catalog -c "SELECT 1"`, `psql -d airflow -c "SELECT 1"`.
3. **Check disk & snapshots:** `docker system df`, MinIO `s3://banking-lakehouse` reachable, Iceberg snapshots not expired beyond time-travel window (Bronze 7d / Silver/Gold 30d per Architecture 6.3).

## Mitigation — Rebuild Silver/Gold from Bronze (no backup needed)
Bronze retains raw `_doc` + lineage (`_id`, `_doc_hash`, `_batch_id`, `_source_ts`, `_ingested_at`) so Silver/Gold are fully reproducible.

1. **Confirm Bronze intact (source of truth):** Spark `SELECT count(*) FROM banking.bronze.<collection>` for 10 collections (150 rows `branches` to 3M `card_transactions`) matches `docs/profiling.md` or `ops.ingestion_watermarks`.
2. **Rebuild Silver (typed/deduped/masked, MERGE idempotent):**
   - Python path (CE proven): `uv run python -m jobs.transform.silver_customers` etc. for all 10, or `make silver` if sbt available. Heavy tables (`transactions` 2M, `card_transactions` 3M) use `ORDER BY _source_ts DESC, _id DESC` tiebreaker per ADR 005.
   - Scala path (Phase 7): `make silver` via `jobs/transform/scala/Silver*` (requires `jobs/transform/scala/build.sbt`).
   - Each job does `MERGE INTO banking.silver.* ON business_key` (e.g. `customer_id`), not append — safe to re-run. Verify counts vs Bronze dedup.
3. **Rebuild Gold (star SCD2, contracts enforced):**
   - `dbt build --select gold` or `make dbt_build` — 5 Gold models (`dim_customer`/`dim_account` SCD2 via snapshots, `dim_branch` Type1, `fct_transactions`/`fct_card_transactions` incremental merge) with `contract: {enforced: true}` per Architecture 7.2. Seeds ×5.
   - Verify no orphan FKs: `dbt test --select fct_transactions` checks `relationships: dim_account`.
4. **Republish to Postgres:** `make publish` or `python -m jobs.publish.serving --table fct_transactions --run-id rebuild-$(date)` for each of `dim_customer`, `dim_account`, `dim_branch`, `fct_transactions`. Publish does staging `serving.<table>_stg` → validate counts vs Gold → atomic swap (`INSERT ON CONFLICT` for facts, rename swap for dims) → `ANALYZE` → `ops.freshness_metrics` row.
5. **Verify serving parity:** `SELECT count(*) FROM serving.fct_transactions` vs `SELECT count(*) FROM banking.gold.fct_transactions` (should match); also `SELECT count(*) FROM serving.dim_customer WHERE is_current` vs Gold dim count minus unknown `-1` row. Check `sql/publish_views.sql` masked views read only from `serving.*` (fixed 2026-09-21).

## Mitigation — Restore Postgres from Backup (when serving/ops/catalog lost)
Backups run nightly via `scripts/sh/backup_postgres.sh` (compressed `backups/<timestamp>.sql.gz`).

1. **Locate backup:** `ls -l backups/` or `docker exec banking_postgres ls /backups/`; pick latest `*.sql.gz`.
2. **Dry-run:** `scripts/sh/restore_postgres.sh --dry-run --file backups/<file>.sql.gz`
3. **Restore (destructive — requires YES):** `scripts/sh/restore_postgres.sh --file backups/<file>.sql.gz` (confirm `YES` prompt). Windows: `scripts\ps1\Restore-Postgres.ps1 -File backups\...`.
4. **Re-apply grants & schemas:** `psql -d banking_dw -f sql/init_postgres.sql` is idempotent (`IF NOT EXISTS`) — re-creates `serving`/`ops`/`rt`/`quarantine` and roles `etl_writer`/`streamlit_reader` per Architecture 8.2.
5. **Rebuild ops watermarks if needed:** `ops.ingestion_watermarks` and `ops.pipeline_runs` restored from backup; if gap exists, re-run ingest for missing window (overlap 10m handles late rows).

## Verify
1. `make health` postgres healthy; `psql banking_dw -c "SELECT schemaname, tablename FROM pg_tables WHERE schemaname='serving' ORDER BY 1,2"` lists `dim_customer`, `dim_account`, `dim_branch`, `fct_transactions`, `customers_masked`, `accounts_masked`.
2. `psql banking_dw -c "SELECT * FROM ops.freshness_metrics ORDER BY checked_at DESC LIMIT 3"` fresh.
3. Streamlit `Pipeline Health` green, `SELECT count(*) FROM serving.fct_transactions` equals Gold count.
4. `iceberg_maintenance` DAG `compaction` / `expire_snapshots` resumes without error.

## Prevention
- Keep Bronze 90d hot per Architecture 6.6 — do not expire snapshots aggressively before rebuild tested.
- Daily `expire_snapshots` (Bronze 7d, Silver/Gold 30d) and weekly `remove_orphan_files` only after backup success (`airflow/dags/iceberg_maintenance.py`).
- Test restore quarterly (Phase 8 drill) and document duration in `docs/incidents/`.

**Refs:** `sql/init_postgres.sql`, `sql/publish_views.sql`, `jobs/transform/silver_*.py`, `jobs/transform/scala/*.scala`, `dbt/banking_dbt/models/gold/*`, `jobs/publish/serving.py`, `scripts/sh/backup_postgres.sh`, `scripts/sh/restore_postgres.sh`, `airflow/dags/iceberg_maintenance.py`, `Architecture.md 6.5-6.6, 8, 19`.
