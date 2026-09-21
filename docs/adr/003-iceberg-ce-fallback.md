# ADR 003 — Iceberg on Databricks CE: Delta fallback for Silver/Gold

**Status:** Amended — Accepted (2026-09-20) → **Iceberg-everywhere verified on CE (2026-09-21)**

**Context:** Architecture 6.1/6.3 uses Iceberg JDBC catalog on `postgres:5432/iceberg_catalog` + `s3://banking-lakehouse` for all layers. On Databricks, Iceberg through Unity Catalog requires a trial/paid workspace + `databricks` Terraform provider. Phase 1 spike question: does Community Edition support reading/writing Iceberg via Unity Catalog?

**Original Decision (2026-09-20):** CE path — Bronze stays Iceberg (JDBC catalog on local `iceberg_catalog` + MinIO `S3_ENDPOINT=http://minio:9000`). **Silver and Gold land as Delta** on the same Spark session / warehouse, read via UniForm/Delta where another engine needs them. Only the dbt target config changes (`dbt-databricks` profile `type: delta`). This matched Architecture 6.1 fallback: “Only the dbt target configuration changes.”

**Amendment 2026-09-21 — Iceberg-everywhere wins:** Live verification on CE proved Iceberg JDBC + MinIO handles **all layers** (Bronze 10, Silver 10, Gold 5 with MERGE idempotent, quarantine, publish swap). No Delta switch was needed; every Silver/Gold DDL is `USING iceberg` and `MERGE INTO banking.silver.* / banking.gold.*` works via `IcebergSparkSessionExtensions`. The Delta fallback is therefore **not triggered on CE** — it remains a documented fallback only for a paid Databricks workspace where Unity Catalog governance requires Delta/UniForm. On CE/local, the single Spark session factory `jobs/common/spark.py` with catalog `banking` (JDBC) serves Bronze/Silver/Gold uniformly.

**Consequences (updated):**
- `docker-compose --profile core` provides MinIO + `iceberg_catalog` for Bronze **and** Silver/Gold (Iceberg-everywhere).
- `sql/bronze_ddl.sql` + Silver/Gold DDL all use `USING iceberg` (`PARTITIONED BY days(_ingested_at)` where needed); Delta equivalents only if deploying to paid Databricks Unity Catalog without Iceberg support.
- `dbt/banking_dbt/dbt_project.yml` and `jobs/common/spark.py` comments updated to Iceberg-everywhere; `type: delta` remains an optional paid-workspace override, not the CE default.
- No Databricks Jobs API on CE — orchestration for Phase 1 batch uses `jobs/ingestion/bronze.py` via `uv run`, cron, or local Airflow `LocalExecutor` (`AIRFLOW__CORE__EXECUTOR` in `.env`), not `databricks jobs create`.
- Terraform `databricks` module deferred to Phase 6 paid workspace; local `docker`/`postgresql`/`minio` modules remain. When paid workspace is used, toggle `dbt-databricks` `type: delta` + `UniForm` per Architecture 6.1 fallback.

**Alternatives considered:** Wait for paid workspace to keep Iceberg everywhere — rejected originally; live CE run now confirms Iceberg-everywhere works without waiting. Switching Silver/Gold to Delta was evaluated and rejected for CE (would contradict proven iceberg MERGE path).

**Reference:** `file.txt:26`, `PROJECT_PLAN.md:97`, `Architecture.md:252-253`, `jobs/common/spark.py:1` (catalog `banking` on JDBC), `jobs/common/config.py:1` (FULL_REFRESH_COLLECTIONS), `jobs/transform/silver_*.py` (`USING iceberg` MERGE), `dbt/banking_dbt/models/gold/schema.yml` (live-verified 2026-09-20).
