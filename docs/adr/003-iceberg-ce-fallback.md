# ADR 003 — Iceberg on Databricks CE: Delta fallback for Silver/Gold

**Status:** Accepted (2026-09-20) — file.txt 3-5 done: Community Edition selected

**Context:** Architecture 6.1/6.3 uses Iceberg JDBC catalog on `postgres:5432/iceberg_catalog` + `s3://banking-lakehouse` for all layers. On Databricks, Iceberg through Unity Catalog requires a trial/paid workspace + `databricks` Terraform provider. Phase 1 spike question: does Community Edition support reading/writing Iceberg via Unity Catalog?

**Decision:** CE path — Bronze stays Iceberg (JDBC catalog on local `iceberg_catalog` + MinIO `S3_ENDPOINT=http://minio:9000`). **Silver and Gold land as Delta** on the same Spark session / warehouse, read via UniForm/Delta where another engine needs them. Only the dbt target config changes (`dbt-databricks` profile `type: delta`). This matches Architecture 6.1 fallback: “Only the dbt target configuration changes.”

**Consequences:**
- `docker-compose --profile core` already provides MinIO + postgres `iceberg_catalog` for Bronze and ops.
- `sql/bronze_ddl.sql` stays Iceberg (`PARTITIONED BY days(_ingested_at)`). Silver/Gold DDL will be Delta equivalents in Phase 2.
- No Databricks Jobs API on CE — orchestration for Phase 1 batch uses `jobs/ingestion/bronze.py` via `uv run`, cron, or local Airflow `LocalExecutor` (`AIRFLOW__CORE__EXECUTOR` in `.env`), not `databricks jobs create`.
- Terraform `databricks` module deferred to Phase 6 paid workspace; local `docker`/`postgresql`/`minio` modules remain.

**Alternatives considered:** Wait for paid workspace to keep Iceberg everywhere — rejected because CE unblocks local dev now and the fallback is explicitly designed in Architecture 15-17.

**Reference:** `file.txt:26`, `PROJECT_PLAN.md:97`, `Architecture.md:252-253`, `jobs/common/spark.py:1` (catalog `banking` on JDBC), `jobs/common/config.py:1` (FULL_REFRESH_COLLECTIONS).
