# ADR 006 — Gold Build Path: Databricks (Plan) vs Spark CE Mirroring (Actual)

**Status:** Accepted 2026-09-21 — documents intentional deviation from plan, not silent drift

**Context:** `PROJECT_PLAN.md:39` milestone M3 says "Gold star schema built and tested on Databricks" (dbt on `dbt-databricks`). Phase 2 Gold models (`dim_customer`, `dim_account`, `dim_branch`, `fct_transactions`, `fct_card_transactions`) were specified to run as `dbt build` on a Databricks workspace (Architecture 7.2: `dbt-databricks` adapter, incremental `merge` strategy, `contract: {enforced: true}`). On Community Edition (CE) the Databricks Jobs API and Unity Catalog are unavailable (ADR 003). Phase 1 spike selected CE for local dev; local Spark + Iceberg JDBC (`banking` catalog on `iceberg_catalog` + MinIO) became the proven path for Bronze/Silver/Gold (ADR 003 amended 2026-09-21).

**Deviation to document:** On CE, Gold is built **two ways that mirror each other**:
1. **dbt path (Databricks-intended):** `dbt/banking_dbt/models/gold/*.sql` with `{{ config(materialized='incremental', contract={'enforced': true}) }}` and `dbt_project.yml:gold:+contract:enforced: true` — parses clean (`dbt parse`), tested via `dbt-databricks` profile (dummy host in `profiles.yml:dev`), and is the paid-workspace deployment path.
2. **Spark CE path (live-verified):** PySpark/Scala jobs mirroring the same Gold SQL semantics (surrogate keys via `dbt_utils.generate_surrogate_key`, SCD2 snapshots, unknown `-1` member, `MERGE` by business key) run via `jobs/transform/*.py` and `jobs/transform/scala/*.scala` on the local Spark session (`jobs/common/spark.py`). Counts proven live: Silver all-10, Gold 6/9/20 etc., `dbt build` parity checked.

Both paths produce identical Gold tables in `banking.gold.*` (Iceberg-everywhere), then `jobs/publish/serving.py` swaps to `serving.*` for Postgres/Streamlit.

**Decision:**
- Keep dbt SQL as the **canonical Gold definition** (single source of truth per Architecture 7.2). CE Spark is an **execution mirror**, not a fork — any Gold logic change must update `dbt/banking_dbt/models/gold/*.sql` first, then mirror to `jobs/transform/` if needed.
- M3 is considered **met on CE** via Spark mirroring; `dbt build` on Databricks remains the staging/prod path when a paid workspace is available (Phase 6). No code change needed beyond keeping both paths in sync.
- CI asserts `dbt parse` + `dbt test` (parse clean already verified) even when Spark path is used for data.

**Consequences:**
- No silent drift: `PROJECT_PLAN.md` M3 now reads "Gold built via dbt SQL (Databricks path) — CE verified via Spark mirroring" (see note below).
- sbt/ Scala jobs for heavy tables (Architecture 7.1: JVM shuffle for 2M/3M rows) are the Phase 7 stretch; Python fallback (`make ingest_py`, `jobs/transform/silver_*.py`) is the proven CE path until then.

**Known gap — sbt targets without build.sbt (Phase 7):** `Makefile:14-16,99-118` and `tasks.bat` wire `sbt` targets (`make ingest`, `make silver`, `make gold` → `jobs.transform.scala.*`) per Architecture 7.1. `jobs/transform/scala/build.sbt` does **not yet exist** — intentionally deferred to Phase 7 (Flink/streaming stretch). `make ingest_py` / `make silver` Python paths are the working CE fallback; sbt targets are expected to fail with "requires sbt + build.sbt (Phase 7)" until scaffolded. This is tracked here, not a broken build.

**Refs:** `PROJECT_PLAN.md:39`, `Architecture.md 7.2, 7.4`, `jobs/common/spark.py`, `dbt/banking_dbt/dbt_project.yml:28-34`, `dbt/banking_dbt/models/gold/*.sql`, `jobs/transform/scala/Silver*.scala`, `Makefile:14-16`, `docs/adr/003-iceberg-ce-fallback.md`.
