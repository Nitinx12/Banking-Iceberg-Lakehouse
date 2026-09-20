# Drill: MongoDB unreachable (Architecture 19)

**Detection:** ingest task error, `sla_monitor` freshness breach.
**Response:** Airflow retry 2×2m, then alert. Fix connectivity, re-run `ingest_mongo_batch` (idempotent delete-by-batch).
**Recovery:** `scripts/sh/wait_for_services.sh` → `scripts/sh/run_ingestion.sh`
