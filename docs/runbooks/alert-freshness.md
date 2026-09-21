# Runbook: Freshness Breach — `sla_monitor` (Architecture 13.4, SLA 13.2)

**Alert:** `FreshnessBreach` — `data_freshness_seconds{table, layer}` > SLO. Fires via `sla_monitor` DAG → `ops.freshness_metrics` + `ops.sla_events` → Prometheus `sla_breach_total` → Alertmanager `slack`/`slack-critical`.
**Severity:** `high` if Gold >25h (warn), `critical` if >26h (SLA breach). Paging window: critical pages immediately (Architecture 12.2: ack 15m).
**SLO link:** Gold facts `now - max(_loaded_at)` SLO 24h30m, SLA 26h (Architecture 13.2). Bronze 25h/26h. Check `monitoring/prometheus/prometheus.yml` + `FRESHNESS_SLO_SECONDS` env (default 93600 = 26h).

## Symptoms
- Slack `#data-alerts` / `#data-critical`: `FreshnessBreach{table="fct_transactions", actual=…}`
- Grafana Dashboard 2 (Freshness & SLO) red / `data_freshness_seconds` gauge > 93600
- `ops.sla_events` new row `breached=true`, `ops.freshness_metrics.freshness_seconds` high
- Streamlit Pipeline Health page banner on affected table
- `sla_monitor` DAG log: `"SLA breach events: N"`

## Impact
- Dashboard shows stale data (Executive Overview, Transactions). SLO error budget burning.
- If >50% budget burned (Architecture 13.3), feature work pauses per error budget policy.

## Triage (5 min)
1. **Confirm scope:** `psql banking_dw -c "SELECT table_name, freshness_seconds/3600 as hrs, max_loaded_at, checked_at FROM ops.freshness_metrics ORDER BY checked_at DESC LIMIT 5"` and `SELECT * FROM ops.sla_events WHERE breached ORDER BY checked_at DESC LIMIT 5`
2. **Check pipeline health:** `make status` or `SELECT run_id, stage, status, rows_written, finished_at FROM ops.pipeline_runs ORDER BY started_at DESC LIMIT 10` — did `daily_banking_pipeline` succeed today? Check Airflow UI for failed stages.
3. **Check source freshness:** `dbt source freshness` on Bronze (`loaded_at_field: _ingested_at`, warn 25h/error 26h per `dbt/banking_dbt/models/sources.yml`) — `make dbt_build --select source:bronze`
4. **Check ingestion watermark:** `SELECT source_collection, last_watermark, last_batch_id, updated_at FROM ops.ingestion_watermarks` — stale watermark means ingest didn't advance.
5. **Check infra:** `make health` (postgres/mongo/minio/airflow), `docker compose ps`, Prometheus `up{job="postgres"}`.

## Diagnosis paths
- **Ingest stalled** (`last_watermark` old): see `drill-mongo-down.md`. Mongo unreachable, overlap window, or watermark not advancing after commit (Architecture 5.2 step 4).
- **Silver/Gold not built:** `daily_banking_pipeline` silver/gold or `gold_dq` failed — check `ops.dq_results` and Airflow logs (`logs/silver/*.log`, `logs/gold/*.log`).
- **Publish not swapped:** Gold built but `serving.fct_transactions` old — check `jobs/publish/serving.py` logs and `ops.pipeline_runs WHERE stage LIKE 'publish_%'`.
- **Clock / timezone:** Ensure UTC storage vs business TZ eval (`.env` `BUSINESS_TZ`); compare `now()` vs `max(_loaded_at)`.
- **Backfill needed:** Late data beyond overlap 10m — requires `backfill_pipeline`.

## Mitigation
- **If ingest stuck:** Fix connectivity (`scripts/sh/wait_for_services.sh`), then re-run: `make ingest_py BATCH_ID=<batch>` or Airflow `ingest_mongo_batch` (idempotent delete-by-`_batch_id` per Architecture 5.2). Verify `SELECT count(*) FROM banking.bronze.* WHERE _batch_id='<batch>'`.
- **If pipeline failed:** Clear failed task in Airflow and re-run from failed stage — idempotent MERGE makes re-run safe. `airflow dags trigger daily_banking_pipeline --conf '{"date":"YYYY-MM-DD"}'`
- **If publish failed mid-swap:** Serving keeps previous version (transaction rollback). Re-run `make publish` or `python -m jobs.publish.serving --table fct_transactions --run-id manual-freshness-fix`. `ANALYZE` runs automatically.
- **If legitimate delay (upstream late):** Document in `ops.sla_events` and Streamlit banner; consider temporary SLO override with recorded reason and post-incident note in `docs/incidents/`.
- **Backfill for late window:** `airflow dags trigger backfill_pipeline --conf '{"start_date":"YYYY-MM-DD", "end_date":"YYYY-MM-DD", "stages":["bronze","silver","gold","publish"]}'` then verify Gold counts match Silver parity.

## Verify recovery
1. `SELECT freshness_seconds FROM ops.freshness_metrics WHERE table_name='fct_transactions' ORDER BY checked_at DESC LIMIT 1` → < 90000 (25h)
2. `SELECT max(_loaded_at) FROM serving.fct_transactions` matches `SELECT max(_loaded_at) FROM banking.gold.fct_transactions` (publish parity)
3. Grafana freshness gauge returns to green, Alertmanager resolved notification
4. Prometheus `sla_breach_total` stops increasing

## Prevention / follow-up
- Tune `FRESHNESS_SLO_SECONDS` and `sources.yml warn_after/error_after` after 2 weeks of real data (Phase 5 SLO review).
- Ensure `iceberg_maintenance` DAG not causing lock contention; check snapshot expiry window (Bronze 7d, Silver/Gold 30d per Architecture 6.5).
- Add post-incident note to `docs/incidents/` per Architecture 13.5 step 4.

**Refs:** `airflow/dags/sla_monitor.py`, `Architecture.md 13.4-13.5`, `dbt/banking_dbt/models/sources.yml:9`, `sql/init_postgres.sql:ops.freshness_metrics/sla_events`, `monitoring/alertmanager/alertmanager.yml`.
