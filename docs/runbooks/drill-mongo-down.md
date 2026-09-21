# Runbook: MongoDB Unreachable — Ingest Failure (Architecture 19, 5.2)

**Alert:** `MongoUnreachable` / `IngestFailure` — `ingest_mongo_batch` task error, connection probe fail, or `sla_monitor` freshness breach on Bronze. Fires via Airflow `on_failure_callback` (Architecture 9.2) → Alertmanager `slack`/`slack-critical` (critical if batch SLA missed).
**Severity:** `critical` if `daily_banking_pipeline` ingest fails for SLA window; `high` for transient retry.

## Symptoms
- Slack: `ingest_mongo_batch failed — mongo unreachable` with `run_id`, `collection`, `batch_id`
- Airflow task `ingest_mongo_batch` (mapped per collection) `failed` after 2 retries ×2m backoff, or `bronze_dq` not reached
- Logs: `logs/ingest/*.log` show `pymongo.errors.ServerSelectionTimeoutError`, `replicaSet rs0 not found`, or `secondaryPreferred` read timeout
- `ops.ingestion_watermarks` not advancing: `SELECT source_collection, last_watermark, updated_at FROM ops.ingestion_watermarks WHERE updated_at < now() - interval '2 hours'`
- `make health` shows `mongo: unhealthy` or `docker compose ps` `banking_mongo` `unhealthy`/`restarting`
- Prometheus `up{job="mongo"}` down if exporter configured

## Impact
- Bronze not landed for affected collection(s) — Silver/Gold stale. No data loss (Bronze append-only with `_batch_id` + `_doc_hash`), but freshness SLO burning.
- CDC path (Phase 7) also blocked if replicaSet down — change streams require `mongod --replSet` (Architecture 4.3).

## Triage (5 min)
1. **Check compose health:** `make health` or `bash scripts/sh/healthcheck.sh`; `docker compose --profile core ps`; `docker logs banking_mongo --tail 50`; `docker exec banking_mongo mongosh --eval 'rs.status()' --quiet`
2. **Check watermark & ops:** `SELECT * FROM ops.pipeline_runs WHERE stage LIKE 'ingest%' ORDER BY started_at DESC LIMIT 10` — which collections failed? Overlap window is 10m (`INGEST_OVERLAP_MINUTES`).
3. **Check network/creds:** `env | grep MONGO`, `.env` `MONGO_INITDB_ROOT_USERNAME/PASSWORD`, `MONGO_PORT` (host vs `mongo:27017` inside Docker). Common pitfall: host runs use `localhost:27017` with `S3_ENDPOINT=http://localhost:9000` vs compose `mongo:27017`.
4. **Check keyfile (compose quirk):** `docker exec banking_mongo ls -l /data/db/mongo-keyfile` — if root-owned stale file, the `999:999` chown fix in `docker-compose.yml` may need volume prune + `make up PROFILE=core` re-init (see `docker-compose.yml:34`).
5. **Check disk/oplog:** `mongosh --eval 'db.adminCommand({replSetGetStatus:1})'` and `db.adminCommand({getReplicationInfo:1})` — oplog too small for paused job to resume (Phase 7).

## Mitigation
- **Transient network (first 2 retries):** Airflow auto-retries 2×2m with exponential backoff (`daily_banking_pipeline.py:32`). Often self-recovers — verify `rs.status().ok == 1` and wait for next schedule before manual action.
- **ReplicaSet lost:** `docker compose --profile core restart mongo mongo-init && bash scripts/sh/wait_for_services.sh && bash scripts/sh/run_ingestion.sh` or `tasks.bat` on Windows. `mongo-init` runs `rs.initiate({_id:"rs0", members:[{_id:0, host:"mongo:27017"}]})` if needed.
- **Auth / keyFile issue:** `docker compose down && docker volume rm hdfc-bank-lakehouse_mongodata` (loses local data — only for dev) then `make up PROFILE=core && make seed_mongo`. For prod, restore from backup, do not prune volume without backup.
- **Re-run ingest idempotently:** Bronze deletes by `_batch_id` before write (Architecture 5.2/9.3) — safe to re-run same window. `make ingest_py BATCH_ID=<batch>` or `airflow dags trigger daily_banking_pipeline --conf '{"date":"YYYY-MM-DD","collections":["transactions","accounts"]}'` or manual `python -m jobs.ingestion.bronze --collection transactions --batch-id <id>`.
- **Overlap protection:** Re-run uses `watermark_filter = {created_at: {$gt: last - 10m}}` (`jobs/ingestion/watermark.py`), so late commits within 10m are not missed.

## Verify
1. `docker exec banking_mongo mongosh --quiet --eval 'db.adminCommand("ping").ok'` → `1`, `rs.status().ok==1`, PRIMARY elected.
2. `SELECT source_collection, last_watermark FROM ops.ingestion_watermarks` advancing; `SELECT count(*) FROM banking.bronze.<collection> WHERE _batch_id='<batch>'` matches Mongo `countDocuments()` for same window.
3. `ops.pipeline_runs` new row `stage='ingest_<collection>', status='success'` with `rows_written >0`.
4. Downstream `bronze_dq` passes and `sla_monitor` freshness recovers (see `alert-freshness.md`).

## Prevention
- Keep `MONGO_PORT` and `S3_ENDPOINT` consistent per host vs Docker (`scripts/sh/lib.sh` `require_env`).
- Ensure `docker-compose.yml` `mongo` healthcheck retries 30×5s and `mongo-init` wait loop covers slow cold start.
- For Phase 7 CDC, ensure oplog size sufficient for pause > checkpoint interval (60s per Architecture 10.3) + savepoint discipline.

**Refs:** `jobs/ingestion/bronze.py:54,256`, `jobs/ingestion/watermark.py`, `jobs/common/config.py:INGEST_OVERLAP_MINUTES`, `docker-compose.yml:26-73`, `scripts/sh/wait_for_services.sh`, `scripts/sh/run_ingestion.sh`, `Architecture.md 5.2, 9.3, 19`.
