# docs/runbooks/alert-dag-failure.md — DagRunFailed

**Fires when:** Airflow task failures increase within 15 minutes
(`increase(airflow_task_failures_total[15m]) > 0`).

## 1. Identify the failed task (2 min)

- Airflow UI -> the red DAG run -> read the task log (stack trace at the bottom).
- `ops.pipeline_runs`: `select * from ops.pipeline_runs where status != 'success' order by started_at desc limit 5`

## 2. Common causes and fixes

| Log signature | Cause | Fix |
|---|---|---|
| `AnalysisException: TABLE_OR_VIEW_NOT_FOUND` | upstream layer missing | run the missing layer first (`make ingest_py` / silver job / `scripts/build_gold.py`) |
| `MONGO_ERROR` / auth failures | Mongo down or keyfile broken | `make up PROFILE=core`; see `drill-mongo-down.md` |
| `UnsupportedOperationException: jdbc.schema-version` | catalog missing view support | fixed in `jobs/common/spark.py` (`jdbc.schema-version=V1`); pull latest |
| `DependentObjectsStillExist` on publish | legacy rename-swap | fixed (upsert publish); rerun `python -m jobs.publish.serving --table <t>` |
| Airflow scheduler dead | host sleep / compose down | `make up PROFILE=core`, scheduler auto-recovers |

## 3. After the fix

1. Clear the failed task in the Airflow UI (or re-trigger the DAG).
2. Confirm the run goes green and `ops.pipeline_runs` shows `success`.
3. If the same task failed 3+ times this week, open a fix ticket — do not keep
   hand-restarting (counts against the reliability work in the budget policy).
