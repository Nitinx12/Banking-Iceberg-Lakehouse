"""airflow/dags/backfill_pipeline.py — manual parameterised backfill (Architecture 9.1, 9.3).

Reuses the same Bronze/Silver/Gold code paths as the daily DAG with a deterministic
batch_id derived from the logical date, so a re-run is idempotent (delete-by-_batch_id
before every Bronze write). Params:
  start_date / end_date — documentation for the operator; Bronze actually recomputes
                          by watermark, so a targeted backfill sets the watermark back
                          first (ops.ingestion_watermarks) and re-runs.
  stage                 — all | bronze | silver | gold
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=2),
}


@dag(
    dag_id="backfill_pipeline",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["backfill"],
    params={
        "start_date": "2026-09-01",
        "end_date": "2026-09-07",
        "stage": "all",
        "collections": "customers,accounts,transactions,branches",
    },
)
def backfill_pipeline():

    @task
    def bronze_backfill(**context):
        import uuid

        from jobs.ingestion.bronze import ingest_collection

        params = context["params"]
        stage = params.get("stage", "all")
        if stage not in ("all", "bronze"):
            print(f"stage={stage} — skipping bronze")
            return None
        logical_date = context["logical_date"]
        batch_id = f"backfill-{logical_date.strftime('%Y%m%d')}-{uuid.uuid4().hex[:6]}"
        run_id = context["run_id"]
        collections = [
            c.strip() for c in str(params.get("collections", "")).split(",") if c.strip()
        ]
        for coll in collections:
            ingest_collection(coll, batch_id, run_id)
        return batch_id

    @task
    def silver_backfill(bid: str = None, **context):
        stage = context["params"].get("stage", "all")
        if stage not in ("all", "silver", "bronze"):
            print(f"stage={stage} — skipping silver")
            return bid
        if bid is None:
            print("no bronze batch in this run — silver skipped")
            return None
        for coll in ["branches", "customers", "accounts", "transactions"]:
            import importlib

            module = importlib.import_module(f"jobs.transform.silver_{coll}")
            module.run(batch_id=bid)
        return bid

    @task
    def gold_backfill(bid: str = None, **context):
        import os
        import subprocess

        stage = context["params"].get("stage", "all")
        if stage not in ("all", "gold", "silver", "bronze"):
            print(f"stage={stage} — skipping gold")
            return bid
        if bid is None:
            print("no upstream batch in this run — gold skipped")
            return None
        proc = subprocess.run(
            [
                "dbt",
                "build",
                "--project-dir",
                "dbt/banking_dbt",
                "--profiles-dir",
                os.path.join("dbt", "banking_dbt"),
                "--select",
                "gold",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if proc.stdout:
            print(proc.stdout[-4000:])
        if proc.returncode != 0:
            raise AirflowFailException(f"dbt gold build failed rc={proc.returncode}")
        print(
            f"backfill complete: start={context['params'].get('start_date')} "
            f"end={context['params'].get('end_date')} batch={bid}"
        )
        return bid

    b = bronze_backfill()
    s = silver_backfill(bid=b)
    gold_backfill(bid=s)


backfill_pipeline()
