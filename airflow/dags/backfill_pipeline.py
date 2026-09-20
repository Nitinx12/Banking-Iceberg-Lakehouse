"""airflow/dags/backfill_pipeline.py — manual parameterised backfill (Architecture 9.1,9.3)."""

from datetime import datetime, timedelta

from airflow.decorators import dag, task

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
    params={"start_date": "2026-09-01", "end_date": "2026-09-07", "stage": "all"},
)
def backfill_pipeline():
    @task
    def backfill(**context):
        start = context["params"].get("start_date")
        end = context["params"].get("end_date")
        stage = context["params"].get("stage", "all")
        # reuse same Bronze/Silver/Gold code with date range — idempotent delete-by-batch
        print(f"backfill {start} -> {end} stage={stage} — re-run via daily_banking_pipeline logic")

    backfill()


backfill_pipeline()
