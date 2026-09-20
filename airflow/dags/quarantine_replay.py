"""airflow/dags/quarantine_replay.py — Manual replay of quarantined rows (Architecture 9.1, 11.4)."""

from datetime import datetime, timedelta

from airflow.decorators import dag, task

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=1),
}


@dag(
    dag_id="quarantine_replay",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["quality", "quarantine"],
    params={"table": "customers", "batch_id": ""},
)
def quarantine_replay():
    @task
    def replay(**context):
        table = context["params"].get("table", "customers")
        batch_id = context["params"].get("batch_id", "")
        # replay logic: read banking.quarantine.<table>, fix, MERGE into silver, mark resolved
        print(f"replay quarantine.{table} batch={batch_id} — requires manual fix + rerun")

    replay()


quarantine_replay()
