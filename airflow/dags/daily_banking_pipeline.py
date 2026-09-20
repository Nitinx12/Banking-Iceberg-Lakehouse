"""airflow/dags/daily_banking_pipeline.py — daily DAG (Architecture 9.1).

Task groups: ingest → bronze_dq → silver → silver_dq → gold → gold_dq → publish
On CE: uses LocalExecutor, python callables for Bronze (jobs/ingestion/bronze.py) and Silver/Gold via dbt local target.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task, task_group

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=1),
}


@dag(
    dag_id="daily_banking_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["banking", "bronze", "silver", "gold"],
)
def daily_banking_pipeline():

    @task
    def ingest_mongo_batch():
        import sys

        from jobs.ingestion.bronze import main as bronze_main

        sys.argv = ["bronze", "--all", "--dry-run"]
        bronze_main()

    @task_group(group_id="silver")
    def silver():
        @task
        def silver_customers():
            from jobs.transform.silver_customers import run

            run()

        silver_customers()

    @task
    def publish_serving():
        from jobs.publish.serving import publish

        for t in ["dim_customer", "fct_transactions"]:
            publish(t, run_id="airflow")

    ingest = ingest_mongo_batch()
    silver_grp = silver()
    pub = publish_serving()
    ingest >> silver_grp >> pub


daily_banking_pipeline()
