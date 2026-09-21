"""airflow/dags/iceberg_maintenance.py — compaction, snapshot expiry, orphan cleanup (Architecture 6.5)."""

from datetime import datetime, timedelta

from airflow.decorators import dag, task

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


@dag(
    dag_id="iceberg_maintenance",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["maintenance", "iceberg"],
)
def iceberg_maintenance():
    @task
    def rewrite_data_files():
        from jobs.common.spark import get_spark

        spark = get_spark("maintenance")
        for tbl in ["banking.bronze.transactions", "banking.bronze.card_transactions"]:
            spark.sql(
                f"CALL banking.system.rewrite_data_files(table => '{tbl}', strategy => 'sort', sort_order => 'zorder(_id)')"
            )

    @task
    def rewrite_manifests():
        from jobs.common.spark import get_spark

        spark = get_spark("maintenance")
        for tbl in ["banking.bronze.transactions", "banking.bronze.card_transactions"]:
            spark.sql(f"CALL banking.system.rewrite_manifests(table => '{tbl}')")

    @task
    def expire_snapshots():
        import os

        from jobs.common.spark import get_spark

        spark = get_spark("maintenance")
        # Bronze 7d per Architecture 6.5 — relative to the current timestamp, never hardcoded
        bronze_days = int(os.getenv("ICEBERG_BRONZE_RETENTION_DAYS", "7"))
        for tbl in ["banking.bronze.transactions", "banking.bronze.card_transactions"]:
            spark.sql(
                f"CALL banking.system.expire_snapshots(table => '{tbl}', "
                f"older_than => current_timestamp() - INTERVAL {bronze_days} DAYS)"
            )

    @task
    def remove_orphan_files():
        from jobs.common.spark import get_spark

        spark = get_spark("maintenance")
        for tbl in ["banking.bronze.transactions", "banking.bronze.card_transactions"]:
            spark.sql(f"CALL banking.system.remove_orphan_files(table => '{tbl}')")

    r = rewrite_data_files()
    m = rewrite_manifests()
    e = expire_snapshots()
    o = remove_orphan_files()
    r >> m >> e >> o


iceberg_maintenance()
