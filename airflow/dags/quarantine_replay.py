"""airflow/dags/quarantine_replay.py — manual replay of quarantined rows (Architecture 9.1, 11.4).

Reads banking.quarantine.<table> rows for the given batch, hands the raw _doc payloads
back through the collection's Silver job (which re-applies typing, dedup and quarantine),
then logs the replay to ops.pipeline_runs. Silver remains the single validation point —
a doc that fails again simply re-quarantines with a new batch_id.

Params: table (required), batch_id (empty = replay all unreplayed quarantine rows).
"""

from datetime import UTC, datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

SILVER_TABLES = {
    "branches",
    "customers",
    "accounts",
    "transactions",
    "loans",
    "cards",
    "card_transactions",
    "loan_payments",
    "support_tickets",
    "employees",
}

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
        import uuid

        from pyspark.sql import functions as F

        from jobs.common.spark import get_spark

        params = context["params"]
        table = params.get("table", "customers")
        batch_id = params.get("batch_id", "")
        run_id = context["run_id"]
        if table not in SILVER_TABLES:
            raise AirflowFailException(
                f"unknown table {table!r} — choose one of {sorted(SILVER_TABLES)}"
            )

        spark = get_spark("quarantine_replay")
        qtbl = f"banking.quarantine.{table}"
        if not spark.catalog.tableExists(qtbl):
            print(f"{qtbl} does not exist — nothing to replay")
            return 0

        q = spark.table(qtbl)
        if batch_id:
            safe = batch_id.replace("'", "''")
            q = q.filter(F.col("_batch_id") == safe)
        rows = q.select("_doc", "_batch_id").collect()
        if not rows:
            print("no quarantine rows matched — nothing to replay")
            return 0

        # feed the raw docs back through Silver as a fresh replay batch
        replay_batch = (
            f"replay-{datetime.now(UTC).strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"
        )
        ingested_at = datetime.now(UTC)
        bronze_rows = [
            (
                f"replay_{i}",
                r["_doc"],
                "insert",
                None,
                ingested_at,
                replay_batch,
                run_id,
                table,
                "1.0",
            )
            for i, r in enumerate(rows)
        ]
        schema = (
            "_id STRING, _doc STRING, _op STRING, _source_ts TIMESTAMP, _ingested_at TIMESTAMP, "
            "_batch_id STRING, _run_id STRING, _source_collection STRING, _schema_version STRING"
        )
        rdf = spark.createDataFrame(bronze_rows, schema=schema)
        spark.sql(f"DELETE FROM banking.bronze.{table} WHERE _batch_id = '{replay_batch}'")
        rdf.writeTo(f"banking.bronze.{table}").append()

        import importlib

        module = importlib.import_module(f"jobs.transform.silver_{table}")
        module.run(batch_id=replay_batch)
        print(
            f"replayed {len(rows)} quarantined docs through silver_{table} (batch {replay_batch})"
        )
        return len(rows)

    replay()


quarantine_replay()
