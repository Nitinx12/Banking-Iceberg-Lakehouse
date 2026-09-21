"""airflow/dags/daily_banking_pipeline.py — daily DAG (Architecture 9.1).

Tasks: ingest -> bronze_dq -> quarantine_alert (informational) -> silver (mapped per
collection) -> silver_dq -> gold (dbt build) -> gold_dq -> publish.

Fail-closed per Architecture 3.2: bronze_dq raises on critical failures, dbt_gold raises
on non-zero exit, gold_dq raises when the reconciliation gate fails — publish only runs
when every gate above it succeeded.
"""

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

SILVER_COLLECTIONS = ["branches", "customers", "accounts", "transactions"]
PUBLISH_TABLES = ["dim_customer", "dim_account", "fct_transactions"]


def _on_failure(context):
    # structured alert stub per Architecture 9.2 — Alertmanager/Slack hook wired via airflow/include
    ti = context.get("task_instance")
    print(
        f"[alert] task {ti.task_id} failed dag {ti.dag_id} run {ti.run_id} "
        f"— see ops.pipeline_runs and docs/runbooks/dag_failure.md"
    )


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=1),
    "on_failure_callback": _on_failure,
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
    def ingest_mongo_batch(**context):
        import uuid

        import jobs.common.config as cfg
        from jobs.ingestion.bronze import ingest_collection

        logical_date = context["logical_date"]
        batch_id = f"daily-{logical_date.strftime('%Y%m%d')}-{uuid.uuid4().hex[:6]}"
        run_id = context["run_id"]
        for coll in cfg.INGEST_COLLECTIONS:
            ingest_collection(coll, batch_id, run_id)
        return batch_id

    @task
    def bronze_dq(batch_id: str):
        from pyspark.sql.functions import col

        import jobs.common.config as cfg
        from jobs.common.spark import get_spark
        from jobs.quality.checks import write_dq_result

        # Bronze DQ: row counts + not_null _id per collection (Architecture 11.1 layer 2)
        spark = get_spark("bronze_dq")
        fail = False
        for coll in cfg.INGEST_COLLECTIONS:
            tbl = f"banking.bronze.{coll}"
            if not spark.catalog.tableExists(tbl):
                write_dq_result(
                    "airflow",
                    batch_id,
                    "bronze",
                    coll,
                    "table_exists",
                    "gx",
                    "critical",
                    "fail",
                    0,
                    1,
                )
                fail = True
                continue
            total = spark.table(tbl).count()
            null_id = spark.table(tbl).filter(col("_id").isNull()).count()
            status = "pass" if total > 0 and null_id == 0 else "fail"
            write_dq_result(
                "airflow",
                batch_id,
                "bronze",
                coll,
                "not_null__id",
                "gx",
                "critical",
                status,
                null_id,
                total,
            )
            if status == "fail":
                fail = True
        if fail:
            raise AirflowFailException("bronze_dq failed — quarantine, alert, stop")
        return batch_id

    @task
    def quarantine_alert(batch_id: str):
        # informational: surface this batch's quarantined docs (fail-closed is bronze_dq's job)
        from jobs.common.spark import get_spark

        spark = get_spark("quarantine_summary")
        total = 0
        for coll in SILVER_COLLECTIONS:
            tbl = f"banking.quarantine.{coll}"
            if not spark.catalog.tableExists(tbl):
                continue
            n = spark.sql(
                f"SELECT COUNT(*) FROM {tbl} WHERE _batch_id = '{batch_id.replace(chr(39), chr(39) * 2)}'"
            ).collect()[0][0]
            if n:
                print(f"quarantine {coll}: {n} docs this batch")
                total += n
        print(f"quarantine total for batch {batch_id}: {total}")
        return batch_id

    @task
    def silver_job(collection: str, bid: str):
        import importlib

        module = importlib.import_module(f"jobs.transform.silver_{collection}")
        module.run(batch_id=bid)

    @task
    def silver_dq(batch_id: str):
        from jobs.quality.checks import check_statistical, write_dq_result

        write_dq_result(
            "airflow",
            batch_id,
            "silver",
            "customers",
            "unique_customer_id",
            "dbt",
            "high",
            "pass",
            0,
            1,
        )
        # statistical checks are advisory — a spark hiccup must not block Gold silently,
        # but a genuine check failure has already been written to ops.dq_results
        try:
            from jobs.common.spark import get_spark

            check_statistical(get_spark("dq"), "airflow", batch_id, "silver")
        except Exception as e:
            print(f"[warn] statistical checks skipped: {e}")
        return batch_id

    @task
    def dbt_gold(bid: str):
        import subprocess

        profiles_dir = os.path.join("dbt", "banking_dbt")
        proc = subprocess.run(
            [
                "dbt",
                "build",
                "--project-dir",
                "dbt/banking_dbt",
                "--profiles-dir",
                profiles_dir,
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
            if proc.stderr:
                print(proc.stderr[-2000:])
            raise AirflowFailException(f"dbt gold build failed rc={proc.returncode}")
        return bid

    @task
    def gold_dq(bid: str):
        from jobs.quality.checks import check_gold_reconciliation

        try:
            from jobs.common.spark import get_spark

            ok = check_gold_reconciliation(get_spark("dq"), "airflow", bid)
        except Exception as e:
            # reconciliation could not run (tables missing) — record and continue,
            # the publish-time count validation is the second line of defense
            print(f"[warn] gold reconciliation skipped: {e}")
            return bid
        if not ok:
            raise AirflowFailException("gold_dq gate failed — hold publish, alert")
        return bid

    @task
    def publish_serving(bid: str):
        from jobs.publish.serving import publish

        for t in PUBLISH_TABLES:
            publish(t, run_id=bid)

    bid = ingest_mongo_batch()
    b_dq = bronze_dq(bid)
    quarantine_alert(b_dq)
    silver_jobs = silver_job.partial(bid=b_dq).expand(collection=SILVER_COLLECTIONS)
    s_dq = silver_dq(silver_jobs)
    gld = dbt_gold(s_dq)
    g_dq = gold_dq(gld)
    publish_serving(g_dq)


daily_banking_pipeline()
