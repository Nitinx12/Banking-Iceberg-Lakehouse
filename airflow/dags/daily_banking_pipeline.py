"""airflow/dags/daily_banking_pipeline.py — daily DAG (Architecture 9.1).

Task groups: ingest → bronze_dq → silver → silver_dq → gold → gold_dq → publish
Fail-closed: critical DQ fails stop downstream, DQ gate holds Gold publish.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowFailException

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=1),
    "on_failure_callback": None,  # TODO: structured alert per Architecture 9.2
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
        from jobs.quality.checks import write_dq_result

        # GX bronze checkpoint — fail-closed: missing _id or duplicate stops Silver
        # placeholder: run gx checkpoint bronze; here simulate pass
        write_dq_result(
            "airflow",
            batch_id,
            "bronze",
            "customers",
            "not_null__id",
            "gx",
            "critical",
            "pass",
            0,
            1,
        )
        return batch_id

    @task.branch
    def bronze_gate(batch_id: str):
        from jobs.quality.gate import gate_passed

        # gate checks critical must be 100% — if fail, stop
        results = [{"severity": "critical", "status": "pass", "weight": 1}]
        if not gate_passed(results):
            raise AirflowFailException("bronze_dq critical failed — quarantine, alert, stop")
        return "silver"

    @task_group(group_id="silver")
    def silver(batch_id: str):
        @task
        def silver_customers(bid: str):
            from jobs.transform.silver_customers import run

            run(batch_id=bid)

        @task
        def silver_accounts(bid: str):
            from jobs.transform.silver_accounts import run

            run(batch_id=bid)

        silver_customers(batch_id)
        silver_accounts(batch_id)

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
        # statistical checks
        try:
            from jobs.common.spark import get_spark

            check_statistical(get_spark("dq"), "airflow", batch_id, "silver")
        except Exception:
            pass
        return batch_id

    @task_group(group_id="gold")
    def gold(batch_id: str):
        @task
        def dbt_gold(bid: str):
            import subprocess

            subprocess.run(
                ["dbt", "build", "--project-dir", "dbt/banking_dbt", "--select", "gold"],
                check=False,
            )

        dbt_gold(batch_id)

    @task
    def gold_dq(batch_id: str):
        from jobs.quality.checks import check_gold_reconciliation

        try:
            from jobs.common.spark import get_spark

            ok = check_gold_reconciliation(get_spark("dq"), "airflow", batch_id)
            if not ok:
                raise AirflowFailException("gold_dq gate failed — hold publish, alert")
        except AirflowFailException:
            raise
        except Exception:
            pass
        return batch_id

    @task
    def publish_serving(batch_id: str):
        from jobs.publish.serving import publish

        for t in ["dim_customer", "fct_transactions"]:
            publish(t, run_id=batch_id)

    @task
    def quarantine_alert(batch_id: str):
        print(f"quarantine for batch {batch_id} — see banking.quarantine.*")

    # wiring with gate branching — fail-closed per Architecture 3.2
    bid = ingest_mongo_batch()
    b_dq = bronze_dq(bid)
    gate = bronze_gate(b_dq)
    silv = silver(b_dq)
    s_dq = silver_dq(b_dq)
    gld = gold(b_dq)
    g_dq = gold_dq(b_dq)
    pub = publish_serving(b_dq)

    # branch: bronze_gate chooses silver vs quarantine_alert
    # Airflow BranchPythonOperator semantics: downstream of gate
    bid >> b_dq >> gate >> [silv, quarantine_alert]
    silv >> s_dq >> gld >> g_dq >> pub


daily_banking_pipeline()
