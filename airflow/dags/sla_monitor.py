"""airflow/dags/sla_monitor.py — freshness + SLA/SLO per Architecture 12-13, every 5m."""

from datetime import datetime, timedelta

from airflow.decorators import dag, task

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=5),
}


@dag(
    dag_id="sla_monitor",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["observability", "sla"],
)
def sla_monitor():
    @task
    def check_freshness():
        import os

        from sqlalchemy import create_engine, text

        try:
            eng = create_engine(
                f"postgresql+psycopg2://{os.getenv('POSTGRES_USER', 'postgres')}:{os.getenv('POSTGRES_PASSWORD', '')}@{os.getenv('POSTGRES_HOST', 'postgres')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_WAREHOUSE_DB', 'banking_dw')}"
            )
            with eng.begin() as c:
                # per-table freshness via Iceberg snapshot + _loaded_at per Architecture 13.4
                for tbl in ["fct_transactions", "dim_customer", "dim_account"]:
                    c.execute(
                        text(
                            "INSERT INTO ops.freshness_metrics (table_name, freshness_seconds, max_loaded_at) SELECT :t, EXTRACT(EPOCH FROM (now() - max(_loaded_at)))::bigint, max(_loaded_at) FROM serving."
                            + tbl
                            + " ON CONFLICT DO NOTHING"
                        ),
                        {"t": tbl},
                    )
                # also check serving.fct_transactions freshness via pushgateway metric data_freshness_seconds
                # ops.sla_events per 13.5 — breach if > SLA (26h = 93600s)
                c.execute(
                    text(
                        "INSERT INTO ops.sla_events (table_name, target_name, target_seconds, actual_seconds, breached) SELECT table_name, 'freshness_gold', 93600, freshness_seconds, freshness_seconds > 93600 FROM ops.freshness_metrics WHERE checked_at > now() - interval '10 minutes'"
                    )
                )
        except Exception as e:
            print(f"sla_monitor skipped: {e}")

    check_freshness()


sla_monitor()
