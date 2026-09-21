"""airflow/dags/sla_monitor.py — freshness + SLA/SLO per Architecture 12-13, every 5m.

Reads serving-layer freshness (max _loaded_at) into ops.freshness_metrics, then raises
an ops.sla_events breach row when a table exceeds the freshness SLO (26h per
Architecture 13.4). Every insert is a plain VALUES row — no cross-table INSERT...SELECT,
so a partial table set degrades to fewer rows instead of an SQL error.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=5),
}

FRESHNESS_SLO_SECONDS = int(__import__("os").getenv("FRESHNESS_SLO_SECONDS", "93600"))  # 26h


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

        import jobs.common.config as cfg

        user = os.getenv("POSTGRES_USER", "postgres")
        pw = os.getenv("POSTGRES_PASSWORD", "")
        eng = create_engine(
            f"postgresql+psycopg2://{user}:{pw}@{cfg.POSTGRES_HOST}:{cfg.POSTGRES_PORT}/{cfg.POSTGRES_WAREHOUSE_DB}",
            pool_pre_ping=True,
        )
        tables = ["fct_transactions", "dim_customer", "dim_account"]
        inserted = 0
        with eng.begin() as c:
            for tbl in tables:
                # one row per table per check: real measured freshness, not a joined guess
                row = c.execute(
                    text(
                        "SELECT COALESCE(EXTRACT(EPOCH FROM (now() - max(_loaded_at)))::bigint, 0), "
                        "max(_loaded_at) FROM serving." + tbl
                    )
                ).fetchone()
                if row is None or row[1] is None:
                    continue
                freshness, max_loaded = int(row[0]), row[1]
                c.execute(
                    text(
                        "INSERT INTO ops.freshness_metrics (table_name, freshness_seconds, max_loaded_at) "
                        "VALUES (:t, :f, :m)"
                    ),
                    {"t": tbl, "f": freshness, "m": max_loaded},
                )
                if freshness > FRESHNESS_SLO_SECONDS:
                    c.execute(
                        text(
                            "INSERT INTO ops.sla_events (table_name, target_name, target_seconds, actual_seconds, breached) "
                            "VALUES (:t, 'freshness_gold', :slo, :a, true)"
                        ),
                        {"t": tbl, "slo": FRESHNESS_SLO_SECONDS, "a": freshness},
                    )
                    inserted += 1
        print(f"sla_monitor: {len(tables)} tables checked, {inserted} SLA breach events")

    check_freshness()


sla_monitor()
