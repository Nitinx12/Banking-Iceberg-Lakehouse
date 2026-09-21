"""airflow/dags/sla_monitor.py — freshness + SLA/SLO per Architecture 12-13, every 5m.

Uses jobs/observability/sla.py (plain module) so proof drills exercise the same
measurement code: per-table measured freshness into ops.freshness_metrics, breach
rows into ops.sla_events, gauges pushed to the Pushgateway.
"""

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
        from jobs.observability.sla import run_check

        checked, breaches = run_check(run_id="sla-dag")
        print(f"sla_monitor: {checked} tables checked, {breaches} SLA breach events")
        return breaches

    check_freshness()


sla_monitor()
