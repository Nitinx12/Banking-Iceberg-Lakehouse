"""jobs/observability/sla.py — freshness SLO measurement (Architecture 13.2/13.4).

Pure module (no Airflow imports) so the sla_monitor DAG and the Phase 5 proof
drills (forced delay, forced breach) exercise the exact same code.
"""

import os

from sqlalchemy import create_engine, text

import jobs.common.config as cfg

FRESHNESS_SLO_SECONDS = int(os.getenv("FRESHNESS_SLO_SECONDS", "93600"))  # 26h

# Freshness is defined on fact tables (_loaded_at). Dims have no load timestamp —
# their freshness equals the freshness of the pipeline that produced them.
SERVING_TABLES = [
    "fct_transactions",
    "fct_card_transactions",
]


def _engine():
    user = os.getenv("POSTGRES_USER", "postgres")
    pw = os.getenv("POSTGRES_PASSWORD", "")
    return create_engine(
        f"postgresql+psycopg2://{user}:{pw}@{cfg.POSTGRES_HOST}:{cfg.POSTGRES_PORT}/{cfg.POSTGRES_WAREHOUSE_DB}",
        pool_pre_ping=True,
    )


def measure_freshness(table: str) -> tuple[int, object] | None:
    """Return (freshness_seconds, max_loaded_at) for one serving table."""
    eng = _engine()
    try:
        with eng.begin() as c:
            row = c.execute(
                text(
                    "SELECT GREATEST(0, COALESCE(EXTRACT(EPOCH FROM (now() - max(_loaded_at)))::bigint, 0)), "
                    "max(_loaded_at) FROM serving." + table
                )
            ).fetchone()
        if row is None or row[1] is None:
            return None
        return int(row[0]), row[1]
    finally:
        eng.dispose()


def record_measurement(table: str, freshness: int, max_loaded) -> bool:
    """Write ops.freshness_metrics + breach event; return True if breached."""
    breached = freshness > FRESHNESS_SLO_SECONDS
    eng = _engine()
    try:
        with eng.begin() as c:
            c.execute(
                text(
                    "INSERT INTO ops.freshness_metrics (table_name, freshness_seconds, max_loaded_at) "
                    "VALUES (:t, :f, :m)"
                ),
                {"t": table, "f": freshness, "m": max_loaded},
            )
            if breached:
                c.execute(
                    text(
                        "INSERT INTO ops.sla_events (table_name, target_name, target_seconds, actual_seconds, breached) "
                        "VALUES (:t, 'freshness_gold', :slo, :a, true)"
                    ),
                    {"t": table, "slo": FRESHNESS_SLO_SECONDS, "a": freshness},
                )
    finally:
        eng.dispose()
    return breached


def push_slo_metrics(fresh_by_table: dict[str, int], breach_count: int, run_id: str) -> bool:
    from jobs.common.metrics import push_metrics

    series: list[tuple[dict, float]] = [
        ({"table": tbl}, float(fresh)) for tbl, fresh in fresh_by_table.items()
    ]
    metrics: dict[str, tuple[dict, float] | list[tuple[dict, float]]] = {
        "data_freshness_seconds": series,
        "sla_breach_total": ({}, float(breach_count)),
    }
    return push_metrics("sla_monitor", metrics, run_id=run_id)


def run_check(run_id: str) -> tuple[int, int]:
    """Full cycle: measure every table, record, push. Returns (checked, breaches)."""
    checked, breaches = 0, 0
    fresh_by_table: dict[str, int] = {}
    for tbl in SERVING_TABLES:
        m = measure_freshness(tbl)
        if m is None:
            continue
        freshness, max_loaded = m
        fresh_by_table[tbl] = freshness
        checked += 1
        if record_measurement(tbl, freshness, max_loaded):
            breaches += 1
    push_slo_metrics(fresh_by_table, breaches, run_id)
    return checked, breaches
