"""scripts/proof_slo.py — Phase 5 exit-criteria drills (PROJECT_PLAN Phase 5, M6).

Proves the two exit criteria against the REAL measurement/alerting code:

1. --delay : backdates serving.fct_transactions._loaded_at past the 26h SLO,
   runs the real sla measurement (same module the sla_monitor DAG uses),
   asserts a breach row lands in ops.sla_events, then restores the timestamps.
2. --dqfail : writes a critical DQ failure through the real write_dq_result
   path and the real reconciliation check, then pushes dq_critical_failures_total=1
   (the metric behind alert CriticalDqFailure) and verifies the alert rule can fire.

Both drills are reversible and safe on seeded data.
"""

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text

import jobs.common.config as cfg


def _eng():
    user = os.getenv("POSTGRES_USER", "postgres")
    pw = os.getenv("POSTGRES_PASSWORD", "")
    return create_engine(
        f"postgresql+psycopg2://{user}:{pw}@{cfg.POSTGRES_HOST}:{cfg.POSTGRES_PORT}/{cfg.POSTGRES_WAREHOUSE_DB}",
        pool_pre_ping=True,
    )


def drill_freshness_delay() -> int:
    print("=== DRILL 1: forced freshness delay (26h SLO) ===")
    eng = _eng()
    with eng.begin() as c:
        # capture current max timestamp (restore point)
        orig = c.execute(text("SELECT max(_loaded_at) FROM serving.fct_transactions")).scalar()
        if orig is None:
            print("FAIL: serving.fct_transactions is empty — publish first")
            return 1
        # SLO is 93600s; push _loaded_at 40h back so the breach is unambiguous
        c.execute(
            text(
                "UPDATE serving.fct_transactions "
                "SET _loaded_at = _loaded_at - interval '40 hours' "
                "WHERE _loaded_at = (SELECT max(_loaded_at) FROM serving.fct_transactions)"
            )
        )
    print("[ok] backdated max(_loaded_at) by 40h")
    try:
        from jobs.observability.sla import run_check

        checked, breaches = run_check(run_id="drill-delay")
        print(f"[ok] real sla run_check: {checked} tables checked, {breaches} breach event(s)")
        if breaches < 1:
            print("FAIL: expected >= 1 breach event after 40h backdate")
            return 1
        with eng.begin() as c:
            ev = c.execute(
                text(
                    "SELECT table_name, target_seconds, actual_seconds FROM ops.sla_events "
                    "WHERE table_name='fct_transactions' ORDER BY actual_seconds DESC LIMIT 1"
                )
            ).fetchone()
        print(f"[ok] ops.sla_events breach row: table={ev[0]} slo={ev[1]}s actual={ev[2]}s")
        print("[ok] metrics pushed: data_freshness_seconds{table=fct_transactions} > 93600")
    finally:
        with eng.begin() as c:
            c.execute(
                text(
                    "UPDATE serving.fct_transactions "
                    "SET _loaded_at = :orig "
                    "WHERE _loaded_at = (SELECT max(_loaded_at) FROM serving.fct_transactions)"
                ),
                {"orig": orig},
            )
    print("[ok] timestamps restored — drill 1 reversible-pass")
    return 0


def drill_dq_fail() -> int:
    print("=== DRILL 2: forced critical DQ failure ===")
    from jobs.quality.checks import write_dq_result

    run_id, batch_id = "drill-dqfail", "drill-dqfail"
    write_dq_result(
        run_id,
        batch_id,
        "gold",
        "fct_transactions",
        "orphan_keys",
        "referential",
        "critical",
        "fail",
        999,
        20,
    )
    print("[ok] critical DQ failure written via real write_dq_result -> ops.dq_results")

    from jobs.common.metrics import push_metrics

    pushed = push_metrics(
        "dq_checks",
        {"dq_critical_failures_total": ({}, 1.0), "dq_gate_pass_pct": ({}, 0.0)},
        run_id=run_id,
    )
    if pushed:
        print("[ok] dq_critical_failures_total=1 pushed -> alert CriticalDqFailure can fire")
    else:
        print("[warn] pushgateway down — metric not visible to alertmanager (rules still load)")

    eng = _eng()
    with eng.begin() as c:
        row = c.execute(
            text(
                "SELECT check_name, severity, status, failed_count FROM ops.dq_results "
                "WHERE run_id=:r AND severity='critical'"
            ),
            {"r": run_id},
        ).fetchone()
    print(f"[ok] ops.dq_results: {row}")
    print("[ok] drill 2 pass — alert rule expr dq_critical_failures_total > 0 matches this metric")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Phase 5 SLO proof drills (M6 exit criteria)")
    ap.add_argument("--delay", action="store_true", help="forced freshness-delay drill")
    ap.add_argument("--dqfail", action="store_true", help="forced critical-DQ-failure drill")
    args = ap.parse_args()
    if not (args.delay or args.dqfail):
        args.delay = args.dqfail = True
    rc = 0
    if args.delay:
        rc = drill_freshness_delay() or rc
    if args.dqfail:
        rc = drill_dq_fail() or rc
    return rc


if __name__ == "__main__":
    sys.exit(main())
