"""scripts/silver_all.py — run every Python Silver job in one Spark session.

The make silver_py path (CE fallback, mirrors scripts/build_gold.py for Gold).
All 10 silver jobs share one Spark session — starting a JVM per job costs
~20s each. Order within a run does not matter: each job MERGEs independently
from its own Bronze table.
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from jobs.common.logging import get_logger

logger = get_logger("silver_all")

JOBS = [
    "customers",
    "accounts",
    "branches",
    "transactions",
    "card_transactions",
    "cards",
    "employees",
    "loans",
    "loan_payments",
    "support_tickets",
]


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Run all Python Silver jobs in one Spark session")
    parser.add_argument("--batch-id", default=None, help="restrict to one Bronze batch")
    args = parser.parse_args()

    # single session for all jobs (jobs/common/spark.py getOrCreate is per-process)
    from jobs.common.spark import get_spark

    get_spark("silver_all")

    failed = []
    total = 0
    for name in JOBS:
        module = __import__(f"jobs.transform.silver_{name}", fromlist=["run"])
        t0 = time.time()
        try:
            n = module.run(batch_id=args.batch_id) or 0
            total += n
            logger.info(f"silver_{name}: wrote {n} rows in {time.time() - t0:.1f}s")
        except Exception as e:  # noqa: BLE001 — run every job, report at the end
            logger.error(f"silver_{name} FAILED: {e}")
            failed.append(name)
    logger.info(f"silver_all: {len(JOBS) - len(failed)}/{len(JOBS)} jobs ok, {total} rows total")
    if failed:
        print(f"SILVER-FAILURES: {', '.join(failed)}")
        return 1
    print("SILVER-ALL-OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
