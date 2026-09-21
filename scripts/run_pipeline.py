"""scripts/run_pipeline.py — full pipeline in ONE Spark process.

The staged path (bronze CLI, silver_all, build_gold, publish CLI, dq CLI) pays
~26s of JVM startup per process (measured on this box) - four processes is two
minutes of dead time. This runner reuses one SparkSession across all stages,
so the whole chain costs a single startup. Same job code, same idempotency.

Usage:
  uv run python scripts/run_pipeline.py                 # everything
  uv run python scripts/run_pipeline.py --skip-seed     # reuse current Mongo data
  uv run python scripts/run_pipeline.py --till silver   # bronze+silver only
"""

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

STAGES = ["seed", "bronze", "silver", "gold", "publish", "dq"]


def main() -> int:
    parser = argparse.ArgumentParser(description="Full pipeline in one Spark process")
    parser.add_argument("--till", choices=STAGES, default="dq", help="stop after this stage")
    parser.add_argument("--skip-seed", action="store_true", help="skip Mongo reseed")
    parser.add_argument("--batch-id", default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--no-publish", action="store_true", help="skip the serving publish")
    args = parser.parse_args()

    t_all = time.time()
    run_id = args.run_id or f"pipe-{time.strftime('%Y%m%d-%H%M%S')}"
    results: dict[str, str] = {}

    def passed(stage: str, detail: str) -> None:
        results[stage] = detail
        print(f"[ok] {stage}: {detail} ({time.time() - t_all:.0f}s elapsed)", flush=True)

    # seed (no Spark needed)
    if not args.skip_seed and STAGES.index("seed") <= STAGES.index(args.till):
        import scripts.seed_mongo as seed  # noqa: PLC0415

        seed.main()
        passed("seed", "mongo loaded")

    # one session for everything below
    from jobs.common.spark import get_spark  # noqa: PLC0415

    spark = get_spark(f"pipeline_{run_id}")

    if STAGES.index("bronze") <= STAGES.index(args.till):
        from datetime import UTC, datetime  # noqa: PLC0415

        import jobs.common.config as cfg  # noqa: PLC0415
        from jobs.ingestion.bronze import ingest_collection  # noqa: PLC0415

        batch_id = (
            args.batch_id
            or f"{cfg.INGEST_BATCH_ID_PREFIX}-{datetime.now(UTC).strftime('%Y%m%d-%H%M%S')}"
        )
        total = 0
        for coll in cfg.INGEST_COLLECTIONS:
            n, _ = ingest_collection(coll, batch_id, run_id, dry_run=False)
            total += n or 0
        passed(
            "bronze", f"{len(cfg.INGEST_COLLECTIONS)} collections, {total} rows, batch {batch_id}"
        )

    if STAGES.index("silver") <= STAGES.index(args.till):
        from scripts.silver_all import JOBS  # noqa: PLC0415

        total = 0
        for name in JOBS:
            module = __import__(f"jobs.transform.silver_{name}", fromlist=["run"])
            n = module.run() or 0
            total += n
        passed("silver", f"{len(JOBS)} jobs, {total} rows")

    if STAGES.index("gold") <= STAGES.index(args.till):
        from scripts.build_gold import BUILD_ORDER, GOLD_SQL  # noqa: PLC0415

        for table in BUILD_ORDER:
            spark.sql(GOLD_SQL[table])
        counts = {t: spark.table(f"banking.gold.{t}").count() for t in BUILD_ORDER}
        passed("gold", ", ".join(f"{t}={n}" for t, n in counts.items()))

    publish_needed = not args.no_publish and STAGES.index("publish") <= STAGES.index(args.till)
    if publish_needed:
        from jobs.publish.serving import publish  # noqa: PLC0415

        published = []
        for table in [
            "dim_branch",
            "dim_customer",
            "dim_account",
            "fct_transactions",
            "fct_card_transactions",
        ]:
            published.append(f"{table}={publish(table, run_id=run_id)}")
        passed("publish", ", ".join(published))

    if STAGES.index("dq") <= STAGES.index(args.till):
        from jobs.quality.checks import (  # noqa: PLC0415
            check_gold_reconciliation,
            check_statistical,
        )

        ok = check_statistical(spark, run_id, "n/a")
        ok = check_gold_reconciliation(spark, run_id, "n/a") and ok
        if not ok:
            print("[FAIL] dq: gate below threshold")
            return 1
        passed("dq", "gate passed")

    print(f"PIPELINE-OK {run_id} in {time.time() - t_all:.0f}s: {results}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
