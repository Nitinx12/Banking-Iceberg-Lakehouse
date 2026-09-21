"""main.py - Banking Data Platform pipeline entrypoint (project root).

Runs the full chain in the correct order with ONE Spark session:
    seed -> bronze -> silver -> gold -> publish -> dq
and prints a rich summary table + timing at the end.

Usage:
    uv run python main.py                 # full run, reusing current Mongo data
    uv run python main.py --seed          # reseed Mongo first
    uv run python main.py --till silver   # stop after a stage
    uv run python main.py --no-publish    # skip serving publish
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from rich.console import Console  # noqa: E402
from rich.panel import Panel  # noqa: E402
from rich.rule import Rule  # noqa: E402
from rich.table import Table  # noqa: E402

console = Console()

STAGES = ["seed", "bronze", "silver", "gold", "publish", "dq"]
PUBLISH_ORDER = [
    "dim_branch",
    "dim_customer",
    "dim_account",
    "fct_transactions",
    "fct_card_transactions",
]


def run_seed() -> str:
    import scripts.seed_mongo as seed  # noqa: PLC0415

    seed.main()
    return "10 collections loaded"


def run_bronze(run_id: str) -> str:
    from datetime import UTC, datetime  # noqa: PLC0415

    import jobs.common.config as cfg  # noqa: PLC0415
    from jobs.ingestion.bronze import ingest_collection  # noqa: PLC0415

    batch_id = f"{cfg.INGEST_BATCH_ID_PREFIX}-{datetime.now(UTC).strftime('%Y%m%d-%H%M%S')}"
    total = 0
    for coll in cfg.INGEST_COLLECTIONS:
        n, _ = ingest_collection(coll, batch_id, run_id, dry_run=False)
        total += n or 0
    return f"{len(cfg.INGEST_COLLECTIONS)} collections, {total} rows (batch {batch_id})"


def run_silver() -> str:
    from scripts.silver_all import JOBS  # noqa: PLC0415

    total = 0
    for name in JOBS:
        module = __import__(f"jobs.transform.silver_{name}", fromlist=["run"])
        total += module.run() or 0
    return f"{len(JOBS)}/10 jobs, {total} rows"


def run_gold(spark) -> dict[str, int]:
    from scripts.build_gold import BUILD_ORDER, GOLD_SQL  # noqa: PLC0415

    for table in BUILD_ORDER:
        spark.sql(GOLD_SQL[table])
    return {t: spark.table(f"banking.gold.{t}").count() for t in BUILD_ORDER}


def run_publish(run_id: str) -> dict[str, int]:
    from jobs.publish.serving import publish  # noqa: PLC0415

    return {t: publish(t, run_id=run_id) for t in PUBLISH_ORDER}


def run_dq(spark, run_id: str) -> bool:
    from jobs.quality.checks import check_gold_reconciliation, check_statistical  # noqa: PLC0415

    ok = check_statistical(spark, run_id, "n/a")
    return check_gold_reconciliation(spark, run_id, "n/a") and ok


def summary(results: list[tuple[str, str, float]], wall: float, failed: str | None) -> None:
    console.print(Rule("[bold]Pipeline summary", style="cyan"))
    table = Table(show_header=True, header_style="bold", expand=True)
    table.add_column("Stage", style="cyan", no_wrap=True)
    table.add_column("Result")
    table.add_column("Time", justify="right")
    for stage, detail, secs in results:
        table.add_row(stage, detail, f"{secs:.1f}s")
    console.print(table)

    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold")
    grid.add_column()
    grid.add_row("Total wall time", f"{wall:.1f}s")
    grid.add_row("Stages passed", f"{len(results)}/{len(STAGES)}")
    status = "[green]PIPELINE OK[/green]" if failed is None else f"[red]FAILED at {failed}[/red]"
    grid.add_row("Status", status)
    console.print(
        Panel(grid, title="[bold]Run report", border_style="green" if failed is None else "red")
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the banking pipeline end to end")
    parser.add_argument("--seed", action="store_true", help="reseed Mongo first")
    parser.add_argument("--till", choices=STAGES, default="dq", help="stop after this stage")
    parser.add_argument("--no-publish", action="store_true", help="skip serving publish")
    args = parser.parse_args()

    t0 = time.time()
    run_id = f"main-{time.strftime('%Y%m%d-%H%M%S')}"
    results: list[tuple[str, str, float]] = []
    failed: str | None = None

    console.print(Rule(f"[bold cyan]Banking Data Platform - run {run_id}"))
    stages_to_run = STAGES[: STAGES.index(args.till) + 1]

    spark = None
    try:
        for stage in stages_to_run:
            t_stage = time.time()
            console.print(f"[bold]>> {stage}", highlight=False)
            detail = ""
            if stage == "seed":
                detail = run_seed()
            elif stage == "bronze":
                detail = run_bronze(run_id)
            elif stage == "silver":
                detail = run_silver()
            elif stage == "gold":
                from jobs.common.spark import get_spark  # noqa: PLC0415

                spark = get_spark(f"main_{run_id}")
                counts = run_gold(spark)
                detail = ", ".join(f"{t}={n}" for t, n in counts.items())
            elif stage == "publish":
                if args.no_publish:
                    console.print("[dim]skipped (--no-publish)", highlight=False)
                    continue
                counts = run_publish(run_id)
                detail = ", ".join(f"{t}={n}" for t, n in counts.items())
            elif stage == "dq":
                if spark is None:
                    from jobs.common.spark import get_spark  # noqa: PLC0415

                    spark = get_spark(f"main_{run_id}")
                ok = run_dq(spark, run_id)
                detail = "gate passed" if ok else "GATE FAILED"
                if not ok:
                    results.append((stage, detail, time.time() - t_stage))
                    failed = stage
                    break
            results.append((stage, detail, time.time() - t_stage))
            console.print(f"   [green]{detail}[/green]", highlight=False)
    except Exception:
        failed = "exception"
        console.print_exception(short=True)
    finally:
        summary(results, time.time() - t0, failed)

    return 0 if failed is None else 1


if __name__ == "__main__":
    sys.exit(main())
