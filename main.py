#!/usr/bin/env python3
"""StreamFlix Lakehouse — main entry-point.

Local:  uv run python main.py pipeline
        uv run python main.py bronze --what watch_events
        uv run python main.py gx --suite watch_events
        uv run python main.py test-connection
        uv run python main.py generate --rows 10000
        uv run python main.py push   # landing/ -> Databricks Volume
CE:     Databricks Job spark_python_task: main.py pipeline

Local runs auto-detect (no Databricks runtime): landing/ sources, .spark/
warehouse + checkpoints, Hive-style table names, quiet rich terminal output.
"""

from __future__ import annotations

import argparse
import sys

from rich.table import Table

from src.utils.console import console, setup_clean_output


def cmd_bronze(args):
    from src.jobs.bronze import run

    run(what=args.what)
    console.print(
        f"[bold green]✔[/bold green] bronze done  [dim]what={args.what}[/dim]"
    )


def cmd_silver(args):
    from src.jobs.silver import run

    run(what=args.what)
    console.print(
        f"[bold green]✔[/bold green] silver done  [dim]what={args.what}[/dim]"
    )


def cmd_gold(args):
    from src.jobs.gold import run

    run(what=args.what)
    console.print(f"[bold green]✔[/bold green] gold done  [dim]what={args.what}[/dim]")


def cmd_pipeline(_args):
    from src.jobs.pipeline import run

    run()
    console.print("[bold green]✔[/bold green] pipeline bronze→silver→gold done")


def cmd_gx(args):
    import pathlib

    from src.core.quality_checks import check_watch_events

    # list suites or run validation on sample
    if args.list:
        suites = sorted(p.stem for p in pathlib.Path("gx/expectations").glob("*.json"))
        console.print(f"GX suites ({len(suites)}): [cyan]{', '.join(suites)}[/cyan]")
        return
    # run sample validation via quality_checks (mirrors GX)
    from src.utils.engine import get_spark

    spark = get_spark()
    console.print(f"GX validate suite=[cyan]{args.suite}[/cyan]")
    if args.suite == "watch_events":
        df = spark.createDataFrame(
            [("e1", "u1", "c1", "play", "2024-01-01T00:00:00+00:00", 100, "tv", "s1")],
            schema=[
                "event_id",
                "user_id",
                "content_id",
                "event_type",
                "event_timestamp",
                "watch_duration_seconds",
                "device_type",
                "session_id",
            ],
        )
        res = check_watch_events(df)
        console.print(
            f"pass={res.pass_count} fail={res.fail_count} reasons={res.reasons}"
        )


def _write_landing(name: str, rows: list[dict]) -> None:
    """Write JSON-lines to landing/<name>/ — same layout the standalone generator scripts use."""
    import json
    from datetime import date
    from pathlib import Path

    out = Path(__file__).resolve().parent / "landing" / name
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"{name}_{date.today().isoformat()}.json"
    path.write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")


def cmd_generate(args):
    from generator.generate_billing import generate_billing
    from generator.generate_cdn_stream_logs import generate_cdn_stream_logs
    from generator.generate_content_catalog import generate_content_catalog
    from generator.generate_content_ratings import generate_content_ratings
    from generator.generate_devices_cdc import generate_devices_cdc
    from generator.generate_profiles import generate_profiles
    from generator.generate_promotion_redemptions import generate_promotion_redemptions
    from generator.generate_promotions import generate_promotions
    from generator.generate_subscriptions_cdc import generate_subscriptions_cdc
    from generator.generate_support_tickets import generate_support_tickets
    from generator.generate_watch_events import generate_watch_events

    console.print(
        f"generating content={args.content} subs_users={args.users} watch={args.watch} billing={args.billing} "
        f"devices={args.devices} profiles={args.profiles} promotions={args.promotions} "
        f"redemptions={args.redemptions} tickets={args.tickets} cdn={args.cdn} ratings={args.ratings}"
    )
    # facts draw user/content ids from the same ranges as the dims (--users/--content)
    # so cross-table joins (gold) actually resolve
    counts = [
        ("content_catalog", generate_content_catalog(n=args.content)),
        ("subscriptions_cdc", generate_subscriptions_cdc(n_users=args.users)),
        (
            "watch_events",
            generate_watch_events(
                n=args.watch, n_users=args.users, n_content=args.content
            ),
        ),
        ("billing", generate_billing(n=args.billing, n_users=args.users)),
        ("devices_cdc", generate_devices_cdc(n_users=args.devices)),
        ("profiles", generate_profiles(n_users=args.profiles)),
        ("promotions", generate_promotions(n=args.promotions)),
        (
            "promotion_redemptions",
            generate_promotion_redemptions(
                n=args.redemptions, n_promos=args.promotions, n_users=args.users
            ),
        ),
        (
            "support_tickets",
            generate_support_tickets(n=args.tickets, n_users=args.users),
        ),
        (
            "cdn_stream_logs",
            generate_cdn_stream_logs(
                n=args.cdn, n_users=args.users, n_content=args.content
            ),
        ),
        (
            "content_ratings",
            generate_content_ratings(
                n=args.ratings, n_users=args.users, n_content=args.content
            ),
        ),
    ]
    t = Table(title="Landing data generated", title_style="bold", header_style="bold")
    t.add_column("source")
    t.add_column("rows", justify="right")
    for name, rows in counts:
        _write_landing(name, rows)
        t.add_row(name, str(len(rows)))
    console.print(t)


def cmd_test_connection(_args):
    from src.config import get_config
    from src.utils.connection import get_sql_connection, get_workspace_client

    cfg = get_config()
    console.print(
        f"catalog={cfg.catalog_name} env={cfg.env} host={cfg.databricks_host}"
    )
    # workspace
    w = get_workspace_client()
    try:
        me = w.current_user.me()
        console.print(f"[bold green]✔[/bold green] workspace: {me.user_name}")
    except Exception as e:
        console.print(
            f"[yellow]⚠[/yellow]  workspace: {e} (CE tokens often lack unity-catalog/jobs scopes — use UI for Jobs)"
        )

    # SQL
    try:
        conn = get_sql_connection()
        cur = conn.cursor()
        cur.execute("SELECT current_catalog()")
        console.print(
            f"[bold green]✔[/bold green] sql warehouse catalog: {cur.fetchall()[0][0]}"
        )
        cur.execute("SHOW SCHEMAS IN `workspace`")
        console.print(
            f"[bold green]✔[/bold green] schemas: {[r[0] for r in cur.fetchall()][:5]}"
        )
        cur.close()
        conn.close()
    except Exception as e:
        console.print(f"[bold red]✘[/bold red] sql: {e}")
        sys.exit(1)

    console.print(
        "[bold green]✔[/bold green] jobs imports ready (src/jobs.pipeline, src/core.scd2)"
    )
    console.print(
        "Ready to run: [cyan]uv run python main.py pipeline[/cyan] | make run"
    )


def cmd_push(_args):
    """Upload landing/ JSON to the Databricks Volume (RAW_DATA_PATH) for Auto Loader."""
    import io
    from pathlib import Path

    from src.config import get_config
    from src.utils.connection import get_workspace_client

    cfg = get_config()
    root = Path(__file__).resolve().parent / "landing"
    if not root.exists() or not any(root.iterdir()):
        console.print(
            "[bold red]✘[/bold red] no landing/ data — run [cyan]uv run python main.py generate[/cyan] first"
        )
        sys.exit(1)

    w = get_workspace_client()
    if w is None:
        console.print(
            "[bold red]✘[/bold red] workspace client unavailable — check DATABRICKS_HOST/TOKEN in .env"
        )
        sys.exit(1)

    base = cfg.landing_root.rstrip("/")
    failures: list[tuple[str, str]] = []  # (file, error)
    uploads: list[tuple[Path, str]] = []  # (local file, target path)
    for table_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        files = sorted(table_dir.glob("*.json"))
        if not files:
            continue
        try:
            w.files.create_directory(directory_path=f"{base}/{table_dir.name}")
        except Exception:
            pass  # already exists
        uploads.extend((f, f"{base}/{table_dir.name}/{f.name}") for f in files)

    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _upload(path: Path, target: str, retries: int = 3) -> None:
        """Upload one file (SDK chunks internally), retrying transient resets."""
        last_err: Exception | None = None
        for _ in range(retries):
            try:
                # part_size=1MB keeps each HTTP request small; the SDK
                # uploads parts in parallel, so a reset only kills one part
                w.files.upload(
                    file_path=target,
                    contents=io.BytesIO(path.read_bytes()),
                    part_size=1024 * 1024,
                    parallelism=4,
                )
                return
            except Exception as e:  # report and continue per-file
                last_err = e
        failures.append((path.name, str(last_err)))

    with console.status("[bold]pushing landing data…") as status, ThreadPoolExecutor(
        max_workers=4
    ) as pool:
        futures = {pool.submit(_upload, p, t): t for p, t in uploads}
        for i, fut in enumerate(as_completed(futures), 1):
            fut.result()  # surface worker crashes (failures handled inside)
            status.update(f"[bold]push {i}/{len(uploads)}[/bold] done")

    if failures:
        t = Table(title="Push failures", title_style="bold", header_style="bold")
        t.add_column("file")
        t.add_column("error")
        for name, err in failures:
            t.add_row(name, err[:120])
        console.print(t)
        console.print(
            f"[yellow]⚠[/yellow]  {len(failures)} file(s) failed — re-run [cyan]uv run python main.py push[/cyan] to retry"
        )
        sys.exit(1)
    console.print(
        f"[bold green]✔[/bold green] pushed {len(uploads)} files to [cyan]{base}[/cyan]"
    )


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="main.py", description="StreamFlix Lakehouse")
    sub = p.add_subparsers(dest="cmd", required=False)

    b = sub.add_parser("bronze", help="run bronze ingestion")
    b.add_argument(
        "--what",
        default="all",
        choices=[
            "all",
            "watch_events",
            "subscriptions",
            "content",
            "billing",
            "devices",
            "profiles",
            "promotions",
            "redemptions",
            "tickets",
            "cdn_logs",
            "ratings",
        ],
    )
    b.set_defaults(func=cmd_bronze)

    s = sub.add_parser("silver", help="run silver transforms")
    s.add_argument(
        "--what",
        default="all",
        choices=[
            "all",
            "watch_events",
            "scd2",
            "billing",
            "devices_scd2",
            "profiles",
            "promotions",
            "redemptions",
            "tickets",
            "cdn_logs",
            "ratings",
        ],
    )
    s.set_defaults(func=cmd_silver)

    g = sub.add_parser("gold", help="run gold aggregates")
    g.add_argument("--what", default="all")
    g.set_defaults(func=cmd_gold)

    pl = sub.add_parser("pipeline", help="bronze->silver->gold")
    pl.set_defaults(func=cmd_pipeline)

    push = sub.add_parser(
        "push", help="upload landing/ to the Databricks Volume for Auto Loader"
    )
    push.set_defaults(func=cmd_push)

    gx = sub.add_parser("gx", help="GX suites")
    gx.add_argument("--suite", default="watch_events")
    gx.add_argument("--list", action="store_true", help="list suites")
    gx.set_defaults(func=cmd_gx)

    gen = sub.add_parser("generate", help="generate synthetic landing data")
    gen.add_argument("--content", type=int, default=1000)
    gen.add_argument("--users", type=int, default=500)
    gen.add_argument("--watch", type=int, default=10000)
    gen.add_argument("--billing", type=int, default=2000)
    gen.add_argument("--devices", type=int, default=500)
    gen.add_argument("--profiles", type=int, default=750)
    gen.add_argument("--promotions", type=int, default=200)
    gen.add_argument("--redemptions", type=int, default=2000)
    gen.add_argument("--tickets", type=int, default=1000)
    gen.add_argument("--cdn", type=int, default=10000)
    gen.add_argument("--ratings", type=int, default=3000)
    gen.set_defaults(func=cmd_generate)

    tc = sub.add_parser(
        "test-connection", help="test Databricks workspace + SQL + imports"
    )
    tc.set_defaults(func=cmd_test_connection)

    return p


def main():
    setup_clean_output()
    parser = build_parser()
    args = parser.parse_args()
    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
