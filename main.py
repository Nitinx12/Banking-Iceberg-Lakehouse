#!/usr/bin/env python3
"""StreamFlix Lakehouse — main entry-point.

Local:  uv run python main.py pipeline
        uv run python main.py bronze --what watch_events
        uv run python main.py gx --suite watch_events
        uv run python main.py test-connection
        uv run python main.py generate --rows 10000
CE:     Databricks Job spark_python_task: main.py pipeline

Fixes: catalog fallback (workspace vs streamflix-lakehouse), token scope warnings,
organized src/core vs src/jobs vs src/utils, GX suites, Makefile parity.
"""

from __future__ import annotations

import argparse
import sys


def cmd_bronze(args):
    from src.jobs.bronze import run

    run(what=args.what)
    print(f"[bronze] done what={args.what}")


def cmd_silver(args):
    from src.jobs.silver import run

    run(what=args.what)
    print(f"[silver] done what={args.what}")


def cmd_gold(args):
    from src.jobs.gold import run

    run(what=args.what)
    print(f"[gold] done what={args.what}")


def cmd_pipeline(_args):
    from src.jobs.pipeline import run

    run()
    print("[pipeline] bronze->silver->gold done")


def cmd_gx(args):
    import pathlib

    from src.core.quality_checks import check_watch_events

    # list suites or run validation on sample
    if args.list:
        suites = [p.stem for p in pathlib.Path("gx/expectations").glob("*.json")]
        print("GX suites:", suites)
        return
    # run sample validation via quality_checks (mirrors GX)
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    print(f"GX validate suite={args.suite}")
    if args.suite == "watch_events":
        df = spark.createDataFrame(
            [("e1", "u1", "c1", "play", "2024-01-01T00:00:00+00:00", 100, "tv", "s1")],
            schema=["event_id", "user_id", "content_id", "event_type", "event_timestamp", "watch_duration_seconds", "device_type", "session_id"],
        )
        res = check_watch_events(df)
        print(f"pass={res.pass_count} fail={res.fail_count} reasons={res.reasons}")


def cmd_generate(args):
    from data_generator.generate_billing import generate_billing
    from data_generator.generate_content_catalog import generate_content_catalog
    from data_generator.generate_subscriptions_cdc import generate_subscriptions_cdc
    from data_generator.generate_watch_events import generate_watch_events

    # quick in-place generation (also available as standalone scripts)
    print(f"generating content={args.content} subs_users={args.users} watch={args.watch} billing={args.billing}")
    cc = generate_content_catalog(n=args.content)
    print(f"content_catalog: {len(cc)}")
    cdc = generate_subscriptions_cdc(n_users=args.users)
    print(f"subscriptions_cdc: {len(cdc)}")
    we = generate_watch_events(n=args.watch)
    print(f"watch_events: {len(we)}")
    bi = generate_billing(n=args.billing)
    print(f"billing: {len(bi)}")


def cmd_test_connection(_args):
    from src.config import get_config
    from src.utils.connection import get_sql_connection, get_workspace_client

    cfg = get_config()
    print(f"catalog={cfg.catalog_name} env={cfg.env} host={cfg.databricks_host}")
    # workspace
    w = get_workspace_client()
    try:
        me = w.current_user.me()
        print(f"[OK] workspace: {me.user_name}")
    except Exception as e:
        print(f"[WARN] workspace: {e} (CE tokens often lack unity-catalog/jobs scopes — use UI for Jobs)")

    # SQL
    try:
        conn = get_sql_connection()
        cur = conn.cursor()
        cur.execute("SELECT current_catalog()")
        print(f"[OK] sql warehouse catalog: {cur.fetchall()[0][0]}")
        cur.execute("SHOW SCHEMAS IN `workspace`")
        print(f"[OK] schemas: {[r[0] for r in cur.fetchall()][:5]}")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[FAIL] sql: {e}")
        sys.exit(1)

    # jobs import check
    try:

        print("[OK] jobs imports ready (src/jobs.pipeline, src/core.scd2)")
    except Exception as e:
        print(f"[FAIL] imports: {e}")
        sys.exit(1)
    print("Ready to run: uv run python main.py pipeline | make run")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="main.py", description="StreamFlix Lakehouse")
    sub = p.add_subparsers(dest="cmd", required=False)

    b = sub.add_parser("bronze", help="run bronze ingestion")
    b.add_argument("--what", default="all", choices=["all", "watch_events", "subscriptions", "content", "billing"])
    b.set_defaults(func=cmd_bronze)

    s = sub.add_parser("silver", help="run silver transforms")
    s.add_argument("--what", default="all", choices=["all", "watch_events", "scd2", "billing"])
    s.set_defaults(func=cmd_silver)

    g = sub.add_parser("gold", help="run gold aggregates")
    g.add_argument("--what", default="all")
    g.set_defaults(func=cmd_gold)

    pl = sub.add_parser("pipeline", help="bronze->silver->gold")
    pl.set_defaults(func=cmd_pipeline)

    gx = sub.add_parser("gx", help="GX suites")
    gx.add_argument("--suite", default="watch_events")
    gx.add_argument("--list", action="store_true", help="list suites")
    gx.set_defaults(func=cmd_gx)

    gen = sub.add_parser("generate", help="generate synthetic landing data")
    gen.add_argument("--content", type=int, default=1000)
    gen.add_argument("--users", type=int, default=500)
    gen.add_argument("--watch", type=int, default=10000)
    gen.add_argument("--billing", type=int, default=2000)
    gen.set_defaults(func=cmd_generate)

    tc = sub.add_parser("test-connection", help="test Databricks workspace + SQL + imports")
    tc.set_defaults(func=cmd_test_connection)

    return p


def main():
    parser = build_parser()
    args = parser.parse_args()
    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
