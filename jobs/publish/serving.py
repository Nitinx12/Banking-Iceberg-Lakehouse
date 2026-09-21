"""jobs/publish/serving.py — publish Gold to PostgreSQL serving (Architecture 8.3).

1. Load Gold from Iceberg via Spark JDBC to a staging table (etl_writer role, 8.2)
2. Validate counts/keys vs Gold
3. Swap in one transaction — facts: upsert by primary key (ON CONFLICT DO UPDATE),
   dims: rename swap with column existence guard
4. ANALYZE + record freshness in ops.freshness_metrics
"""

import os

from jobs.common.logging import get_logger

logger = get_logger("publish")

# Serving primary keys — ON CONFLICT needs a real unique constraint or the upsert is a no-op lie
SERVING_KEYS = {
    "dim_customer": ["customer_id"],
    "dim_account": ["account_id"],
    "dim_branch": ["branch_id"],
    "fct_transactions": ["transaction_id"],
    "fct_card_transactions": ["card_txn_id"],
}


def _pg_creds():
    # coherent pairing: the etl_writer app role with its own password, falling back to
    # the superuser pair — never mixing one role's user with another role's password
    user = os.getenv("POSTGRES_ETL_WRITER_USER", "etl_writer")
    pw = os.getenv("POSTGRES_ETL_WRITER_PASSWORD", os.getenv("POSTGRES_PASSWORD", ""))
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_WAREHOUSE_DB", "banking_dw")
    return user, pw, host, port, db


def _pg_url():
    user, pw, host, port, db = _pg_creds()
    return f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"


def publish(table: str, run_id: str):
    try:
        from sqlalchemy import create_engine, text

        from jobs.common.spark import get_spark

        spark = get_spark("publish")
        df = spark.table(f"banking.gold.{table}")
        cnt = df.count()
        logger.info(f"publish {table}: gold count {cnt}")

        if cnt == 0:
            logger.warning(f"publish {table}: no rows in Gold — skipping")
            return 0

        key_cols = SERVING_KEYS.get(table)
        if not key_cols:
            raise ValueError(f"publish {table}: no serving primary key defined in SERVING_KEYS")

        pg_url = _pg_url()
        # credential-free JDBC URL — credentials go in the user/password options below,
        # a raw password inside the URL breaks parsing on special characters
        jdbc_host, jdbc_port, jdbc_db = _pg_creds()[2:]
        pg_jdbc_url = f"jdbc:postgresql://{jdbc_host}:{jdbc_port}/{jdbc_db}"
        eng = create_engine(pg_url, pool_pre_ping=True)

        staging = f"serving.{table}_stg"
        target = f"serving.{table}"
        with eng.begin() as c:
            # schema ownership belongs to sql/init_postgres.sql; the app role only verifies
            # (CREATE SCHEMA IF NOT EXISTS needs database-level CREATE, which etl_writer lacks)
            exists = c.execute(
                text("SELECT 1 FROM pg_namespace WHERE nspname = 'serving'")
            ).scalar()
            if not exists:
                raise RuntimeError("schema 'serving' missing — run sql/init_postgres.sql first")
            c.execute(text(f"DROP TABLE IF EXISTS {staging}"))

        # Load Gold to staging via Spark JDBC (batched writes, explicit credentials)
        user, pw, _host, _port, _db = _pg_creds()
        (
            df.write.format("jdbc")
            .option("url", pg_jdbc_url)
            .option("driver", "org.postgresql.Driver")
            .option("dbtable", staging)
            .option("user", user)
            .option("password", pw)
            .option("batchsize", "10000")
            .mode("overwrite")
            .save()
        )

        with eng.begin() as c:
            # Validate counts vs Gold
            stg_cnt = c.execute(text(f"SELECT COUNT(*) FROM {staging}")).scalar_one()
            if stg_cnt != cnt:
                raise RuntimeError(f"publish {table}: staging count {stg_cnt} != gold count {cnt}")

            # both facts and dims upsert by primary key. The classic dim rename-swap
            # (ALTER TABLE ... RENAME + DROP old) is view-unsafe: Postgres views bind the
            # table OID and follow the rename, so dropping the old table fails with
            # DependentObjectsStillExist while serving.customers_masked exists.
            c.execute(text(f"CREATE TABLE IF NOT EXISTS {target} (LIKE {staging} INCLUDING ALL)"))
            c.execute(
                text(
                    f"CREATE UNIQUE INDEX IF NOT EXISTS {table}_pk ON {target} ({', '.join(key_cols)})"
                )
            )
            # Column discovery must run in THIS transaction — a separate inspector
            # connection cannot see the table created above.
            cols = [
                r[0]
                for r in c.execute(
                    text(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_schema = 'serving' AND table_name = :t "
                        "ORDER BY ordinal_position"
                    ),
                    {"t": table},
                ).fetchall()
            ]
            key_list = ", ".join(key_cols)
            update_set = ", ".join(f"{col} = EXCLUDED.{col}" for col in cols if col not in key_cols)
            c.execute(
                text(
                    f"INSERT INTO {target} SELECT * FROM {staging} "
                    f"ON CONFLICT ({key_list}) DO UPDATE SET {update_set}"
                )
            )
            c.execute(text(f"DROP TABLE IF EXISTS {staging}"))

            c.execute(text(f"ANALYZE {target}"))
            c.execute(
                text(
                    "INSERT INTO ops.freshness_metrics (table_name, freshness_seconds, max_loaded_at) VALUES (:t, 0, now())"
                ),
                {"t": table},
            )
            c.execute(
                text(
                    "INSERT INTO ops.pipeline_runs (run_id, batch_id, stage, status, rows_read, rows_written, started_at, finished_at) VALUES (:r, :b, :s, :st, :rr, :rw, now(), now()) ON CONFLICT (run_id, stage) DO UPDATE SET status=:st, rows_read=:rr, rows_written=:rw, finished_at=now()"
                ),
                {
                    "r": run_id,
                    "b": run_id,
                    "s": f"publish_{table}",
                    "st": "success",
                    "rr": cnt,
                    "rw": stg_cnt,
                },
            )  # readers (streamlit_reader, the Gold agent) always see published tables —
            # publish may create a table the init grants ran before
            c.execute(text(f"GRANT SELECT ON {target} TO streamlit_reader"))

        # Prometheus: published rows + freshness 0 (just written) per table
        from jobs.common.metrics import push_metrics

        push_metrics(
            f"publish_{table}",
            {
                "serving_rows_total": ({"table": table}, float(stg_cnt)),
                "data_freshness_seconds": ({"table": table}, 0.0),
            },
            run_id=run_id,
        )

        logger.info(f"publish {table} done run {run_id} cnt {cnt}")
        return cnt
    except Exception as e:
        logger.error(f"publish {table} failed: {e}")
        raise


def main() -> None:
    """CLI: python -m jobs.publish.serving --table fct_transactions --run-id run-123."""
    import argparse

    parser = argparse.ArgumentParser(description="Publish gold tables to Postgres serving")
    parser.add_argument("--table", required=True, choices=sorted(SERVING_KEYS))
    parser.add_argument("--run-id", default="publish-cli")
    args = parser.parse_args()
    n = publish(args.table, run_id=args.run_id)
    logger.info(f"published {args.table} rows={n}")


if __name__ == "__main__":
    main()
