"""scripts/build_gold.py — CE Gold builder (ADR 006: dbt SQL is canonical, this mirrors it).

Builds banking.gold.* on the Iceberg JDBC catalog via Spark SQL mirroring
dbt/banking_dbt/models/gold/*.sql exactly, then optionally publishes each
table to Postgres serving via jobs.publish.serving (the Databricks path runs
`dbt build` instead — same SQL, different engine).

Usage:
  uv run python scripts/build_gold.py                 # build all 5 gold tables
  uv run python scripts/build_gold.py --publish       # build, then publish to serving
  uv run python scripts/build_gold.py --only dim_branch --publish
"""

import argparse
import sys

GOLD_SQL = {
    # mirrors dbt/banking_dbt/models/gold/dim_customer.sql
    "dim_customer": """
        CREATE OR REPLACE TABLE banking.gold.dim_customer USING iceberg AS
        with dedup as (
          select customer_id, name, max(silver_loaded_at) as valid_from
          from banking.silver.customers group by 1, 2
        )
        select
          md5(concat_ws('|', coalesce(cast(customer_id as string), '_null'),
                        coalesce(cast(valid_from as string), '_null'))) as customer_sk,
          customer_id, name, true as is_current, valid_from, cast(null as timestamp) as valid_to
        from dedup
        union all
        select '-1', -1, 'Unknown', false, cast('1900-01-01' as timestamp), cast(null as timestamp)
    """,
    # mirrors dbt/banking_dbt/models/gold/dim_account.sql
    "dim_account": """
        CREATE OR REPLACE TABLE banking.gold.dim_account USING iceberg AS
        with dedup as (
          select account_id, customer_id, branch_id, account_type, status,
                 max(silver_loaded_at) as valid_from
          from banking.silver.accounts group by 1, 2, 3, 4, 5
        )
        select
          md5(concat_ws('|', coalesce(cast(account_id as string), '_null'),
                        coalesce(cast(valid_from as string), '_null'))) as account_sk,
          account_id, customer_id, branch_id, account_type, status,
          true as is_current, valid_from, cast(null as timestamp) as valid_to
        from dedup
        union all
        select '-1', -1, -1, -1, 'Unknown', 'Unknown', false,
               cast('1900-01-01' as timestamp), cast(null as timestamp)
    """,
    # mirrors dbt/banking_dbt/models/gold/dim_branch.sql (Type 1)
    "dim_branch": """
        CREATE OR REPLACE TABLE banking.gold.dim_branch USING iceberg AS
        select cast(branch_id as int) as branch_id, branch_name, city, state, ifsc_code
        from banking.silver.branches
        union all select -1, 'Unknown', 'Unknown', 'Unknown', 'Unknown'
    """,
    # mirrors dbt/banking_dbt/models/gold/fct_transactions.sql (full-refresh form)
    "fct_transactions": """
        CREATE OR REPLACE TABLE banking.gold.fct_transactions USING iceberg AS
        select
          s.transaction_id,
          s.account_id,
          coalesce(d.account_sk, '-1') as account_sk,
          s.amount,
          cast(s.created_at as date) as txn_date,
          s.channel,
          s.silver_loaded_at as _loaded_at
        from banking.silver.transactions s
        left join banking.gold.dim_account d on s.account_id = d.account_id and d.is_current
    """,
    # mirrors dbt/banking_dbt/models/gold/fct_card_transactions.sql (full-refresh form)
    "fct_card_transactions": """
        CREATE OR REPLACE TABLE banking.gold.fct_card_transactions USING iceberg AS
        select
          s.card_txn_id,
          s.card_id,
          s.amount,
          s.is_fraud,
          cast(s.txn_date as date) as txn_date,
          s.silver_loaded_at as _loaded_at
        from banking.silver.card_transactions s
    """,
}

# order matters: facts join dims
BUILD_ORDER = [
    "dim_branch",
    "dim_customer",
    "dim_account",
    "fct_transactions",
    "fct_card_transactions",
]


def main() -> int:
    parser = argparse.ArgumentParser(description="Build CE Gold layer mirroring dbt gold models")
    parser.add_argument("--only", choices=BUILD_ORDER, help="build a single gold table")
    parser.add_argument("--publish", action="store_true", help="publish built tables to serving")
    parser.add_argument("--run-id", default="gold-build")
    args = parser.parse_args()

    from jobs.common.spark import get_spark

    spark = get_spark("gold_build")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS banking.gold")

    tables = [args.only] if args.only else BUILD_ORDER
    for table in tables:
        spark.sql(GOLD_SQL[table])
        print(f"GOLD-BUILT {table} = {spark.table(f'banking.gold.{table}').count()}")

    if not args.publish:
        return 0

    from jobs.publish.serving import publish

    failed = []
    for table in tables:
        try:
            n = publish(table, run_id=args.run_id)
            print(f"PUBLISH-OK {table} = {n}")
        except Exception as e:  # noqa: BLE001 — publish all tables, report at the end
            print(f"PUBLISH-FAIL {table}: {e}")
            failed.append(table)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
