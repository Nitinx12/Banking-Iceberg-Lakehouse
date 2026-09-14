#!/usr/bin/env bash
# =====================================================================
# monitor-pipeline.sh — detailed monitor for the local lakehouse state
#
# Reports on the Delta warehouse under .spark/:
#   1. row counts per bronze/silver/gold table
#   2. quarantine by reason (the quality gate's rejects)
#   3. recent audit_log runs (pass/fail per table)
#   4. landing-data freshness vs newest bronze ingest
#   5. disk footprint of local state
#
# Usage:   bash scripts/monitor-pipeline.sh
#          bash scripts/monitor-pipeline.sh --report   # also write .reports/pipeline-<ts>.txt
# Exit:    0 always — this is a monitor, not a gate.
# Requires: a local run of 'uv run python main.py pipeline' first.
# =====================================================================
set -uo pipefail

cd "$(dirname "$0")/.."

REPORT=""
if [ "${1:-}" = "--report" ]; then
  mkdir -p .reports
  REPORT=".reports/pipeline-$(date +%Y%m%d-%H%M%S).txt"
  exec > >(tee "$REPORT") 2>&1
fi

echo "StreamFlix pipeline monitor — $(date)"
echo "======================================================"

# The heavy lifting needs Spark; delegate to an inline python session.
uv run python - <<'PY'
from src.config import get_config
from src.utils.engine import get_spark

cfg = get_config()
spark = get_spark()

def safe(fn):
    try:
        return fn()
    except Exception:
        return "?"

print("\n[1] Table row counts")
print("-" * 46)
print(f"{'table':<40}{'rows':>10}")
print("-" * 46)
for schema in ("bronze", "silver", "gold"):
    try:
        tables = spark.catalog.listTables(schema)
    except Exception:
        print(f"{schema + '.*':<40}{'(missing)':>10}")
        continue
    for t in sorted(tables, key=lambda t: t.name):
        n = safe(lambda: spark.table(f"{schema}.{t.name}").count())
        print(f"{schema + '.' + t.name:<40}{n:>10}")

print("\n[2] Quarantine by reason")
print("-" * 46)
try:
    q = spark.table(f"{cfg.silver_schema}.quarantine")
    rows = q.groupBy("quarantine_reason").count().orderBy(
        "count", ascending=False
    ).collect()
    if not rows:
        print("no quarantined rows — gate rejected nothing")
    for r in rows:
        print(f"{r.quarantine_reason:<40}{r['count']:>10}")
    total = sum(r["count"] for r in rows)
    print(f"{'TOTAL':<40}{total:>10}")
except Exception as e:
    print(f"quarantine table unavailable ({type(e).__name__}) — run the pipeline first")

print("\n[3] Recent audit runs (last 12)")
print("-" * 78)
print(f"{'run_at':<28}{'layer':<10}{'table':<24}{'pass':>8}{'fail':>8}")
print("-" * 78)
try:
    audit = spark.table(cfg.audit_table)
    rows = audit.orderBy("run_at", ascending=False).limit(12).collect()
    for r in rows:
        print(
            f"{str(r.run_at)[:19]:<28}"
            f"{r.layer:<10}"
            f"{r.table_name[:24]:<24}"
            f"{r.pass_count:>8}"
            f"{r.fail_count:>8}"
        )
except Exception as e:
    print(f"audit table unavailable ({type(e).__name__}) — run the pipeline first")

print("\n[4] Freshness — newest bronze ingest vs landing file dates")
print("-" * 78)
try:
    newest = (
        spark.table(f"{cfg.bronze_schema}.watch_events")
        .agg({"_ingested_at": "max"})
        .collect()[0][0]
    )
    print(f"bronze.watch_events max(_ingested_at): {newest}")
except Exception as e:
    print(f"bronze.watch_events unavailable ({type(e).__name__})")
PY

echo ""
echo "[5] Local state footprint"
echo "--------------------------------------------------------------"
if [ -d .spark ]; then
  du -sh .spark 2>/dev/null
  du -sh .spark/warehouse .spark/checkpoints .spark/quarantine 2>/dev/null
else
  echo "no .spark/ state — pipeline has not run locally"
fi

echo ""
echo "monitor complete${REPORT:+ — report written to ${REPORT}}"
