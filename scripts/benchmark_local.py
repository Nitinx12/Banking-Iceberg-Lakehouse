"""One-shot local benchmark: silver.watch_events before/after OPTIMIZE ZORDER.

Writes timings to stdout for docs/benchmarks.md. Run:
    uv run python scripts/benchmark_local.py
"""

import time

from src.utils.engine import get_spark

spark = get_spark()
t = spark.table("silver.watch_events")
n = t.count()
print(f"rows: {n}")

# before: point filter on user_id
t0 = time.perf_counter()
r1 = t.filter("user_id = 'user_000042'").count()
t1 = time.perf_counter()
print(f"BEFORE filter user_id: {r1} rows in {t1-t0:.2f}s")

# before: gold-style join + groupby
cat = spark.table("silver.content_catalog")
t0 = time.perf_counter()
r2 = t.join(cat, "content_id").groupBy("genre").count().count()
t1 = time.perf_counter()
print(f"BEFORE join+groupby genre: {r2} genres in {t1-t0:.2f}s")

# optimize
t0 = time.perf_counter()
spark.sql("OPTIMIZE silver.watch_events ZORDER BY (user_id)")
t1 = time.perf_counter()
print(f"OPTIMIZE ZORDER(user_id): {t1-t0:.2f}s")

# after
t0 = time.perf_counter()
r3 = t.filter("user_id = 'user_000042'").count()
t1 = time.perf_counter()
print(f"AFTER filter user_id: {r3} rows in {t1-t0:.2f}s")

t0 = time.perf_counter()
r4 = t.join(cat, "content_id").groupBy("genre").count().count()
t1 = time.perf_counter()
print(f"AFTER join+groupby genre: {r4} genres in {t1-t0:.2f}s")
