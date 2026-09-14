---
description: Run a before/after performance benchmark for partitioning and Z-ordering on a Gold table, and write up the result.
argument-hint: [table_name] e.g. watch_time_by_genre
disable-model-invocation: true
---

Benchmark query performance on `gold.$ARGUMENTS` before and after partitioning/Z-ordering, and document real numbers.

1. Identify a representative query against this table (a filter/aggregation matching how the BI layer or a Gold notebook actually queries it).
2. Measure baseline: run the query against the table as it exists today, capture wall-clock time (`%timeit`-style repeated runs, `spark.time()` in a notebook, or Python `time.perf_counter()` around the query in a script).
3. Apply a partitioning and/or `OPTIMIZE ... ZORDER BY (...)` change.
4. Re-run the same query and capture the new timing.
5. Report before/after numbers plainly — no rounding up or cherry-picking the best run. Run each version at least 3 times and report the median.
6. Write the result into `docs/benchmarks.md` (create it if it doesn't exist) and update the resume bullet draft (`Improved Gold-layer query performance by [X]% ...`) with the real measured percentage.
7. Flag anything that makes the benchmark less credible (tiny dataset, cache effects between runs, single-node cluster limits) — the goal is a number the user can defend under a follow-up question, not just report.
