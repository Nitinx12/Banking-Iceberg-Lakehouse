# Benchmarks

Record before/after OPTIMIZE ZORDER here.

| query | before | after | improvement |
|---|---|---|---|
| SELECT * FROM silver.watch_events WHERE user_id='x' | TBD | TBD | TBD |
| gold.watch_time_by_genre join | TBD | TBD with broadcast | TBD |

Instructions: run query, capture scan bytes + time, then OPTIMIZE + ZORDER BY (user_id) and re-run.
