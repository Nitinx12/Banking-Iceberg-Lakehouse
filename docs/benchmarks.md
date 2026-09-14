# Benchmarks

OPTIMIZE/ZORDER before-and-after on the local warehouse
(`silver.watch_events`). Reproduce with:

```bash
uv run python main.py generate && uv run python main.py pipeline
uv run python scripts/benchmark_local.py
```

`benchmark_local.py` measures a point filter on `user_id` and a
join+groupBy against `silver.content_catalog`, runs
`OPTIMIZE silver.watch_events ZORDER BY (user_id)`, then re-measures both.
Paste its output rows below when run — local[2] single-node numbers, so
treat them as directional (file skipping) rather than absolute.

| query | before | after | improvement |
|---|---|---|---|
| `SELECT * FROM silver.watch_events WHERE user_id='x'` | pending run | pending run | — |
| watch_events ⋈ content_catalog, groupBy genre | pending run | pending run | — |
