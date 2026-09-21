# notebooks — EDA & walkthroughs

```bash
uv sync --group dev
uv run jupyter lab notebooks/00_eda.ipynb
```

| Notebook | Phase | Focus |
|---|---|---|
| 00_eda | 0 | nulls, orphans, boxplots, volume log bar |
| 01_bronze_silver | 1-2 | bronze lineage, silver dedup, HMAC, quarantine |
| 02_gold_star | 2 | star, contracts, SCD2, no orphans |
| 03_quality_gate | 3 | GX, gate 98%, freshness, quarantine replay |
| 04_serving_agent | 4 | serving publish, masked views, agent mock |
