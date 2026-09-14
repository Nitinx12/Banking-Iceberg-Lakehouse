# StreamFlix Lakehouse

Medallion lakehouse (Bronze/Silver/Gold) on Databricks Community Edition.

## Architecture
Watch events (streaming) + Subscriptions CDC + Content catalog + Billing -> Bronze (Auto Loader, Delta, mergeSchema) -> Silver (clean, dedupe, SCD2, quality gate + quarantine) -> Gold (DAU/WAU, watch time by genre, churn, MRR) -> BI

See `StreamFlix_Databricks_Project_Plan.md` for full plan and term mapping.

## Setup
```bash
uv sync
uv run pytest
uv run ruff check .
# generate synthetic landing data
uv run python data_generator/generate_content_catalog.py --rows 3000
uv run python data_generator/generate_subscriptions_cdc.py --users 5000
uv run python data_generator/generate_watch_events.py --rows 100000
uv run python data_generator/generate_billing.py --rows 20000
```

## Databricks CE Notes
- Hive metastore `bronze`/`silver`/`gold` (no Unity Catalog on CE)
- Auto Loader `cloudFiles` with `mergeSchema`, `trigger(availableNow=True)` (cluster auto-terminates ~1hr)
- Local dev uses `uv`; notebooks use `%pip install -r requirements.txt` — export via `uv export --no-hashes -o requirements.txt`

## Known Limitations & Production Upgrade Path
Unity Catalog, multi-node autoscaling, real S3/ADLS external locations, dbt-databricks, Databricks Workflows (continuous trigger), Kafka + Debezium for CDC, Databricks Secrets + KMS.

## Tests
`uv run pytest` covers SCD2 idempotency, dedup, and quality gate (4 messiness types).
