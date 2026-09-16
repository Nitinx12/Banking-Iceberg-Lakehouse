# Cluster Configuration

> This file exists to satisfy `area:databricks` labeler glob `docs/cluster.md`.
> Canonical cluster docs live in `DATABRICKS_CE_SETUP.md`; this file re-exports
> the Community Edition cluster requirements for labeler path matching.

See [DATABRICKS_CE_SETUP.md](DATABRICKS_CE_SETUP.md) for full Databricks CE setup:
- Runtime 13.3+ (Spark 3.4+, Delta 2.4+ on CE)
- `availableNow=True` trigger, single-node driver
- `spark.sql.shuffle.partitions=8` locally, `200` on CE
- Auto Loader `cloudFiles` with `mergeSchema`
