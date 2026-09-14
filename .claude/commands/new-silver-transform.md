---
description: Scaffold a new Silver-layer cleaning/transformation notebook, wired into the data quality gate.
argument-hint: [table_name] e.g. watch_events
disable-model-invocation: true
---

Scaffold a new Silver transformation notebook for `$ARGUMENTS`, reading from `bronze.$ARGUMENTS`.

1. Look at existing notebooks in `notebooks/silver/` to match the established pattern.
2. Create `notebooks/silver/0N_clean_$ARGUMENTS.py`, numbered after the last existing file.
3. The notebook must:
   - Deduplicate on the natural key for this table (check `docs/data_dictionary.md` or the data generator spec for the primary key).
   - Run records through `src/quality_checks.py`'s quality gate; route failures to the quarantine table rather than dropping them or failing the batch.
   - Partition the output table by date (or the appropriate high-cardinality column — check how other Silver tables are partitioned first).
   - Write to the `silver` Hive metastore database as `silver.$ARGUMENTS`.
   - Log pass/fail counts from the quality gate (required by the project's Definition of Done).
4. If this table needs SCD2 (only `subscriptions` does today), use `/scd2-scaffold` instead of this command.
5. Update `docs/lineage.md` (Bronze → Silver) and `docs/data_dictionary.md`.
