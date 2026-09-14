---
description: Scaffold a new Bronze-layer Auto Loader ingestion notebook following the project's conventions.
argument-hint: [source_name] e.g. watch_events
disable-model-invocation: true
---

Scaffold a new Bronze ingestion notebook for the `$ARGUMENTS` source.

1. Look at the existing notebooks in `notebooks/bronze/` to match the established pattern (imports, Auto Loader config, trigger, write).
2. Create `notebooks/bronze/0N_ingest_$ARGUMENTS.py`, numbered after the last existing file in that folder.
3. The notebook must:
   - Read from the landing folder using Auto Loader (`cloudFiles`) with schema-on-read.
   - Use `trigger(availableNow=True)` micro-batches, matching the other Bronze notebooks (Community Edition clusters auto-terminate after ~1hr idle).
   - Use `mergeSchema` on write.
   - Write to the `bronze` Hive metastore database as a Delta table named `bronze.$ARGUMENTS`.
   - Include a short comment block at the top: source, expected schema, and any known messiness from `data_generator/generate_$ARGUMENTS.py` if that file exists.
4. Add or update the corresponding entry in `docs/lineage.md` (source → Bronze) and `docs/data_dictionary.md` (columns, types, meaning).
5. Don't write Silver/Gold logic here — Bronze stays close to raw.
