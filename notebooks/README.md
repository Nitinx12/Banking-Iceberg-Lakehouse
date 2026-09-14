# Notebooks — Databricks mirrors, not source of truth

`src/jobs/` is the single source of truth for pipeline logic. The notebooks in
`bronze/`, `silver/`, `gold/` mirror that logic for interactive Databricks CE
demos — they are **not executed in CI** and are not guaranteed to stay in sync.

Known differences from the jobs (deliberate, documented in
[docs/STATUS.md](../docs/STATUS.md)):

- Notebooks hard-code two-part Hive names and `/dbfs/mnt/...` landing paths;
  jobs read paths and table names from `src/config.py` (`.env` / Volume).
- Notebook silver flows print gate counts but skip `log_audit()`; jobs write
  `silver.audit_log` on every run.
- Notebooks add a `_batch_id` column on Bronze ingest; jobs do not.

If a notebook and a job disagree, the job is right. When changing pipeline
logic, change `src/jobs/*.py` first and mirror it here (or regenerate the
notebook from the job).
