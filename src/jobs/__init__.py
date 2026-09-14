"""src.jobs — Databricks Jobs entry-points.

Each module exposes a `run(spark)` function importable as a Job task:
  - `src.jobs.bronze.run`
  - `src.jobs.silver.run`
  - `src.jobs.gold.run`
  - `src.jobs.pipeline.run` (orchestrates bronze → silver → gold)

Notebooks in `notebooks/` wrap these for interactive dev; Jobs run the same
code via `spark_python_task` / `notebook_task`.
"""

from src.jobs.bronze import run as bronze_run
from src.jobs.gold import run as gold_run
from src.jobs.pipeline import run as pipeline_run
from src.jobs.silver import run as silver_run

__all__ = ["bronze_run", "gold_run", "pipeline_run", "silver_run"]
