# AGENTS.md

Guidance for any AI coding agent (or human) working in this repository.

## 1. Project

**HDFC Bank Lakehouse** — MongoDB → Apache Iceberg banking data platform. PySpark and dbt on Databricks transform the data, Great Expectations plus custom checks validate it, PostgreSQL and Streamlit serve it, Airflow orchestrates it. Full design lives in `Architecture.md`; the delivery sequence lives in `PROJECT_PLAN.md`. The project is currently in **Phase 0: Foundations** — repo scaffolding, tooling, and CI, not yet real pipelines.

Read `Architecture.md` before touching orchestration, contracts, or the lake layers. Read `PROJECT_PLAN.md` before starting work, to confirm the task belongs to the current phase — don't build ahead of it.

## 2. Stack

Python ≥3.11 · PySpark 3.5.5 · `uv` (dependency groups: `ingestion`, `transform`, `quality`, `dashboard`, `dev`) · dbt Core (`dbt-databricks`) · Apache Iceberg · Airflow (TaskFlow) · Great Expectations · Terraform (HCL) · Docker Compose · GitHub Actions.

## 3. Repository map

```
spark_jobs/       ingestion, transform, quality, publish, maintenance, common/
dbt/banking_dbt/  staging, silver, gold models, snapshots, tests
contracts/        one YAML contract per MongoDB collection
airflow/dags/      DAGs; shared config in airflow/include/
great_expectations/ suites, checkpoints, data docs
terraform/         modules + one var file per environment
scripts/sh|ps1|bat  automation, mirrored across OSes
```

Full tree in `Architecture.md` §18 — match it for any new module rather than inventing a new location.

## 4. Setup commands

- `make setup && make up` — fresh clone to a running local stack. On Windows without Make, use `tasks.bat` (same targets, calls the PowerShell scripts under `scripts/ps1/`).
- `make lint` / `make test` — run before every commit; CI reruns both and cannot be skipped.
- `make hooks` — installs `.githooks/` locally.

## 5. Core principles

- **Idempotent everywhere.** Re-running a task for the same window must produce the same result.
- **Contracts at every boundary.** Source → Bronze, Silver → Gold, Gold → serving all have a declared schema.
- **Fail closed on Gold.** Bad data quarantines or holds; it never reaches Gold or the dashboard silently.
- **Least privilege, environment parity, everything as code.** Full list in `Architecture.md` §2.

## 6. Comment style

Comments should read like a quick note from a teammate, not generated documentation.

- Comment the *why*, not the *what*. If the code already says it, don't say it again.
- Skip comments on self-explanatory lines — simple filters, assignments, imports, obvious loops.
- Keep it short: a phrase, not a paragraph. No banners, no `# Step 1:`, no restating the function name.
- Docstrings go on public functions and classes only, one or two lines unless the signature is genuinely non-obvious.
- Never leave a comment that exists only to mark that an AI wrote the code, or to narrate what you're about to do next.
- If a workaround needs explaining, reference the ticket or ADR instead of re-explaining the bug inline.

```python
# Bad — states the obvious, sounds generated
# Loop through each row in the dataframe and filter out nulls
df = df.filter(col("account_id").isNotNull())

# Good — explains a non-obvious reason
# Nulls here mean the MongoDB doc predates the account_id backfill (Jan 2026)
df = df.filter(col("account_id").isNotNull())
```

## 7. Python (PySpark jobs, `spark_jobs/`)

- Format and lint with `ruff` (line length 100, target `py311`); imports sorted, no unused code.
- Type hints on function signatures; avoid `Any` unless the schema is genuinely dynamic.
- One job = one purpose. Shared logic (logging, config, I/O, metrics) goes in `spark_jobs/common/`, not copy-pasted.
- Explicit schemas on every DataFrame read — no relying on inference for anything reaching Silver or Gold.
- Every write is idempotent: delete-by-`_batch_id` or an equivalent merge, never a blind append.
- Unit tests use a local `SparkSession` and small fixtures; mark anything needing real infra `@pytest.mark.integration`.

## 8. Scala / Flink (`flink/`)

- Prefer Flink SQL for CDC and windowed aggregates; drop to the DataStream API only when SQL can't express the logic.
- Checkpoint configuration is explicit in every job — no relying on defaults.
- Keep connector and format versions pinned; Flink is version-sensitive.

## 9. dbt / SQL (`dbt/banking_dbt/`)

- `snake_case` model names, one model per file, staging → silver → gold folders match the layer.
- Every model has at least a `not_null` and `unique` (or relationship) test where a natural key exists.
- SQLFluff (`postgres` dialect) must pass clean before commit; run `sqlfluff fix` rather than hand-formatting.
- Python models only where SQL is genuinely awkward (heavy joins, complex masking) — default to SQL.
- Use `ref()` and `source()` exclusively; no hardcoded schema or table names.

## 10. Shell scripts (`scripts/sh/`)

- `#!/usr/bin/env bash` and `set -euo pipefail` at the top of every script.
- Source `scripts/sh/lib.sh` for logging, retries, and `require_env` rather than reimplementing them.
- Every destructive script accepts `--dry-run` and is idempotent.
- `shellcheck` must pass clean. Add a matching PowerShell script under `scripts/ps1/` for anything that runs locally on Windows.

## 11. Terraform (`terraform/`)

- One module per concern, remote state, one variable file per environment — no copy-pasted environment blocks.
- `terraform fmt` and `terraform validate` must pass before commit; `tflint` runs in CI on every PR touching `terraform/`.
- Never apply from a laptop against staging or prod — that's `cd.yml`'s job, with manual approval gating prod.

## 12. Git

- **Conventional Commits**: `feat:`, `fix:`, `docs:`, `chore:`, etc. Keep the subject line under ~70 characters, written like you're telling a colleague what changed — not a changelog entry.
- Small, short-lived branches; one focused change per pull request, with tests in the same PR.
- Hooks (`pre-commit`, `commit-msg`, `pre-push`) are mandatory. Never use `--no-verify` — CI repeats every check anyway, so skipping locally only delays the failure.
- No secrets, `.env` files, or environment-specific values in any commit. Gitleaks runs on staged files at commit time.

## 13. Testing

- Unit tests for transform functions, watermark logic, masking, and DQ scoring run on every commit.
- Target at least 80% coverage on transformation and quality logic — not a hard gate everywhere else.
- Fault-injection fixtures (nulls, duplicates, orphan keys, bad currency) live under `tests/data/`; use them rather than inventing new bad-data cases per test.

## 14. CI/CD

- PRs run lint, unit tests, `dbt parse`, GX config validation, secret scan, and dependency audit.
- Merges to `main` promote automatically to dev, then staging after smoke tests; production requires manual approval.
- Releases are semantic version tags. Rollback means redeploying the previous tag, not patching forward under pressure.

## 15. Definition of done

A task is done when: code, tests, and docs are in the same PR; lint passes and CI is green; no secrets or environment-specific values are committed; any new pipeline stage emits logs, metrics, and an `ops.pipeline_runs` row; any new alert ships with a runbook.

## 16. When in doubt

Check `Architecture.md` for the *why* behind a design choice (including the ADR table in §20), and `PROJECT_PLAN.md` for whether the work is in scope for the current phase.
