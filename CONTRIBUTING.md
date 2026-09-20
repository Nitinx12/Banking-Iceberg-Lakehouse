# Contributing to the HDFC Banking Data Platform

Read [`AGENTS.md`](AGENTS.md) for the working conventions (they bind humans and AI agents alike) and [`PROJECT_PLAN.md`](PROJECT_PLAN.md) to check whether work belongs to the current phase.

## Quick start

```bash
git clone https://github.com/Nitinx12/Databricks-Streamflix-Lakehouse.git
cd Databricks-Streamflix-Lakehouse
make setup          # uv sync + git hooks
make up PROFILE=core
make lint && make test
```

On Windows without Make, use `tasks.bat` (same targets).

## Ground rules

- **Conventional Commits** (`feat:`, `fix:`, `docs:`, `chore:`, …) — enforced by the `commit-msg` hook.
- **Hooks are mandatory.** `pre-commit`, `commit-msg` and `pre-push` gate every change; `--no-verify` is not used — CI repeats every check anyway.
- **Small PRs, one concern each**, with tests in the same PR (unit tests run in CI; mark anything needing real infra `@pytest.mark.integration`).
- **Idempotency everywhere**: re-running a task for the same window must produce the same result.
- **No secrets, ever.** `.env` is local-only; gitleaks scans every commit (locally and in CI).
- **Match the repo map** in `Architecture.md` §18 before adding a new module.
- SQL is formatted with `sqlfluff fix`, Python with `ruff format` — don't hand-format.

## Definition of done

Code, tests, and docs in the same PR; lint green; CI green; no environment-specific values committed; any new pipeline stage writes an `ops.pipeline_runs` row; any new alert ships with a runbook. Full list in `AGENTS.md` §15.
