# Contributing to StreamFlix Lakehouse

Thanks for contributing. This is a portfolio project with production-grade
habits: everything that lands on `main` is linted, tested, and documented.

Before you start, skim [docs/STATUS.md](docs/STATUS.md) — it lists what the
project is currently facing, so you don't rediscover a known issue.

## Setup

```bash
git clone https://github.com/Nitinx12/Databricks-Streamflix-Lakehouse.git
cd Databricks-Streamflix-Lakehouse
bash scripts/setup.sh        # uv sync + .env + git hooks + health check
```

`setup.sh` does everything below; manually it is: `uv sync` (Python 3.13+,
JDK 17 required for Spark tests), `cp .env.example .env` and fill
`DATABRICKS_HOST`/`TOKEN` for CE runs (gitignored), and
`git config core.hooksPath .githooks` for local commit-msg lint.

## Everyday commands

```bash
uv run ruff check .                 # lint — must pass before every commit
uv run pytest -q                    # full suite (~2-3 min, local Spark)
uv run python main.py generate      # synthetic landing data
uv run python main.py pipeline      # bronze -> silver -> gold, locally
bash scripts/monitor-pipeline.sh    # row counts, quarantine, audit runs
bash scripts/smoke.sh               # the nightly E2E workflow, locally
bash scripts/audit-secrets.sh       # pre-push secret hygiene check
```

## Ground rules

1. **`uv` only.** No bare `pip`, no manual `requirements.txt` edits —
   `uv.lock` is frozen and CI syncs from it.
2. **Conventional Commits.** `feat:`, `fix:`, `docs:`, `chore:`, `refactor:`,
   `test:`, `ci:`, `build:`, `perf:`, `style:`, `revert:` — header ≤ 72 chars.
   Enforced by `.githooks/commit-msg` locally and `commitlint.yml` on PRs.
3. **Tests gate every push.** `ci.yml` runs ruff + the full suite on push and
   PR. The workflow files themselves are under test
   (`tests/test_workflows.py`) — if your PR edits a workflow, its triggers,
   steps, action pins, and permissions must keep passing those contracts.
4. **Idempotency is a product feature.** Any Silver write must be a MERGE on
   a natural key (or a full overwrite). A change that makes re-runs
   duplicate rows is a bug, and the idempotency tests exist to catch it.
5. **Keep docs in sync.** Schema change → update `docs/SCHEMA.md` and
   `docs/lineage.md`. New issue or resolved one → update
   `docs/STATUS.md`. Notable change → `docs/CHANGELOG.md`.
6. **Quarantine, don't drop.** New validation belongs in
   `src/core/quality_checks.py` with a `quarantine_reason`, and ideally a
   matching GX suite in `gx/expectations/`.

## Pull requests

- One logical change per PR; the PR template includes the local checklist.
- PR titles follow Conventional Commits (commitlint checks them).
- Path-based area labels are applied automatically from
  `.github/labeler.yml`.
- Docs-only changes skip CI via `paths-ignore`.

## Reporting problems

- Bugs and feature requests: use the issue templates in `.github/ISSUE_TEMPLATE`.
- Data-quality observations: the "data quality" issue template — include the
  `quarantine_reason` and a row sample if you can.
- Security: see [SECURITY.md](SECURITY.md) — never open a public issue for it.
