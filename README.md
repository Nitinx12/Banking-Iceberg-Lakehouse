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
uv run python generator/generate_content_catalog.py --rows 3000
uv run python generator/generate_subscriptions_cdc.py --users 5000
uv run python generator/generate_watch_events.py --rows 100000
uv run python generator/generate_billing.py --rows 20000
```

## Databricks CE Notes
- Hive metastore `bronze`/`silver`/`gold` (no Unity Catalog on CE)
- Auto Loader `cloudFiles` with `mergeSchema`, `trigger(availableNow=True)` (cluster auto-terminates ~1hr)
- Local dev uses `uv`; notebooks use `%pip install -r requirements.txt` — export via `uv export --no-hashes -o requirements.txt`

## Known Limitations & Production Upgrade Path
Unity Catalog, multi-node autoscaling, real S3/ADLS external locations, dbt-databricks, Databricks Workflows (continuous trigger), Kafka + Debezium for CDC, Databricks Secrets + KMS.

## Tests
`uv run pytest` covers SCD2 idempotency, dedup, and quality gate (4 messiness types).

## CI (GitHub Actions)
| Workflow | Trigger | What it does |
|---|---|---|
| `ci.yml` | push to `main`, PRs | ruff check + format, pre-commit on all files, pytest (local Spark, Java 17) |
| `commitlint.yml` | PRs | Conventional Commits lint on every commit in the PR (`.commitlintrc.json`) |
| `label.yml` | PRs | path-based area labels from `.github/labeler.yml` (`area:bronze`…, `dependencies`, …) |
| `label-sync.yml` | push of `.github/labels.yml`, manual | reconcile GitHub labels with the version-controlled taxonomy |
| `nightly-e2e.yml` | daily 02:30 UTC, manual | cold-runner smoke: generate landing data, resolve GX suites, full test suite |

CI mirrors the local gates (`.githooks/*`, `.pre-commit-config.yaml`, `make lint` / `make test`) — if it passes locally, it passes in CI. Docs-only changes skip CI via `paths-ignore`.

## Repo governance
`.github/` also carries: issue templates (bug / feature / data quality), PR template with the local checklist, `CODEOWNERS` (path rules aligned with the labeler areas), `SECURITY.md` (private vulnerability reporting), and `dependabot.yml` (weekly, grouped updates for Actions + uv deps). Config consistency is enforced by `tests/test_label_config.py`.
