# Changelog

All notable changes to this project are documented here.
Format: [Keep a Changelog](https://keepachangelog.com/) · Versioning: [SemVer](https://semver.org/).

## [Unreleased]

### Added
- **Local pipeline runs** — `uv run python main.py pipeline` now works off-machine:
  Auto Loader (`cloudFiles`) is Databricks-only, so local runs detect the absence of
  a Databricks runtime and batch-read `landing/` JSON into a Delta warehouse under
  `.spark/` (gitignored) with Hive-style table names; Silver's MERGE idempotency is
  unchanged. See README § Databricks CE Notes.
- **Rich terminal output** — shared console (`src/utils/console.py`): colored log
  lines, per-layer progress bars, landing-data and pipeline-output tables; JVM
  startup/shutdown fd noise (log4j banners, taskkill chatter) is captured and only
  surfaced on failure
- Bronze jobs data-driven via a single `SOURCES` map (landing dir, table, stream vs batch)
- `engine.ensure_schema`/`ensure_schemas` + `table_fqn` used across all jobs —
  UC three-part names on Databricks, two-part Hive names locally
- Silver: `watch_events`/`billing` now go through the `_gate` quarantine+audit helper;
  MERGE sources deduped into temp views (no stray `rn` column)
- **7 new tables, end-to-end** (generator → Bronze → Silver → GX suite → Gold):
  - `devices` (CDC) — 2nd SCD2 dimension via generalized `src/core/scd2.py` (`apply_scd2_generic`)
  - `profiles` — conformed household dimension (1 user → N profiles)
  - `promotions` — reference dim with validity windows
  - `promotion_redemptions` — bridge/factless fact; orphaned promo codes quarantined (referential integrity)
  - `support_tickets` — semi-structured: nested JSON `payload` flattened via `from_json`; malformed → quarantine
  - `cdn_stream_logs` — high-volume QoE telemetry, gap-based **sessionization** (new `src/core/sessionization.py`)
  - `content_ratings` — upsert semantics: latest-wins on `(user_id, content_id)`, not on PK
- 4 Gold aggregates: `qoe_by_device`, `promo_effectiveness`, `support_ticket_summary`, `content_engagement`
- 7 GX suites in `gx/expectations/` (11 total)
- 33 new tests: generic SCD2 idempotency, sessionization gap boundaries, per-table quality gates, latest-wins dedup
- `main.py generate` flags for all 11 tables; bronze/silver `--what` targets extended
- `main.py push` — uploads `landing/` JSON to the CE Volume (`RAW_DATA_PATH`) so Auto
  Loader has source data on Databricks
- `SCHEMA.md` — colored pipeline flow + ER diagram + table reference
- 12 topic docs filled in with colored mermaid diagrams: `ARCHITECTURE`,
  `DATA_SOURCES`, `MEDALLION_MAPPING`, `SCD2_DESIGN`, `STREAMING_DESIGN`,
  `DATA_QUALITY`, `BUSINESS_METRICS`, `CI_CD`, `TESTING`, `PIPELINE_RUNBOOK`,
  `DATABRICKS_CE_SETUP`, `PRODUCTION_UPGRADE`
- README rewritten: centered logo image, tech-stack badge row, colored mermaid
  architecture diagram, quick start, and a single Documentation index — no emoji
- Workflow unit + smoke tests running on every push: `tests/test_workflows.py`
  (triggers, CI steps, action pinning, `pull_request_target` no-checkout rule,
  read-only push permissions) and `tests/test_ci_smoke.py` (ruff, collection,
  generators, shared ID spaces, GX suites, CLI)

### Fixed
- `main.py` still imported `data_generator.*` (broken since package rename) — also broke the nightly E2E generate step
- `generate_subscriptions_cdc` minted random `user_{uuid}` ids instead of the shared
  `user_{i:06d}` space — subscriptions couldn't join to any fact table
- `label-sync.yml` granted top-level `issues: write`; escalated to the sync job only
- Workflows missing repo standards (top-level `permissions`, `timeout-minutes`): `ci.yml`, `codacy.yml`, `greetings.yml`, `summary.yml` — `tests/test_label_config.py` now passes

## [0.1.0] — 2026-09-14

### Added
- Medallion scaffold (Bronze/Silver/Gold) on Databricks CE: Auto Loader `cloudFiles`, `mergeSchema`, `trigger(availableNow=True)`
- Core tables: `watch_events`, `subscriptions_cdc` (SCD2), `content_catalog`, `billing`
- SCD2 merge logic (`src/core/scd2.py`) + quality gate with quarantine & audit (`src/core/quality_checks.py`, `io_utils.py`)
- Jobs organized under `src/jobs/` (bronze/silver/gold/pipeline) with `main.py` CLI
- GX expectation suites + validation runner; data validation tests
- CI (ruff + pytest), commitlint, labeler, nightly E2E, Codacy scan, issue/PR templates, CODEOWNERS, Dependabot
- Generator package (`generator/`) with seeded Faker sources and intentional messiness (duplicates, nulls, late/out-of-order/future timestamps)

[Unreleased]: https://github.com/Nitinx12/Databricks-Streamflix-Lakehouse/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Nitinx12/Databricks-Streamflix-Lakehouse/releases/tag/v0.1.0
