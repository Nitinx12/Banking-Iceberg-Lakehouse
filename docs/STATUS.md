# Status — What We're Facing Currently

Honest snapshot of the project state: what works, what's blocking, what's
deliberately deferred. Update this page when an item is resolved or a new
one appears — it is the "where are we" page for contributors and reviewers.

Last reviewed: 2026-09-14

## Currently green

- Local pipeline runs end-to-end: `generate → bronze (11) → silver (10) → gold (7)`
  with sensible quarantine rates and a rich summary table.
- Full test suite passes (unit + quality gates + SCD2 idempotency + sessionization
  boundaries + workflow contracts + CI smoke).
- CI runs ruff + pytest on every push; nightly E2E rebuilds cold.
- Workflow files are themselves unit-tested (`tests/test_workflows.py`) —
  trigger, steps, pinning, and permission drift fail the build on push.

## Open issues

| # | Area | Issue | Impact | Workaround / next step |
|---|---|---|---|---|
| 1 | Databricks CE | Workspace schemas not yet populated end-to-end | portfolio demo depends on local runs | follow [DATABRICKS_CE_SETUP.md](DATABRICKS_CE_SETUP.md): DDL → `main.py push` → Job; verify in Catalog Explorer |
| 2 | Databricks CE | CE personal access tokens often lack `jobs` / Unity Catalog `files` scopes | `main.py push` or Job automation can 403 even with a valid token | regenerate token with broader scopes; Jobs may need the UI (CE limitation, not a bug) |
| 3 | Databricks CE | CE Unity Catalog support is limited (catalog creation / permissions vary by workspace) | `CREATE CATALOG streamflix-lakehouse` may be unavailable | fall back to the workspace's default catalog and set `CATALOG_NAME` in `.env`; three-part names keep working |
| 4 | Silver | `silver.billing` appends instead of MERGE | re-running billing ingest can duplicate transactions | route through `_merge_or_create` on `transaction_id` (mechanical; flagged in [SCHEMA.md](SCHEMA.md)) |
| 5 | Docs | `data_dictionary.md` is still a stub | SCHEMA.md links to it for column-level detail | fill it from the Silver schemas (one compact table per source) |
| 6 | Modeling | No users dimension — `user_id` is a shared logical key only | user attributes must be joined from `subscriptions_scd2` | add a `silver.users` dim when a users source exists |
| 7 | Notebooks | `notebooks/` mirror `src/jobs` by hand and are not executed in CI | silent drift between notebook and job logic | treat `src/jobs` as source of truth; regenerate notebooks when jobs change |
| 8 | Local mode | `REPLACE TABLE AS SELECT` unsupported on local Delta | gold rebuilds use `_overwrite_from_sql` instead | none needed — behavior identical, keep the helper |
| 9 | Local mode | Auto Loader (`cloudFiles`) is Databricks-only | local runs batch-read `landing/` | by design; Silver MERGE idempotency is unaffected |
| 10 | Windows | Spark shutdown chatter (`taskkill SUCCESS:`) can still leak when Spark runs outside `main.py` (e.g. plain `pytest`) | noisy terminal in test runs | the fd-mute is wired into `get_spark`; extend it to the pytest path or filter output |
| 11 | CI | Test suite takes ~2–3 min (Spark session startup dominates) | slow feedback loop on every push | acceptable for now; split fast tests (workflows, smoke, pure functions) from Spark tests if it hurts |
| 12 | Ops | No alerting on quarantine-rate spikes or failed runs | a silent regression could go unnoticed between nightly runs | production answer is in [PRODUCTION_UPGRADE.md](PRODUCTION_UPGRADE.md); locally, run `scripts/monitor-pipeline.sh` |

## Environment constraints (fixed, not bugs)

- **Community Edition**: single-node clusters, ~1h auto-terminate, no Jobs-as-code
  via REST for restricted tokens, no DLT, limited UC.
- **Local**: JVM + Python via `uv`; `.spark/` warehouse is disposable state —
  delete it and re-run the pipeline to reset everything.

## Deliberate deferrals

Kafka/Debezium CDC, continuous triggers, watermarks, surrogate keys, secrets
management — tracked in [PRODUCTION_UPGRADE.md](PRODUCTION_UPGRADE.md), not here.
