<p align="center">
  <img src="assets/streamflix_lakehouse_logo.png" alt="Databricks logo" width="180">
</p>

<h1 align="center">Databricks StreamFlix Lakehouse</h1>

<p align="center">
  A medallion-architecture (bronze → silver → gold) data pipeline for a fictional streaming service, built on Spark + Delta and designed to run identically locally or as a Databricks Job on Unity Catalog — chunked, idempotent, and interview-defensible.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.13+-3776AB?logo=python&logoColor=white" alt="Python 3.13+">
  <img src="https://img.shields.io/badge/PySpark-4.2-e25a1c?logo=apachespark&logoColor=white" alt="PySpark 4.2">
  <img src="https://img.shields.io/badge/Delta_Lake-4.4-0097FD?logo=deltalake&logoColor=white" alt="Delta Lake 4.4">
  <img src="https://img.shields.io/badge/Databricks-Community_Edition-FF3621?logo=databricks&logoColor=white" alt="Databricks Community Edition">
  <img src="https://img.shields.io/badge/Great_Expectations-1.x-EC6A21" alt="Great Expectations">
  <img src="https://img.shields.io/badge/uv-DE5BDA?logo=uv&logoColor=white" alt="uv">
  <img src="https://img.shields.io/badge/Ruff-261230?logo=ruff&logoColor=white" alt="Ruff">
  <img src="https://img.shields.io/badge/Pytest-0A9EDC?logo=pytest&logoColor=white" alt="pytest">
  <img src="https://img.shields.io/badge/GitHub_Actions-2088FF?logo=githubactions&logoColor=white" alt="GitHub Actions">
</p>

## Architecture

```mermaid
flowchart LR
    subgraph SOURCES["Landing - synthetic JSON"]
        G["11 seeded Faker generators"]
    end

    subgraph BRONZE["Bronze - raw, append-only"]
        AL["Auto Loader cloudFiles<br/>mergeSchema, availableNow"]
        BT["Batch overwrite<br/>small dimensions"]
    end

    subgraph SILVER["Silver - clean and conform"]
        CL["clean + dedupe + quality gate"]
        SCD["SCD2 MERGE<br/>subscriptions, devices"]
        SES["sessionize 30-min gap<br/>CDN logs"]
        FJ["from_json flatten<br/>support tickets"]
        UP["latest-wins upsert<br/>content ratings"]
    end

    subgraph GOLD["Gold - business aggregates"]
        GA["DAU / WAU, watch by genre,<br/>churn, MRR, QoE by device,<br/>promo effectiveness, support, engagement"]
    end

    subgraph QUALITY["Quality sidecars"]
        Q[("silver.quarantine<br/>+ reason")]
        A[("silver.audit_log<br/>pass / fail counts")]
    end

    BI["BI / dashboards"]

    G --> AL
    G --> BT
    AL --> CL
    BT --> CL
    CL --> SCD
    CL --> SES
    CL --> FJ
    CL --> UP
    SCD --> GA
    SES --> GA
    FJ --> GA
    UP --> GA
    GA --> BI
    SCD -. failures routed, not dropped .-> Q
    SCD -. every run .-> A

    classDef sources fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
    classDef bronze fill:#fde8d7,stroke:#c2591b,color:#5c2b0d
    classDef silver fill:#eceff1,stroke:#78909c,color:#263238
    classDef gold fill:#fdf3c9,stroke:#b58b00,color:#5c4a00
    classDef quality fill:#fde2e4,stroke:#c9184a,color:#800f2f
    classDef bi fill:#e6f4ea,stroke:#137333,color:#0d3b21
    class G sources
    class AL,BT bronze
    class CL,SCD,SES,FJ,UP silver
    class GA gold
    class Q,A quality
    class BI bi
```

Every Silver write is a MERGE on a natural key, so the pipeline is idempotent
— re-runs are no-ops, backfills are safe. Rows that fail the quality gate are
quarantined with a reason, never dropped.

## Quick start

```bash
uv sync
uv run pytest                              # full test suite (local Spark)
uv run python main.py generate             # synthetic landing data (11 Faker sources)
uv run python main.py pipeline             # bronze -> silver -> gold, locally (~5 min, chunked)
uv run python main.py test-connection      # check Databricks workspace + SQL
uv run python main.py push                 # upload landing/ to CE Volume (chunked, idempotent)
```

Local runs auto-detect (no Databricks runtime): sources are read from
`landing/`, Delta tables land under `.spark/` (gitignored), and terminal
output is rich-formatted with progress bars and summary tables. Every Silver
write is a `MERGE` on a natural key — re-runs are no-ops, quarantine is partitioned by date.

## Running on Databricks CE

1. Create catalog/schemas/volume once — `sql/init_schema.sql` or SQL Editor.
2. `uv run python main.py push` — upload `landing/` to `streamflix-lakehouse.bronze.raw_data`.
3. Create Job: **Python script**, Git source `main`, file `main.py`, params `["pipeline"]`, PyPI `rich`.

Full walkthrough: [docs/DATABRICKS_CE_SETUP.md](docs/DATABRICKS_CE_SETUP.md).

### Performance — chunked for CE

Bronze `cloudFiles.maxFilesPerTrigger=500/128m` `src/jobs/bronze.py:92`, Silver `repartition(4/8)` before window `src/jobs/silver.py:69` + `src/core/sessionization.py:34`, local `repartition(8)` for `watch_events`/`cdn_logs` — avoids single-partition spill. Verified `10:50` run: 9593 watch_events, 1092 sessions, 29 QoE rows.

## Documentation

> Trimmed to 7 core docs — single source of truth, no duplication.

| Doc | Contents |
|---|---|
| [SCHEMA.md](docs/SCHEMA.md) | pipeline flow, ER diagram, Bronze/Silver/Gold table reference |
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | system flow, repo layout, local vs Databricks execution modes |
| [DATABRICKS_CE_SETUP.md](docs/DATABRICKS_CE_SETUP.md) | populate the CE workspace: DDL, push, Job setup |
| [STREAMING_DESIGN.md](docs/STREAMING_DESIGN.md) | Auto Loader design and 30-minute gap sessionization |
| [SCD2_DESIGN.md](docs/SCD2_DESIGN.md) | SCD Type 2 row model, merge decision flow, idempotency |
| [DATA_QUALITY.md](docs/DATA_QUALITY.md) | quality gate, quarantine reason catalog, audit log |
| [PIPELINE_RUNBOOK.md](docs/PIPELINE_RUNBOOK.md) | commands, fresh end-to-end recipe, common issues |

## Monitoring and ops scripts

`scripts/` carries bash tooling for the three things that can rot silently —
the environment, the local warehouse, and CI — plus bootstrap, smoke, reset,
and secret-audit helpers:

| Script | What it does |
|---|---|
| `bash scripts/setup.sh` | one-shot bootstrap: uv sync, `.env` from template, git hooks, health check |
| `bash scripts/health-check.sh` | pre-flight: uv, python, JDK, lockfile, `.env` keys, landing data per source, `.spark/` state, git hygiene |
| `bash scripts/monitor-pipeline.sh` | local warehouse detail: row counts per bronze/silver/gold table, quarantine by reason, last 12 audit runs, ingest freshness, disk footprint |
| `bash scripts/monitor-ci.sh` | GitHub Actions via `gh`: last runs, per-workflow status, failures in the last 7 days with URLs, nightly E2E health |
| `bash scripts/smoke.sh` | run the nightly E2E workflow locally: ruff, generate (nightly volumes), GX suites, full pytest |
| `bash scripts/reset.sh --rebuild` | wipe disposable local state (`.spark/`) and optionally rebuild with generate + pipeline |
| `bash scripts/audit-secrets.sh` | pre-push hygiene: `.env` tracked status, credential patterns in tracked files and staged changes |

The monitors exit 0 (they report, they don't gate — `health-check.sh` and
`audit-secrets.sh` are the exceptions: they fail on what they find). The
monitors accept `--report` to also write a timestamped file under
`.reports/` (gitignored).

## Tests and CI

`uv run pytest` covers SCD2 idempotency (subscriptions and devices), dedup,
latest-wins upsert, sessionization gap boundaries, and quality gates for all
11 sources. CI (`ci.yml`) runs ruff and the full suite on every push and PR;
a nightly E2E workflow rebuilds everything from a cold runner. Conventional
Commits are enforced on PRs and locally via `.githooks`.

## Repo governance

[CONTRIBUTING.md](CONTRIBUTING.md) covers setup, ground rules, and the PR
checklist; [SECURITY.md](SECURITY.md) the vulnerability-reporting policy and
current watch items. `.github/` carries issue templates, a PR template with
the local checklist, `CODEOWNERS` aligned with labeler areas, and grouped
dependabot updates. Config consistency is enforced by
`tests/test_label_config.py`.
