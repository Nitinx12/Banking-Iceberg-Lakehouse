# Architecture

StreamFlix is a medallion lakehouse (Bronze → Silver → Gold) that runs **identically local and on Databricks CE** — same jobs, same idempotency, different I/O plumbing. Table reference: [`SCHEMA.md`](SCHEMA.md) · sources: [`DATA_SOURCES.md`](DATA_SOURCES.md).

## System flow

```mermaid
flowchart LR
    subgraph SRC["🎲 Sources"]
        GEN["generator/<br/>11 seeded Faker scripts"]
        LAND["landing/<br/>JSON-lines"]
        GEN --> LAND
    end

    subgraph BRONZE["🥉 Bronze — raw, append-only"]
        AL["Auto Loader cloudFiles<br/>mergeSchema · availableNow"]
        BT["batch overwrite<br/>(small dims)"]
    end

    subgraph SILVER["🥈 Silver — clean & conform"]
        CL["clean + dedupe"]
        SCD["SCD2 MERGE<br/>subscriptions · devices"]
        SES["sessionize 30-min gap"]
        FJ["from_json flatten"]
        UP["latest-wins upsert"]
    end

    subgraph GOLD["🥇 Gold — aggregates"]
        GA["11 business tables"]
    end

    subgraph DQ["🛡 Quality sidecars"]
        Q[("silver.quarantine<br/>+ reason")]
        A[("silver.audit_log<br/>pass/fail per run")]
    end

    BI["📊 BI / interview demo"]

    LAND -->|"local: batch read.json<br/>CE: Volume (main.py push)"| AL
    LAND --> BT
    AL & BT --> CL
    CL --> SCD & SES & FJ & UP
    SCD & SES & FJ & UP --> GA --> BI
    SCD & SES & FJ & UP -.->|"failures routed, not dropped"| Q
    SCD & SES & FJ & UP -.->|"every run"| A

    classDef src fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
    classDef bronze fill:#fde8d7,stroke:#c2591b,color:#5c2b0d
    classDef silver fill:#eceff1,stroke:#78909c,color:#263238
    classDef gold fill:#fdf3c9,stroke:#b58b00,color:#5c4a00
    classDef dq fill:#fde2e4,stroke:#c9184a,color:#800f2f
    classDef bi fill:#e6f4ea,stroke:#137333,color:#0d3b21
    class GEN,LAND src
    class AL,BT bronze
    class CL,SCD,SES,FJ,UP silver
    class GA gold
    class Q,A dq
    class BI bi
```

## Repo layout

| Path | Role |
|---|---|
| `generator/` | 11 seeded Faker scripts — the only source of data |
| `src/jobs/` | `bronze.py`, `silver.py`, `gold.py`, `pipeline.py` — the medallion layers |
| `src/core/` | engine-agnostic logic: `scd2.py`, `sessionization.py`, `transformations.py`, `quality_checks.py`, `io_utils.py` |
| `src/utils/` | `engine.py` (Spark session), `config.py`, `console.py` (rich), `logger.py`, `connection.py` |
| `gx/expectations/` | 11 Great Expectations suites (mirror the quality gate) |
| `tests/` | 11 pytest files — SCD2 idempotency, session boundaries, quality gates |
| `notebooks/` | Databricks notebook mirrors of `src/jobs` |
| `main.py` | CLI: `generate · bronze · silver · gold · pipeline · push · gx · test-connection` |

## Execution modes

`src/utils/engine.py` auto-detects the runtime via `DATABRICKS_RUNTIME_VERSION` — no flags, no separate code paths beyond Bronze's read:

| | Local (`uv run python main.py pipeline`) | Databricks CE (Job: `main.py pipeline`) |
|---|---|---|
| Source read | `spark.read.json(landing/…)` | Auto Loader `cloudFiles` from Volume |
| Warehouse | `.spark/warehouse` (Delta, gitignored) | Unity Catalog `streamflix-lakehouse` |
| Table names | two-part (`silver.x`) | three-part (`` `streamflix-lakehouse`.silver.x ``) |
| Checkpoints | `.spark/checkpoints` | Volume / workspace storage |
| Output | rich console (tables, progress bars) | Job run logs |

Design rule: **all business logic lives in `src/core/` and `src/jobs/`** — pure DataFrame in/out, no environment branches — so a local green run means a Databricks green run.
