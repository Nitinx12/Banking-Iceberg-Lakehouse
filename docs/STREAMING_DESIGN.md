# Streaming Design

Two streaming problems, solved with bounded machinery: **Auto Loader ingestion** and **gap-based sessionization**. Both are trigger-per-batch (`availableNow`) — the "streaming" is incremental state, not always-on clusters.

## Ingestion path

```mermaid
flowchart LR
    V[("Volume<br/>raw_data/<table>/")] -->|"new JSON files"| AL["Auto Loader<br/>cloudFiles format=json"]
    AL --> CK[("checkpoint<br/>per-source")]
    CK -->|"files already seen<br/>are skipped"| BR["🥉 bronze.<table><br/>mergeSchema"]
    V -.->|"local mode"| BJ["spark.read.json<br/>(same bronze output)"]
    BR & BJ --> SIL["🥈 silver"]

    classDef vol fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
    classDef stream fill:#f3e8fd,stroke:#7c3aed,color:#4c1d95
    classDef bronze fill:#fde8d7,stroke:#c2591b,color:#5c2b0d
    classDef silver fill:#eceff1,stroke:#78909c,color:#263238
    class V vol
    class AL,CK stream
    class BR,BJ bronze
    class SIL silver
```

Key choices:

- **`cloudFiles` + `mergeSchema`** — new fields in source JSON don't break ingestion; they land in Bronze for triage.
- **`trigger(availableNow=True)`** — processes everything pending once, then stops. Cheaper than continuous mode, and the checkpoint still guarantees exactly-once file tracking across runs.
- **Per-source checkpoints** — re-running one table's ingest can never double-count another's.
- **Local fallback** — the same Bronze schema is produced by a batch `read.json`, so Silver/Gold never know which mode ran.

## Sessionization (cdn_stream_logs)

CDN logs are heartbeats: 5–30 logs per playback session, positions advancing. One `session_id` can span real breaks — a **>30-min inactivity gap** starts a new session segment.

```mermaid
flowchart LR
    subgraph TIMELINE["one session_id, wall-clock →"]
        direction LR
        L1["log"] ---|"40 s"| L2["log"] ---|"25 min"| L3["log"] -.-|"45 min gap<br/>> 30 min ⚠"| L4["log"] ---|"2 min"| L5["log"]
    end
    TIMELINE --> S1["session_number = 1"] & S2["session_number = 2"]
    S1 --> R["rollup per session:<br/>start/end, total rebuffer,<br/>avg bitrate, startup_ms, n_logs"]

    classDef log fill:#eceff1,stroke:#78909c,color:#263238
    classDef split fill:#fde2e4,stroke:#c9184a,color:#800f2f
    classDef out fill:#fdf3c9,stroke:#b58b00,color:#5c4a00
    class L1,L2,L3,L5 log
    class L4 split
    class S1,S2 out
    class R out
```

`src/core/sessionization.py`:

- `sessionize(df, key, ts_col, gap_minutes=30)` — per-key `lag` over an event-time window; running sum of gap-indicators = `session_number`. A gap of **exactly** 30 min is still the same session (boundary is `>`, not `>=`).
- `rollup_sessions(df)` — one row per (key, session): duration, rebuffer total, avg bitrate, first-log startup latency.

Both are pure DataFrame functions — unit-tested in `tests/test_sessionization.py` (gap boundary, single-event sessions, per-key independence, out-of-order input).

## Late & out-of-order data

Event streams aren't sorted by arrival or event time. Silver handles it in two places: the **quality gate** flags events older than the 7-day SLA or in the future (→ quarantine), and **sessionization orders by event time within its window**, so out-of-order arrival doesn't corrupt session assignment.
