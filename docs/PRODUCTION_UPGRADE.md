# Production Upgrade

What changes moving from a CE portfolio project to production. Every gap below is a **conscious simplification**, not an oversight — each is the standard "what would you do at scale?" interview answer.

## Now → target

```mermaid
flowchart LR
    subgraph NOW["🏠 Current (CE)"]
        direction TB
        N1["Faker JSON in Volume"]
        N2["Auto Loader availableNow<br/>(job-triggered batches)"]
        N3["workspace-local state"]
        N4["manual Job runs"]
    end
    subgraph PROD["🏭 Production"]
        direction TB
        P1["Kafka + Debezium CDC<br/>real event bus"]
        P2["continuous streaming<br/>+ DLT expectations"]
        P3["Unity Catalog governance<br/>row/column ACLs, lineage"]
        P4["Workflows orchestration<br/>+ alerting/SLAs"]
    end
    N1 --> P1
    N2 --> P2
    N3 --> P3
    N4 --> P4

    classDef now fill:#eceff1,stroke:#78909c,color:#263238
    classDef prod fill:#e6f4ea,stroke:#137333,color:#0d3b21
    class N1,N2,N3,N4 now
    class P1,P2,P3,P4 prod
```

## Upgrade items

| Area | Today | Production |
|---|---|---|
| CDC source | synthetic changelog JSON | Kafka + Debezium capturing the OLTP database; schema-registry-managed topics |
| Trigger | `availableNow` per job run | continuous streaming with watermarks; Workflows for batch dims |
| Orchestration | single `pipeline` command | Databricks Workflows DAG (bronze → silver → gold with task dependencies, retries, SLA alerts) |
| Quality | custom gate + GX suites | DLT expectations (quarantine semantics preserved) + incident paging |
| Governance | single catalog, PAT in `.env` | service principals, scoped tokens / OAuth, row + column-level security, UC lineage capture |
| Storage layout | default Delta | OPTIMIZE + Z-ORDER / liquid clustering; VACUUM retention policy |
| Monitoring | `silver.audit_log` + rich tables | same audit tables surfaced in dashboards; alert on quarantine-rate spikes |
| Backfills | re-generate + re-run (idempotent MERGEs make it safe) | same MERGE keys are exactly what makes controlled backfills possible — this part survives unchanged |

## Deliberate deferrals (and why they're fine here)

- **`silver.billing` appends instead of MERGE** — pre-existing; the natural key (`transaction_id`) and `_merge_or_create` helper make the fix mechanical, but it's out of scope and flagged in [`SCHEMA.md`](SCHEMA.md).
- **No surrogate keys on SCD2 dims** — `subscription_id`/`device_id` are stable business keys; surrogate keys buy nothing at this scale.
- **Event-time watermarks** — `availableNow` batches make them moot locally; they arrive with continuous streaming.
- **No users dimension** — `user_id` is a shared logical key across facts (documented in [`SCHEMA.md`](SCHEMA.md)).

The load-bearing design choices — **quality gate with quarantine, MERGE-on-natural-key idempotency, SCD2 history, sessionization on event time** — are the same ones production would run; only the transport and governance around them change.
