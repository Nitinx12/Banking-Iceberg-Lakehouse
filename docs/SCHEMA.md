# Schema

Data model and pipeline map for the StreamFlix lakehouse (11 source tables, Bronze → Silver → Gold).
Full column-level detail: [`data_dictionary.md`](data_dictionary.md) · lineage: [`lineage.md`](lineage.md).

## Pipeline flow

```mermaid
flowchart LR
    subgraph LANDING["⬇️ Landing (JSON)"]
        direction TB
        L1["11 seeded Faker sources<br/>(generator/)"]
    end

    subgraph BRONZE["🥉 Bronze — raw, append-only"]
        direction TB
        B1["Auto Loader · cloudFiles<br/>mergeSchema · availableNow"]
        B2["bronze.* Delta tables"]
    end

    subgraph SILVER["🥈 Silver — clean & conform"]
        direction TB
        S1["clean + dedupe"]
        S2["SCD2 (subscriptions, devices)"]
        S3["sessionize (cdn logs)"]
        S4["from_json flatten (tickets)"]
    end

    subgraph GOLD["🥇 Gold — business aggregates"]
        direction TB
        G1["DAU/WAU · watch by genre<br/>churn · MRR"]
        G2["QoE by device · promo effectiveness<br/>support summary · engagement"]
    end

    Q[("silver.quarantine<br/>+ quarantine_reason")]
    A[("silver.audit_log<br/>pass/fail counts")]
    BI["📊 BI / dashboards"]

    LANDING --> B1 --> B2 --> S1
    S1 --> S2 & S3 & S4
    S1 & S2 & S3 & S4 --> G1 & G2 --> BI
    S1 -. "failures routed, not dropped" .-> Q
    S1 & S2 & S3 & S4 -. "every run" .-> A

    classDef landing fill:#e0f2fe,stroke:#0284c7,color:#0c4a6e
    classDef bronze fill:#fde8d7,stroke:#c2591b,color:#5c2b0d
    classDef silver fill:#eceff1,stroke:#78909c,color:#263238
    classDef gold fill:#fdf3c9,stroke:#b58b00,color:#5c4a00
    classDef side fill:#fde2e4,stroke:#c9184a,color:#800f2f
    classDef bi fill:#e6f4ea,stroke:#137333,color:#0d3b21
    class L1 landing
    class B1,B2 bronze
    class S1,S2,S3,S4 silver
    class G1,G2 gold
    class Q,A side
    class BI bi
```

## Relationships

`user_id` is a logical key shared by all event/fact tables (no users dim yet). Solid links are enforced-by-quarantine FKs.

```mermaid
erDiagram
    CONTENT_CATALOG ||--o{ WATCH_EVENTS : "content_id"
    CONTENT_CATALOG ||--o{ CONTENT_RATINGS : "content_id"
    CONTENT_CATALOG ||--o{ CDN_STREAM_LOGS : "content_id"
    PROMOTIONS ||--o{ PROMOTION_REDEMPTIONS : "promo_code (orphaned -> quarantine)"
    SUBSCRIPTIONS ||--o{ PROMOTION_REDEMPTIONS : "subscription_id"
    DEVICES ||--o{ CDN_STREAM_SESSIONS : "user_id + device_type"

    WATCH_EVENTS  { string event_id PK  string user_id FK  string content_id FK  timestamp event_timestamp }
    SUBSCRIPTIONS { string subscription_id PK  string user_id  string plan_tier  bool is_current }
    BILLING       { string transaction_id PK  string user_id  double amount  string transaction_type }
    DEVICES       { string device_id PK  string user_id  string os_version  bool is_current }
    PROFILES      { string profile_id PK  string user_id  bool is_kids  string language }
    PROMOTIONS    { string promo_code PK  int discount_pct  timestamp starts_at  timestamp ends_at }
    PROMOTION_REDEMPTIONS { string redemption_id PK  string promo_code FK  string user_id  date billing_period_start }
    SUPPORT_TICKETS { string ticket_id PK  string user_id  string status  json payload }
    CDN_STREAM_LOGS  { string log_id PK  string session_id  int bitrate_kbps  int rebuffer_ms }
    CONTENT_RATINGS  { string rating_id PK  string user_id  string content_id FK  int rating }
    CONTENT_CATALOG  { string content_id PK  string genre  string content_type  date release_date }
```

## Table reference

### Bronze (raw, `_ingested_at`/`_source_file` added)

| Table | Key | Pattern |
|---|---|---|
| `watch_events` | `event_id` | Auto Loader stream |
| `subscriptions_cdc` | `subscription_id` + ts | CDC changelog (insert/update/delete) |
| `content_catalog` | `content_id` | Batch overwrite dim |
| `billing_transactions` | `transaction_id` | Auto Loader stream |
| `devices_cdc` | `device_id` + ts | CDC changelog (2nd SCD2 source) |
| `profiles` | `profile_id` | Batch overwrite dim |
| `promotions` | `promo_code` | Batch overwrite dim |
| `promotion_redemptions` | `redemption_id` | Auto Loader stream (bridge) |
| `support_tickets` | `ticket_id` | Auto Loader stream (JSON payload) |
| `cdn_stream_logs` | `log_id` | Auto Loader stream (high volume) |
| `content_ratings` | `rating_id` | Auto Loader stream |

### Silver (cleaned, quality-gated)

| Table | Natural key | Idempotency | Partition |
|---|---|---|---|
| `watch_events` | `event_id` | MERGE | `event_date` |
| `subscriptions_scd2` | `subscription_id` | SCD2 MERGE | — |
| `content_catalog` | `content_id` | overwrite | — |
| `billing` | `transaction_id` | append* | — |
| `devices_scd2` | `device_id` | SCD2 MERGE | — |
| `profiles` | `profile_id` | overwrite | — |
| `promotions` | `promo_code` | overwrite | — |
| `promotion_redemptions` | `redemption_id` | MERGE | — |
| `support_tickets` | `ticket_id` | MERGE | `created_date` |
| `cdn_stream_sessions` | `session_id` + `session_number` | MERGE | `session_date` |
| `content_ratings` | `user_id` + `content_id` | MERGE (latest-wins upsert) | — |

\* pre-existing; candidate for a MERGE follow-up.

### Gold (one business question per table)

| Table | Question |
|---|---|
| `daily_active_users` / `weekly_active_users` | How many users are active? |
| `watch_time_by_genre` | What do people watch? |
| `churn_signals` / `mrr_trend` | Who churned, what's the revenue trend? |
| `qoe_by_device` | Which devices/genres stream worst? |
| `promo_effectiveness` | Which promos drove revenue? |
| `support_ticket_summary` | Is support load/CSAT improving? |
| `content_engagement` | Which genres rate highest? |
