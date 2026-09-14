# StreamFlix — Databricks Lakehouse Portfolio Project

A simulated OTT streaming platform (watch events, subscriptions, billing, content
catalog) built as a full medallion lakehouse on Databricks Community Edition.
Designed to be a portfolio piece that demonstrates real, defensible decisions
across data modeling, pipelines, streaming, orchestration, performance tuning,
and data quality — the full range of a senior data engineering interview.

**Timeline:** 3–4 weeks
**Platform:** Databricks Community Edition (free tier)
**Core stack:** PySpark, Delta Lake, Structured Streaming / Auto Loader,
Databricks Repos (Git), GitHub Actions, Great Expectations (or hand-rolled
checks), a lightweight BI layer
**Dependency management:** `uv` — `pyproject.toml` + `uv.lock` for local dev and CI

---

## 1. Why this project

- Covers nearly every concept from a standard data engineering interview
  checklist — medallion architecture, SCD2, CDC, idempotency, streaming vs.
  batch, partitioning/Z-ordering, data quality, CI/CD, orchestration, semantic
  layers — in one coherent, explainable system.
- Works entirely on the free tier, but is built to *acknowledge* that tier's
  limits and explain the production upgrade path — which is itself a strong
  interview signal ("I know what I'd change with a paid workspace").
- Produces concrete artifacts: a public GitHub repo, a short walkthrough video,
  benchmark numbers, and resume bullet points — not just a set of notebooks
  that only make sense to you.

---

## 2. Architecture overview

```
 Watch events        Subscriptions          Content catalog
 (streaming)         (CDC change events)    (slow-changing dim)
       \                    |                     /
        \                   |                    /
         v                  v                   v
                 ┌───────────────────────┐
                 │      BRONZE LAYER      │  raw, Auto Loader,
                 │                        │  schema-on-read
                 └───────────┬───────────┘
                              v
                 ┌───────────────────────┐
                 │      SILVER LAYER      │  cleaned, deduped,
                 │                        │  SCD2, quality-gated
                 └───────────┬───────────┘
                              v
                 ┌───────────────────────┐
                 │       GOLD LAYER       │  business aggregates:
                 │                        │  DAU/WAU, churn, MRR
                 └───────────┬───────────┘
                              v
                 ┌───────────────────────┐
                 │    BI / DASHBOARDS     │  semantic layer,
                 │                        │  final consumption
                 └───────────────────────┘
```

Cross-cutting concerns (not a "layer" but present at every stage):
orchestration (Bronze → Silver → Gold dependency chain), data quality
checks and a quarantine table, and version control / CI via Git.

---

## 3. Constraints & workarounds (Community Edition)

| Limitation | Workaround | Interview talking point |
|---|---|---|
| Single-node cluster, no autoscaling | Right-sized synthetic data (5–20M rows), tune partitions to cluster size | "I designed for the constraint and can speak to how it changes with a multi-node cluster." |
| No Unity Catalog | Hive metastore databases (`bronze`, `silver`, `gold`) | "I know the UC migration path — catalogs, external locations, grants, lineage." |
| Cluster auto-terminates after idle (~1 hr) | `trigger(availableNow=True)` micro-batches instead of a continuous stream | "In production this becomes a continuous trigger running under a Workflow on an always-on job cluster." |
| No native Workflows-style multi-task orchestration (or limited) | Databricks Jobs UI if available; otherwise a local Airflow instance (Docker) calling the Databricks Jobs REST API | Shows orchestration *concepts*, not dependence on one UI. |
| No SQL Warehouse for a real dbt connection | PySpark/SQL modules organized like dbt's staging → intermediate → marts layering | "I mirrored dbt's layering conventions; wiring up dbt-databricks is a config change, not a redesign." |
| No production-grade secrets management | `.env` / Databricks widgets for local dev, documented as a stand-in for Databricks Secrets + a cloud KMS | Shows awareness of the gap without pretending it's solved. |

---

## 4. Repository structure

```
streamflix-lakehouse/
├── README.md
├── docs/
│   ├── architecture.png
│   ├── data_dictionary.md
│   └── lineage.md
├── data_generator/
│   ├── generate_content_catalog.py
│   ├── generate_subscriptions_cdc.py
│   ├── generate_watch_events.py
│   └── generate_billing.py
├── notebooks/
│   ├── bronze/
│   │   ├── 01_ingest_watch_events.py
│   │   ├── 02_ingest_subscriptions_cdc.py
│   │   ├── 03_ingest_content_catalog.py
│   │   └── 04_ingest_billing.py
│   ├── silver/
│   │   ├── 01_clean_watch_events.py
│   │   ├── 02_scd2_subscriptions.py
│   │   ├── 03_quality_gate.py
│   │   └── 04_clean_billing.py
│   └── gold/
│       ├── 01_dau_wau.py
│       ├── 02_watch_time_by_genre.py
│       ├── 03_churn_signals.py
│       └── 04_mrr_trend.py
├── src/
│   ├── transformations.py      # reusable PySpark functions
│   ├── scd2.py                 # SCD2 merge logic, unit-testable
│   ├── quality_checks.py       # data quality assertions
│   └── io_utils.py
├── tests/
│   ├── test_scd2.py
│   ├── test_dedup.py
│   └── test_quality_checks.py
├── .github/workflows/ci.yml
├── pyproject.toml
└── uv.lock
```

### Local setup & CI with `uv`

```bash
# one-time setup
uv init --no-readme
uv add pyspark delta-spark faker pytest ruff great-expectations

# day to day
uv run pytest              # run tests
uv run ruff check .        # lint
uv sync                    # install/refresh the locked environment
```

`uv` covers local development and CI. Databricks **notebook-scoped** installs
on the cluster still go through `%pip install <package>` — that's how
Databricks itself works, uv doesn't replace it there. Keep the two in sync by
occasionally running `uv export --no-hashes -o requirements.txt` and using
that file as the source of truth for any `%pip install -r requirements.txt`
cell. Mention this hybrid explicitly in the README — it's a reasonable,
defensible pattern, not a workaround to hide.

`.github/workflows/ci.yml` sketch:

```yaml
name: CI
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v3
        with:
          enable-cache: true
      - run: uv sync --frozen
      - run: uv run ruff check .
      - run: uv run pytest
```

---

## 5. Week-by-week plan

### Week 1 — Foundations & Bronze layer

- [ ] Create GitHub repo, connect via **Databricks Repos**
- [ ] Set up folder structure above; commit skeleton
- [ ] Initialize the project with `uv` (`uv init`, `uv add pyspark delta-spark
      faker pytest ruff`); commit `pyproject.toml` and `uv.lock`
- [ ] Create a single-node cluster (document its spec in `docs/`)
- [ ] Build the synthetic data generator (see §6 for schemas):
  - [ ] `content_catalog` — slow-changing dimension
  - [ ] `subscriptions` — Debezium-style insert/update/delete change events
  - [ ] `watch_events` — high-volume clickstream
  - [ ] `billing_transactions` — payment events
- [ ] Land all four sources into **Bronze** using Auto Loader (`cloudFiles`)
  - [ ] Schema inference + `mergeSchema` for evolving sources
  - [ ] Add `_ingested_at`, `_source_file`, `_batch_id` metadata columns
  - [ ] Store as Delta tables in a `bronze` Hive database
- [ ] Commit history should show incremental, reviewable progress — not one giant commit

**Deliverable:** working Bronze notebooks, generator scripts, first commits pushed.

### Week 2 — Silver layer: cleaning, SCD2, data quality

- [ ] Standardize types, timezones, trim/normalize strings
- [ ] Deduplicate on natural keys (`event_id`, `subscription_id`, etc.)
- [ ] Implement **SCD Type 2** for subscription plan history
  - [ ] `MERGE INTO` with `effective_date`, `expiry_date`, `is_current` flag
  - [ ] Unit test the merge logic in isolation (`src/scd2.py` + `tests/test_scd2.py`)
- [ ] Idempotent incremental merge for `watch_events`, keyed on `event_id`
  - [ ] Prove idempotency: run the same batch twice, assert row counts don't change
- [ ] Build a data quality gate:
  - [ ] Null / uniqueness checks on primary keys
  - [ ] Referential integrity (`watch_events.user_id` exists in `users`)
  - [ ] Range / sanity checks (timestamps not in the future, durations ≥ 0)
  - [ ] Freshness check (latest batch within expected window)
  - [ ] Use Great Expectations **or** hand-rolled PySpark assertions — document which and why
  - [ ] Add it locally/CI with `uv add great-expectations`; inside cluster
        notebooks, install it with `%pip install great_expectations` (see the
        uv/pip note in §4)
- [ ] Route failing records to a `_quarantine` Delta table instead of hard-failing
- [ ] Log pass/fail row counts per run (simple audit table)

**Deliverable:** Silver notebooks with a working quality gate, quarantine table,
before/after row counts, passing unit tests in CI.

### Week 3 — Gold layer, streaming, performance tuning

- [ ] Build Gold aggregate tables:
  - [ ] `daily_active_users`, `weekly_active_users`
  - [ ] `watch_time_by_genre`
  - [ ] `churn_signals` (cancellations, downgrades, engagement drop-off)
  - [ ] `content_engagement_leaderboard`
  - [ ] `mrr_trend` (from billing)
- [ ] Run `watch_events` through **Structured Streaming + Auto Loader**
  - [ ] Use `trigger(availableNow=True)` given cluster auto-termination
  - [ ] Document exactly why, and what changes with a continuous trigger + Workflow
- [ ] **Performance tuning pass** — this is the section to get concrete numbers from:
  - [ ] Partition Silver/Gold tables by date
  - [ ] Run `OPTIMIZE table ZORDER BY (user_id)` / `(content_id)` on high-cardinality
        filter columns
  - [ ] Capture a query's runtime and scanned bytes **before and after**
  - [ ] Save the screenshots / numbers into `docs/` — this becomes a real
        interview story ("I cut a 40s scan to 9s by Z-ordering on user_id")
  - [ ] Note any join skew found and how it was addressed (salting, broadcast join, etc.)

**Deliverable:** Gold tables answering real business questions, a working
(if limited) streaming path, and a documented before/after performance benchmark.

### Week 4 — Orchestration, CI/CD, semantic layer, polish

- [ ] Orchestrate Bronze → Silver → Gold as a dependency chain
  - [ ] Native Databricks Jobs UI if available on your workspace
  - [ ] Otherwise: a small local Airflow DAG (Docker) calling the Databricks
        Jobs REST API — arguably the stronger story, since it shows real
        orchestration knowledge independent of one vendor's UI
  - [ ] Include retries and a simple failure notification (even just a logged
        alert if webhook/email isn't available)
- [ ] Set up **CI** with GitHub Actions (`.github/workflows/ci.yml`), using `uv`
      (see the workflow sketch in §4):
  - [ ] `astral-sh/setup-uv` action to install uv and cache dependencies
  - [ ] `uv sync --frozen` to install the locked environment
  - [ ] `uv run pytest` for `src/` transformation functions against a local
        PySpark session — no cluster required, runs on every push
  - [ ] `uv run ruff check .` as the lint step
- [ ] Build a lightweight semantic layer / dashboard on top of Gold:
  - [ ] Databricks SQL if you have access, otherwise Streamlit or Power BI Desktop
  - [ ] Answer three real questions: what's driving churn, what content drives
        watch time, what's the MRR trend
- [ ] Write final documentation:
  - [ ] `README.md` — architecture diagram, setup steps, how to run it
  - [ ] `docs/data_dictionary.md` — every table, every column, types and meaning
  - [ ] `docs/lineage.md` — source → Bronze → Silver → Gold → BI, table by table
  - [ ] A **"Known limitations & production upgrade path"** section — explicit,
        not apologetic: Unity Catalog, multi-node autoscaling, real cloud
        storage (S3/ADLS), dbt, real Workflows, secrets management
- [ ] Record a 3–5 minute walkthrough video (Loom/YouTube unlisted)
  - [ ] Link it from your resume and LinkedIn
  - [ ] Reference it live in interviews when asked "tell me about a project"

**Deliverable:** a polished, documented, orchestrated, tested repository —
plus a video you can point to in any interview.

---

## 6. Data generator spec (starting schemas)

Use Python + `Faker` + `random` to generate these as JSON/CSV files dropped
into a "landing" DBFS folder, simulating what a real source system would emit.

**`content_catalog`** (slow-changing dimension, ~2–5k rows)
```
content_id (string, PK), title, genre, release_date,
content_type (movie/series), runtime_minutes, rating, added_at
```

**`subscriptions`** (CDC change events — emit as append-only event files)
```
event_type (insert/update/delete), subscription_id, user_id,
plan_tier (basic/standard/premium), status (active/canceled/paused),
change_timestamp, previous_plan_tier (nullable)
```

**`watch_events`** (high-volume clickstream, millions of rows)
```
event_id (string, PK), user_id, content_id, event_type (play/pause/seek/stop),
event_timestamp, watch_duration_seconds, device_type, session_id
```

**`billing_transactions`**
```
transaction_id (string, PK), user_id, amount, currency,
transaction_type (charge/refund), transaction_timestamp, plan_tier
```

Design the generator to produce **realistic messiness on purpose**: a few
late-arriving events, a few duplicate `event_id`s, a few nulls in optional
fields, occasional out-of-order timestamps. This is what your Silver-layer
quality gate is supposed to catch — and it's a much better demo than clean data.

---

## 7. Full term-to-project mapping

Use this as a rehearsal sheet — for each row, practice explaining the concept
*and* pointing to where it lives in this project.

| Term | Where it shows up in StreamFlix |
|---|---|
| Data modeling fundamentals | Overall Bronze/Silver/Gold design decisions |
| Dimensional modeling, fact vs. dimension | `watch_events`/`billing_transactions` (facts) vs. `content_catalog`/`users` (dimensions) |
| Star vs. snowflake schema | Gold layer design decision — document which you chose and why |
| Normalization vs. denormalization | Bronze (near-raw) vs. Gold (denormalized aggregates) |
| Slowly Changing Dimensions | `subscriptions` SCD2 implementation |
| Data partitioning | Date-based partitioning on Silver/Gold tables |
| Database vs. data warehouse | Discussed in README's architecture rationale |
| Data pipeline | The whole Bronze → Silver → Gold chain |
| ETL vs. ELT | This project is ELT — raw lands first, transforms happen in-platform |
| CDC | `subscriptions` change events |
| Idempotency | `watch_events` upsert logic, proven with a repeat-run test |
| DAG | The Bronze → Silver → Gold dependency chain in orchestration |
| Medallion architecture | The entire project structure |
| Batch processing | Daily Gold aggregate jobs |
| Real-time / stream processing | `watch_events` via Structured Streaming + Auto Loader |
| Event-driven architecture | `subscriptions` CDC events, conceptually discussed for a Kafka-based production version |
| Apache Airflow | Optional orchestration fallback (Week 4) |
| dbt | Transformation layer mirrors dbt's staging/marts pattern |
| Apache Kafka / Flink | Discussed in README as the production-scale streaming path (not implemented on free tier) |
| Data lake / Lakehouse | Bronze layer is the "lake," the whole system is the "lakehouse" |
| Apache Iceberg / Delta Lake | Delta Lake used throughout; document how Iceberg would compare |
| Snowflake / Databricks | Databricks is the platform; Snowflake discussed as an alternative in README |
| PySpark | All transformation logic |
| Distributed systems | Discussed w.r.t. cluster behavior, retries, fault tolerance |
| Data quality | Silver-layer quality gate + quarantine table |
| Great Expectations | Used (or hand-rolled equivalent) for the quality gate |
| SQL | Gold-layer aggregations, ad hoc analysis |
| Python | Data generator, PySpark jobs, tests, orchestration scripts |
| Semantic layer | Final BI/dashboard layer |
| Performance tuning | Documented Z-order/partitioning benchmark |
| CI/CD | GitHub Actions running pytest on every push |
| File formats | Landing files as JSON/CSV, storage as Delta (Parquet + transaction log) |
| Schema evolution | `mergeSchema` in Bronze ingestion |
| Data lineage | `docs/lineage.md` |
| SLA / SLO / freshness | Documented target (e.g., "Gold refreshed within 2 hours of Bronze landing") and how you'd measure it |

---

## 8. Definition of done

- [ ] Public GitHub repo with clean commit history (not one giant commit)
- [ ] README with architecture diagram, setup instructions, and a "limitations
      & production upgrade path" section
- [ ] Data dictionary and lineage doc
- [ ] Working Bronze → Silver → Gold pipeline, orchestrated end to end
- [ ] SCD2 implementation with a passing unit test
- [ ] Data quality gate with a quarantine table and logged pass/fail counts
- [ ] At least one documented before/after performance improvement with real numbers
- [ ] CI pipeline (GitHub Actions) running tests on every push
- [ ] A working dashboard or semantic layer answering 2–3 real business questions
- [ ] A 3–5 minute walkthrough video

---

## 9. Resume / LinkedIn bullet drafts

Adjust numbers once you have real ones from Week 3's benchmark.

- Built an end-to-end lakehouse on Databricks using the medallion architecture
  (Bronze/Silver/Gold) with Delta Lake, processing [N]M synthetic streaming
  events through Auto Loader and Structured Streaming.
- Implemented SCD Type 2 dimension tracking and idempotent CDC-style upserts
  using Delta `MERGE`, verified with unit tests.
- Designed a data-quality gate (null, referential-integrity, and freshness
  checks) that routes failing records to a quarantine table rather than
  failing the pipeline.
- Improved Gold-layer query performance by [X]% through partitioning and
  Z-ordering, backed by measured before/after benchmarks.
- Set up CI with GitHub Actions running automated PySpark unit tests on every
  commit; orchestrated the pipeline as a dependency-managed DAG.

---

## 10. Interview talking points to rehearse

- "Walk me through your architecture" → use the diagram in §2, narrate top to bottom.
- "Why medallion instead of a single flat layer?" → traceability, clear
  ownership boundaries, ability to reprocess from Bronze without re-extracting.
- "How did you handle a schema change mid-project?" → `mergeSchema`, and what
  you'd do differently with Iceberg/Delta schema evolution guarantees.
- "What would you change with a bigger budget?" → Unity Catalog, multi-node
  autoscaling cluster, real cloud storage, dbt-databricks, Databricks Workflows,
  Kafka for true real-time ingestion.
- "Show me a time you improved performance" → your Z-order benchmark, with
  actual before/after numbers.
- "How do you know your data is trustworthy?" → the Silver quality gate,
  quarantine table, and freshness/SLA discussion.

---

## 11. Stretch goals (if you later get paid workspace access)

- [ ] Migrate to Unity Catalog (catalogs, external locations, column-level grants)
- [ ] Swap the transformation layer for real dbt-databricks models
- [ ] Move orchestration to native Databricks Workflows with proper alerting
- [ ] Point Auto Loader at real cloud storage (S3/ADLS) instead of DBFS
- [ ] Swap the CDC simulation for a real Debezium + Kafka pipeline
- [ ] Scale the synthetic dataset up an order of magnitude and re-run the
      performance benchmark on a multi-node cluster
