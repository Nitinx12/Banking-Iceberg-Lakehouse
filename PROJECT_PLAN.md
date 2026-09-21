# Banking Data Platform: Project Plan

Status: Draft v1
Last updated: 2026-09-20

This plan turns `architecture.md` into a sequence of deliverable phases. Each phase ends with something that runs end to end, so progress is always demonstrable and the project can stop at a useful point.

---

## 1. Summary

* **Objective:** Build a production grade banking data pipeline: MongoDB to an Iceberg lakehouse, transformed with PySpark and dbt on Databricks, validated by layered data quality checks, served through PostgreSQL and Streamlit, with monitoring, SLAs, SLOs and freshness tracking.
* **Approach:** Vertical slices. A thin working pipeline first, then deepen each stage. CI and git hooks start in Phase 0, not at the end.
* **Assumption:** One engineer working about 10 to 12 focused hours per week, which gives roughly 17 weeks. Full time work compresses this to about 5 to 6 weeks.
* **MVP:** Phases 0 to 4. If time gets tight, ship Phases 0 to 5, do a light version of Phase 6, and drop Phase 7 (Flink).

---

## 2. Timeline overview

| Phase | Name | Weeks | Effort (hours) | Outcome |
|---|---|---|---|---|
| 0 | Foundations | 1 | 12 | Repo, tooling, local stack, CI skeleton, hooks |
| 1 | Ingestion and Bronze | 2 to 3 | 24 | MongoDB data in Bronze Iceberg tables, first Airflow DAG |
| 2 | Silver and Gold | 4 to 6 | 36 | Tested star schema built with PySpark and dbt |
| 3 | Data quality | 7 to 8 | 24 | Great Expectations, reconciliation, quarantine, DQ gate |
| 4 | Serving and Streamlit | 9 to 10 | 24 | Full daily DAG, PostgreSQL serving layer, dashboard (MVP) |
| 5 | Observability, SLA, SLO, freshness | 11 to 12 | 24 | Metrics, alerts, freshness tracking, error budgets |
| 6 | IaC and CI/CD hardening | 13 to 14 | 24 | Terraform modules, automated deploys with approvals |
| 7 | Flink streaming (stretch) | 15 to 16 | 24 | Near real time CDC and alerts |
| 8 | Hardening and release | 17 | 12 | Drills, docs, v1.0.0 |
| | **Total** | **17** | **204** | |

---

## 3. Milestones

| ID | Milestone | Target |
|---|---|---|
| M1 | Local stack starts with one command and a secret commit is blocked | End of week 1 |
| M2 | First data in Bronze on Iceberg, counts match MongoDB | End of week 3 |
| M3 | Gold star schema built and tested on Databricks | End of week 6 | ← CE deviation (ADR 006): dbt SQL is canonical (Databricks path); CE verified via Spark mirroring same SQL on Iceberg JDBC (`banking.gold.*` counts proven live) — `dbt parse` stays Databricks path |
| M4 | DQ gate stops a deliberately bad batch | End of week 8 |
| M5 | Dashboard live on served data, MVP complete | End of week 10 |
| M6 | SLOs measured, freshness alert proven | End of week 12 |
| M7 | Automated deploy to staging, approved deploy to prod | End of week 14 |
| M8 | Streaming path live with measured latency | End of week 16 |
| M9 | v1.0.0 tagged | End of week 17 |

---

## 4. Phase details

### Phase 0: Foundations (week 1)

**Objective:** A repository where every later phase can plug in, with quality gates already active.

Tasks:

* [ ] Create the repository with the structure in `architecture.md` section 18
* [ ] `.gitignore` covering `.env`, `logs/`, `target/`, `.venv/`, Terraform state and local data
* [ ] `uv init`, `pyproject.toml` with dependency groups (`ingestion`, `transform`, `quality`, `dashboard`, `dev`), commit `uv.lock`
* [ ] Copy `.env.example` to `.env` and generate local secrets
* [ ] Compose skeleton with the `core` profile: PostgreSQL, MongoDB replica set plus init container, S3 compatible store plus bucket init
* [ ] Makefile and `tasks.bat` with `help`, `env`, `setup`, `hooks`, `up`, `down`, `lint`, `test`
* [ ] Git hooks: `pre-commit`, `commit-msg`, `pre-push`, installed by `make hooks`
* [ ] Minimal `ci.yml`: lint and unit tests on pull requests
* [ ] Branch protection on `main`
* [ ] Shared logging helper with stage scoped log files, and `scripts/sh/lib.sh`
* [ ] Load the MongoDB dataset into local Mongo (`seed_mongo`)
* [ ] Profile every collection: counts, field types, null rates, key candidates, watermark candidates
* [ ] Draft one data contract per collection in `contracts/`

Deliverables: repository on GitHub, working `make up`, green CI, profiling notes in `docs/`.

Exit criteria:

* A fresh clone followed by `make setup && make up` gives healthy services in under 10 minutes
* Committing a fake secret is blocked by the hook
* CI runs on a pull request and passes

### Phase 1: Ingestion and Bronze (weeks 2 to 3)

**Objective:** Reliable, idempotent incremental loads from MongoDB into Bronze Iceberg tables.

Tasks:

* [ ] Spark session factory configured for the Iceberg JDBC catalog and the S3 endpoint
* [ ] Bronze DDL script following the contract in `architecture.md` section 5.3
* [ ] `ops` schema DDL: `pipeline_runs`, `ingestion_watermarks`
* [ ] Batch ingestion job: watermark read, overlap window, partitioned reads from the secondary, lineage columns, atomic write, watermark advance after commit
* [ ] Idempotent re run: delete by `_batch_id` before writing
* [ ] Full refresh path for small collections
* [ ] Schema drift detector against the YAML contracts
* [ ] Unit tests with local Spark and small fixtures, integration test against the compose MongoDB
* [ ] Airflow image built with `uv`, minimal `ingest_mongo_batch` DAG with dynamic task mapping per collection
* [ ] Spike in a Databricks workspace to decide ADR 3 (Iceberg through Unity Catalog, or the Delta fallback)
* [ ] Manual Databricks dev workspace setup and secret scope (Terraform takes over in Phase 6)

Deliverables: ingestion package, Bronze tables, first DAG, ADR 3 decision recorded.

Exit criteria:

* Running the same batch twice gives identical Bronze row counts
* Row counts match MongoDB for every collection
* The watermark only advances after a successful commit
* ADR 3 is decided and documented

### Phase 2: Silver and Gold (weeks 4 to 6)

**Objective:** A tested dimensional model built from Bronze.

Tasks:

* [ ] dbt project with `dbt-databricks`, profiles driven by environment variables, sources with freshness
* [ ] Silver PySpark jobs: parse JSON with explicit schemas, cast types, deduplicate, standardise, explode nested arrays, mask PII with HMAC
* [ ] Seed tables: currencies, transaction types, statuses, channels
* [ ] Silver dbt models (incremental merge), Python models where SQL is awkward
* [ ] SCD Type 2 snapshots for `dim_customer` and `dim_account`
* [ ] Gold dimensions and facts with surrogate keys, unknown members and enforced contracts
* [ ] Generated `dim_date`
* [ ] Aggregates and KPI marts for the dashboard
* [ ] Generic tests, singular SQL tests, and dbt unit tests for money and balance logic
* [ ] SQLFluff configuration with a clean lint pass
* [ ] `selectors.yml` for stages, `dbt docs generate`

Deliverables: Silver and Gold models, snapshots, tests, dbt docs.

Exit criteria:

* `dbt build` passes on the dev target
* No orphan foreign keys in any fact table
* Simulated source changes produce correct SCD Type 2 history
* Docs site generated with lineage

### Phase 3: Data quality (weeks 7 to 8)

**Objective:** Bad data is detected, contained and explained, and never reaches Gold silently.

Tasks:

* [ ] Great Expectations project with Spark and PostgreSQL datasources, Bronze and Silver suites, checkpoints
* [ ] Custom PySpark checks: debit and credit balance, balance roll forward, Silver to Gold parity, orphan keys
* [ ] Statistical checks: volume z score, null rate drift, amount distribution shift
* [ ] `ops.dq_results` DDL and a shared writer used by GX, dbt and custom checks
* [ ] Severity model, DQ score calculation and the gate task with `DQ_GATE_MIN_PASS_PCT`
* [ ] Quarantine tables, routing logic and the `quarantine_replay` DAG
* [ ] Data Docs publishing
* [ ] Fault injection dataset: nulls, duplicates, invalid currency, orphan keys, negative amounts, type change
* [ ] Wire checks and gate branches into `daily_banking_pipeline`

Deliverables: quality framework, fault injection tests, published Data Docs.

Exit criteria:

* Each injected fault is stopped or quarantined at the intended layer
* A clean batch passes with a recorded score
* Results are visible in `ops.dq_results`
* The gate threshold changes by configuration only

### Phase 4: Serving and Streamlit (weeks 9 to 10)

**Objective:** A scheduled, hands off run from MongoDB to a live dashboard. This completes the MVP.

Tasks:

* [ ] Complete `daily_banking_pipeline` with Cosmos task groups, gate branching and failure callbacks
* [ ] PostgreSQL `serving`, `ops` and `rt` schemas, roles and grants (manual now, Terraform in Phase 6)
* [ ] Publish job with staging table and atomic swap, indexes and `ANALYZE`
* [ ] Masked views for customer data
* [ ] Streamlit pages: executive overview, transactions, customer 360, data quality, pipeline health (basic)
* [ ] Query caching, connection pool, authentication, container healthcheck
* [ ] `backfill_pipeline` DAG and a backfill runbook

Deliverables: full daily DAG, serving layer, dashboard container.

Exit criteria:

* One scheduled run moves data from MongoDB to the dashboard with no manual step
* The dashboard connects only through the read only role
* A 7 day backfill completes and gives correct results

### Phase 5: Observability, SLA, SLO and freshness (weeks 11 to 12)

**Objective:** Know when the platform is unhealthy before a consumer does, and measure reliability against targets.

Tasks:

* [ ] `monitoring` compose profile: Prometheus, Grafana, Alertmanager, Pushgateway, exporters
* [ ] Metrics from task summaries and Airflow StatsD
* [ ] `ops.freshness_metrics` and `ops.sla_events`, and the `sla_monitor` DAG
* [ ] dbt source freshness at the start of each run
* [ ] Five Grafana dashboards provisioned as code
* [ ] Alert rules: freshness breach, DAG failure, critical DQ failure, error budget burn
* [ ] Slack and email contact points, severity routing
* [ ] Full pipeline health page in Streamlit
* [ ] A runbook for every alert
* [ ] `iceberg_maintenance` DAG (compaction, snapshot expiry, orphan cleanup)
* [ ] SLO review after two weeks of data: tune targets, document the error budget policy

Deliverables: monitoring stack, alert rules, runbooks, SLO report.

Exit criteria:

* A forced delay triggers the freshness alert within the SLO window
* A forced task failure produces an alert with a runbook link
* The SLO report shows measured attainment for the available window

### Phase 6: IaC and CI/CD hardening (weeks 13 to 14)

**Objective:** Any environment can be created and updated from code, with controlled promotion.

Tasks:

* [ ] Terraform modules: `postgres`, `object_storage`, `databricks`, `monitoring`, with remote state
* [ ] `dev`, `staging` and `prod` environment directories with tfvars, and import of resources built by hand earlier
* [ ] `terraform_plan.yml` and `cd.yml` using OIDC, GitHub Environments and prod approval
* [ ] Slim CI for dbt with deferred state
* [ ] `nightly_integration.yml` on dev Databricks with sample data
* [ ] Image build, push and scan
* [ ] Secret scanning, dependency audit and Dependabot
* [ ] Release tagging and a rollback drill

Deliverables: Terraform code, workflows, release process.

Exit criteria:

* A new environment is created from scratch by Terraform plus the deploy workflow, with no manual steps
* A prod deploy requires approval
* Rollback to the previous tag is verified

### Phase 7: Flink streaming, stretch (weeks 15 to 16)

**Objective:** Near real time change capture and simple streaming alerts.

Tasks:

* [ ] Confirm the replica set configuration and oplog size, and test a change stream
* [ ] `streaming` compose profile, job submission through the SQL gateway
* [ ] Flink SQL: CDC source with Iceberg upsert sinks for `transactions_cdc` and `accounts_cdc`
* [ ] Windowed aggregates for transaction velocity and a rule based alert sink into `rt`
* [ ] Checkpoints, savepoints and a deploy script that restores from a savepoint
* [ ] `flink_supervisor` DAG and Prometheus metrics
* [ ] Streaming latency SLI and a Grafana panel
* [ ] Hourly reconciliation between CDC tables and the batch path
* [ ] Live alerts panel in Streamlit
* [ ] Decide ADR 9 (direct CDC or a Kafka buffer)

Deliverables: streaming jobs, supervisor, live panel.

Exit criteria:

* Measured p95 latency is under 60 seconds
* The job restarts from a savepoint after a forced stop with no data loss
* Reconciliation shows parity with the batch path

### Phase 8: Hardening and release (week 17)

**Objective:** Prove the platform survives failure, then document and release it.

Tasks:

* [ ] Load test with scaled synthetic data (10 times and 100 times), tune partitions and file sizes
* [ ] Failure drills: stop MongoDB, kill a Databricks job, corrupt a batch, expire a secret
* [ ] Recovery drills: rebuild Silver and Gold from Bronze, restore PostgreSQL from backup
* [ ] Security review against `architecture.md` section 14
* [ ] Final documentation: README, runbooks, ADRs, diagrams
* [ ] Demo walkthrough and portfolio write up
* [ ] Tag `v1.0.0`

Exit criteria: every item in the production readiness checklist (section 8) is ticked.

---

## 5. Backlog priority (MoSCoW)

| Priority | Items |
|---|---|
| Must | Phases 0 to 4, DQ gate and quarantine, hooks and CI, secret hygiene, Terraform for PostgreSQL and storage |
| Should | Freshness and SLO tracking, Grafana dashboards, alerts and runbooks, Databricks Terraform, slim CI, maintenance DAG |
| Could | Flink streaming, OpenLineage with Marquez, Loki logs, statistical checks, Kafka buffer |
| Won't (v1) | Machine learning fraud model, multi region disaster recovery, real customer data |

---

## 6. Testing strategy

| Level | Scope | Tooling | When |
|---|---|---|---|
| Unit | Transform functions, watermark logic, masking, DQ score | pytest with local Spark | Every commit (fast subset), CI |
| Contract | Contracts against sample documents | pytest | CI |
| dbt | Generic, singular and unit tests | dbt | CI (parse and unit), every pipeline run |
| Integration | MongoDB to Bronze to Silver on compose | pytest with Docker | Nightly and before release |
| End to end | Full pipeline on dev Databricks | Airflow trigger plus assertions | Nightly |
| Fault injection | Bad batches at each layer | pytest and sample data | Nightly |
| Infrastructure | Terraform validate, tflint, plan | CI | Every pull request |
| Performance | Scaled data | Scripts | Phase 8 |
| Failure and recovery | Drills | Runbooks | Phase 8 and quarterly |

Target: at least 80 percent coverage on transformation and quality logic.

---

## 7. Definition of done

**A task or pull request is done when:**

* Code, tests and docs are in the same pull request
* Lint passes and CI is green
* No secrets or environment specific values are committed
* New stages emit logs, metrics and an `ops.pipeline_runs` row
* Any new alert has a runbook

**A phase is done when:**

* Every exit criterion is met and demonstrated
* Architecture and ADRs reflect what was actually built
* The changelog is updated and a short retrospective is written

---

## 8. Production readiness checklist

**Reliability**

* [ ] Every task is idempotent and has retries and timeouts
* [ ] Backfill tested
* [ ] Restore from backup tested
* [ ] Rebuild of Silver and Gold from Bronze tested

**Data**

* [ ] Contracts exist for every source collection
* [ ] DQ gate and quarantine work and are tested by fault injection
* [ ] Reconciliation checks pass on real runs

**Observability**

* [ ] Metrics, structured logs and lineage are available
* [ ] Every alert has a runbook and a tested route
* [ ] Dashboards are provisioned as code

**SLA, SLO and freshness**

* [ ] Targets are defined, measured and reviewed
* [ ] Error budget policy is documented
* [ ] Freshness alerts proven by a forced delay

**Security**

* [ ] Secrets are never in git, and scans run in hooks and CI
* [ ] Least privilege roles exist for every service
* [ ] PII is masked before serving
* [ ] Dependency and image scans are clean

**Delivery**

* [ ] Infrastructure is fully in Terraform
* [ ] CI/CD promotes through environments with an approval for prod
* [ ] Rollback verified

**Documentation**

* [ ] README, architecture, runbooks and ADRs are current
* [ ] A new engineer can run the platform from a fresh clone using the README alone

---

## 9. Risk register

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Scope is too large for the time available | High | High | Phase cut line, MVP first, Flink last |
| Databricks Community Edition cannot be managed by Terraform or the jobs API | High | Medium | Use a trial or paid workspace, or keep Terraform on local resources |
| Iceberg support on Databricks is limited for the workspace tier | Medium | Medium | Phase 1 spike, Delta fallback for Silver and Gold (ADR 3) |
| Databricks cost overrun | Medium | Medium | Job clusters with auto termination, small nodes, budget alerts, local Spark for development |
| Flink operating complexity | High | Medium | Stretch phase, optional Kafka buffer, savepoint discipline |
| Schema drift in MongoDB collections | Medium | High | Contracts and drift detector, fail closed on type changes |
| Dependency conflicts between Airflow, dbt and Spark | Medium | Medium | Isolated virtual environments, constraint files, pinned versions in `uv.lock` |
| Windows and Linux differences | Medium | Low | `tasks.bat` and PowerShell scripts, Linux CI, Docker for runtime, WSL as an option |
| Secret leakage | Low | High | Hooks, CI scanning, `.env.example` template, rotation schedule |
| Small files and metadata growth in Iceberg | Medium | Medium | Maintenance DAG, checkpoint interval discipline |
| Alert fatigue | Medium | Medium | Severity model, digests, tuning after two weeks |
| Local machine memory limits | Medium | Low | Compose profiles, start only what a task needs. Plan for 16 GB of RAM to run core, orchestration and monitoring together. |

---

## 10. Prerequisites and dependencies

**Accounts and access**

* GitHub repository with Actions and Environments enabled
* Databricks workspace (trial or paid) for Phases 1 onward that need real compute or Terraform
* Cloud object storage account for staging and prod, optional for local work
* Slack incoming webhook and an SMTP relay or email service for alerts

**Software**

* Docker Desktop, Git, `uv`, Terraform, Java 17 for local Spark
* Make, or `tasks.bat` on Windows without Make
* PowerShell 7 or WSL

**Data**

* The MongoDB dataset as a dump or JSON export, plus a small sample per collection for tests

---

## 11. Success metrics

| Metric | Target |
|---|---|
| SLO attainment (batch delivery, freshness, reliability) | Meets targets in `architecture.md` section 13 |
| Weighted DQ score at Gold | At least 99 percent |
| Time to detect a failure | Under 10 minutes |
| Time to restore | Under 4 hours |
| Coverage on transformation and quality logic | At least 80 percent |
| CI duration for a typical pull request | Under 10 minutes |
| Secrets committed | Zero |
| Cost per pipeline run | Tracked and within budget |

---

## 12. Working cadence

* Weekly planning session: pick the next tasks from the current phase, review last week's alerts and SLO numbers.
* Short lived branches and small pull requests, each with tests.
* Update ADRs when a decision changes, and keep `CHANGELOG.md` current.
* Time box spikes to one session. If a spike overruns, record the finding and take the fallback.

---

## 13. First week, session by session

| Session | Focus | Result |
|---|---|---|
| 1 | Repository, `uv`, `.gitignore`, `.env` from the template | Project skeleton committed |
| 2 | Compose `core` profile with Mongo replica set and object store, `seed_mongo` | Local stack running with data |
| 3 | Makefile and `tasks.bat` | One command surface on every OS |
| 4 | Git hooks and minimal CI | Fake secret blocked, CI green |
| 5 | Collection profiling | Profiling notes and watermark choices |
| 6 | Contract drafts and logging helper | `contracts/` populated, stage scoped logs working |

---

## 14. Open questions to close early

1. Real volume and growth per collection
2. Sample documents from each collection
3. Databricks workspace tier and Iceberg support (ADR 3)
4. Cloud provider for storage and Terraform state
5. Required refresh cadence per collection
6. Monthly cloud budget