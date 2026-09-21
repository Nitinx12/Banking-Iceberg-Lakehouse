# docs/runbooks/alert-freshness.md — FreshnessBreachHigh / FreshnessBreachCritical

**Fires when:** `data_freshness_seconds{table=...}` exceeds the 26h SLO (93600s) for
5 minutes (high), or 36h (129600s) for 5 minutes (critical). Measured by
`sla_monitor` every 5 minutes from `serving.fct_transactions._loaded_at` /
`serving.fct_card_transactions._loaded_at` via `jobs/observability/sla.py`.

## 1. Confirm the breach is real (2 min)

```bash
make status            # latest ops.pipeline_runs + watermarks
docker exec banking_postgres psql -U postgres -d banking_dw -c \
  "select table_name, freshness_seconds, max_loaded_at from ops.freshness_metrics order by checked_at desc limit 5"
```

- If `freshness_seconds` is small but the alert fired: check Prometheus target health
  (http://localhost:9090/targets) — a stale pushgateway series can ghost-fire.
- If `max_loaded_at` is genuinely old: proceed.

## 2. Find where the chain stopped

1. `ops.pipeline_runs` — latest `bronze_*` / `gold_*` / `publish_*` rows and their status.
2. Airflow UI (`daily_banking_pipeline`) — which task failed or is stuck.
3. Source side: is Mongo reachable and does `ops.watermarks` show an old watermark?
   An old watermark means Bronze did not run or found nothing (upstream problem).

## 3. Remediate by failure point

| Failure point | Action |
|---|---|
| Bronze did not run / failed | `make ingest_py` (or re-trigger the DAG); batch-id idempotency makes reruns safe |
| Bronze ran, Silver failed | rerun the failing silver job; check `banking.quarantine.<coll>` for poisoned docs |
| Silver ran, publish failed | `python -m jobs.publish.serving --table fct_transactions --run-id manual-freshness` |
| Mongo down | `make up PROFILE=core`, then `drill-mongo-down.md` |

## 4. After recovery

- Verify freshness drops below SLO within one `sla_monitor` cycle (5 min).
- Post-mortem: if critical fired (36h), consumers were served stale data — record
  breach minutes in the error-budget log (`docs/slo/error-budget-policy.md`).

## Escalation

Critical for >2h: page the data owner; consider pausing downstream consumers
(dashboard still serves, but flag it) per the 50% budget-burn policy.
