# Error Budget Policy (Architecture 13.3)

Status: active since Phase 5 (2026-09-21)

## SLO

| Objective | Target | Window | Budget |
|---|---|---|---|
| Serving freshness: Gold fact tables younger than 26h (93600s) | 99% | 30 days | 432 miss-minutes (~7.2h) |

The SLO is measured continuously by the `sla_monitor` DAG (every 5 minutes) against
`serving.fct_transactions` and `serving.fct_card_transactions._loaded_at`. Every
breach row in `ops.sla_events` contributes its duration to the burn.

## Budget states

| Burned | State | Action |
|---|---|---|
| 0-50% | healthy | normal feature work (Phase 6+ continues) |
| >50% | **feature freeze** | new feature work pauses; sessions work on reliability items only until burn rate recovers (Arch 13.3 policy) |
| 100% | reliability only | all work is reliability; consider re-baselining the SLO with stakeholders if the target proves unrealistic |

## Measurement

- Metric source: `data_freshness_seconds` (Pushgateway -> Prometheus, pushed by
  `jobs/observability/sla.py`), breach bookkeeping in `ops.sla_events`.
- Dashboards: Grafana `5 - Error Budget` (burned %, remaining minutes, burn rate).
- Fast-burn alert: `ErrorBudgetBurnFast` (>1 breach event/hour) pages via
  Alertmanager -> `#data-alerts`.

## Accounting rules

1. Overlapping breaches of the same table count once (max duration, not summed).
2. Breaches caused by planned maintenance windows are recorded but marked
   `maintenance` in `ops.sla_events.target_name` and excluded from burn.
3. The SLO is reviewed after two weeks of real data (PROJECT_PLAN Phase 5 task);
   adjustments to the 26h target require an ADR note and stakeholder sign-off.
