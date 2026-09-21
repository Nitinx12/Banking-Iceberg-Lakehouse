# docs/runbooks/alert-budget-burn.md — ErrorBudgetBurnFast

**Fires when:** `sum(increase(sla_breach_total[1h])) > 1` — more than one SLA breach
event within an hour, i.e. the 30-day error budget (432 miss-minutes at 99%, Arch
13.3) is burning faster than 2% per hour.

**Policy (Architecture 13.3):** when >50% of the budget is burned, feature work
pauses in favour of reliability work until the burn rate recovers.

## 1. Size the burn (2 min)

```bash
docker exec banking_postgres psql -U postgres -d banking_dw -c \
  "select table_name, target_seconds, actual_seconds, breached_at \
   from ops.sla_events where breached=true order by breached_at desc limit 10"
```

- One burst of 2-3 events from the same outage window: normal aftershock of a single
  incident — work the incident runbook, budget impact is bounded.
- Events spread over hours from *different* tables: systemic (infra degrading) — treat
  as an incident.

## 2. Check the budget state

Grafana `5 - Error Budget` dashboard shows: burned %, remaining minutes, burn rate.
Below 50% burned: proceed normally, note the incident. Above 50%: **feature freeze
per policy** — next sessions work only on reliability items until recovery.

## 3. Contain

- If breaches come from freshness: `alert-freshness.md`.
- If from DQ gate holds (serving stale while gate holds): `alert-dq-critical.md`.
- If infra (postgres/minio/mongo flapping): `make down && make up PROFILE=core`, then
  `drill-restore-postgres.md` if data integrity is in doubt.

## 4. Recovery

The budget recovers only through a clean 30-day window (it is a rolling allowance, not
a counter you reset). Recovery actions: reduce batch latency, widen the margin between
scheduled runs and the SLO, add the failing stage to `iceberg_maintenance` tuning.
