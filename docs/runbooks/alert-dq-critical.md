# docs/runbooks/alert-dq-critical.md — CriticalDqFailure

**Fires when:** `dq_critical_failures_total > 0` — a critical-severity data quality
check failed (e.g. gold reconciliation orphan keys, or a layer-5 statistical gate
below `DQ_GATE_MIN_PASS_PCT`, default 98).

**Effect:** the DQ gate fails closed — downstream publish of the affected batch is
held. This is intentional (M4: the gate must stop a bad batch).

## 1. Read the failure (3 min)

```bash
docker exec banking_postgres psql -U postgres -d banking_dw -c \
  "select run_id, table_name, check_name, severity, status, failed_count, total_count, checked_at \
   from ops.dq_results where severity='critical' and status='fail' order by checked_at desc limit 5"
```

GX Data Docs (http://localhost:8082) hold the full expectation output for GX-run suites.

## 2. Interpret by check

| Check | Meaning | Usual cause |
|---|---|---|
| `orphan_keys` (gold reconciliation) | fct rows with no dim match | publish order — publish dims before facts; or a dim row got filtered in Silver |
| `volume_z_score` | daily volume far off 14-day baseline | upstream outage window, or a partial batch |
| `amount_shift` / `null_rate` | distribution drift | source-side change (new channel, bug in seeding) |
| parity checks | bronze/silver counts diverge | quarantine absorbing more than expected |

## 3. Decide: bad data or bad expectation?

- **Bad data** (verify against source Mongo): fix upstream, delete the affected
  `_batch_id` from Bronze, re-run ingestion for that batch, then re-run DQ.
- **Bad expectation** (data is legitimately different): adjust the check threshold in
  code with a note, never delete the DQ row — `ops.dq_results` is the audit trail.

## 4. Release the gate

Once the re-run passes, the gate opens on its own for the next batch. If publish was
skipped for the affected run, run it manually:

```bash
python -m jobs.publish.serving --table fct_transactions --run-id dq-recovery
```

## 5. Record

Critical DQ failures count as SLA misses for the affected table — log the minutes in
the error-budget log (`docs/slo/error-budget-policy.md`).
