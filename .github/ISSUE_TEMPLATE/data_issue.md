---
name: Data quality issue
about: Bad data — failed expectations, quarantine rows, SCD2 history anomalies, wrong Gold numbers
labels: ["bug", "area:data-quality"]
title: "data: "
---

## Where did you see it?

- **Layer:** Bronze / Silver / Gold
- **Table:** e.g. `silver.subscriptions_scd2`
- **GX suite (if any):** e.g. `gx/expectations/watch_events.json`

## What's wrong with the data?

<!-- Duplicated rows, missing end_date, is_current violations, null keys, wrong totals... -->

## How was it detected?

- [ ] GX validation failed
- [ ] Quality gate quarantined rows
- [ ] Gold numbers don't add up
- [ ] Spotted manually

## Reproduce

```sql
-- minimal query that shows the bad rows
```

## Suspected root cause

<!-- Late-arriving events? Replayed batch breaking idempotency? Generator bug? -->
