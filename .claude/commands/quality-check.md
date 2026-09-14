---
description: Scaffold a data quality check for a table — null checks, referential integrity, freshness — wired to the quarantine table.
argument-hint: [table_name] e.g. billing_transactions
disable-model-invocation: true
---

Add data quality checks for `$ARGUMENTS` to `src/quality_checks.py`.

1. Read the table's schema from `docs/data_dictionary.md` or the relevant `data_generator/generate_$ARGUMENTS.py` file.
2. Implement checks appropriate to this table, at minimum:
   - **Null checks** on required (non-nullable) fields.
   - **Referential integrity** against any dimension this table references (e.g. `billing_transactions.user_id` should exist, `watch_events.content_id` should exist in `content_catalog`).
   - **Freshness**: is this table's data arriving within the documented SLA (e.g. "Gold refreshed within 2 hours of Bronze landing")?
   - Any table-specific check suggested by the "realistic messiness" the data generator injects (duplicates, out-of-order timestamps, late arrivals).
3. Records failing any check should be written to the quarantine table with a reason column (which check failed), not silently dropped.
4. Log a pass/fail count summary — this is checked in the Definition of Done and is the direct answer to the interview question "how do you know your data is trustworthy?"
5. Add or extend `tests/test_quality_checks.py` with a test per check, each using a deliberately bad record to prove the check actually catches it.
