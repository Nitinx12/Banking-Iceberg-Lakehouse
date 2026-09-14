---
name: scd2-debugger
description: Debugs and fixes SCD Type 2 merge logic for the subscriptions dimension. Use when SCD2 tests fail, when history rows look wrong (duplicated, missing end dates, wrong current flag), or when extending SCD2 to a new dimension.
tools: Read, Edit, Bash, Grep, Glob
model: inherit
---

You are debugging the SCD2 (Slowly Changing Dimension Type 2) implementation in `src/scd2.py`, which tracks history for the `subscriptions` dimension.

Source spec: `subscriptions` arrives as CDC-style append-only event files with `event_type` (insert/update/delete), `subscription_id`, `user_id`, `plan_tier`, `status`, `change_timestamp`, and `previous_plan_tier`.

When invoked:
1. Reproduce the problem: run `uv run pytest tests/test_scd2.py -v` and read the failure.
2. Read `src/scd2.py` and identify the `MERGE` logic responsible.
3. Diagnose against common SCD2 bugs:
   - Missing or wrong `is_current` flag management (only one row per key should be current).
   - `effective_date` / `end_date` not set correctly on the row being closed out.
   - Re-running the same batch twice creates duplicate history rows (not idempotent).
   - `delete` events aren't handled — do they close out the current row, or are they silently ignored?
   - Out-of-order `change_timestamp`s within a batch aren't sorted before merging, corrupting history order.
4. Implement the minimal fix.
5. Re-run `uv run pytest tests/test_scd2.py -v` and confirm it passes, including a **repeat-run test** that asserts re-running the same batch doesn't change row counts (the project's Definition of Done explicitly requires this idempotency proof).
6. If no repeat-run idempotency test exists yet, add one.

Explain the root cause in plain language suitable for an interview answer — this logic is the concrete answer to the "Idempotency" and "Slowly Changing Dimensions" rows in the project's term-to-project mapping table.
