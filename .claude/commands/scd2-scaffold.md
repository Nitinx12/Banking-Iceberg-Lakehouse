---
description: Scaffold SCD Type 2 merge logic and a matching idempotency test for a dimension.
argument-hint: [dimension_name] e.g. subscriptions
disable-model-invocation: true
---

Scaffold SCD2 tracking for the `$ARGUMENTS` dimension.

1. Use `src/scd2.py`'s existing `subscriptions` implementation as the reference pattern if one exists; otherwise design from scratch following standard SCD2 conventions (`effective_date`, `end_date`, `is_current` flag, surrogate key).
2. Implement the `MERGE` logic as a reusable, unit-testable function in `src/scd2.py` — not inline in a notebook.
3. Handle CDC-style `insert`/`update`/`delete` event types explicitly (use the `subscriptions` event schema as a model).
4. Write `tests/test_scd2.py` (or extend it) with, at minimum:
   - A test that a single insert creates one current row.
   - A test that an update closes out the old row (`is_current = false`, `end_date` set) and opens a new current row.
   - A **repeat-run idempotency test**: running the exact same batch twice produces the same row count and history, not duplicates. This is explicitly required in the project's Definition of Done.
5. Run `uv run pytest tests/test_scd2.py -v` and confirm everything passes.
6. Wire the function into the relevant Silver notebook (e.g. `notebooks/silver/02_scd2_subscriptions.py`).
