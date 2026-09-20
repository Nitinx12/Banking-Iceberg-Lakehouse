# ADR 005 — Watermark field is `created_at` + expanded collections

**Status:** Accepted 2026-09-20

**Context:** Architecture 5.1 lists 6 collections (`customers, accounts, transactions, branches, loans, cards`) with watermark `updated_at` for slowly-changing entities. Profiling `docs/profiling.md` on the real Mongo dump shows 10 collections (adds `card_transactions 3M, loan_payments 600k, support_tickets 25k, employees 1.8k`) and that every collection has `created_at` 100% populated, no `updated_at` field, no nulls. `transactions`/`card_transactions` have many docs sharing same `created_at` (append-heavy), so ordering needs `_id` tiebreaker per Architecture 5.2.

**Decision:**
- Watermark field unified to `created_at` per `jobs/common/config.py:WATERMARK_FIELDS` and `contracts/*.yml: watermark_field: created_at` (branches remains `None` = full refresh).
- Large append collections deduplicate by `ORDER BY _source_ts DESC, _id DESC` in Silver (`silver_transactions.py`, `silver_customers.py` window).
- Incremental filter is `{created_at: {$gt: last - 10m overlap}}` via `jobs/ingestion/watermark.py: watermark_filter`. Second phase may add compound cursor `(created_at, _id)` if profiling shows missed rows at same timestamp.
- Collection count expands from 6 to 10; contracts added for 4 new collections. Architecture 5.1 table to be updated next revision.

**Consequences:** `updated_at` backfill would be treated as new `created_at` rows on next run; if source later adds `updated_at`, revisit this ADR and switch incremental collections to `updated_at` with migration.

**Refs:** `docs/profiling.md:12`, `.env.example:71-73`, `jobs/common/config.py:49-73`, `jobs/ingestion/bronze.py:54`.
