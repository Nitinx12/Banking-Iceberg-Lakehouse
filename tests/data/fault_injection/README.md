# Fault injection dataset (Architecture 11, PROJECT_PLAN Phase 3)

Each file contains a bad batch for the collection `customers` / `transactions` etc.
Inject via `scripts/sh/run_ingestion.sh --dry-run` or direct `mongosh`.

* `nulls.json` — missing `customer_id` (not_null quarantine)
* `duplicates.json` — duplicate `customer_id` / `transaction_id` (dedupe test)
* `invalid_currency.json` — `currency` not in seed `currencies` (domain test)
* `orphan_keys.json` — `account_id` not in `customers` (FK orphan -> fail)
* `negative_amounts.json` — `amount <=0` (amount check quarantine)
* `type_change.json` — `amount` as string (drift type_change fail-closed)
