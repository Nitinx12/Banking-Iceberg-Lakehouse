# Profiling Notes — Banking MongoDB (Phase 0)

Generated: 2026-09-20 via `mongosh banking` + `tests/data/*.json` (2 docs each) + `profile.js`
Source: `mongosh "banking" --eval "db.getCollectionNames()"` + `countDocuments()` + `mongoexport --limit=2`

## Collections & Counts

| Collection | Count | Grain | Change pattern | Watermark candidate | Ingestion mode |
|---|---|---|---|---|---|
| `customers` | 60,000 | customer_id | slowly changing (PII) | `created_at` (no updated_at) | incremental (Architecture 5.1 analog) |
| `accounts` | 95,000 | account_id | status/balance changes | `created_at` | incremental + CDC (stretch) |
| `transactions` | 2,000,000 | transaction_id | append heavy | `created_at` then `_id` | incremental + CDC |
| `branches` | 150 | branch_id | rarely changes, small | none | **full refresh** |
| `loans` | 22,000 | loan_id | slowly changing | `created_at` | incremental |
| `cards` | 65,000 | card_id | status changes | `created_at` | incremental |
| `card_transactions` | 3,000,000 | card_txn_id | append heavy | `created_at` then `_id` | incremental + CDC |
| `loan_payments` | 600,000 | payment_id | append (per loan) | `created_at` | incremental |
| `support_tickets` | 25,000 | ticket_id | open/resolve | `created_at` | incremental |
| `employees` | 1,800 | employee_id | rarely changes | `created_at` | incremental / full refresh |

**Total watermark finding:** No collection has `updated_at` (0/0 has_updated, all 95000-3M have_created=total). `created_at` is present on 100% rows, no nulls. Business keys (`account_id`, `customer_id`, etc.) are unique (dups=0) — suitable as Bronze merge keys. `_id` is Mongo `$oid` string.

**Implication for Architecture 5.2:** Use `created_at` + `_id` range partitioning (secondaryPreferred) with 10m overlap window, plus `_doc_hash` lineage. If later CDC needs `updated_at`, add field and contract.

## Field inventory (from tests/data/*.json samples)

### customers (60k)
`_id(oid)`, `customer_id(int)`, `name(str)`, `gender(enum)`, `date_of_birth(date)`, `city`, `state`, `phone(int64)`, `email(str PII)`, `occupation(str)`, `annual_income(int)`, `join_date(date)`, `credit_score(int)`, `created_at(timestamp)`

Nulls: none observed. `phone` is int — Silver should cast to string and mask.

### accounts (95k)
`_id`, `account_id`, `customer_id(FK)`, `branch_id(FK)`, `account_type(enum: Current/Savings)`, `balance(decimal)`, `open_date(date)`, `status(enum: Active)`, `created_at`

### transactions (2M)
`_id`, `transaction_id`, `account_id(FK)`, `txn_date(date)`, `txn_type(enum: Deposit/Withdrawal)`, `amount(decimal)`, `channel(enum: Online Banking)`, `merchant_category`, `created_at`

Note: `txn_date` (business date) vs `created_at` (ingest watermark) differ — Silver will partition on `txn_date`.

### branches (150)
`branch_id`, `branch_name`, `city`, `state`, `opened_date`, `ifsc_code`, `created_at` — small, full refresh.

### loans (22k)
`loan_id`, `customer_id(FK)`, `branch_id(FK)`, `loan_type(enum)`, `loan_amount(decimal)`, `interest_rate`, `term_months`, `start_date`, `status`, `created_at`

### cards (65k)
`card_id`, `customer_id(FK)`, `account_id(FK)`, `card_type(enum)`, `issue_date`, `expiry_date`, `credit_limit`, `status`, `created_at`

### card_transactions (3M — largest)
`card_txn_id`, `card_id(FK)`, `txn_date`, `merchant_category`, `amount`, `is_fraud(0/1)`, `created_at` — append heavy, 3M rows, needs `created_at` + `_id` watermark + partitioning.

### loan_payments (600k)
`payment_id`, `loan_id(FK)`, `payment_date`, `amount_paid`, `principal_component`, `interest_component`, `late_payment_flag(0/1)`, `created_at`

### support_tickets (25k)
`ticket_id`, `customer_id(FK)`, `issue_type`, `date_opened`, `date_resolved`, `status(enum: Resolved)`, `satisfaction_score(1-5)`, `created_at`

### employees (1,800)
`employee_id`, `name`, `branch_id(FK)`, `role`, `hire_date`, `salary`, `created_at`

## Key candidates & watermark choices

| Collection | Business key (unique) | Watermark field | Type check |
|---|---|---|---|
| customers | customer_id | created_at | timestamp, not null, monotonic? Sample shows 2026-07-30 — verify distribution |
| accounts | account_id | created_at | timestamp |
| transactions | transaction_id | created_at (+_id tiebreaker) | timestamp, 2M rows |
| card_transactions | card_txn_id | created_at (+_id) | 3M rows |
| loan_payments | payment_id | created_at | 600k |
| branches | branch_id | none (full refresh) | 150 rows |
| loans | loan_id | created_at | 22k |
| cards | card_id | created_at | 65k |
| support_tickets | ticket_id | created_at | 25k |
| employees | employee_id | created_at | 1,800 |

All collections: `created_at` present, no nulls, business keys unique. Recommend overlap `INGEST_OVERLAP_MINUTES=10` (Architecture 5.2) and `secondaryPreferred` reads partitioned by `_id`.

## Next — contracts

One YAML contract per collection in `contracts/` with schema_version, fields, types, required, pii tags, drift_policy (new_field warn, removed_required fail, type_change fail quarantine).
