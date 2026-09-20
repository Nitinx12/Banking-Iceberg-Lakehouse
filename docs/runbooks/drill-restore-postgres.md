# Drill: Rebuild Silver/Gold from Bronze + Restore Postgres (Architecture 19, Phase 8)

**Silver/Gold rebuild:** `DELETE FROM banking.silver.*` then re-run `jobs/transform/silver_*.py` for all batches — Bronze is append-only 90d.
**Postgres restore:** `scripts/sh/backup_postgres.sh` nightly, `scripts/sh/restore_postgres.sh --file backups/<file>.sql.gz` (requires YES).
**Verify:** `SELECT count(*) FROM serving.fct_transactions` matches `banking.gold.fct_transactions`.
