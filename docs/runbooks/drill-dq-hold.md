# Runbook: DQ Gate Hold — Gold Publish Blocked (Architecture 11.3, 11.2)

**Alert:** `CriticalDQFailure` / `DQGateHold` — `ops.dq_results` critical fail or `DQ score < DQ_GATE_MIN_PASS_PCT` (default `98%`, critical must be `100%`). Fires from `gold_dq` → `AirflowFailException("gold_dq gate failed — hold publish, alert")` → Alertmanager `slack-critical`.
**Severity:** `critical` — never reaches Gold/dashboard silently (fail-closed per Architecture 2.3). `high` for significant defects.
**Gate logic:** `dq_score = weighted passed / weighted total` per layer/run (`jobs/quality/gate.py`). Gate at `98%`, critical checks `100%` regardless of score.

## Symptoms
- Slack `#data-critical`: `gold_dq gate failed — hold publish, alert` with `run_id/batch_id`
- Airflow `daily_banking_pipeline` failed at `gold_dq` or `bronze_dq` task; downstream `publish` skipped
- `ops.dq_results` rows `status='fail'`, `severity='critical'`; `pass_pct` low; Streamlit Data Quality page red, quarantine count >0
- Grafana Dashboard 3 (Data Quality) score trend dips; `quarantine.*` counts spike
- Logs: `logs/gold_dq/*.log`, `gx/uncommitted/data_docs`, `dbt test` failures

## Impact
- Gold held, `serving.*` not updated — dashboard shows previous day's Gold (stale but correct).
- Quarantine rows in `banking.quarantine.*` (Bronze 10, Silver) with `_dq_rule`, `_dq_reason`, `_batch_id`, `_quarantined_at` per Architecture 11.4.
- Freshness may breach if hold persists — watch `sla_monitor`.

## Triage (10 min)
1. **Identify failing layer:** `SELECT layer, table_name, check_name, check_type, severity, status, failed_count, total_count, pass_pct FROM ops.dq_results WHERE run_id='<run_id>' AND status='fail' ORDER BY severity, pass_pct`
2. **Check quarantine:** `SELECT _dq_rule, count(*) FROM banking.quarantine.<table> WHERE _batch_id='<batch>' GROUP BY 1` (if Spark) or `SELECT * FROM ops.dq_results WHERE layer='silver' AND table_name='<table>'`
3. **Check which defense fired (Architecture 11.1):**
   - L1 Contracts drift → quarantine batch, alert
   - L2 Bronze GX → stop Silver
   - L3 Silver dbt/GX → row quarantine or stop Gold
   - L4 Gold reconciliation (debits=credits, balance roll-forward, row parity, orphans) → hold publish
   - L5 Statistical (z-score 14d, null drift) → warn
   - L6 Freshness → `alert-freshness.md`
4. **Pull evidence:** Great Expectations Data Docs (`gx/uncommitted/data_docs`), `dbt test --select gold` output, `jobs/quality/checks.py` logs.
5. **Classify:** Fault-injection type? `tests/data/fault_injection/` covers nulls, duplicates, orphan keys, bad currency, negative amounts, type change — compare.

## Diagnosis examples
- **Orphan foreign keys:** `SELECT count(*) FROM banking.gold.fct_transactions f LEFT JOIN banking.gold.dim_account d ON f.account_id=d.account_id AND d.is_current WHERE d.account_id IS NULL` >0 → upstream Silver missing dimension, check Silver dedup/merge.
- **Balance roll-forward mismatch:** Silver→Gold parity off → Silver job partial write, check `_batch_id` idempotency (delete-by-batch before write).
- **DQ score <98% but no critical fail:** Weighted `warn` checks failing (volume z-score, null drift) — review `jobs/quality/checks.py` thresholds.

## Mitigation
- **Do NOT force publish** without recorded override reason. Bad data never reaches Gold silently.
- **Fix data/logic:**
  - If contract type change / schema drift: update `contracts/<collection>.yml`, Silver logic (`jobs/transform/silver_*.py`), then re-run Silver. If source added new required field, add to contract per Architecture 5.4.
  - If row-level quarantine (nulls, bad currency): fix source or add masking/cast in Silver; rows remain in `quarantine.<table>` 180d.
- **Replay quarantined rows:** `airflow dags trigger quarantine_replay --conf '{"table":"customers","batch_id":"<batch>"}'` (Architecture 9.1). Replay marks resolved only after checks pass.
- **Re-run from failed stage:** `airflow dags trigger daily_banking_pipeline` with same logical date — idempotent MERGE (`MERGE INTO banking.silver.*` by business key, Gold `merge` strategy) makes re-run safe. Or `make dbt_build --select gold` + `make dq`.
- **Override (exceptional):** If gate threshold is wrong and data is verified correct, raise `DQ_GATE_MIN_PASS_PCT` override with recorded reason and approval; document in `ops.pipeline_runs` and `docs/incidents/`.

## Verify
1. `make dq` or `airflow dags trigger ...` succeeds: `SELECT pass_pct FROM ops.dq_results WHERE run_id='<new_run>' ORDER BY checked_at DESC LIMIT 5` all `pass` and `dq_score >=98%`, critical `100%`.
2. `quarantine_replay` cleared rows: `SELECT count(*) FROM banking.quarantine.<table> WHERE _batch_id='<batch>' AND _quarantined_at IS NOT NULL` → 0 or resolved.
3. `daily_banking_pipeline` reaches `publish` and `ops.freshness_metrics` advances.
4. Streamlit Data Quality page green, Grafana score recovers.

## Prevention
- Add dbt generic tests (`not_null`, `unique`, `relationships`, `accepted_values`) + singular tests for money `decimal(18,2)` and `closed_at >= opened_at` where applicable (Architecture 7.2).
- Ensure `jobs/transform/silver_*.py` quarantine routing writes `_dq_rule` per 11.4.
- Tune gate `DQ_GATE_MIN_PASS_PCT` env, not code (Architecture 11.3).
- Run fault-injection suite `tests/data/fault_injection/` in nightly CI.

**Refs:** `jobs/quality/gate.py`, `jobs/quality/checks.py`, `airflow/dags/daily_banking_pipeline.py:207`, `airflow/dags/quarantine_replay.py`, `gx/suites/*`, `dbt/banking_dbt/tests/*`, `Architecture.md 11`.
