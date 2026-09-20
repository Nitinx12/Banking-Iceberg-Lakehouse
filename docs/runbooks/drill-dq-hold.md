# Drill: DQ gate hold on Gold (Architecture 11.3)

**Detection:** `gold_dq` AirflowFailException, `ops.dq_results` critical fail.
**Response:** Hold publish, quarantine rows in `banking.quarantine.*`, alert.
**Recovery:** Fix data/logic, run `quarantine_replay` DAG with `table/batch_id`, re-run `gold` + `gold_dq`.
