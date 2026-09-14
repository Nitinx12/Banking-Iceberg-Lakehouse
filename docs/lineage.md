# Lineage

landing/*.json (Faker generator) -> bronze.* (Auto Loader, Delta, mergeSchema)
  bronze.watch_events -> silver.watch_events (clean + dedupe + quarantine) -> gold.daily_active_users, gold.weekly_active_users, gold.watch_time_by_genre
  bronze.subscriptions_cdc -> silver.subscriptions_scd2 (SCD2 MERGE) -> gold.churn_signals
  bronze.content_catalog -> silver.content_catalog -> gold.watch_time_by_genre (broadcast join)
  bronze.billing_transactions -> silver.billing -> gold.mrr_trend

Quarantine: silver.*_quarantine partitioned by _q_date with quarantine_reason
Audit: silver.audit_log (pass/fail counts per run)
