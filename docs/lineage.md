# Lineage

landing/*.json (Faker generator) -> bronze.* (Auto Loader, Delta, mergeSchema)
  bronze.watch_events -> silver.watch_events (clean + dedupe + quarantine) -> gold.daily_active_users, gold.weekly_active_users, gold.watch_time_by_genre
  bronze.subscriptions_cdc -> silver.subscriptions_scd2 (SCD2 MERGE) -> gold.churn_signals
  bronze.content_catalog -> silver.content_catalog -> gold.watch_time_by_genre (broadcast join)
  bronze.billing_transactions -> silver.billing -> gold.mrr_trend, gold.promo_effectiveness
  bronze.devices_cdc -> silver.devices_scd2 (generic SCD2 MERGE) -> gold.qoe_by_device (current devices)
  bronze.profiles -> silver.profiles (overwrite dim + quarantine)
  bronze.promotions -> silver.promotions (overwrite dim + quarantine) -> gold.promo_effectiveness
  bronze.promotion_redemptions -> silver.promotion_redemptions (bridge: referential integrity vs promotions, MERGE) -> gold.promo_effectiveness
  bronze.support_tickets -> silver.support_tickets (from_json payload flatten + quarantine, partitioned by created_date) -> gold.support_ticket_summary
  bronze.cdn_stream_logs -> silver.cdn_stream_sessions (quality gate + 30-min gap sessionization, partitioned by session_date) -> gold.qoe_by_device
  bronze.content_ratings -> silver.content_ratings (latest-wins upsert on user_id+content_id) -> gold.content_engagement

Quarantine: silver.*_quarantine partitioned by _q_date with quarantine_reason
Audit: silver.audit_log (pass/fail counts per run)
