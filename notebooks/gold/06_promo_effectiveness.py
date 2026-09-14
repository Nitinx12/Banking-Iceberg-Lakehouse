# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: promotion effectiveness (bridge-table attribution)
# MAGIC - silver.promotion_redemptions x silver.promotions x silver.billing
# MAGIC - Answers: "which promos actually drove revenue in the period they were redeemed?"
# MAGIC - Revenue is attributed by joining redemptions to billing charges in the same
# MAGIC   billing period (month) for the same user

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.promo_effectiveness AS
# MAGIC SELECT r.promo_code,
# MAGIC        p.discount_pct,
# MAGIC        count(*) AS redemptions,
# MAGIC        count(DISTINCT r.user_id) AS unique_users,
# MAGIC        round(sum(b.amount), 2) AS attributed_revenue
# MAGIC FROM silver.promotion_redemptions r
# MAGIC LEFT JOIN silver.promotions p
# MAGIC   ON r.promo_code = p.promo_code
# MAGIC LEFT JOIN silver.billing b
# MAGIC   ON r.user_id = b.user_id
# MAGIC  AND to_date(date_trunc('month', b.transaction_timestamp)) = to_date(r.billing_period_start)
# MAGIC GROUP BY r.promo_code, p.discount_pct;

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT promo_code, discount_pct, redemptions, unique_users, attributed_revenue
# MAGIC FROM gold.promo_effectiveness ORDER BY attributed_revenue DESC NULLS LAST LIMIT 20;
