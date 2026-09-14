# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: support ticket summary
# MAGIC - silver.support_tickets aggregated weekly by channel
# MAGIC - Answers: "is support load and satisfaction trending the right way per channel?"

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.support_ticket_summary AS
# MAGIC SELECT date_trunc('week', created_at) AS week_start,
# MAGIC        channel,
# MAGIC        count(*) AS tickets,
# MAGIC        round(avg(csat_score), 2) AS avg_csat
# MAGIC FROM silver.support_tickets
# MAGIC GROUP BY 1, 2;

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT week_start, channel, tickets, avg_csat
# MAGIC FROM gold.support_ticket_summary ORDER BY week_start DESC, channel;
