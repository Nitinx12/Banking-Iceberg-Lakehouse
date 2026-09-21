-- sql/publish_views.sql — masked serving views (Architecture 8.2-8.3, 14.2)
-- Postgres cannot read Iceberg `banking.*` directly (separate Spark catalog on MinIO/S3).
-- All lake data arrives in Postgres via the publish job (`jobs/publish/serving.py`) into
-- `serving.*` (Gold copies, e.g. serving.dim_customer, serving.dim_account, serving.fct_transactions).
-- These views therefore read ONLY from `serving.*`, which already holds HMAC'd PII (Silver
-- masked email/phone to email_hmac/phone_hmac — no raw PII ever lands in Postgres). The views
-- add display-level masking for dashboard consumers via `streamlit_reader`.
CREATE SCHEMA IF NOT EXISTS serving;

-- Masked customers: built over published Gold dim_customer (HMAC already applied in Silver).
-- If Silver HMAC columns are later published as serving.silver_customers, extend with
-- left(email_hmac,8)||'***' as email_masked, left(phone_hmac,8)||'***' as phone_masked.
CREATE OR REPLACE VIEW serving.customers_masked AS
SELECT
  customer_id,
  name,
  is_current,
  valid_from,
  valid_to,
  left(customer_sk, 8) || '***' AS customer_sk_masked
FROM serving.dim_customer;

-- Masked accounts: balance via serving dim_account (Gold), no raw PII.
CREATE OR REPLACE VIEW serving.accounts_masked AS
SELECT account_id, customer_id, branch_id, account_type, status, is_current
FROM serving.dim_account;

GRANT SELECT ON serving.customers_masked TO streamlit_reader;
GRANT SELECT ON serving.accounts_masked TO streamlit_reader;
GRANT SELECT ON serving.dim_customer TO streamlit_reader;
GRANT SELECT ON serving.dim_account TO streamlit_reader;
GRANT SELECT ON serving.dim_branch TO streamlit_reader;
GRANT SELECT ON serving.fct_transactions TO streamlit_reader;
GRANT SELECT ON serving.fct_card_transactions TO streamlit_reader;
