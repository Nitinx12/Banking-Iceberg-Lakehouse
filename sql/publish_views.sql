-- sql/publish_views.sql — masked serving views (Architecture 8.2-8.3, 14.2)
CREATE SCHEMA IF NOT EXISTS serving;

-- Masked customers: HMAC already in Silver, expose last4 where display needed, PII never raw
CREATE OR REPLACE VIEW serving.customers_masked AS
SELECT
  customer_id,
  name,
  city,
  state,
  credit_score,
  left(email_hmac, 8) || '***' as email_masked,
  left(phone_hmac, 8) || '***' as phone_masked
FROM banking.silver.customers;

-- Masked accounts: balance via masked view for dashboard
CREATE OR REPLACE VIEW serving.accounts_masked AS
SELECT account_id, customer_id, branch_id, account_type, status, balance
FROM banking.silver.accounts;

GRANT SELECT ON serving.customers_masked TO streamlit_reader;
GRANT SELECT ON serving.accounts_masked TO streamlit_reader;
GRANT SELECT ON serving.dim_customer TO streamlit_reader;
GRANT SELECT ON serving.fct_transactions TO streamlit_reader;
