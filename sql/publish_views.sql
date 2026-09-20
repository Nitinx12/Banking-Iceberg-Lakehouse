-- sql/publish_views.sql — masked serving views (Architecture 8.2-8.3, 14.2)
CREATE SCHEMA IF NOT EXISTS serving;

-- Masked customers: email/phone HMAC already in Silver, expose only last4 where needed
CREATE OR REPLACE VIEW serving.customers_masked AS
SELECT customer_id, name, city, state, credit_score, left(email_hmac, 8) as email_masked FROM silver.customers;

GRANT SELECT ON serving.customers_masked TO streamlit_reader;
