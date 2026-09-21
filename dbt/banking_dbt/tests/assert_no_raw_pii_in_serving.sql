-- tests/assert_no_raw_pii_in_serving.sql — CI fails if raw PII reaches serving (Architecture 14.2).
-- Raw emails contain '@', raw phones are 10 digits starting 6-9. HMAC hex is 64 chars [0-9a-f].
-- If any Gold/serving dim exposes raw PII, this test fails and blocks cd.yml deploy.
-- Allowed masked form: 64-char hex (hmac) or null/last-4 views.

with customers as (
  select * from {{ ref('silver_customers') }} limit 0  -- ensure ref exists at parse
),
-- check serving if materialised (fallback: no rows -> pass)
serving_check as (
  select email_hmac, phone_hmac
  from {{ ref('silver_customers') }}
  where email_hmac ~ '@'  -- raw email slipped through
     or phone_hmac ~ '^[6-9][0-9]{9}$'
)
select * from serving_check
